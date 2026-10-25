#!/usr/bin/env node
import { dirname, resolve } from 'node:path';
import { lstatSync, realpathSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { userInfo } from 'node:os';
import { fileURLToPath } from 'node:url';
import { credentialName, credentialPath, preflightCredentialDir, readCredential, writeCredential } from './credentials.mjs';
import { register, available, pinnedSession } from './register.mjs';
import { PLUGIN_ID, credentialOnly, pluginAdapter, readPluginConfig, writePluginConfig } from './plugin-config.mjs';
import { SAFE_URL, localError, resolveBinding, tmOptions, unsafeOption } from './plugin-env.mjs';
import { CLAUDE_SESSION, findProject, projectError, projectHint, projectOptions, projectTarget, readAdoptMarker, readSessionRecord, verifyMac, writeAdoptMarker, writeProjectOptions } from './project-options.mjs';
import { Channel } from './channel.mjs';
import { ChannelError, config, requireValue } from './config.mjs';
import { topicPattern } from '../shared/address.mjs';
import { brokerFetch } from './transport.mjs';

const hints = {
  JOIN_NAME_MISMATCH: 'The saved credential belongs to another user; remove it or choose that user.',
  USER_EXISTS: 'This name stays reserved; choose another name, or ask the workspace owner for a new invite.',
  ALREADY_MEMBER: 'Already a member; the invite was not consumed.',
  UNAUTHORIZED: 'The existing credential is inactive; choose another name or ask the workspace owner for help.',
  INVITE_INVALID: 'Ask the owner for a valid invite.', INVITE_EXPIRED: 'Ask the owner for a fresh invite.',
  INVITE_USED: 'Ask the owner for a new single-use invite.', INVITE_REVOKED: 'Ask the owner for a new invite.',
  RATE_LIMITED: 'Wait for the join rate limit to refill and retry.',
};
export function normaliseUrl(value, env = process.env) {
  requireValue(env.NODE_TLS_REJECT_UNAUTHORIZED !== '0', 'TLS_VERIFY_DISABLED', 'Enable TLS certificate verification before joining.');
  let url; try { url = new URL(value); } catch {}
  requireValue(url && ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password && !url.search && !url.hash && !url.href.includes('?') && !url.href.includes('#'),
    'INVALID_BROKER_URL', 'Use an HTTP(S) broker URL without userinfo, query or fragment.');
  requireValue(url.protocol === 'https:' || ['127.0.0.1','[::1]','localhost'].includes(url.hostname),
    'INSECURE_BROKER_URL', 'Use HTTPS for a remote broker; HTTP is allowed only on loopback.');
  // The URL reaches single-quoted shell commands in the plugin skills, so it
  // may carry only characters that cannot leave the quotes.
  requireValue(SAFE_URL.test(url.href), 'UNSAFE_BROKER_URL', 'Use a broker URL made only of letters, digits and : / . _ ~ % - [ ] characters.');
  return url.href.replace(/\/$/, '');
}
async function request(fetcher, url, path, args, token, timeoutMs) {
  try {
    const response = await fetcher(url + path, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(timeoutMs),
      headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify(args) });
    const result = await response.json();
    if (!response.ok) {
      const code = Object.hasOwn(hints, result.error) ? result.error : 'JOIN_FAILED';
      const error=new ChannelError(code,hints[code]??'The broker refused this request; check the invite and broker availability.');
      if(code==='ALREADY_MEMBER')error.binding={user:result.user,workspace:result.workspace,topic:result.topic};
      throw error;
    }
    return result;
  } catch (error) {
    if (error instanceof ChannelError) throw error;
    throw new ChannelError('JOIN_UNAVAILABLE', 'Check the broker URL, TLS and availability; redirects are refused.');
  }
}
// Read-only /session/status check of a pinned session. X-Topic is omitted for
// a workspace-level binding: Headers would send an undefined value as the
// literal string "undefined".
export function statusProbe({ fetcher, url, token, name, workspace, topic, timeoutMs }) {
  return async session=>{
    try{
      const response=await fetcher(url+'/session/status',{method:'GET',redirect:'error',signal:AbortSignal.timeout(timeoutMs),headers:{Authorization:`Bearer ${token}`,'X-Session-Id':session,'X-Workspace':workspace,...(topic==null?{}:{'X-Topic':topic})}}),data=await response.json();
      if(!response.ok)return {error:data.error};
      return data.user===name&&data.workspace===workspace&&data.topic===(topic??null)&&data.session_id===session&&['active','unregistered'].includes(data.state)?{state:data.state}:{error:'INVALID_STATUS_RESPONSE'};
    }catch{return {error:'STATUS_UNAVAILABLE'};}
  };
}
// Plan project-options P-12. bind and join run in the agent session: the
// skills pass --claude-session '${CLAUDE_SESSION_ID}' and --data-dir
// '${CLAUDE_PLUGIN_DATA}', and the project comes from the SessionStart record
// (P-11), never from a path pasted into a shell. A terminal user may pass
// --project <dir> by hand.
function agentProject({ claudeSession, dataDir, project }, env) {
  requireValue(typeof claudeSession === 'string' && CLAUDE_SESSION.test(claudeSession), 'SESSION_ID_UNKNOWN', 'Pass --claude-session with the Claude session id of the agent session; the skills pass ${CLAUDE_SESSION_ID}.');
  let dir = null;
  try { dir = project !== undefined ? resolve(project) : dataDir ? readSessionRecord(dataDir, claudeSession, { home: env.HOME }) : null; }
  catch (error) { if (error instanceof ChannelError) throw error; }
  requireValue(typeof dir === 'string', 'PROJECT_UNKNOWN', projectHint('PROJECT_UNKNOWN'));
  return dir;
}
const where = binding => `workspace "${binding.workspace}", ${binding.topic === null ? 'at workspace level' : `topic "${binding.topic}"`}`;
function alreadyBound(found, unused) {
  // The names passed the project-file patterns; the path is the user's own file.
  return new ChannelError('PLUGIN_ALREADY_BOUND', `This project is already bound in ${found.path} to ${where(found.binding)}, with agent Claude session ${found.options.claude_session_id}. Run /team-messaging:claim in the session that should be the agent, or remove the team-messaging entry from that file to bind the project again.${unused}`);
}
// The local pre-check (P-12), before any broker mutation or invite. join
// passes no binding: the invite's workspace and topic are unknown until it
// is redeemed, so any binding refuses. bind passes its binding: the same
// binding for the same Claude session is skipped, once its MAC verifies.
function projectPrecheck({ projectDir, claudeSession, binding, env, token, url, unused = '' }) {
  const found = findProject(projectDir, { home: env.HOME });
  if (found.state === 'invalid') throw projectError(found.code);
  if (found.state === 'absent') return null;
  if (binding && found.binding.workspace === binding.workspace && found.binding.topic === binding.topic && found.options.claude_session_id === claudeSession) {
    requireValue(verifyMac(token, found, url), 'PROJECT_OPTIONS_UNTRUSTED', projectHint('PROJECT_OPTIONS_UNTRUSTED'));
    return found;
  }
  throw alreadyBound(found, unused);
}
// P-12, P-13: a saved credential for another broker or name conflicts
// before the redeem (and pluginAdapter checks again after it). A 0.9 global
// workspace never blocks. The saved values are user-editable, so none is echoed.
function credentialPrecheck({ spawn, env, url, path, unused = '' }) {
  const current = readPluginConfig(PLUGIN_ID, { spawn, env });
  requireValue(current.status === 'ok', 'PLUGIN_NOT_READY', `Install the plugin (claude plugin install ${PLUGIN_ID}) or fix its configuration, then run this again.${unused}`);
  const saved = current.values;
  if ((saved.broker_url !== '' || saved.token_file !== '') && (saved.broker_url !== url || saved.token_file !== path))
    throw new ChannelError('PLUGIN_ALREADY_BOUND', `The plugin's saved credential is for another broker or name; clear broker_url and token_file with claude plugin configure, then run this again.${unused}`);
  return saved;
}
// Saves the credential globally when none is saved, then writes the P-5
// project file with a fresh session and the AM-4 MAC. The project file is
// written only when the global credential is this one.
async function bindProjectFile({ url, path, token, binding, claudeSession, projectDir, env, deps, used = '' }) {
  const adapter = await pluginAdapter({ url, workspace: binding.workspace, topic: binding.topic, path }, { spawn: deps.spawn, env, cwd: deps.cwd ?? process.cwd() });
  if (!['registered', 'skipped'].includes(adapter.status)) return adapter;
  // A binding written by another process since the pre-check is not replaced.
  const found = findProject(projectDir, { home: env.HOME });
  if (found.state === 'invalid') throw projectError(found.code);
  // join passes ' The invite was used.': this check runs after the redeem.
  if (found.state === 'found') throw alreadyBound(found, used);
  const root = projectTarget(projectDir, { home: env.HOME });
  writeProjectOptions(root, projectOptions(token, { root, url, claudeSession, binding }));
  return { ...adapter, status: 'registered', project_root: root, session_id: binding.session_id,
    hint: adapter.status === 'registered' ? 'The plugin credential was saved and this project is bound to this Claude session; restart Claude Code with claude --continue to load them.'
      : 'This project is bound to this Claude session; reconnect the team-messaging MCP server (/mcp, then Reconnect) to load its tools.' };
}
async function recover(fetcher, url, token, timeoutMs) {
  try { await request(fetcher, url, '/credentials/revoke', {}, token, timeoutMs); return true; }
  catch { return false; }
}
export async function join(options, deps = {}) {
  const env = deps.env ?? process.env, fetcher = deps.fetch ?? brokerFetch, timeoutMs = deps.timeoutMs ?? 10000;
  const url = normaliseUrl(options.url, env), name = options.name ?? (deps.username ?? (()=>userInfo().username))();
  const path = credentialPath(url, name, { env });
  requireValue(!options.client || ['claude','claude-plugin','none'].includes(options.client), 'INVALID_CLIENT', 'Use --client claude, claude-plugin or none.');
  requireValue(typeof options.code === 'string' && options.code.length === 80 && /^tmi_[0-9a-f]{32}_[A-Za-z0-9_-]{43}$/.test(options.code),
    'INVALID_INVITE_CODE', 'Paste a complete invite code from the owner; use - to read it from stdin.');
  // The plugin saves these paths for its skills' shell commands; refuse before
  // the invite is redeemed rather than after a credential exists.
  requireValue(options.client!=='claude-plugin'||!unsafeOption({BROKER_URL:url,TOKEN_FILE:path}),'UNSAFE_OPTION','The credential path contains characters the plugin cannot use; set HOME or XDG_CONFIG_HOME to a path of letters, digits and / . _ ~ % - only.');
  preflightCredentialDir(dirname(path),{home:env.HOME});
  const existing = lstatSync(path, { throwIfNoEntry: false });
  const token = existing ? readCredential(path,{home:env.HOME}) : undefined;
  // J-6 and P-12: a bound project or another saved credential is refused
  // before /join, so the single-use invite is never burned by a conflict.
  let projectDir;
  if (options.client === 'claude-plugin') {
    projectDir = agentProject(options, env);
    projectPrecheck({ projectDir, claudeSession: options.claudeSession, env, unused: ' The invite was not used.' });
    credentialPrecheck({ spawn: deps.spawn, env, url, path, unused: ' The invite was not used.' });
  }
  let joined,already=false;
  try { joined = await request(fetcher, url, '/join', { code: options.code, name }, token, timeoutMs); }
  catch (error) {
    if(existing&&error.code==='ALREADY_MEMBER'){joined=error.binding;already=true;}else throw error;
  }
  const validIdentity = joined && joined.user === name && typeof joined.workspace === 'string' &&
    /^[a-zA-Z0-9_.-]{1,128}$/.test(joined.workspace)&&joined.workspace!=='broker'&&typeof joined.topic==='string'&&topicPattern.test(joined.topic);
  const validToken = existing || (typeof joined?.token === 'string' && joined.token.length === 80 && /^tmc_[0-9a-f]{32}_[A-Za-z0-9_-]{43}$/.test(joined.token));
  if (!validIdentity || !validToken) {
    const recovered = !existing && typeof joined?.token === 'string' && await recover(fetcher, url, joined.token, timeoutMs);
    throw new ChannelError('INVALID_JOIN_RESPONSE', recovered ? 'The broker response was invalid; the newly minted credential and membership were revoked.' : 'The broker response was invalid; ask the owner to check and remove any new membership.');
  }
  if (!existing) {
    try { await (deps.write ?? writeCredential)(path, joined.token,{home:env.HOME}); }
    catch {
      const recovered = await recover(fetcher, url, joined.token, timeoutMs);
      throw new ChannelError('CREDENTIAL_SAVE_FAILED', 'Could not save the credential; choose another name, or ask the workspace owner for a new invite.' +
        (recovered ? ' The new credential and membership were revoked.' : ' Recovery could not be confirmed; ask the owner to remove the new membership.'));
    }
  }
  const clients = options.client === 'none' ? [] : options.client ? [options.client] : ['claude'].filter(client=>(deps.available ?? available)(client,env));
  const probe=statusProbe({fetcher,url,token:token??joined.token,name,workspace:joined.workspace,topic:joined.topic,timeoutMs});
  const adapters=[];for(const client of clients){
    const binding={url,workspace:joined.workspace,topic:joined.topic,path},adapterDeps={spawn:deps.spawn,env,probe,cwd:deps.cwd??process.cwd()};
    adapters.push(client==='claude-plugin'
      ? await bindProjectFile({ url, path, token: token ?? joined.token, binding: { workspace: joined.workspace, topic: joined.topic, session_id: pinnedSession('claude-project', url, joined.workspace, joined.topic) }, claudeSession: options.claudeSession, projectDir, env, deps, used: ' The invite was used.' })
      : await register(client,binding,adapterDeps));
  }
  return {user:joined.user,workspace:joined.workspace,topic:joined.topic,credential_path:path,adapters,...(already?{status:'already_member'}:{})};
}
const bindHints = {
  NOT_A_MEMBER: 'This name is not a member of that workspace; join it with an invite first.',
  TOPIC_REQUIRED: 'Only the workspace owner can bind at workspace level; pass --topic for a subscribed topic.',
  NOT_SUBSCRIBED: 'You are not subscribed to that topic; ask the workspace owner for an invite to it and use /team-messaging:join.',
  UNAUTHORIZED: 'The saved credential is inactive; ask the workspace owner for a new invite.',
  WORKSPACE_LIMIT: 'You own as many workspaces as the broker allows; bind one you already own.',
  TOPIC_CLOSED: 'That topic is closed; choose another topic name.',
  SESSION_ENDED: 'That broker session has ended; run /team-messaging:bind or /team-messaging:join for this project.',
};
// One broker request for the owner flow, with fixed local hints.
function brokerCaller(fetcher, url, token, timeoutMs) {
  return async (method, path, { session, workspace, body } = {}) => {
    let response, data;
    try {
      response = await fetcher(url + path, { method, redirect: 'error', signal: AbortSignal.timeout(timeoutMs),
        headers: { Authorization: `Bearer ${token}`, ...(session ? { 'X-Session-Id': session, 'X-Workspace': workspace } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) }, ...(body ? { body: JSON.stringify(body) } : {}) });
      data = await response.json();
    } catch { throw new ChannelError('BIND_UNAVAILABLE', 'Check the broker URL, TLS and availability; redirects are refused.'); }
    if (!response.ok) {
      const code = /^[A-Z_]{1,64}$/.test(data?.error ?? '') ? data.error : 'BIND_REFUSED';
      throw new ChannelError(code, bindHints[code] ?? 'The broker refused this step; check the name, workspace, topic and broker availability.');
    }
    return data;
  };
}
// Plan project-options P-16 with AM-5: the one owner command. Each step is
// idempotent, so a rerun after a partial failure skips what exists and a
// rerun after success is `skipped`. Issuing invites stays a separate step.
export async function bind(options, deps = {}) {
  if (options.adoptGlobal) return adopt(options, deps);
  const env = deps.env ?? process.env, fetcher = deps.fetch ?? brokerFetch, timeoutMs = deps.timeoutMs ?? 10000;
  const url = normaliseUrl(options.url, env);
  const path = credentialPath(url, options.name, { env });
  requireValue(options.client === undefined || options.client === 'claude-plugin', 'INVALID_CLIENT', 'bind supports only --client claude-plugin.');
  requireValue(typeof options.workspace === 'string' && /^[a-zA-Z0-9_.-]{1,128}$/.test(options.workspace) && options.workspace !== 'broker', 'INVALID_WORKSPACE', 'Pass --workspace with 1-128 letters, digits, underscores, dots or hyphens, other than broker.');
  requireValue(options.topic === undefined || (typeof options.topic === 'string' && topicPattern.test(options.topic)), 'INVALID_TOPIC', 'Pass --topic with a valid topic name, or omit it for a workspace-level binding.');
  requireValue(options.mode === undefined || (options.topic !== undefined && ['broadcast', 'queue'].includes(options.mode)), 'INVALID_TOPIC_MODE', 'Pass --mode broadcast or queue, and only with --topic.');
  requireValue(lstatSync(path, { throwIfNoEntry: false }), 'CREDENTIAL_MISSING', 'No saved credential for this broker and name; run /team-messaging:join with an invite first.');
  const token = readCredential(path, { home: env.HOME }), workspace = options.workspace, topic = options.topic ?? null;
  const done = adapter => ({ user: options.name, workspace, topic, credential_path: path, adapters: [adapter] });
  // Step 1: the local pre-check. A bound project stops here.
  const projectDir = agentProject(options, env);
  const found = projectPrecheck({ projectDir, claudeSession: options.claudeSession, binding: { workspace, topic }, env, token, url });
  if (found) return done({ client: 'claude-plugin', plugin_id: PLUGIN_ID, status: 'skipped', project_root: found.root, session_id: found.binding.session_id, hint: 'already bound' });
  credentialPrecheck({ spawn: deps.spawn, env, url, path });
  const call = brokerCaller(fetcher, url, token, timeoutMs);
  // Step 2: membership. GET /workspaces needs no session; its rows are
  // {name, role, joined}. WORKSPACE_EXISTS with the name still not listed as
  // ours means it exists for someone else, or was deleted.
  const role = async () => { const rows = (await call('GET', '/workspaces')).workspaces; const row = Array.isArray(rows) ? rows.find(entry => entry?.name === workspace) : undefined; return row ? row.role : null; };
  let owned = await role();
  if (owned === null) {
    try { await call('POST', '/workspaces', { body: { name: workspace } }); owned = 'owner'; }
    catch (error) {
      if (error.code !== 'WORKSPACE_EXISTS') throw error;
      owned = await role();
      // AM-9: a topic binding in a workspace that exists without you needs an invite first.
      requireValue(owned !== null || topic === null, 'NOT_A_MEMBER', bindHints.NOT_A_MEMBER);
    }
  }
  // AM-9: a member (not owner) subscribed to the topic binds another project
  // without an invite. No owner step runs; the probe in step 5 answers
  // NOT_SUBSCRIBED for a topic the member is not subscribed to. Every other
  // case needs an owner step.
  const member = owned === 'member' && topic !== null;
  requireValue(!member || options.mode === undefined, 'INVALID_TOPIC_MODE', '--mode applies only to a new topic, which only the workspace owner creates; leave it out.');
  requireValue(owned === 'owner' || member, 'WORKSPACE_NOT_OWNED', `Workspace "${workspace}" exists and is not yours; choose another name, or ask its owner for an invite and use /team-messaging:join. Nothing was written.`);
  // Steps 3 and 4 run on a one-off workspace-scope setup session: a broker
  // session's scope is fixed at its first heartbeat (AM-5), so the project's
  // session is only ever used at its binding scope. The broker has no route
  // to end a session, so the setup session is left unused afterwards.
  if (topic !== null && !member) {
    const setup = { session: `claude-setup-${randomBytes(16).toString('hex')}`, workspace };
    try { await call('POST', '/topics', { ...setup, body: { name: topic, ...(options.mode ? { mode: options.mode } : {}) } }); }
    catch (error) {
      if (error.code !== 'TOPIC_EXISTS') throw error;
      // GET /topics ignores a query (AM-5), so the topic is found here.
      const rows = (await call('GET', '/topics', setup)).topics, row = Array.isArray(rows) ? rows.find(entry => entry?.name === topic) : undefined;
      requireValue(row, 'BIND_REFUSED', 'The broker reported the topic as existing but did not list it; retry shortly.');
      requireValue(row.closed !== true, 'TOPIC_CLOSED', bindHints.TOPIC_CLOSED);
      requireValue(options.mode === undefined || row.mode === options.mode, 'TOPIC_MODE_MISMATCH', 'That topic exists with another mode; omit --mode to reuse it, or choose another topic name.');
    }
    await call('POST', '/topics/subscribe', { ...setup, body: { name: topic } });
  }
  // Step 5: probe the project's new session at its binding scope, then
  // write the project file. Only a confirmed state passes.
  const session = pinnedSession('claude-project', url, workspace, topic);
  const checked = await statusProbe({ fetcher, url, token, name: options.name, workspace, topic, timeoutMs })(session);
  if (!checked.state) {
    const code = /^[A-Z_]{1,64}$/.test(checked.error ?? '') ? checked.error : 'BIND_REFUSED';
    throw new ChannelError(code, bindHints[code] ?? 'The broker refused this binding; check the name, workspace, topic and broker availability.');
  }
  return done(await bindProjectFile({ url, path, token, binding: { workspace, topic, session_id: session }, claudeSession: options.claudeSession, projectDir, env, deps }));
}
// Plan project-options P-15 with AM-7: copy a 0.9 global binding into this
// project, keeping its broker session (inbox, cursors, leases), and only then
// rewrite the global options to credential-only. A rerun that finds its own
// adopted file finishes the clearing; the adopt marker refuses a second root.
async function adopt(options, deps) {
  const env = deps.env ?? process.env, fetcher = deps.fetch ?? brokerFetch, timeoutMs = deps.timeoutMs ?? 10000;
  requireValue(['url', 'name', 'workspace', 'topic', 'mode', 'client'].every(key => options[key] === undefined), 'INVALID_BIND_ARGS', 'Use team-messaging bind --adopt-global --claude-session id --data-dir dir [--project dir].');
  requireValue(typeof options.dataDir === 'string' && options.dataDir !== '', 'DATA_DIR_REQUIRED', 'Pass --data-dir ${CLAUDE_PLUGIN_DATA}.');
  const projectDir = agentProject(options, env), claudeSession = options.claudeSession;
  const current = readPluginConfig(PLUGIN_ID, { spawn: deps.spawn, env });
  requireValue(current.status === 'ok', 'PLUGIN_NOT_READY', `Install the plugin (claude plugin install ${PLUGIN_ID}) or fix its configuration, then run this again.`);
  const saved = current.values, nothing = 'The plugin holds no usable 0.9 global binding to adopt; use /team-messaging:bind or /team-messaging:join.';
  requireValue(/^[A-Za-z0-9_.-]{1,128}$/.test(saved.workspace) && saved.workspace !== 'broker' && (saved.topic === '' || topicPattern.test(saved.topic)) && /^[A-Za-z0-9_.-]{1,128}$/.test(saved.session_id) && saved.session_id !== 'broker', 'NOTHING_TO_ADOPT', nothing);
  const url = normaliseUrl(saved.broker_url, env), name = credentialName(url, saved.token_file, { env: { HOME: env.HOME, XDG_CONFIG_HOME: env.XDG_CONFIG_HOME } });
  const token = readCredential(saved.token_file, { home: env.HOME }), binding = { workspace: saved.workspace, topic: saved.topic === '' ? null : saved.topic, session_id: saved.session_id };
  const finish = root => {
    const status = writePluginConfig(PLUGIN_ID, credentialOnly(saved), { spawn: deps.spawn, env });
    return { user: name, workspace: binding.workspace, topic: binding.topic, credential_path: saved.token_file, adapters: [{ client: 'claude-plugin', plugin_id: PLUGIN_ID, status: status === 'registered' ? 'adopted' : status, project_root: root, session_id: binding.session_id,
      hint: status === 'registered' ? 'The 0.9 binding now lives in this project and the global options hold only the credential; restart Claude Code with claude --continue to load them.' : 'The project file was written but the global binding could not be cleared; run this command again.' }] };
  };
  const found = findProject(projectDir, { home: env.HOME });
  if (found.state === 'invalid') throw projectError(found.code);
  if (found.state === 'found') {
    const own = found.options.claude_session_id === claudeSession && found.binding.workspace === binding.workspace && found.binding.topic === binding.topic && found.binding.session_id === binding.session_id && verifyMac(token, found, url);
    if (!own) throw alreadyBound(found, '');
    return finish(found.root);
  }
  const root = projectTarget(projectDir, { home: env.HOME }), marker = readAdoptMarker(options.dataDir, { home: env.HOME });
  if (marker && marker.root !== root) throw new ChannelError('PLUGIN_ALREADY_BOUND', `The 0.9 global binding was already adopted into another project (${marker.root}); bind this project with /team-messaging:bind or /team-messaging:join instead.`);
  // An ended 0.9 session cannot carry anything over, so it is refused.
  const checked = await statusProbe({ fetcher, url, token, name, workspace: binding.workspace, topic: binding.topic, timeoutMs })(binding.session_id);
  if (!checked.state) {
    const code = /^[A-Z_]{1,64}$/.test(checked.error ?? '') ? checked.error : 'BIND_REFUSED';
    throw new ChannelError(code, bindHints[code] ?? 'The 0.9 global binding could not be verified with the broker; it was not adopted.');
  }
  writeAdoptMarker(options.dataDir, { root, claudeSession }, { home: env.HOME });
  writeProjectOptions(root, projectOptions(token, { root, url, claudeSession, binding }));
  return finish(root);
}
// Plan project-options P-14: the claim skill is user-only
// (disable-model-invocation), but that gates only the Skill tool; a model
// with Bash can run this command, so the Bash permission prompt is the real
// gate (review round 1, S1). It needs a trusted file, rewrites only
// claude_session_id and the MAC, keeps the broker session (inbox, cursors,
// leases and queue claims carry over) and makes no broker call.
export async function claim(options, deps = {}) {
  const env = deps.env ?? process.env;
  const projectDir = agentProject(options, env), claudeSession = options.claudeSession;
  const found = findProject(projectDir, { home: env.HOME });
  if (found.state === 'invalid') throw projectError(found.code);
  requireValue(found.state === 'found', 'PROJECT_NOT_BOUND', 'This project has no team-messaging binding; run /team-messaging:bind or /team-messaging:join.');
  const saved = tmOptions(env);
  requireValue(saved.broker_url && saved.token_file, 'PLUGIN_NOT_CONFIGURED', 'The plugin has no saved credential; run /team-messaging:join or /team-messaging:bind.');
  const url = normaliseUrl(saved.broker_url, env);
  credentialName(url, saved.token_file, { env: { HOME: env.HOME, XDG_CONFIG_HOME: env.XDG_CONFIG_HOME } });
  const token = readCredential(saved.token_file, { home: env.HOME });
  requireValue(verifyMac(token, found, url), 'PROJECT_OPTIONS_UNTRUSTED', projectHint('PROJECT_OPTIONS_UNTRUSTED'));
  const out = { workspace: found.binding.workspace, topic: found.binding.topic, project_root: found.root };
  if (found.options.claude_session_id === claudeSession) return { status: 'already_agent', ...out };
  writeProjectOptions(found.root, projectOptions(token, { root: found.root, url, claudeSession, binding: found.binding }));
  return { status: 'claimed', previous_agent: found.options.claude_session_id, ...out,
    hint: 'Reconnect the team-messaging MCP server in this session (/mcp, then Reconnect) or restart with claude --continue. The previous agent session stops within about 5 s.' };
}
// Plugin status: the credential comes from TM_* (the skill passes the saved
// options inline) and the binding from the project file. Only fixed states,
// validated names and the project root are printed, never broker strings.
async function status(options, env) {
  const projectDir = agentProject(options, env);
  const plugin = resolveBinding({ options: tmOptions(env), claudeSession: options.claudeSession, projectDir, env });
  if (plugin.state === 'invalid') throw new ChannelError(plugin.code, plugin.hint);
  if (plugin.state === 'idle') return { state: 'idle', agent: null, project_root: null, workspace: null, topic: null };
  if (plugin.state === 'inert') return { state: 'inert', agent: plugin.agent, project_root: plugin.root, workspace: plugin.binding?.workspace ?? null, topic: plugin.binding?.topic ?? null };
  let data;
  try { data = await new Channel(config({ ...plugin.env, CHANNEL_MODE: 'poll' }), async () => {}, () => {}).api('/session/status'); }
  catch (error) { const safe = localError(error, 'Check the plugin binding and broker availability.'); throw new ChannelError(safe.code ?? 'STATUS_FAILED', safe.hint); }
  return { state: ['active', 'unregistered'].includes(data?.state) ? data.state : 'UNKNOWN_STATE', agent: plugin.agent, project_root: plugin.root, workspace: plugin.binding.workspace, topic: plugin.binding.topic };
}
// --name value pairs, each once; `booleans` take no value. Kebab-case flags
// become camelCase options (--claude-session -> claudeSession).
function flags(args, allowed, code, hint, booleans = []) {
  const out = {};
  for (let i = 0; i < args.length;) {
    const flag = args[i], key = typeof flag === 'string' && flag.startsWith('--') ? flag.slice(2).replace(/-([a-z])/g, (_, c) => c.toUpperCase()) : undefined;
    if (booleans.includes(flag)) { requireValue(!Object.hasOwn(out, key), code, hint); out[key] = true; i++; continue; }
    requireValue(allowed.includes(flag) && args[i + 1] && !Object.hasOwn(out, key), code, hint);
    out[key] = args[i + 1]; i += 2;
  }
  return out;
}
const AGENT_FLAGS = ['--claude-session', '--data-dir', '--project'];
async function main() {
  try {
    const [command,url,argument,...rest] = process.argv.slice(2);
    if (command === 'status' || command === 'claim') {
      const code = command === 'status' ? 'INVALID_STATUS_ARGS' : 'INVALID_CLAIM_ARGS';
      const options = flags(process.argv.slice(3), AGENT_FLAGS, code, `Use team-messaging ${command} --claude-session id --data-dir dir [--project dir], with TM_BROKER_URL and TM_TOKEN_FILE in the environment.`);
      process.stdout.write(JSON.stringify(command === 'status' ? await status(options, process.env) : await claim(options))+'\n');
      return;
    }
    if (command === 'bind') {
      const hint = 'Use team-messaging bind <broker-url> --name user --workspace ws [--topic t [--mode broadcast|queue]] --claude-session id --data-dir dir [--project dir], or team-messaging bind --adopt-global --claude-session id --data-dir dir.';
      const positional = url !== undefined && !url.startsWith('--');
      const bindOptions = { ...flags(process.argv.slice(positional ? 4 : 3), ['--name','--workspace','--topic','--mode','--client',...AGENT_FLAGS], 'INVALID_BIND_ARGS', hint, ['--adopt-global']), ...(positional ? { url } : {}) };
      requireValue(positional !== Boolean(bindOptions.adoptGlobal), 'INVALID_BIND_ARGS', hint);
      const result = await bind(bindOptions);
      process.stdout.write(JSON.stringify(result)+'\n');
      if (result.adapters.some(adapter=>adapter.status === 'conflict')) process.exitCode = 1;
      return;
    }
    requireValue(command === 'join' && url && argument, 'INVALID_JOIN_ARGS', 'Use team-messaging join <broker-url> <code|-> [--name user] [--client claude|claude-plugin|none] [--claude-session id --data-dir dir [--project dir]].');
    const options = { ...flags(rest, ['--name','--client',...AGENT_FLAGS], 'INVALID_JOIN_ARGS', 'Pass each supported option once with its value.'), url };
    // Refuse unsafe transport before reading stdin, as well as before network access.
    normaliseUrl(url);
    if (argument === '-') {
      let input='';for await (const chunk of process.stdin) { input+=chunk;requireValue(input.length<=4096,'INVALID_INVITE_CODE','Paste one complete invite code.'); }
      options.code=input.trim();
    } else options.code=argument;
    const result = await join(options);
    process.stdout.write(JSON.stringify(result)+'\n');
    if (result.adapters.some(adapter=>adapter.status === 'conflict')) process.exitCode = 1;
  } catch (error) {
    const safe = error instanceof ChannelError ? error : new ChannelError('JOIN_FAILED','Check the private credential directory, arguments and broker availability.');
    process.stderr.write(JSON.stringify({error:safe.code,hint:safe.hint})+'\n');process.exitCode=1;
  }
}
if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
