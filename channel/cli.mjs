#!/usr/bin/env node
import { dirname } from 'node:path';
import { lstatSync, realpathSync } from 'node:fs';
import { userInfo } from 'node:os';
import { fileURLToPath } from 'node:url';
import { credentialPath, preflightCredentialDir, readCredential, writeCredential } from './credentials.mjs';
import { register, available, pinnedSession } from './register.mjs';
import { PLUGIN_ID, pluginAdapter, readPluginConfig } from './plugin-config.mjs';
import { SAFE_URL, localError, pluginEnv, unsafeOption } from './plugin-env.mjs';
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
// join used to redeem the single-use invite first and only then find the
// plugin bound elsewhere: the member lost the invite and got a conflict. So
// the plugin binding is read here, before /join, and any saved binding or an
// unreadable configuration is refused. pluginAdapter keeps its own checks after the redeem for a
// binding that changes between this read and its write.
async function pluginPrecheck({ spawn, env, fetcher, name, timeoutMs }) {
  const current = readPluginConfig(PLUGIN_ID, { spawn, env });
  requireValue(current.status === 'ok', 'PLUGIN_NOT_READY', `Install the plugin (claude plugin install ${PLUGIN_ID}) or fix its configuration, then run join again. The invite was not used.`);
  const saved = current.values;
  if (!['broker_url', 'token_file', 'workspace', 'session_id'].some(key => saved[key] !== '')) return;
  // J-6: an ended binding is refused too. The invite's workspace and topic
  // are unknown until it is redeemed, and pluginAdapter replaces only an
  // identical ended binding, so letting an ended one through burned the
  // invite for every other target. The probe only words the hint: a saved
  // URL that join itself would refuse, an unreadable credential or a failed
  // probe cannot be verified and is reported as live, as in pluginAdapter.
  // The saved credential goes only to the broker it was saved for: the saved
  // options are user-editable, so a token_file that is not this name's
  // credential for that broker_url is never read or sent (review round 1).
  let checked;
  try {
    requireValue(/^[A-Za-z0-9_.-]{1,128}$/.test(saved.session_id) && saved.session_id !== 'broker', 'INVALID_SESSION', 'unverifiable');
    const url = normaliseUrl(saved.broker_url, env);
    requireValue(saved.token_file === credentialPath(url, name, { env }), 'CREDENTIAL_MISMATCH', 'unverifiable');
    const token = readCredential(saved.token_file, { home: env.HOME });
    checked = await statusProbe({ fetcher, url, token, name, workspace: saved.workspace, topic: saved.topic === '' ? null : saved.topic, timeoutMs })(saved.session_id);
  } catch {}
  // The saved names come from a file the user can edit, so only valid ones
  // are echoed; the credential path and its contents never are.
  const workspace = /^[a-zA-Z0-9_.-]{1,128}$/.test(saved.workspace) ? `workspace "${saved.workspace}"` : 'an unrecognised workspace';
  const topic = saved.topic === '' ? 'at workspace level' : topicPattern.test(saved.topic) ? `topic "${saved.topic}"` : 'an unrecognised topic';
  const reset = 'reset the plugin (uninstall it, then install it again) and run join again. The invite was not used.';
  throw new ChannelError('PLUGIN_ALREADY_BOUND', checked?.error === 'SESSION_ENDED'
    ? `The plugin is still bound to ${workspace}, ${topic}, but that session has ended. Even for the same workspace and topic, ${reset}`
    : `The plugin is already bound to ${workspace}, ${topic}, and that session is live or could not be checked. If this invite is for that same binding, nothing is needed. Otherwise ${reset}`);
}
async function recover(fetcher, url, token, timeoutMs) {
  try { await request(fetcher, url, '/credentials/revoke', {}, token, timeoutMs); return true; }
  catch { return false; }
}
export async function join(options, deps = {}) {
  const env = deps.env ?? process.env, fetcher = deps.fetch ?? brokerFetch, timeoutMs = deps.timeoutMs ?? 10000;
  const url = normaliseUrl(options.url, env), name = options.name ?? (deps.username ?? (()=>userInfo().username))();
  const path = credentialPath(url, name, { env });
  requireValue(!options.client || ['claude','codex','claude-plugin','none'].includes(options.client), 'INVALID_CLIENT', 'Use --client claude, codex, claude-plugin or none.');
  requireValue(typeof options.code === 'string' && options.code.length === 80 && /^tmi_[0-9a-f]{32}_[A-Za-z0-9_-]{43}$/.test(options.code),
    'INVALID_INVITE_CODE', 'Paste a complete invite code from the owner; use - to read it from stdin.');
  // The plugin saves these paths for its skills' shell commands; refuse before
  // the invite is redeemed rather than after a credential exists.
  requireValue(options.client!=='claude-plugin'||!unsafeOption({BROKER_URL:url,TOKEN_FILE:path}),'UNSAFE_OPTION','The credential path contains characters the plugin cannot use; set HOME or XDG_CONFIG_HOME to a path of letters, digits and / . _ ~ % - only.');
  preflightCredentialDir(dirname(path),{home:env.HOME});
  const existing = lstatSync(path, { throwIfNoEntry: false });
  const token = existing ? readCredential(path,{home:env.HOME}) : undefined;
  if (options.client === 'claude-plugin') await pluginPrecheck({ spawn: deps.spawn, env, fetcher, name, timeoutMs });
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
  const clients = options.client === 'none' ? [] : options.client ? [options.client] : ['claude','codex'].filter(client=>(deps.available ?? available)(client,env));
  const probe=statusProbe({fetcher,url,token:token??joined.token,name,workspace:joined.workspace,topic:joined.topic,timeoutMs});
  const adapters=[];for(const client of clients){
    const binding={url,workspace:joined.workspace,topic:joined.topic,path},adapterDeps={spawn:deps.spawn,env,probe,cwd:deps.cwd??process.cwd()};
    adapters.push(client==='claude-plugin'?await pluginAdapter(binding,adapterDeps):await register(client,binding,adapterDeps));
  }
  return {user:joined.user,workspace:joined.workspace,topic:joined.topic,credential_path:path,adapters,...(already?{status:'already_member'}:{})};
}
const bindHints = {
  NOT_A_MEMBER: 'This name is not a member of that workspace; join it with an invite first.',
  TOPIC_REQUIRED: 'Only the workspace owner can bind at workspace level; pass --topic for a subscribed topic.',
  NOT_SUBSCRIBED: 'Subscribe to the topic before binding to it.',
  UNAUTHORIZED: 'The saved credential is inactive; ask the workspace owner for a new invite.',
};
// Owner path: reuse the saved credential for another workspace-level or topic
// binding without consuming an invite, then write the plugin configuration.
export async function bind(options, deps = {}) {
  const env = deps.env ?? process.env, fetcher = deps.fetch ?? brokerFetch, timeoutMs = deps.timeoutMs ?? 10000;
  const url = normaliseUrl(options.url, env);
  const path = credentialPath(url, options.name, { env });
  requireValue(options.client === 'claude-plugin', 'INVALID_CLIENT', 'bind supports only --client claude-plugin.');
  requireValue(typeof options.workspace === 'string' && /^[a-zA-Z0-9_.-]{1,128}$/.test(options.workspace) && options.workspace !== 'broker', 'INVALID_WORKSPACE', 'Pass --workspace with 1-128 letters, digits, underscores, dots or hyphens, other than broker.');
  requireValue(options.topic === undefined || (typeof options.topic === 'string' && topicPattern.test(options.topic)), 'INVALID_TOPIC', 'Pass --topic with a valid topic name, or omit it for a workspace-level binding.');
  requireValue(lstatSync(path, { throwIfNoEntry: false }), 'CREDENTIAL_MISSING', 'No saved credential for this broker and name; run /team-messaging:join with an invite first.');
  const token = readCredential(path, { home: env.HOME }), topic = options.topic ?? null, session = pinnedSession('claude-plugin', url, options.workspace, topic);
  const probe = statusProbe({ fetcher, url, token, name: options.name, workspace: options.workspace, topic, timeoutMs });
  const checked = await probe(session);
  // Only a confirmed state passes: a refusal without an error code must not
  // write a binding.
  if (!checked.state) {
    const code = /^[A-Z_]{1,64}$/.test(checked.error ?? '') ? checked.error : 'BIND_REFUSED';
    throw new ChannelError(code, bindHints[code] ?? 'The broker refused this binding; check the name, workspace, topic and broker availability.');
  }
  const adapter = await pluginAdapter({ url, workspace: options.workspace, topic, path, session }, { spawn: deps.spawn, env, probe, cwd: deps.cwd ?? process.cwd() });
  return { user: options.name, workspace: options.workspace, topic, credential_path: path, adapters: [adapter] };
}
// Plugin status: the binding comes from TM_* (the skill passes the saved
// options inline); only the session state is printed.
async function status(env) {
  const plugin = pluginEnv(env);
  requireValue(plugin.configured, 'PLUGIN_NOT_CONFIGURED', 'Run /team-messaging:join or /team-messaging:bind, then restart Claude Code.');
  let data;
  try { data = await new Channel(config({ ...plugin.env, CHANNEL_MODE: 'poll' }), async () => {}, () => {}).api('/session/status'); }
  catch (error) { const safe = localError(error, 'Check the plugin binding and broker availability.'); throw new ChannelError(safe.code ?? 'STATUS_FAILED', safe.hint); }
  // Only a known state and the configured binding are printed, never broker strings.
  return { state: ['active', 'unregistered'].includes(data?.state) ? data.state : 'UNKNOWN_STATE', workspace: plugin.env.CHANNEL_WORKSPACE, topic: plugin.env.CHANNEL_TOPIC ?? null };
}
async function main() {
  try {
    const [command,url,argument,...rest] = process.argv.slice(2), options = { url };
    if (command === 'status') {
      requireValue(url === undefined, 'INVALID_STATUS_ARGS', 'Use team-messaging status with the TM_* binding in the environment.');
      process.stdout.write(JSON.stringify(await status(process.env))+'\n');
      return;
    }
    if (command === 'bind') {
      const args = process.argv.slice(4), bindOptions = { url };
      requireValue(url && !url.startsWith('--'), 'INVALID_BIND_ARGS', 'Use team-messaging bind <broker-url> --name user --workspace ws [--topic t] --client claude-plugin.');
      for (let i=0;i<args.length;i+=2) {
        requireValue(['--name','--workspace','--topic','--client'].includes(args[i]) && args[i+1] && !Object.hasOwn(bindOptions,args[i].slice(2)), 'INVALID_BIND_ARGS', 'Pass each supported option once with its value.');
        bindOptions[args[i].slice(2)] = args[i+1];
      }
      const result = await bind(bindOptions);
      process.stdout.write(JSON.stringify(result)+'\n');
      if (result.adapters.some(adapter=>adapter.status === 'conflict')) process.exitCode = 1;
      return;
    }
    requireValue(command === 'join' && url && argument, 'INVALID_JOIN_ARGS', 'Use team-messaging join <broker-url> <code|-> [--name user] [--client claude|codex|claude-plugin|none].');
    for (let i=0;i<rest.length;i+=2) {
      requireValue(['--name','--client'].includes(rest[i]) && rest[i+1] && !Object.hasOwn(options,rest[i].slice(2)), 'INVALID_JOIN_ARGS', 'Pass each supported option once with its value.');
      options[rest[i].slice(2)] = rest[i+1];
    }
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
