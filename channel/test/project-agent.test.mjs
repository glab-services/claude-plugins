import { test } from 'node:test';
import { execFileSync } from 'node:child_process';
import assert from 'node:assert/strict';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import fs, { chmodSync, copyFileSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, symlinkSync, utimesSync, writeFileSync } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { join as pathJoin } from 'node:path';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { ToolListChangedNotificationSchema } from '@modelcontextprotocol/sdk/types.js';
import { brokerFixture, tokens, pluginScratch, runChannel, bindProject, credentialScratch, writeProjectFile, projectMac, eventually } from './helpers.mjs';
import { prepareTopicFixture, FIXTURE_TOPIC } from '../../test-support/topic-fixture.mjs';
import { bind, claim, join } from '../cli.mjs';
import { findProject, projectOptions, projectTarget, readSessionRecord, writeProjectOptions, writeSessionRecord } from '../project-options.mjs';
import { credentialPath } from '../credentials.mjs';

// Plan project-options: one agent session per bound project. Hooks get the
// project from --project-dir and their Claude session from the hook input;
// the MCP server gets both from CLAUDE_PROJECT_DIR and CLAUDE_CODE_SESSION_ID.
const hookInput = (event, session) => JSON.stringify({ hook_event_name: event, session_id: session, ...(event === 'Stop' ? { stop_hook_active: false } : { source: 'startup' }) });
function hooks(s, env, project, session, { deadline = '2' } = {}) {
  const args = ['--project-dir', project, '--data-dir', s.data];
  return Promise.all([
    runChannel('plugin-session-start.mjs', args, env, hookInput('SessionStart', session)).result,
    runChannel('stop-hook.mjs', args, env, hookInput('Stop', session)).result,
    runChannel('watch.mjs', ['--mode', 'rewake', ...args, '--poll-s', '1', '--deadline-s', deadline], env, hookInput('Stop', session)).result,
  ]);
}
async function mcp(t, env) {
  const client = new Client({ name: 'project-agent-test', version: '1.0.0' }), logs = [], changed = [];
  client.setNotificationHandler(ToolListChangedNotificationSchema, () => changed.push(Date.now()));
  const transport = new StdioClientTransport({ command: process.execPath, args: [fileURLToPath(new URL('../plugin-main.mjs', import.meta.url))], env, stderr: 'pipe' });
  transport.stderr.on('data', chunk => logs.push(chunk.toString()));
  t.after(() => client.close());
  await client.connect(transport);
  return { client, logs, changed };
}
// The MCP env .mcp.json gives an agent: the four TM_* plus what Claude Code sets.
const mcpEnv = (s, f, claudeSession, project, extra = {}) => ({ ...s.tmEnv(f), TM_MODE: '', TM_ARM: '', CLAUDE_CODE_SESSION_ID: claudeSession, CLAUDE_PROJECT_DIR: project, CHANNEL_POLL_MS: '200', ...extra });
const gitDir = (...parts) => { const dir = pathJoin(...parts); mkdirSync(pathJoin(dir, '.git'), { recursive: true, mode: 0o700 }); return dir; };
const contextOf = result => result.stdout ? JSON.parse(result.stdout).hookSpecificOutput.additionalContext : '';
// A stand-in for the claude CLI's plugin configure (shape from claude 2.1.309,
// see plugin-config.mjs). `state.inputs` is what --json reports; a write
// updates it, as the real CLI would. `fail` makes the write fail.
function claudeConfig(inputs = {}) {
  const state = { inputs: { broker_url: '', token_file: '', workspace: '', topic: '', session_id: '', mode: 'poll', arm: 'rewake', ...inputs }, writes: [], fail: false };
  state.spawn = (command, args, options) => {
    if (args.includes('--json')) return { status: 0, stdout: JSON.stringify({ inputs: state.inputs, configured: [] }) };
    if (state.fail) return { status: 1 };
    const values = JSON.parse(options.input); state.writes.push(values); state.inputs = { ...values };
    return { status: 0, stdout: '' };
  };
  return state;
}
const requestsOf = (f, session) => f.requests.filter(request => request.session === session);
async function recorder(t) {
  const seen = [];
  const server = http.createServer((req, res) => { seen.push({ path: req.url, auth: req.headers.authorization ?? null }); req.resume(); res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{"messages":[]}'); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  return { seen, url: `http://127.0.0.1:${server.address().port}` };
}

// Phase 0.6 (red on c9543a1/2361ab1): a 0.9 global binding is still saved,
// and the project names another Claude session as its agent.
test('a second Claude session in a bound project sends no broker request', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), tokenFile = credentialScratch(s, f.url);
  const project = pathJoin(s.dir, 'project');
  bindProject(project, { url: f.url, claudeSession: 'agent-1' });
  const saved = { CLAUDE_PLUGIN_OPTION_BROKER_URL: f.url, CLAUDE_PLUGIN_OPTION_TOKEN_FILE: tokenFile, CLAUDE_PLUGIN_OPTION_WORKSPACE: 'default', CLAUDE_PLUGIN_OPTION_SESSION_ID: 'shared-b' };
  const env = { HOME: s.home, TMPDIR: process.env.TMPDIR, CLAUDE_PLUGIN_DATA: s.data, ...saved };
  const [start, stop, watcher] = await hooks(s, env, project, 'other-2');
  assert.deepEqual(f.requests, [], 'no hook or watcher request from the non-agent session');
  const { client } = await mcp(t, { HOME: s.home, TM_BROKER_URL: f.url, TM_TOKEN_FILE: tokenFile, TM_WORKSPACE: 'default', TM_TOPIC: '', TM_SESSION_ID: 'shared-b', TM_MODE: '', TM_ARM: '', CLAUDE_CODE_SESSION_ID: 'other-2', CLAUDE_PROJECT_DIR: project });
  const tools = (await client.listTools()).tools;
  await new Promise(resolve => setTimeout(resolve, 1500));
  assert.deepEqual(f.requests, [], 'no MCP request from the non-agent session');
  assert.deepEqual(tools, []);
  for (const result of [start, stop, watcher]) assert.equal(result.code, 0, result.stderr);
  assert.equal(stop.stdout, ''); assert.equal(watcher.stdout + watcher.stderr, '');
});

// Phase 0.6 (red on c9543a1/2361ab1), S-1: a cloned repo's settings.json env
// block can set CLAUDE_PLUGIN_OPTION_* for an unsaved option (U-2).
test('a repo env CLAUDE_PLUGIN_OPTION_BROKER_URL never makes a hook send a token', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), tokenFile = credentialScratch(s, f.url), attacker = await recorder(t);
  const bound = pathJoin(s.dir, 'bound'), unbound = pathJoin(s.dir, 'unbound');
  bindProject(bound, { url: f.url, claudeSession: 'agent-1' });
  const repoEnv = { CLAUDE_PLUGIN_OPTION_BROKER_URL: attacker.url, CLAUDE_PLUGIN_OPTION_TOKEN_FILE: tokenFile, CLAUDE_PLUGIN_OPTION_WORKSPACE: 'default', CLAUDE_PLUGIN_OPTION_SESSION_ID: 'shared-b', TM_BROKER_URL: f.url, TM_TOKEN_FILE: tokenFile };
  const env = { HOME: s.home, TMPDIR: process.env.TMPDIR, CLAUDE_PLUGIN_DATA: s.data, ...repoEnv };
  const results = [];
  for (const project of [bound, unbound]) results.push(...await hooks(s, env, project, 'agent-1'));
  assert.deepEqual(attacker.seen, [], 'the repo-chosen broker URL received nothing');
  assert.deepEqual(f.requests, [], 'nor did the real broker, under the wrong URL');
  for (const result of results) {
    assert.equal(result.code, 0, result.stderr);
    assert.ok(!(result.stdout + result.stderr).includes(tokens.bob));
  }
});

// Phase 1 step 5 starts here: the broker facts the owner flow relies on
// (plan project-options AM-5), pinned against a real HTTP broker.
test('the broker lists workspaces as {name, role, joined} without a session, and fixes a session scope at its first heartbeat', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), auth = { Authorization: `Bearer ${tokens.bob}` };
  const listed = await (await fetch(f.url + '/workspaces', { headers: auth })).json();
  assert.deepEqual(listed.workspaces.map(row => Object.keys(row).sort()), [['joined', 'name', 'role']]);
  assert.deepEqual(listed.workspaces.map(row => [row.name, row.role]), [['default', 'owner']]);
  assert.deepEqual(f.requests.at(-1).session, undefined, 'no X-Session-Id was needed');
  prepareTopicFixture(f.broker);
  const scoped = topic => fetch(f.url + '/heartbeat', { method: 'POST', headers: { ...auth, 'Content-Type': 'application/json', 'X-Session-Id': 'scope-s', ...(topic ? { 'X-Topic': topic } : {}) }, body: '{}' });
  assert.equal((await scoped(null)).status, 200);
  const moved = await scoped(FIXTURE_TOPIC);
  assert.deepEqual([moved.status, (await moved.json()).error], [409, 'SESSION_TOPIC_MISMATCH'], 'a workspace-scope session cannot move to a topic');
});

async function twoMembers(f) {
  // w2 with topic tb: bob owns it, alice is a subscribed member.
  const now = Date.now();
  assert.equal((await fetch(f.url + '/workspaces', { method: 'POST', headers: { Authorization: `Bearer ${tokens.bob}`, 'Content-Type': 'application/json' }, body: '{"name":"w2"}' })).status, 201);
  f.store.run("INSERT INTO workspace_members VALUES ('w2','alice','member',?,'invite')", now);
  f.store.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('w2','tb','broadcast','bob',?)", now);
  for (const user of ['alice', 'bob']) f.store.run("INSERT INTO topic_subscriptions VALUES ('w2','tb',?,?)", user, now);
  prepareTopicFixture(f.broker);
}
test('two projects with two agent sessions each heartbeat, wake and send in their own workspace and session', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  await twoMembers(f);
  const a = gitDir(s.dir, 'project-a'), b = gitDir(s.dir, 'project-b');
  bindProject(a, { url: f.url, claudeSession: 'agent-a', session: 'sa' });
  bindProject(b, { url: f.url, claudeSession: 'agent-b', workspace: 'w2', topic: 'tb', session: 'sb' });
  const ma = await mcp(t, mcpEnv(s, f, 'agent-a', a)), mb = await mcp(t, mcpEnv(s, f, 'agent-b', b));
  assert.equal((await ma.client.callTool({ name: 'send', arguments: { to: 'user:alice', body: 'from a', topic: FIXTURE_TOPIC } })).isError, undefined);
  assert.equal((await mb.client.callTool({ name: 'send', arguments: { to: 'user:alice', body: 'from b' } })).isError, undefined);
  const rows = f.store.all("SELECT session_id, workspace, topic FROM agents WHERE session_id IN ('sa','sb') ORDER BY session_id");
  assert.deepEqual(rows.map(row => ({ ...row })), [{ session_id: 'sa', workspace: 'default', topic: null }, { session_id: 'sb', workspace: 'w2', topic: 'tb' }]);
  const sentFrom = body => ({ ...f.store.get('SELECT from_session, workspace FROM messages WHERE body=?', body) });
  assert.deepEqual([sentFrom('from a'), sentFrom('from b')], [{ from_session: 'sa', workspace: 'default' }, { from_session: 'sb', workspace: 'w2' }]);
  f.broker.heartbeat('alice', 'alice-a', 'worker', 'default', FIXTURE_TOPIC); f.broker.heartbeat('alice', 'alice-b', 'worker', 'w2', 'tb');
  const toA = f.broker.send('alice', 'alice-a', { to: 'user:bob', body: 'to a' }), toB = f.broker.send('alice', 'alice-b', { to: 'user:bob', body: 'to b' });
  const env = s.env(f), [ra, rb] = await Promise.all([hooks(s, env, a, 'agent-a'), hooks(s, env, b, 'agent-b')]);
  for (const [results, own, other, workspace] of [[ra, toA, toB, 'default'], [rb, toB, toA, 'w2']]) {
    const [start, stop, watcher] = results;
    assert.match(contextOf(start), new RegExp(`"id":${own.id}[,}]`)); assert.doesNotMatch(contextOf(start), new RegExp(`"id":${other.id}[,}]`));
    // The Stop hook and the watcher race for the claim, so each ID is
    // delivered by exactly one of them, and only this project's IDs appear
    // (w2 also holds its creation notice).
    const fromStop = stop.stdout ? JSON.parse(stop.stdout).reason.match(/ids ([\d, ]+)\)/)[1].split(', ').map(Number) : [];
    const fromWatcher = [...watcher.stderr.matchAll(/team-message id=(\d+) /g)].map(match => Number(match[1]));
    const delivered = [...fromStop, ...fromWatcher];
    assert.equal(new Set(delivered).size, delivered.length, 'no ID delivered twice');
    assert.ok(delivered.includes(own.id) && !delivered.includes(other.id), JSON.stringify(delivered));
    for (const id of delivered) assert.equal(f.store.get('SELECT workspace FROM messages WHERE id=?', id).workspace, workspace);
  }
  assert.ok(contextOf(ra[0]).includes(`agent session for workspace default, project root ${a}`));
  assert.ok(contextOf(rb[0]).includes(`agent session for workspace w2, topic tb, project root ${b}`));
  // A rewake watcher with nothing claimed yet wakes for its own project only.
  const wakeA = f.broker.send('alice', 'alice-a', { to: 'user:bob', body: 'wake a' }), wakeB = f.broker.send('alice', 'alice-b', { to: 'user:bob', body: 'wake b' });
  const [wa, wb] = await Promise.all([['agent-a', a], ['agent-b', b]].map(([session, project]) => runChannel('watch.mjs', ['--mode', 'rewake', '--project-dir', project, '--data-dir', s.data, '--poll-s', '1', '--deadline-s', '5'], env, hookInput('Stop', session)).result));
  assert.equal(wa.code, 2); assert.match(wa.stderr, new RegExp(`^team-message id=${wakeA.id} `)); assert.ok(!wa.stderr.includes(`id=${wakeB.id} `));
  assert.equal(wb.code, 2); assert.match(wb.stderr, new RegExp(`^team-message id=${wakeB.id} `)); assert.ok(!wb.stderr.includes(`id=${wakeA.id} `));
  assert.ok(f.requests.every(request => [undefined, 'sa', 'sb', 'alice-a', 'alice-b'].includes(request.session)), 'only the two pinned sessions');
});

test('a second Claude session in the same project is inert: zero broker requests, empty tool list, one SessionStart line', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = gitDir(s.dir, 'project');
  bindProject(project, { url: f.url, claudeSession: 'agent-1' });
  const [start, stop, watcher] = await hooks(s, s.env(f), project, 'other-2');
  assert.equal(contextOf(start), "team-messaging: this project's agent is Claude session agent-1; run /team-messaging:claim here to take over");
  assert.deepEqual([start.stderr, stop.stdout, stop.stderr, watcher.code, watcher.stdout, watcher.stderr], ['', '', '', 0, '', '']);
  const m = await mcp(t, mcpEnv(s, f, 'other-2', project));
  assert.deepEqual(m.client.getServerCapabilities().tools, { listChanged: true });
  assert.deepEqual(m.client.getServerVersion(), { name: 'team-messaging', version: '0.10.0' });
  assert.deepEqual((await m.client.listTools()).tools, []);
  const refused = await m.client.callTool({ name: 'inbox', arguments: {} });
  assert.deepEqual([refused.isError, refused.structuredContent.error], [true, 'NOT_AGENT_SESSION']);
  await new Promise(resolve => setTimeout(resolve, 1000));
  assert.deepEqual(f.requests, []);
  assert.deepEqual(m.logs.join('').trim().split('\n').map(line => JSON.parse(line)), [{ event: 'plugin_inert', state: 'inert', code: 'NOT_AGENT_SESSION' }]);
});

test('claim moves the agent; the old MCP refuses its next call and stops within 6 s; the old watcher exits silently; session_id unchanged', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = gitDir(s.dir, 'project'), timed = [];
  f.server.on('request', req => timed.push({ session: req.headers['x-session-id'], at: Date.now() }));
  bindProject(project, { url: f.url, claudeSession: 'agent-1', session: 'sp' });
  const old = await mcp(t, mcpEnv(s, f, 'agent-1', project));
  assert.deepEqual(old.client.getServerCapabilities().tools, { listChanged: true });
  assert.equal(old.client.getServerVersion().version, '0.10.0', 'the active server reports the release too');
  assert.ok((await old.client.listTools()).tools.length > 10);
  await eventually(() => requestsOf(f, 'sp').length > 0, 4000);
  const watcher = runChannel('watch.mjs', ['--mode', 'rewake', '--project-dir', project, '--data-dir', s.data, '--poll-s', '1', '--deadline-s', '30'], s.env(f), hookInput('Stop', 'agent-1'));
  t.after(() => { if (watcher.child.exitCode === null && watcher.child.signalCode === null) watcher.child.kill('SIGKILL'); });
  await eventually(() => requestsOf(f, 'sp').filter(r => r.path === '/inbox?since=0').length > 1, 4000);
  // A non-user agent session cannot claim on its own: the skill is user-only.
  const claimed = await runChannel('cli.mjs', ['claim', '--claude-session', 'agent-2', '--project', project], s.tmEnv(f)).result;
  const at = Date.now();
  assert.equal(claimed.code, 0, claimed.stderr);
  assert.deepEqual(JSON.parse(claimed.stdout), { status: 'claimed', previous_agent: 'agent-1', workspace: 'default', topic: null, project_root: project, hint: JSON.parse(claimed.stdout).hint });
  const stopped = await watcher.result;
  assert.deepEqual([stopped.code, stopped.stdout, stopped.stderr], [0, '', ''], 'the old watcher exits silently');
  assert.ok(Date.now() - at < 3000, 'within one poll');
  // Review round 1: count once the 6 s bound has passed, then require that
  // nothing more arrives over the next 2 s.
  await new Promise(resolve => setTimeout(resolve, Math.max(0, at + 6500 - Date.now())));
  const settled = timed.filter(r => r.session === 'sp').length;
  await new Promise(resolve => setTimeout(resolve, 2000));
  assert.ok(timed.some(r => r.session === 'sp' && r.at > at), 'positive control: the old MCP polled after the claim until it noticed');
  assert.equal(timed.filter(r => r.session === 'sp').length, settled, 'the old MCP stopped polling within 6.5 s');
  assert.ok(old.changed.length >= 1, 'tools/list_changed reached the client');
  const refused = await old.client.callTool({ name: 'inbox', arguments: {} });
  assert.deepEqual([refused.isError, refused.structuredContent.error], [true, 'NOT_AGENT_SESSION']);
  assert.deepEqual((await old.client.listTools()).tools, []);
  const now = findProject(project);
  assert.deepEqual([now.options.claude_session_id, now.binding.session_id], ['agent-2', 'sp'], 'the broker session carries over');
  const fresh = await mcp(t, mcpEnv(s, f, 'agent-2', project));
  assert.ok((await fresh.client.listTools()).tools.length > 10);
  assert.equal((await fresh.client.callTool({ name: 'inbox', arguments: {} })).isError, undefined);
  const again = await claim({ claudeSession: 'agent-2', project }, { env: s.tmEnv(f) });
  assert.equal(again.status, 'already_agent');
});

test('no project file: idle, zero broker requests, idle line', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = gitDir(s.dir, 'project');
  const [start, stop, watcher] = await hooks(s, s.env(f), project, 'any');
  assert.equal(contextOf(start), 'team-messaging is idle in this project; run /team-messaging:bind or /team-messaging:join in the session that should be the agent');
  assert.deepEqual([stop.stdout, stop.stderr, watcher.stdout, watcher.stderr], ['', '', '', '']);
  const m = await mcp(t, mcpEnv(s, f, 'any', project));
  assert.deepEqual((await m.client.listTools()).tools, []);
  // Review round 1: an idle project has its own code, distinct from a non-agent session.
  assert.equal((await m.client.callTool({ name: 'inbox', arguments: {} })).structuredContent.error, 'PROJECT_NOT_BOUND');
  await new Promise(resolve => setTimeout(resolve, 500));
  assert.deepEqual(f.requests, []);
});

test('0.9 global binding: idle with the migration hint; --adopt-global keeps the session and clears global; a second adopt finds nothing', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), tokenFile = credentialScratch(s, f.url), env = { HOME: s.home };
  const a = gitDir(s.dir, 'a'), b = gitDir(s.dir, 'b');
  f.broker.heartbeat('bob', 'old-09', 'worker', 'default', null);
  const global = { broker_url: f.url, token_file: tokenFile, workspace: 'default', topic: '', session_id: 'old-09', mode: 'push', arm: 'bash' };
  const [start] = await hooks(s, s.env(f, { CLAUDE_PLUGIN_OPTION_WORKSPACE: 'default', CLAUDE_PLUGIN_OPTION_SESSION_ID: 'old-09' }), a, 'agent-a');
  assert.match(contextOf(start), /^team-messaging is idle in this project; .* 0\.9 binding to workspace default, .*\/team-messaging:bind --adopt-global/);
  assert.deepEqual(f.requests.filter(r => r.session === 'old-09'), [], 'the 0.9 binding is ignored');
  // Failure after the rename: the file is written, the global rewrite fails.
  const cli = claudeConfig(global); cli.fail = true;
  const failed = await bind({ adoptGlobal: true, claudeSession: 'agent-a', dataDir: s.data, project: a }, { env, spawn: cli.spawn });
  assert.equal(failed.adapters[0].status, 'failed');
  assert.deepEqual(findProject(a).binding, { workspace: 'default', topic: null, session_id: 'old-09' }); assert.equal(cli.inputs.workspace, 'default', 'global still full');
  // The rerun finds its own adopted file and finishes clearing the global binding.
  cli.fail = false;
  const adopted = await bind({ adoptGlobal: true, claudeSession: 'agent-a', dataDir: s.data, project: a }, { env, spawn: cli.spawn });
  assert.equal(adopted.adapters[0].status, 'adopted');
  assert.deepEqual(cli.writes, [{ ...global, workspace: '', topic: '', session_id: '' }], 'credential-only, mode and arm kept');
  await assert.rejects(bind({ adoptGlobal: true, claudeSession: 'agent-a', dataDir: s.data, project: a }, { env, spawn: cli.spawn }), { code: 'NOTHING_TO_ADOPT' });
  // The adopt marker refuses a second root even if a global binding reappears.
  await assert.rejects(bind({ adoptGlobal: true, claudeSession: 'agent-b', dataDir: s.data, project: b }, { env, spawn: claudeConfig(global).spawn }), error => error.code === 'PLUGIN_ALREADY_BOUND' && error.hint.includes(a));
  assert.equal(findProject(b).state, 'absent');
  // The adopted session works at once: the MCP server of the agent uses old-09.
  const m = await mcp(t, mcpEnv(s, f, 'agent-a', a));
  assert.equal((await m.client.callTool({ name: 'inbox', arguments: {} })).isError, undefined);
  assert.ok(requestsOf(f, 'old-09').length > 0);
  // An ended 0.9 session is refused and nothing is written.
  const ended = { ...global, session_id: 'ended-09' }, c = gitDir(s.dir, 'c'), other = pathJoin(s.dir, 'other-data');
  f.broker.heartbeat('bob', 'ended-09', 'worker', 'default', null); f.store.run("UPDATE agents SET ended_at=?, ended_reason='membership_ended' WHERE session_id='ended-09'", Date.now());
  await assert.rejects(bind({ adoptGlobal: true, claudeSession: 'agent-c', dataDir: other, project: c }, { env, spawn: claudeConfig(ended).spawn }), { code: 'SESSION_ENDED' });
  assert.equal(findProject(c).state, 'absent');
});

// The owner flow (P-16) against a real broker; `bob` is the owner.
function owner(t, f, s, fetcher = fetch) {
  const tokenFile = credentialScratch(s, f.url), cli = claudeConfig({ broker_url: f.url, token_file: tokenFile }), sent = [];
  const record = (url, options) => { sent.push(`${options.method} ${new URL(url).pathname}`); return fetcher(url, options); };
  return { cli, sent, tokenFile, run: (project, options = {}, deps = {}) => bind({ url: f.url, name: 'bob', claudeSession: 'owner-s', project, ...options }, { env: { HOME: s.home }, spawn: cli.spawn, fetch: record, ...deps }) };
}
test('owner bind creates workspace, topic and subscription, then writes the file; a rerun is skipped; a rerun after a failure at each step completes', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), o = owner(t, f, s), project = gitDir(s.dir, 'owned');
  const result = await o.run(project, { workspace: 'neww', topic: 'area', mode: 'queue' });
  assert.equal(result.adapters[0].status, 'registered');
  assert.deepEqual(o.sent, ['GET /workspaces', 'POST /workspaces', 'POST /topics', 'POST /topics/subscribe', 'GET /session/status']);
  assert.equal(f.store.get("SELECT role FROM workspace_members WHERE workspace='neww' AND user='bob'").role, 'owner');
  assert.equal(f.store.get("SELECT mode FROM topics WHERE workspace='neww' AND name='area'").mode, 'queue');
  assert.ok(f.store.get("SELECT 1 FROM topic_subscriptions WHERE workspace='neww' AND topic='area' AND user='bob'"));
  const bound = findProject(project);
  assert.deepEqual([bound.binding.workspace, bound.binding.topic, bound.options.claude_session_id], ['neww', 'area', 'owner-s']);
  assert.ok(f.requests.filter(r => r.path.startsWith('/topics')).every(r => /^claude-setup-[0-9a-f]{32}$/.test(r.session)), 'topic steps ran on a setup session');
  // AM-5: the written session heartbeats at topic scope.
  const m = await mcp(t, mcpEnv(s, f, 'owner-s', project));
  assert.equal((await m.client.callTool({ name: 'topics', arguments: {} })).isError, undefined);
  assert.deepEqual({ ...f.store.get('SELECT workspace, topic FROM agents WHERE session_id=?', bound.binding.session_id) }, { workspace: 'neww', topic: 'area' });
  o.sent.length = 0;
  assert.equal((await o.run(project, { workspace: 'neww', topic: 'area', mode: 'queue' })).adapters[0].status, 'skipped'); assert.deepEqual(o.sent, []);
  // Each step failing once: the rerun skips what exists and completes.
  for (const [i, step] of ['POST /workspaces', 'POST /topics', 'POST /topics/subscribe', 'GET /session/status'].entries()) {
    let armed = true;
    const flaky = owner(t, f, s, (url, options) => { if (armed && `${options.method} ${new URL(url).pathname}` === step) { armed = false; return Promise.reject(new Error('down')); } return fetch(url, options); });
    const dir = gitDir(s.dir, `retry-${i}`);
    await assert.rejects(flaky.run(dir, { workspace: `retry-${i}`, topic: 't' }), { code: step === 'GET /session/status' ? 'STATUS_UNAVAILABLE' : 'BIND_UNAVAILABLE' }, step);
    assert.equal(findProject(dir).state, 'absent', step);
    const retried = await flaky.run(dir, { workspace: `retry-${i}`, topic: 't' });
    assert.equal(retried.adapters[0].status, 'registered', step); assert.equal(findProject(dir).binding.workspace, `retry-${i}`);
  }
  // The global write failing: no project file; the rerun completes.
  const empty = owner(t, f, s); empty.cli.inputs.broker_url = ''; empty.cli.inputs.token_file = ''; empty.cli.fail = true;
  const dir = gitDir(s.dir, 'retry-global');
  assert.equal((await empty.run(dir, { workspace: 'retry-g' })).adapters[0].status, 'failed'); assert.equal(findProject(dir).state, 'absent');
  empty.cli.fail = false;
  assert.equal((await empty.run(dir, { workspace: 'retry-g' })).adapters[0].status, 'registered');
  assert.deepEqual(empty.cli.writes.at(-1), { broker_url: f.url, token_file: empty.tokenFile, workspace: '', topic: '', session_id: '', mode: 'poll', arm: 'rewake' });
});

test('bind into an existing owned workspace creates only the topic', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), o = owner(t, f, s), project = gitDir(s.dir, 'owned');
  const result = await o.run(project, { workspace: 'default', topic: 'fresh' });
  assert.equal(result.adapters[0].status, 'registered');
  assert.deepEqual(o.sent, ['GET /workspaces', 'POST /topics', 'POST /topics/subscribe', 'GET /session/status']);
  assert.equal(f.store.get("SELECT count(*) n FROM workspaces").n, 1);
});

test('an existing open topic is reused; closed gives TOPIC_CLOSED; a mode mismatch is refused', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), o = owner(t, f, s);
  await o.run(gitDir(s.dir, 'p1'), { workspace: 'default', topic: 'shared' });
  o.sent.length = 0;
  const reused = await o.run(gitDir(s.dir, 'p2'), { workspace: 'default', topic: 'shared', mode: 'broadcast' });
  assert.equal(reused.adapters[0].status, 'registered');
  assert.deepEqual(o.sent, ['GET /workspaces', 'POST /topics', 'GET /topics', 'POST /topics/subscribe', 'GET /session/status']);
  const p3 = gitDir(s.dir, 'p3');
  await assert.rejects(o.run(p3, { workspace: 'default', topic: 'shared', mode: 'queue' }), { code: 'TOPIC_MODE_MISMATCH' });
  f.store.run("UPDATE topics SET closed_at=?, closed_by_user='bob' WHERE workspace='default' AND name='shared'", Date.now());
  const p4 = gitDir(s.dir, 'p4');
  await assert.rejects(o.run(p4, { workspace: 'default', topic: 'shared' }), { code: 'TOPIC_CLOSED' });
  assert.deepEqual([findProject(p3).state, findProject(p4).state], ['absent', 'absent']);
});

test('a workspace owned by someone else is WORKSPACE_NOT_OWNED and nothing is written', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), o = owner(t, f, s), project = gitDir(s.dir, 'p');
  assert.equal((await fetch(f.url + '/workspaces', { method: 'POST', headers: { Authorization: `Bearer ${tokens.alice}`, 'Content-Type': 'application/json' }, body: '{"name":"alices"}' })).status, 201);
  await assert.rejects(o.run(project, { workspace: 'alices' }), error => error.code === 'WORKSPACE_NOT_OWNED' && error.hint.includes('invite'));
  assert.deepEqual(o.sent, ['GET /workspaces', 'POST /workspaces', 'GET /workspaces']);
  // AM-9: with a topic the caller would bind as a member, and is not one.
  await assert.rejects(o.run(project, { workspace: 'alices', topic: 't' }), { code: 'NOT_A_MEMBER' });
  assert.equal(findProject(project).state, 'absent'); assert.deepEqual(o.cli.writes, []);
  assert.equal(f.store.get("SELECT count(*) n FROM topics WHERE workspace='alices'").n, 0);
});

// AM-9 (review round 1): bind keeps 0.9's member path. A subscribed member
// binds another project to the topic without an invite or any owner step.
test('member bind: a subscribed topic binds without owner steps; an unsubscribed topic is NOT_SUBSCRIBED; a non-member is NOT_A_MEMBER', async t => {
  const f = await brokerFixture(t), s = pluginScratch(t), env = { HOME: s.home }, first = gitDir(s.dir, 'first');
  const cli = claudeConfig(), code = await invite(f);
  await join({ url: f.url, code, name: 'newbie', client: 'claude-plugin', claudeSession: 'agent-1', project: first }, { env, spawn: cli.spawn });
  const sent = [], record = (url, options) => { sent.push(`${options.method} ${new URL(url).pathname}`); return fetch(url, options); };
  const run = (project, options) => bind({ url: f.url, name: 'newbie', workspace: 'default', claudeSession: 'agent-2', project, ...options }, { env, spawn: cli.spawn, fetch: record });
  const second = gitDir(s.dir, 'second'), topics = () => f.store.get('SELECT count(*) n FROM topics').n, subscriptions = () => f.store.get("SELECT count(*) n FROM topic_subscriptions WHERE user='newbie'").n;
  const before = [topics(), subscriptions()];
  const result = await run(second, { topic: FIXTURE_TOPIC });
  assert.equal(result.adapters[0].status, 'registered');
  assert.deepEqual(sent, ['GET /workspaces', 'GET /session/status'], 'no workspace, topic or subscription step');
  const bound = findProject(second);
  assert.deepEqual([bound.binding.workspace, bound.binding.topic, bound.options.claude_session_id], ['default', FIXTURE_TOPIC, 'agent-2']);
  assert.notEqual(bound.binding.session_id, findProject(first).binding.session_id, 'each project has its own broker session');
  f.store.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('default','other','broadcast','alice',?)", Date.now());
  const third = gitDir(s.dir, 'third');
  await assert.rejects(run(third, { topic: 'other' }), error => error.code === 'NOT_SUBSCRIBED' && error.hint.includes('invite'));
  await assert.rejects(run(third, { topic: FIXTURE_TOPIC, mode: 'queue' }), { code: 'INVALID_TOPIC_MODE' });
  await assert.rejects(run(third, {}), { code: 'WORKSPACE_NOT_OWNED' }, 'a workspace-level binding needs the owner');
  assert.equal((await fetch(f.url + '/workspaces', { method: 'POST', headers: { Authorization: `Bearer ${tokens.alice}`, 'Content-Type': 'application/json' }, body: '{"name":"alices"}' })).status, 201);
  await assert.rejects(run(third, { workspace: 'alices', topic: 't' }), { code: 'NOT_A_MEMBER' });
  assert.equal(findProject(third).state, 'absent');
  assert.deepEqual([topics(), subscriptions()], [before[0] + 1, before[1]], 'only the test inserted a topic; no subscription was added');
});

test('a bound project refuses bind before any broker mutation', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), o = owner(t, f, s), project = gitDir(s.dir, 'p');
  bindProject(project, { url: f.url, claudeSession: 'owner-s' });
  for (const options of [{ workspace: 'other', topic: 't' }, { workspace: 'default', topic: 'x' }, { workspace: 'default', claudeSession: 'someone-else' }])
    await assert.rejects(o.run(project, options), error => error.code === 'PLUGIN_ALREADY_BOUND' && error.hint.includes(pathJoin(project, '.claude', 'settings.local.json')), JSON.stringify(options));
  assert.deepEqual(o.sent, []); assert.deepEqual(f.requests, []); assert.deepEqual(o.cli.writes, []);
  assert.equal(f.store.get("SELECT count(*) n FROM workspaces").n, 1);
});

test('a committed file with any ids stays inert and claim refuses it', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = gitDir(s.dir, 'cloned');
  for (const agent of ['agent-1', 'someone']) {
    // The attacker cannot compute the MAC without the user's token.
    const file = writeProjectFile(project, { version: 1, claude_session_id: agent, bindings: ['default::stolen-session'], mac: 'a'.repeat(64) });
    const [start, stop, watcher] = await hooks(s, s.env(f), project, 'agent-1');
    if (agent === 'agent-1') assert.match(contextOf(start), /^team-messaging: PROJECT_OPTIONS_UNTRUSTED: /);
    else assert.equal(contextOf(start), "team-messaging: this project's agent is Claude session someone; run /team-messaging:claim here to take over");
    assert.equal(watcher.stdout + watcher.stderr, '');
    if (agent === 'agent-1') assert.match(JSON.parse(stop.stdout).systemMessage, /^team-messaging: PROJECT_OPTIONS_UNTRUSTED: /);
    const m = await mcp(t, mcpEnv(s, f, 'agent-1', project));
    assert.deepEqual((await m.client.listTools()).tools, []);
    const before = readFileSync(file, 'utf8');
    await assert.rejects(claim({ claudeSession: 'agent-1', project }, { env: s.tmEnv(f) }), { code: 'PROJECT_OPTIONS_UNTRUSTED' });
    await assert.rejects(claim({ claudeSession: 'agent-2', project }, { env: s.tmEnv(f) }), { code: 'PROJECT_OPTIONS_UNTRUSTED' });
    assert.equal(readFileSync(file, 'utf8'), before, 'claim rewrote nothing');
  }
  await new Promise(resolve => setTimeout(resolve, 500));
  assert.deepEqual(f.requests, [], 'SESSION_OWNED is never even reached');
});

test('a file copied to another clone is untrusted', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), origin = gitDir(s.dir, 'origin'), clone = gitDir(s.dir, 'clone');
  const file = bindProject(origin, { url: f.url, claudeSession: 'agent-1' });
  mkdirSync(pathJoin(clone, '.claude'), { mode: 0o700 }); copyFileSync(file, pathJoin(clone, '.claude', 'settings.local.json'));
  const [start] = await hooks(s, s.env(f), clone, 'agent-1');
  assert.match(contextOf(start), /^team-messaging: PROJECT_OPTIONS_UNTRUSTED: /);
  const m = await mcp(t, mcpEnv(s, f, 'agent-1', clone));
  assert.deepEqual((await m.client.listTools()).tools, []);
  await new Promise(resolve => setTimeout(resolve, 500));
  assert.deepEqual(f.requests, []);
  // AM-4: editing any binding field or the agent breaks the MAC too.
  const options = JSON.parse(readFileSync(file, 'utf8')).pluginConfigs['team-messaging@team-messaging-local'].options;
  for (const edited of [{ bindings: ['other::shared-b'] }, { bindings: ['default:fixture:shared-b'] }, { bindings: ['default::stolen'] }, { claude_session_id: 'agent-2' }]) {
    writeProjectFile(origin, { ...options, ...edited });
    const [edit] = await hooks(s, s.env(f), origin, edited.claude_session_id ?? 'agent-1');
    assert.match(contextOf(edit), /^team-messaging: PROJECT_OPTIONS_UNTRUSTED: /, JSON.stringify(edited));
  }
  assert.deepEqual(f.requests, []);
});

test('a file with broker_url or token_file is invalid and no token leaves', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), attacker = await recorder(t), project = gitDir(s.dir, 'p');
  const good = projectOptions(tokens.bob, { root: project, url: f.url, claudeSession: 'agent-1', binding: { workspace: 'default', topic: null, session_id: 'shared-b' } });
  for (const extra of [{ broker_url: attacker.url }, { token_file: s.tokenFile }, { broker_url: attacker.url, token_file: credentialPath(attacker.url, 'bob', { env: { HOME: s.home } }) }]) {
    writeProjectFile(project, { ...good, ...extra });
    const results = await hooks(s, s.env(f), project, 'agent-1');
    assert.match(contextOf(results[0]), /^team-messaging: PROJECT_OPTIONS_INVALID: /, JSON.stringify(extra));
    for (const result of results) assert.ok(!(result.stdout + result.stderr).includes(tokens.bob));
    const m = await mcp(t, mcpEnv(s, f, 'agent-1', project));
    assert.deepEqual((await m.client.listTools()).tools, []);
  }
  assert.deepEqual(attacker.seen, []); assert.deepEqual(f.requests, []);
});

test('a file with two bindings or version 2 is UNSUPPORTED', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = gitDir(s.dir, 'p');
  const good = projectOptions(tokens.bob, { root: project, url: f.url, claudeSession: 'agent-1', binding: { workspace: 'default', topic: null, session_id: 'shared-b' } });
  for (const options of [{ ...good, bindings: ['default::a', 'w2:t:b'] }, { ...good, version: 2 }, { ...good, version: 2, new_field: 'x' }]) {
    writeProjectFile(project, options);
    assert.equal(findProject(project).code, 'PROJECT_OPTIONS_UNSUPPORTED', JSON.stringify(options));
    const [start] = await hooks(s, s.env(f), project, 'agent-1');
    assert.equal(contextOf(start), "team-messaging: PROJECT_OPTIONS_UNSUPPORTED: This project's team-messaging binding was written by a newer plugin; upgrade the plugin.");
  }
  for (const options of [{ ...good, version: '1' }, { ...good, bindings: [] }, { ...good, bindings: [{ workspace: 'default' }] }, { ...good, bindings: ['default:a'] }, { ...good, bindings: ['broker::s'] }, { ...good, bindings: ['default::broker'] }, { ...good, claude_session_id: 'a.b' }, { ...good, mac: 'x' }, { ...good, extra: 1 }, (({ mac, ...rest }) => rest)(good)]) {
    writeProjectFile(project, options);
    assert.equal(findProject(project).code, 'PROJECT_OPTIONS_INVALID', JSON.stringify(options));
  }
  await new Promise(resolve => setTimeout(resolve, 300));
  assert.deepEqual(f.requests, []);
});

// AM-3: Claude Code skips the whole settings.local.json for an array of
// objects, so every value the CLI writes is a string, a number or an array
// of strings.
test('the project options the CLI writes hold only strings, numbers and string arrays', t => {
  const s = pluginScratch(t), root = gitDir(s.dir, 'r');
  const options = projectOptions('t'.repeat(32), { root, url: 'https://b.example', claudeSession: 'a', binding: { workspace: 'w', topic: 'a/b', session_id: 's' } });
  assert.deepEqual(Object.keys(options), ['version', 'claude_session_id', 'bindings', 'mac']);
  for (const value of Object.values(options)) assert.ok(typeof value === 'string' || typeof value === 'number' || (Array.isArray(value) && value.every(item => typeof item === 'string')), JSON.stringify(value));
  assert.deepEqual(options.bindings, ['w:a/b:s']);
  assert.equal(options.mac, projectMac({ root, url: 'https://b.example', claudeSession: 'a', bindings: ['w:a/b:s'], token: 't'.repeat(32) }), 'the MAC matches the independent test implementation');
});

test('other-writable, foreign or symlinked files fail closed; 0664/0775 accepted; no key means absent', async t => {
  const s = pluginScratch(t), url = 'http://127.0.0.1:9';
  const fresh = name => { const dir = gitDir(s.dir, name); bindProject(dir, { url, claudeSession: 'a' }); return dir; };
  const state = dir => { const found = findProject(dir, { home: s.home }); return found.code ?? found.state; };
  const group = fresh('group'); chmodSync(pathJoin(group, '.claude', 'settings.local.json'), 0o664); chmodSync(pathJoin(group, '.claude'), 0o775); chmodSync(group, 0o775);
  assert.equal(state(group), 'found', 'umask 002 modes are accepted');
  const world = fresh('world'); chmodSync(pathJoin(world, '.claude', 'settings.local.json'), 0o666);
  assert.equal(state(world), 'PROJECT_OPTIONS_UNTRUSTED');
  const worldDir = fresh('world-dir'); chmodSync(pathJoin(worldDir, '.claude'), 0o777);
  assert.equal(state(worldDir), 'PROJECT_OPTIONS_UNTRUSTED');
  const parent = pathJoin(s.dir, 'open-parent'); mkdirSync(parent, { mode: 0o777 }); chmodSync(parent, 0o777);
  const below = gitDir(parent, 'p'); bindProject(below, { url, claudeSession: 'a' });
  assert.equal(state(below), 'PROJECT_OPTIONS_UNTRUSTED', 'an other-writable ancestor');
  const linked = fresh('linked'), real = pathJoin(s.dir, 'elsewhere.json');
  copyFileSync(pathJoin(linked, '.claude', 'settings.local.json'), real); rmSync(pathJoin(linked, '.claude', 'settings.local.json'));
  symlinkSync(real, pathJoin(linked, '.claude', 'settings.local.json'));
  assert.equal(state(linked), 'PROJECT_OPTIONS_UNTRUSTED');
  const linkedDir = gitDir(s.dir, 'linked-dir'), target = fresh('target');
  symlinkSync(pathJoin(target, '.claude'), pathJoin(linkedDir, '.claude'));
  assert.equal(state(linkedDir), 'PROJECT_OPTIONS_UNTRUSTED');
  // Without root no foreign file can be made here; seen by another uid, every
  // directory is foreign, so (AM-10) the walk ends before reading anything.
  const uid = process.getuid; process.getuid = () => uid() + 1;
  try { assert.equal(state(group), 'absent', 'nothing owned by another user is read'); } finally { process.getuid = uid; }
  const fifo = gitDir(s.dir, 'fifo'); mkdirSync(pathJoin(fifo, '.claude'));
  const made = spawnFifo(pathJoin(fifo, '.claude', 'settings.local.json'));
  if (made) assert.equal(state(fifo), 'PROJECT_OPTIONS_INVALID', 'a FIFO is not read and fails closed');
  const other = gitDir(s.dir, 'other');
  mkdirSync(pathJoin(other, '.claude')); writeFileSync(pathJoin(other, '.claude', 'settings.local.json'), JSON.stringify({ permissions: {}, pluginConfigs: { 'other@x': { options: {} } } }));
  assert.equal(state(other), 'absent', 'no key means absent');
  writeFileSync(pathJoin(other, '.claude', 'settings.local.json'), '{ not json');
  assert.equal(state(other), 'absent', 'bad JSON without our id is somebody else\'s');
  writeFileSync(pathJoin(other, '.claude', 'settings.local.json'), '{ "team-messaging@team-messaging-local": ');
  assert.equal(state(other), 'PROJECT_OPTIONS_INVALID', 'bad JSON naming our id fails closed');
  writeFileSync(pathJoin(other, '.claude', 'settings.local.json'), JSON.stringify({ pluginConfigs: { 'team-messaging@team-messaging-local': { options: { version: 1 } } } }));
  chmodSync(pathJoin(other, '.claude', 'settings.local.json'), 0o666);
  assert.equal(state(other), 'PROJECT_OPTIONS_UNTRUSTED', 'trust is checked once the key is present');
});
function spawnFifo(path) { return spawnSyncSafe('mkfifo', [path]); }
function spawnSyncSafe(command, args) { try { return execFileSync(command, args) !== undefined; } catch { return false; } }

// AM-10 (review round 1, S2): a keyed file in a shared ancestor, such as a
// 1777 directory another user can write to, leaves the project idle.
test('a keyed file in a 1777 ancestor is never read: the project stays idle, not untrusted', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), shared = pathJoin(s.dir, 'shared');
  bindProject(shared, { url: f.url, claudeSession: 'agent-1', git: false });
  chmodSync(shared, 0o1777);
  const project = pathJoin(shared, 'mine'); mkdirSync(project, { mode: 0o700 });
  assert.equal(findProject(project, { home: s.home }).state, 'absent');
  const [start, stop] = await hooks(s, s.env(f), project, 'agent-1');
  assert.match(contextOf(start), /^team-messaging is idle in this project; /);
  assert.equal(stop.stdout, '');
  chmodSync(shared, 0o700);
  assert.equal(findProject(project, { home: s.home }).state, 'found', 'positive control: the same file is found from a private ancestor');
  assert.deepEqual(f.requests, []);
});

// Review round 2 (AM-10 widened): a keyed file, or a .claude directory, that
// another user owns is skipped and the walk goes on; it cannot make the
// project untrusted. Without root no foreign file can be made, so lstat is
// wrapped to report another owner for chosen paths (node:module
// syncBuiltinESMExports updates the named import project-options.mjs uses).
test('a foreign-owned keyed file or .claude directory is skipped and the walk goes on; our own symlink still fails', t => {
  const s = pluginScratch(t), url = 'http://127.0.0.1:9', outer = pathJoin(s.dir, 'outer');
  bindProject(outer, { url, claudeSession: 'a', git: false });
  const repo = gitDir(outer, 'repo'), inner = pathJoin(repo, 'inner');
  writeProjectFile(inner, { version: 1 }, { git: false }); chmodSync(inner, 0o770);
  const real = fs.lstatSync, foreignPaths = new Set();
  fs.lstatSync = (path, ...rest) => { const stat = real(path, ...rest); if (stat && foreignPaths.has(String(path))) { const copy = Object.create(Object.getPrototypeOf(stat), Object.getOwnPropertyDescriptors(stat)); copy.uid = stat.uid + 1; return copy; } return stat; };
  syncBuiltinESMExports();
  try {
    assert.equal(findProject(inner, { home: s.home }).code, 'PROJECT_OPTIONS_INVALID', 'positive control: our own keyed file is read');
    for (const path of [pathJoin(inner, '.claude', 'settings.local.json'), pathJoin(inner, '.claude')]) {
      foreignPaths.clear(); foreignPaths.add(path);
      assert.equal(findProject(inner, { home: s.home }).state, 'absent', `${path}: skipped, and the walk stops at the git root`);
    }
    bindProject(repo, { url, claudeSession: 'a' });
    assert.equal(findProject(inner, { home: s.home }).root, repo, 'the walk went on to the next directory');
  } finally { fs.lstatSync = real; syncBuiltinESMExports(); }
  rmSync(pathJoin(inner, '.claude', 'settings.local.json'));
  symlinkSync(pathJoin(repo, '.claude', 'settings.local.json'), pathJoin(inner, '.claude', 'settings.local.json'));
  assert.equal(findProject(inner, { home: s.home }).code, 'PROJECT_OPTIONS_UNTRUSTED', 'our own symlink still fails');
});

test('the root walk stops at the git root', async t => {
  const s = pluginScratch(t), url = 'http://127.0.0.1:9';
  const outer = pathJoin(s.dir, 'outer'); bindProject(outer, { url, claudeSession: 'a' }); rmSync(pathJoin(outer, '.git'), { recursive: true });
  const repo = gitDir(outer, 'repo'), sub = pathJoin(repo, 'src', 'deep'); mkdirSync(sub, { recursive: true });
  assert.equal(findProject(sub, { home: s.home }).state, 'absent', 'the file above the git root is not used');
  bindProject(repo, { url, claudeSession: 'a' });
  const found = findProject(sub, { home: s.home });
  assert.deepEqual([found.state, found.root], ['found', repo], 'found from a subdirectory, rooted at the git root');
  // Without .git the walk stops at HOME.
  const home = pathJoin(s.dir, 'h'), inside = pathJoin(home, 'work', 'p'); mkdirSync(inside, { recursive: true });
  bindProject(s.dir, { url, claudeSession: 'a', git: false });
  assert.equal(findProject(inside, { home }).state, 'absent', 'nothing above HOME');
  bindProject(home, { url, claudeSession: 'a', git: false });
  assert.equal(findProject(inside, { home }).root, home, 'HOME itself is checked');
});

// P-12: where a new file goes, and how it is written.
test('bind and join write at the git root inside HOME, else the project directory, keeping other settings and the mode', t => {
  const s = pluginScratch(t), home = pathJoin(s.dir, 'h'), repo = gitDir(home, 'repo'), sub = pathJoin(repo, 'sub'), nogit = pathJoin(home, 'nogit'), outside = gitDir(s.dir, 'outside'), deep = pathJoin(outside, 'deep');
  for (const dir of [sub, nogit, deep]) mkdirSync(dir, { recursive: true });
  assert.deepEqual([projectTarget(sub, { home }), projectTarget(nogit, { home }), projectTarget(deep, { home }), projectTarget(deep, {})], [repo, nogit, deep, deep]);
  const options = projectOptions(tokens.bob, { root: repo, url: 'http://127.0.0.1:9', claudeSession: 'a', binding: { workspace: 'default', topic: null, session_id: 's' } });
  const file = writeProjectFile(repo, {}, { other: { permissions: { allow: ['Bash(ls)'] } }, mode: 0o640 });
  writeProjectOptions(repo, options);
  const written = readFileSync(file, 'utf8');
  assert.deepEqual(JSON.parse(written), { permissions: { allow: ['Bash(ls)'] }, pluginConfigs: { 'team-messaging@team-messaging-local': { options } } });
  assert.ok(written.startsWith('{\n  "permissions"'), '2-space indent'); assert.equal(statSync(file).mode & 0o777, 0o640, 'the mode is kept');
  assert.equal(findProject(sub, { home }).state, 'found');
  writeProjectOptions(nogit, options);
  assert.deepEqual([statSync(pathJoin(nogit, '.claude')).mode & 0o777, statSync(pathJoin(nogit, '.claude', 'settings.local.json')).mode & 0o777], [0o700, 0o600]);
  writeFileSync(file, '{ broken');
  assert.throws(() => writeProjectOptions(repo, options), { code: 'PROJECT_OPTIONS_INVALID' }); assert.equal(readFileSync(file, 'utf8'), '{ broken', 'invalid JSON is never touched');
  rmSync(file); symlinkSync(pathJoin(nogit, '.claude', 'settings.local.json'), file);
  assert.throws(() => writeProjectOptions(repo, options), { code: 'PROJECT_OPTIONS_UNTRUSTED' });
  const locked = gitDir(s.dir, 'locked'); chmodSync(locked, 0o500);
  try { assert.throws(() => writeProjectOptions(locked, options), { code: 'PROJECT_WRITE_FAILED' }); } finally { chmodSync(locked, 0o700); }
});

test('MCP without CLAUDE_CODE_SESSION_ID is inert', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = gitDir(s.dir, 'p');
  bindProject(project, { url: f.url, claudeSession: 'agent-1' });
  for (const id of [undefined, '', 'bad id', '${CLAUDE_CODE_SESSION_ID}']) {
    const { CLAUDE_CODE_SESSION_ID: _drop, ...env } = mcpEnv(s, f, 'agent-1', project);
    const m = await mcp(t, id === undefined ? env : { ...env, CLAUDE_CODE_SESSION_ID: id });
    assert.deepEqual((await m.client.listTools()).tools, []);
    assert.deepEqual(JSON.parse(m.logs.join('').trim()), { event: 'plugin_inert', state: 'inert', code: 'SESSION_ID_UNKNOWN' }, String(id));
  }
  await new Promise(resolve => setTimeout(resolve, 500));
  assert.deepEqual(f.requests, []);
});

test('status reports agent, root and workspace', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = gitDir(s.dir, 'p'), unbound = gitDir(s.dir, 'u');
  bindProject(project, { url: f.url, claudeSession: 'agent-1', session: 'status-s' });
  const status = (session, dir) => runChannel('cli.mjs', ['status', '--claude-session', session, '--project', dir], s.tmEnv(f)).result;
  const active = await status('agent-1', project), inert = await status('other', project), idle = await status('agent-1', unbound);
  assert.deepEqual(JSON.parse(active.stdout), { state: 'unregistered', agent: 'agent-1', project_root: project, workspace: 'default', topic: null });
  assert.deepEqual(JSON.parse(inert.stdout), { state: 'inert', agent: 'agent-1', project_root: project, workspace: 'default', topic: null });
  assert.deepEqual(JSON.parse(idle.stdout), { state: 'idle', agent: null, project_root: null, workspace: null, topic: null });
  assert.deepEqual(requestsOf(f, 'status-s').map(r => r.path), ['/session/status'], 'only the agent probes the broker');
  // The skill form: the project comes from the SessionStart record.
  const recorded = await runChannel('plugin-session-start.mjs', ['--project-dir', project, '--data-dir', s.data], s.env(f), hookInput('SessionStart', 'agent-1')).result;
  assert.equal(recorded.code, 0);
  const viaRecord = await runChannel('cli.mjs', ['status', '--claude-session', 'agent-1', '--data-dir', s.data], s.tmEnv(f)).result;
  assert.equal(JSON.parse(viaRecord.stdout).project_root, project);
  const unknown = await runChannel('cli.mjs', ['status', '--claude-session', 'never-started', '--data-dir', s.data], s.tmEnv(f)).result;
  assert.equal(JSON.parse(unknown.stderr).error, 'PROJECT_UNKNOWN');
});

// Review round 1: one record per (Claude session, project) pair; the newest
// wins, records older than 30 days are pruned on write, the current one never.
test('session records are kept per session and project, the newest wins, and old ones are pruned on write', t => {
  const s = pluginScratch(t), home = { home: s.home }, state = pathJoin(s.data, 'state'), day = 24 * 3600 * 1000;
  writeSessionRecord(s.data, 'sess', '/p/one', { ...home, now: Date.now() - 2000 });
  writeSessionRecord(s.data, 'sess', '/p/two', home);
  writeSessionRecord(s.data, 'other', '/p/three', home);
  const records = () => readdirSync(state).filter(name => name.endsWith('.session.json')).sort();
  assert.equal(records().length, 3, 'one file per pair');
  assert.equal(readSessionRecord(s.data, 'sess', home), '/p/two', 'the newest record of a session wins');
  assert.equal(readSessionRecord(s.data, 'other', home), '/p/three'); assert.equal(readSessionRecord(s.data, 'none', home), null);
  const old = records().find(name => name.startsWith('other.')), ago = (Date.now() - 31 * day) / 1000;
  utimesSync(pathJoin(state, old), ago, ago);
  writeSessionRecord(s.data, 'sess', '/p/two', home);
  assert.ok(records().includes(old), 'review round 2: an old mtime with a fresh `at` is kept');
  writeSessionRecord(s.data, 'other', '/p/three', { ...home, now: Date.now() - 31 * day }); utimesSync(pathJoin(state, old), ago, ago);
  // Review round 2: a record rewritten between the checks and the unlink is kept.
  writeSessionRecord(s.data, 'sess', '/p/two', { ...home, beforeUnlink: entry => { const fresh = pathJoin(state, `${entry}.new`); writeFileSync(fresh, readFileSync(pathJoin(state, entry))); renameSync(fresh, pathJoin(state, entry)); } });
  assert.ok(records().includes(old), 'a replaced inode is not unlinked');
  utimesSync(pathJoin(state, old), ago, ago);
  writeSessionRecord(s.data, 'sess', '/p/two', home);
  assert.ok(!records().includes(old), 'a 31-day-old record, by mtime and `at`, is pruned'); assert.equal(records().length, 2);
  // Far in the future every other record is stale, but the current one stays.
  writeSessionRecord(s.data, 'late', '/p/four', { ...home, now: Date.now() + 60 * day });
  assert.deepEqual(records().map(name => name.split('.')[0]), ['late']);
  assert.equal(readSessionRecord(s.data, 'late', home), '/p/four');
});

async function invite(f, workspace = 'default') {
  prepareTopicFixture(f.broker, workspace);
  const response = await fetch(f.url + '/invites', { method: 'POST', headers: { Authorization: `Bearer ${tokens.alice}`, 'X-Session-Id': `issuer-${workspace}`, 'X-Workspace': workspace, 'Content-Type': 'application/json' }, body: JSON.stringify({ topics: [FIXTURE_TOPIC] }) });
  assert.equal(response.status, 201);
  return (await response.json()).code;
}
const unredeemed = (f, code) => f.store.get('SELECT redeemed_at FROM invites WHERE id=?', code.slice(4, 36)).redeemed_at === null;

test('a credential-only global does not block; a credential for another broker refuses before the invite', async t => {
  const f = await brokerFixture(t), s = pluginScratch(t), env = { HOME: s.home }, path = credentialPath(f.url, 'newbie', { env });
  const first = claudeConfig(), code = await invite(f), p1 = gitDir(s.dir, 'p1');
  const joined = await join({ url: f.url, code, name: 'newbie', client: 'claude-plugin', claudeSession: 'agent-1', project: p1 }, { env, spawn: first.spawn });
  assert.equal(joined.adapters[0].status, 'registered');
  assert.deepEqual(first.inputs, { broker_url: f.url, token_file: path, workspace: '', topic: '', session_id: '', mode: 'poll', arm: 'rewake' });
  assert.equal(findProject(p1).binding.topic, FIXTURE_TOPIC);
  // The credential-only global is this credential: it does not block a second project.
  const code2 = await invite(f), p2 = gitDir(s.dir, 'p2'), writes = first.writes.length;
  const second = await join({ url: f.url, code: code2, name: 'newbie', client: 'claude-plugin', claudeSession: 'agent-2', project: p2 }, { env, spawn: first.spawn });
  assert.equal(second.adapters[0].status, 'registered'); assert.equal(first.writes.length, writes, 'nothing written globally');
  // A credential for another broker refuses before the invite.
  const code3 = await invite(f), p3 = gitDir(s.dir, 'p3'), joins = f.requests.filter(r => r.path === '/join').length;
  const other = claudeConfig({ broker_url: 'https://other.example', token_file: credentialPath('https://other.example', 'newbie', { env }) });
  await assert.rejects(join({ url: f.url, code: code3, name: 'newbie', client: 'claude-plugin', claudeSession: 'agent-3', project: p3 }, { env, spawn: other.spawn }), error => error.code === 'PLUGIN_ALREADY_BOUND' && error.hint.includes('not used'));
  assert.ok(unredeemed(f, code3)); assert.equal(f.requests.filter(r => r.path === '/join').length, joins); assert.equal(findProject(p3).state, 'absent');
});

test('join on a bound project keeps the invite', async t => {
  const f = await brokerFixture(t), s = pluginScratch(t), env = { HOME: s.home }, project = gitDir(s.dir, 'p');
  bindProject(project, { url: f.url, claudeSession: 'agent-1' });
  const code = await invite(f), cli = claudeConfig();
  for (const claudeSession of ['agent-1', 'agent-2'])
    await assert.rejects(join({ url: f.url, code, name: 'newbie', client: 'claude-plugin', claudeSession, project }, { env, spawn: cli.spawn }), error => error.code === 'PLUGIN_ALREADY_BOUND' && error.hint.includes('not used') && error.hint.includes(project));
  const copied = gitDir(s.dir, 'copied'); mkdirSync(pathJoin(copied, '.claude')); copyFileSync(pathJoin(project, '.claude', 'settings.local.json'), pathJoin(copied, '.claude', 'settings.local.json'));
  chmodSync(pathJoin(copied, '.claude', 'settings.local.json'), 0o646);
  await assert.rejects(join({ url: f.url, code, name: 'newbie', client: 'claude-plugin', claudeSession: 'agent-1', project: copied }, { env, spawn: cli.spawn }), { code: 'PROJECT_OPTIONS_UNTRUSTED' });
  assert.ok(unredeemed(f, code)); assert.equal(f.requests.filter(r => r.path === '/join').length, 0); assert.deepEqual(cli.writes, []);
  // Review round 1: a binding written by another process after the redeem
  // is not replaced, and the refusal says the invite was used.
  const racing = gitDir(s.dir, 'racing'), raced = claudeConfig(), spawn = raced.spawn;
  raced.spawn = (command, args, options) => { const result = spawn(command, args, options); if (args.includes('--values-stdin')) bindProject(racing, { url: f.url, claudeSession: 'other' }); return result; };
  await assert.rejects(join({ url: f.url, code, name: 'newbie', client: 'claude-plugin', claudeSession: 'agent-1', project: racing }, { env, spawn: raced.spawn }), error => error.code === 'PLUGIN_ALREADY_BOUND' && error.hint.endsWith(' The invite was used.'));
  assert.equal(unredeemed(f, code), false); assert.equal(findProject(racing).options.claude_session_id, 'other', 'the other binding is kept');
});
