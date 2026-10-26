// Project-scoped sharing A2: the team-message window. The scope hook runs as a
// real subprocess through runChannel, so on a tree without
// channel/scope-hook.mjs each test fails on the missing file rather than the
// whole file failing to load (plan B1). The window marker is written here
// independently of channel/scope-window.mjs, which pins its format: the file
// state/scope-<sha256(realpath root)>.json under the plugin data directory (FD-1).
import { topicHeartbeat } from '../../test-support/topic-fixture.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import http from 'node:http';
import { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, realpathSync, rmSync, statSync, symlinkSync, utimesSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { bindProject, brokerFixture, eventually, pluginScratch, runChannel, session, writeProjectFile } from './helpers.mjs';

const OFFLINE = { url: 'http://127.0.0.1:9' };
// Review r1 C1: every denial names the way out (checked in decision()).
const WAY_OUT_START = ' Ack or answer the pending team messages; strict mode ends at the end of the turn once none are pending, or within 2 h. To end it now, run ';
const FILE_REASON = 'While a team message is being handled, file tools stay inside the project root.';
const OTHER_REASON = 'While a team message is being handled, only file reads inside the project root and team-messaging tools are allowed.';
const SCOPE_LINE = 'team-messaging: share only project content and project-relative paths, nothing about this machine or outside the root.';
const OURS = { source: 'plugin', name: 'plugin:team-messaging:team-messaging' };
const TOOL = name => `mcp__plugin_team-messaging_team-messaging__${name}`;

const markerPath = (s, root) => join(s.data, 'state', `scope-${createHash('sha256').update(realpathSync(root)).digest('hex')}.json`);
function openMarker(s, root, { since = Date.now(), opened = Date.now(), session: agent = 'agent-k', markerRoot } = {}) {
  mkdirSync(join(s.data, 'state'), { recursive: true, mode: 0o700 });
  writeFileSync(markerPath(s, root), JSON.stringify({ v: 1, since, opened, root: markerRoot === undefined ? realpathSync(root) : markerRoot, claude_session_id: agent }), { mode: 0o600 });
}
const marker = (s, root) => JSON.parse(readFileSync(markerPath(s, root), 'utf8'));
const transcript = s => join(s.home, '.claude', 'projects', 'p', 'agent-k.jsonl');
const args = (s, project, event = 'pre') => ['--event', event, '--project-dir', project, '--data-dir', s.data];
const hookEnv = (s, extra = {}) => ({ HOME: s.home, TMPDIR: process.env.TMPDIR, ...extra });
function pre(s, project, tool_name, tool_input = {}, { extra = {}, env = {}, sessionId = 'agent-k' } = {}) {
  return runChannel('scope-hook.mjs', args(s, project), hookEnv(s, env), JSON.stringify({ session_id: sessionId, transcript_path: transcript(s), cwd: project, permission_mode: 'default', hook_event_name: 'PreToolUse', tool_name, tool_input, tool_use_id: 'toolu_1', ...extra })).result;
}
// Review r2 CC4: the way out names paths, so it is checked on its own and
// cut off here; the decision is the fixed text before it.
function decision(result) {
  assert.equal(result.code, 0, result.stderr);
  if (result.stdout === '') return 'none';
  const output = JSON.parse(result.stdout).hookSpecificOutput;
  assert.equal(output.hookEventName, 'PreToolUse');
  if (output.permissionDecision !== 'deny') return output.permissionDecision;
  const reason = output.permissionDecisionReason, cut = reason.indexOf(WAY_OUT_START);
  assert.ok(cut > 0, `the denial names the way out: ${reason}`);
  return `deny: ${reason.slice(0, cut)}`;
}
const stop = (s, project, env, event = 'stop', sessionId = 'agent-k') => runChannel('scope-hook.mjs', args(s, project, event), env, JSON.stringify({ session_id: sessionId, transcript_path: transcript(s), cwd: project, hook_event_name: event === 'stop' ? 'Stop' : 'StopFailure', stop_hook_active: false })).result;

// The bundled entrypoint over real stdio, as the plugin's .mcp.json starts it.
async function agent(t, f, s, key, { binding, env = {}, data = s.data } = {}) {
  const client = new Client({ name: 'scope-hook-test', version: '1.0.0' }), notifications = [], logs = [];
  const base = { ...s.tmEnv(f), TM_MODE: '', TM_ARM: '', CLAUDE_CODE_SESSION_ID: key, CLAUDE_PROJECT_DIR: s.project(f, key, binding), CHANNEL_POLL_MS: '100', ...env };
  if (data === undefined) delete base.CLAUDE_PLUGIN_DATA; else base.CLAUDE_PLUGIN_DATA = data;
  client.fallbackNotificationHandler = async notification => { if (notification.method === 'notifications/claude/channel') notifications.push({ ...notification.params, markerSeen: existsSync(markerPath(s, s.project(f, key, binding))) }); };
  const transport = new StdioClientTransport({ command: process.execPath, args: [fileURLToPath(new URL('../plugin-main.mjs', import.meta.url))], env: base, stderr: 'pipe' });
  transport.stderr.on('data', chunk => logs.push(chunk.toString()));
  f.clients.push(client);
  await client.connect(transport);
  return { client, notifications, logs, project: s.project(f, key, binding), call: (name, input = {}) => client.callTool({ name, arguments: input }, undefined, { timeout: 5000 }) };
}

test('outside a window nothing is blocked', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  for (const [tool, input] of [['Bash', { command: 'cat /etc/passwd' }], ['WebFetch', { url: 'https://example.com', prompt: 'x' }], ['Read', { file_path: '/etc/passwd' }]]) {
    const result = await pre(s, project, tool, input);
    assert.equal(decision(result), 'none', tool); assert.equal(result.stderr, '');
  }
  assert.equal(existsSync(markerPath(s, project)), false);
});

test('the human\'s own send, ask and invite open no marker; an inbox returning a teammate message does, before the result arrives', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), a = await agent(t, f, s, 'scope-own', { binding: { topic: 'fixture', session: 'own-b' } });
  const path = markerPath(s, a.project);
  for (const [name, input] of [['send', { to: 'user:alice', body: 'hello' }], ['ask', { to: 'user:alice', body: 'a question?' }], ['invite', { topics: ['fixture'] }], ['topics', {}], ['members', {}]]) {
    const result = await a.call(name, input);
    assert.equal(result.isError, undefined, `${name}: ${JSON.stringify(result.structuredContent)}`);
    assert.equal(existsSync(path), false, `${name} opened the window`);
  }
  // An inbox holding only the human's own traffic opens nothing either.
  assert.equal((await a.call('inbox')).isError, undefined); assert.equal(existsSync(path), false);
  topicHeartbeat(f.broker, 'alice', 'a');
  const sent = f.broker.send('alice', 'a', { to: 'user:bob', body: 'teammate text' });
  const inbox = await a.call('inbox');
  // The marker is written before the result is returned, so it exists now.
  assert.equal(existsSync(path), true);
  assert.deepEqual(inbox.structuredContent.messages.map(message => message.id), [sent.id]);
  const written = marker(s, a.project);
  assert.deepEqual([written.v, written.root, written.claude_session_id, written.since <= written.opened], [1, realpathSync(a.project), 'scope-own', true]);
  assert.equal(statSync(path).mode & 0o777, 0o600);
});

test('ack and claim open; a name-only topics call does not', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), a = await agent(t, f, s, 'scope-ack', { binding: { topic: 'fixture', session: 'ack-b' } });
  topicHeartbeat(f.broker, 'alice', 'a');
  const sent = f.broker.send('alice', 'a', { to: 'user:bob', body: 'ack me' });
  assert.equal((await a.call('topics')).isError, undefined);
  assert.equal(existsSync(markerPath(s, a.project)), false, 'topics is name-only (R-9)');
  const acked = await a.call('ack', { id: sent.id });
  assert.equal(acked.isError, undefined, JSON.stringify(acked.structuredContent));
  assert.equal(existsSync(markerPath(s, a.project)), true, 'ack opens');
  // Claim: a queue offer from a teammate to a topic-bound agent.
  const topic = 'queue/Build', owner = await session(f, 'bob', 'queue-owner', { CHANNEL_TOPIC: undefined });
  assert.equal((await owner.call('create_topic', { name: topic, mode: 'queue' })).isError, undefined);
  // Subscriptions are per user, so bob's agent bound to the topic starts subscribed.
  assert.equal((await owner.call('subscribe', { name: topic })).isError, undefined);
  // alice is a member, not the owner, so her session is bound to the topic.
  const alice = await session(f, 'alice', 'queue-a', { CHANNEL_TOPIC: topic });
  const q = await agent(t, f, s, 'scope-claim', { binding: { topic, session: 'claim-b' } });
  const offer = (await alice.call('send', { to: 'topic:' + topic, body: 'work item' })).structuredContent;
  assert.ok(offer.id, JSON.stringify(offer));
  assert.equal(existsSync(markerPath(s, q.project)), false);
  const claimed = await q.call('claim', { id: offer.id });
  assert.equal(claimed.isError, undefined, JSON.stringify(claimed.structuredContent));
  assert.equal(existsSync(markerPath(s, q.project)), true, 'claim opens');
});

test('a failed window write returns SCOPE_WINDOW_UNAVAILABLE and no bodies', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  topicHeartbeat(f.broker, 'alice', 'a');
  f.broker.send('alice', 'a', { to: 'user:bob', body: 'SECRET-BODY-TEXT' });
  // FA-6: a state directory with group or other bits is refused by stateDir
  // (INSECURE_CREDENTIAL_DIR) even when the tests run as root.
  mkdirSync(join(s.data, 'state'), { recursive: true, mode: 0o700 }); chmodSync(join(s.data, 'state'), 0o755);
  const cases = [['insecure state', s.data], ['unset CLAUDE_PLUGIN_DATA', undefined], ['relative CLAUDE_PLUGIN_DATA', 'relative/data']];
  for (const [label, data] of cases) {
    const a = await agent(t, f, s, `fail-${cases.findIndex(entry => entry[0] === label)}`, { binding: { topic: 'fixture', session: `fail-${label.length}` }, data });
    const result = await a.call('inbox');
    assert.equal(result.isError, true, label);
    assert.equal(result.structuredContent.error, 'SCOPE_WINDOW_UNAVAILABLE', label);
    assert.ok(!result.content[0].text.includes('SECRET-BODY-TEXT'), `${label}: no body`);
  }
});

test('in the window Bash, WebFetch, WebSearch, Agent, LSP and another MCP server are denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  for (const [tool, input, extra] of [['Bash', { command: 'ls' }], ['WebFetch', { url: 'https://example.com', prompt: 'x' }], ['WebSearch', { query: 'x' }], ['Agent', { prompt: 'read /etc/hosts', subagent_type: 'general-purpose' }],
    ['Task', { prompt: 'x' }], ['LSP', { operation: 'hover' }], ['mcp__other__read', { path: '/etc/hosts' }, { mcp_server: { source: 'user', name: 'other' } }]])
    assert.equal(decision(await pre(s, project, tool, input, { extra })), `deny: ${OTHER_REASON}`, tool);
});

test('in the window Read of /etc/passwd, ~/.bashrc and ../other-repo/x is denied; Read of ./README.md and Glob/Grep inside the root are allowed; a symlink out of the root is denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  writeFileSync(join(project, 'README.md'), '# project\n');
  mkdirSync(join(project, 'src'));
  symlinkSync('/etc', join(project, 'etc-link'));
  for (const file of ['/etc/passwd', join(s.home, '.bashrc'), join(project, '..', 'other-repo', 'x'), join(project, 'etc-link', 'passwd')])
    assert.equal(decision(await pre(s, project, 'Read', { file_path: file })), `deny: ${FILE_REASON}`, file);
  for (const [tool, input] of [['Read', { file_path: join(project, 'README.md') }], ['Read', { file_path: join(project, 'src', 'new.mjs') }], ['Glob', { pattern: '**/*.mjs', path: project }], ['Glob', { pattern: '**/*.md' }],
    ['Grep', { pattern: 'project', path: 'src' }], ['Grep', { pattern: 'x', glob: '*.md' }]])
    assert.equal(decision(await pre(s, project, tool, input)), 'none', `${tool} ${JSON.stringify(input)}`);
  for (const [tool, input] of [['Glob', { pattern: '/etc/*' }], ['Glob', { pattern: '../other/**' }], ['Glob', { pattern: '*', path: '/etc' }], ['Grep', { pattern: 'root', path: '/etc' }], ['Grep', { pattern: 'x', glob: '../../*' }], ['Grep', { pattern: 'x', path: '..' }]])
    assert.equal(decision(await pre(s, project, tool, input)), `deny: ${FILE_REASON}`, `${tool} ${JSON.stringify(input)}`);
  // D-14: a root of HOME or / is recorded as null and every file read is denied.
  openMarker(s, project, { markerRoot: null });
  assert.equal(decision(await pre(s, project, 'Read', { file_path: join(project, 'README.md') })), `deny: ${FILE_REASON}`);
});

test('Write, Edit and NotebookEdit inside the root are denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  for (const [tool, input] of [['Write', { file_path: join(project, 'a.txt'), content: 'x' }], ['Edit', { file_path: join(project, 'a.txt'), old_string: 'x', new_string: 'y' }], ['NotebookEdit', { notebook_path: join(project, 'a.ipynb'), new_source: 'x' }]])
    assert.equal(decision(await pre(s, project, tool, input)), `deny: ${OTHER_REASON}`, tool);
  for (const tool of ['ToolSearch', 'TodoWrite', 'AskUserQuestion', 'EnterPlanMode', 'ExitPlanMode']) assert.equal(decision(await pre(s, project, tool, {})), 'none', tool);
});

test('our tools pass only with plugin provenance; an absent or foreign mcp_server is denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  assert.equal(decision(await pre(s, project, TOOL('send'), { to: 'user:alice', body: 'x' }, { extra: { mcp_server: OURS } })), 'none');
  for (const mcp_server of [undefined, { source: 'user', name: OURS.name }, { source: 'plugin', name: 'plugin:other:team-messaging' }, { source: 'project', name: 'team-messaging' }])
    assert.equal(decision(await pre(s, project, TOOL('send'), { to: 'user:alice', body: 'x' }, { extra: mcp_server ? { mcp_server } : {} })), `deny: ${OTHER_REASON}`, JSON.stringify(mcp_server));
  assert.equal(decision(await pre(s, project, 'mcp__team-messaging__send', {}, { extra: { mcp_server: OURS } })), `deny: ${OTHER_REASON}`, 'the tool name must be ours too');
});

test('tool-results: a fresh file passes, a file older than since is denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k'), since = Date.now() - 10000;
  openMarker(s, project, { since });
  const results = join(dirname(transcript(s)), 'agent-k', 'tool-results');
  mkdirSync(results, { recursive: true, mode: 0o700 });
  writeFileSync(join(results, 'fresh.txt'), 'x'); writeFileSync(join(results, 'old.txt'), 'x');
  const old = new Date(since - 60000); utimesSync(join(results, 'old.txt'), old, old);
  assert.equal(decision(await pre(s, project, 'Read', { file_path: join(results, 'fresh.txt') })), 'none');
  assert.equal(decision(await pre(s, project, 'Read', { file_path: join(results, 'old.txt') })), `deny: ${FILE_REASON}`);
  // Another session's results are not this session's.
  const other = join(dirname(transcript(s)), 'other', 'tool-results'); mkdirSync(other, { recursive: true }); writeFileSync(join(other, 'f.txt'), 'x');
  assert.equal(decision(await pre(s, project, 'Read', { file_path: join(other, 'f.txt') })), `deny: ${FILE_REASON}`);
});

test('arm=bash: the exact watcher command passes; near misses are denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  const exact = `TM_BROKER_URL='http://127.0.0.1:8091' TM_TOKEN_FILE='${s.home}/.config/team-messaging/credentials/0123456789abcdef-bob.token' team-messaging-watch --mode bash --session-key 'agent-k' --claude-session 'agent-k' --data-dir '${s.data}'`;
  assert.equal(decision(await pre(s, project, 'Bash', { command: exact, run_in_background: true })), 'none');
  for (const [label, input] of [['appended command', { command: exact + '; cat /etc/passwd', run_in_background: true }], ['another data dir', { command: exact.replace(`'${s.data}'`, `'${s.data}x'`), run_in_background: true }],
    ['foreground', { command: exact, run_in_background: false }], ['no background flag', { command: exact }], ['changed quote', { command: exact.replace(`'agent-k' --claude`, `"agent-k" --claude`), run_in_background: true }],
    ['another session', { command: exact.replaceAll('agent-k', 'agent-x'), run_in_background: true }]])
    assert.equal(decision(await pre(s, project, 'Bash', input)), `deny: ${OTHER_REASON}`, label);
});

test('Stop and StopFailure close the window; an expired marker closes', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = s.project(f, 'agent-k');
  for (const event of ['stop', 'stop-failure']) {
    openMarker(s, project);
    const result = await stop(s, project, s.env(f), event);
    assert.deepEqual([result.code, result.stdout], [0, ''], result.stderr);
    assert.equal(existsSync(markerPath(s, project)), false, event);
  }
  openMarker(s, project, { since: Date.now() - 2 * 3600 * 1000 - 1000, opened: Date.now() - 2 * 3600 * 1000 - 1000 });
  assert.equal(decision(await pre(s, project, 'Bash', { command: 'ls' })), 'none', 'an expired marker is closed');
  assert.equal(existsSync(markerPath(s, project)), false, 'and unlinked');
});

// Review r1 C1 (replaces D-13's refresh): the window is capped 2 h after
// `since`; no decision in it rewrites the marker.
test('C1: the 2 h cap holds under a stream of decisions, and team-messaging scope-close ends the window', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project, { since: Date.now() - 3600 * 1000, opened: Date.now() - 3600 * 1000 });
  const before = readFileSync(markerPath(s, project), 'utf8');
  for (const [tool, input] of [['Bash', { command: 'ls' }], ['Read', { file_path: join(project, 'README.md') }], ['WebFetch', { url: 'https://example.com' }]]) await pre(s, project, tool, input);
  assert.equal(readFileSync(markerPath(s, project), 'utf8'), before, 'no decision moves since or opened');
  // A marker refreshed by an old r0 hook (opened now) still ends 2 h after since.
  openMarker(s, project, { since: Date.now() - 2 * 3600 * 1000 - 1000, opened: Date.now() });
  assert.equal(decision(await pre(s, project, 'Bash', { command: 'ls' })), 'none');
  assert.equal(existsSync(markerPath(s, project)), false);
  // The human's CLI, from a terminal in the project, with the plugin data dir.
  const cli = (argv, cwd, env = hookEnv(s)) => spawnSync(process.execPath, [fileURLToPath(new URL('../cli.mjs', import.meta.url)), ...argv], { cwd, env, encoding: 'utf8' });
  openMarker(s, project);
  const closed = cli(['scope-close', '--data-dir', s.data], join(project));
  assert.equal(closed.status, 0, closed.stderr);
  assert.deepEqual(JSON.parse(closed.stdout), { closed: true, project_root: realpathSync(project) });
  assert.equal(existsSync(markerPath(s, project)), false);
  assert.deepEqual(JSON.parse(cli(['scope-close', '--data-dir', s.data], project).stdout), { closed: false, project_root: realpathSync(project) }, 'nothing to close');
  // Without --data-dir it uses the plugin's data directory under ~/.claude.
  const data = join(s.home, '.claude', 'plugins', 'data', 'team-messaging-team-messaging-local');
  mkdirSync(join(data, 'state'), { recursive: true, mode: 0o700 });
  const key = `scope-${createHash('sha256').update(realpathSync(project)).digest('hex')}.json`;
  writeFileSync(join(data, 'state', key), JSON.stringify({ v: 1, since: Date.now(), opened: Date.now(), root: realpathSync(project), claude_session_id: 'agent-k' }), { mode: 0o600 });
  assert.equal(cli(['scope-close', '--project-dir', project], s.dir).status, 0);
  assert.equal(existsSync(join(data, 'state', key)), false);
  const unbound = cli(['scope-close', '--data-dir', s.data], s.unbound());
  assert.deepEqual([unbound.status, JSON.parse(unbound.stderr).error], [1, 'PROJECT_NOT_BOUND']);
});

test('a Stop with teammate messages pending keeps the window (FD-2)', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = s.project(f, 'agent-k');
  topicHeartbeat(f.broker, 'alice', 'a');
  const sent = f.broker.send('alice', 'a', { to: 'user:bob', body: 'still pending' });
  openMarker(s, project);
  for (const event of ['stop', 'stop-failure']) {
    const result = await stop(s, project, s.env(f), event);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(existsSync(markerPath(s, project)), true, `${event} keeps the window while a message is pending`);
  }
  // A Stop in a session that is not the project's agent cannot read the
  // agent's pending state, so it keeps the window (fail closed).
  f.broker.heartbeat('bob', 'shared-b', 'worker', 'default', null);
  f.broker.ack('bob', 'shared-b', sent.id);
  assert.equal(existsSync(markerPath(s, project)), true);
  await stop(s, project, s.env(f), 'stop', 'other-session');
  assert.equal(existsSync(markerPath(s, project)), true, 'a non-agent Stop keeps the window');
  // Without options the pending state is unknown: the window stays.
  await stop(s, project, hookEnv(s));
  assert.equal(existsSync(markerPath(s, project)), true, 'unknown pending state keeps the window');
  const result = await stop(s, project, s.env(f));
  assert.equal(result.code, 0, result.stderr);
  assert.equal(existsSync(markerPath(s, project)), false, 'with nothing pending the agent Stop closes it');
});

test('a marker written under one session id blocks another session of the same root (FD-1)', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project, { session: 'agent-k' });
  assert.equal(decision(await pre(s, project, 'Bash', { command: 'ls' }, { sessionId: 'after-clear-1' })), `deny: ${OTHER_REASON}`);
  assert.equal(decision(await pre(s, project, 'Read', { file_path: '/etc/hosts' }, { sessionId: 'after-clear-1' })), `deny: ${FILE_REASON}`);
});

test('an internal error denies: the marker path is a directory, or state is a regular file; invalid stdin or session id passes with a stderr code', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  mkdirSync(markerPath(s, project).replace(/[^/]+$/, ''), { recursive: true, mode: 0o700 });
  mkdirSync(markerPath(s, project));
  assert.match(decision(await pre(s, project, 'Bash', { command: 'ls' })), /^deny/, 'EISDIR');
  rmSync(join(s.data, 'state'), { recursive: true });
  writeFileSync(join(s.data, 'state'), 'not a directory');
  assert.match(decision(await pre(s, project, 'Read', { file_path: join(project, 'README.md') })), /^deny/, 'ENOTDIR');
  for (const [label, input] of [['invalid stdin', 'not json'], ['invalid session id', JSON.stringify({ session_id: 'a/b', hook_event_name: 'PreToolUse', tool_name: 'Bash', tool_input: {} })]]) {
    const result = await runChannel('scope-hook.mjs', args(s, project), hookEnv(s), input).result;
    assert.deepEqual([result.code, result.stdout], [0, ''], label);
    assert.match(JSON.parse(result.stderr).error, /^[A-Z_]+$/, label);
    assert.ok(!result.stderr.includes('not json'), 'the input is never echoed');
  }
  const wrong = await runChannel('scope-hook.mjs', ['--data-dir', s.data, '--event', 'pre', '--project-dir', project], hookEnv(s), JSON.stringify({ session_id: 'agent-k', tool_name: 'Bash' })).result;
  assert.deepEqual([wrong.code, wrong.stdout], [0, ''], 'arguments out of order pass with a code (R-5)');
  assert.equal(JSON.parse(wrong.stderr).error, 'INVALID_SCOPE_HOOK_ARGS');
});

// SessionStart (D-11, FA-5).
const startInput = key => JSON.stringify({ hook_event_name: 'SessionStart', session_id: key, source: 'startup' });
const context = result => JSON.parse(result.stdout).hookSpecificOutput.additionalContext;
test('SessionStart with pending messages opens the window and adds the scope line; with none it adds the line and opens nothing; a fetch failure opens nothing', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  const run = (fixture, key, binding) => runChannel('plugin-session-start.mjs', ['--project-dir', s.project(fixture, key, binding), '--data-dir', s.data], s.env(fixture), startInput(key)).result;
  const none = await run(f, 'ss-none', { session: 'none-b' });
  assert.ok(context(none).includes(`project root ${s.project(f, 'ss-none', { session: 'none-b' })}\n\n${SCOPE_LINE}\n\n`), context(none));
  assert.equal(existsSync(markerPath(s, s.project(f, 'ss-none', { session: 'none-b' }))), false, 'no messages, no window');
  const failed = await run(OFFLINE, 'ss-offline');
  assert.ok(context(failed).includes(SCOPE_LINE));
  assert.equal(existsSync(markerPath(s, s.project(OFFLINE, 'ss-offline'))), false, 'a fetch failure opens nothing');
  topicHeartbeat(f.broker, 'alice', 'a');
  f.broker.send('alice', 'a', { to: 'user:bob', body: 'pending at startup' });
  const pending = await run(f, 'ss-pending', { session: 'pending-b' });
  assert.match(context(pending), /pending at startup/); assert.ok(context(pending).includes(SCOPE_LINE));
  assert.equal(marker(s, s.project(f, 'ss-pending', { session: 'pending-b' })).claude_session_id, 'ss-pending', 'pending messages open the window');
  // FA-5: a window that cannot be written withholds the messages.
  mkdirSync(markerPath(s, s.project(f, 'ss-broken', { session: 'broken-b' })));
  const broken = await run(f, 'ss-broken', { session: 'broken-b' });
  assert.equal(broken.code, 0);
  assert.doesNotMatch(context(broken), /pending at startup/);
  assert.match(context(broken), /^team-messaging: SCOPE_WINDOW_UNAVAILABLE: /m);
  assert.ok(context(broken).includes(SCOPE_LINE));
});

test('the largest SessionStart context stays 500 characters under the host limit (FA-16)', async t => {
  const s = pluginScratch(t);
  assert.ok(SCOPE_LINE.length <= 120, `${SCOPE_LINE.length}`);
  // A root as long as a path to its .claude/settings.local.json allows, with
  // the longest workspace and topic names and the bash arm text.
  let project = join(s.dir, 'deep');
  while (project.length < 4000) project = join(project, 'd'.repeat(Math.min(200, 4060 - project.length - 1)));
  const workspace = 'w'.repeat(128), topic = 'a'.repeat(63) + '/' + 'b'.repeat(64);
  bindProject(project, { url: OFFLINE.url, claudeSession: 'ss-big', workspace, topic });
  const result = await runChannel('plugin-session-start.mjs', ['--project-dir', project, '--data-dir', s.data], s.env(OFFLINE, { CLAUDE_PLUGIN_OPTION_ARM: 'bash' }), startInput('ss-big')).result;
  const text = context(result);
  assert.ok(text.includes(SCOPE_LINE) && text.includes('arm=bash') && text.includes(workspace) && text.includes(topic), text.slice(0, 200));
  assert.ok(project.length >= 4000);
  assert.ok(text.length <= 9500, `${text.length} characters`);
});

test('push mode: the marker exists when the notification arrives; a push from our own user opens nothing; a broken marker path drops the push (FA-4)', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  const a = await agent(t, f, s, 'push-a', { binding: { topic: 'fixture', session: 'push-b' }, env: { TM_MODE: 'push' } });
  await eventually(() => f.broker.streams.has('push-b'), 4000);
  // Our own user: another of bob's sessions sends to user:bob.
  const own = await session(f, 'bob', 'bob-other');
  await own.call('send', { to: 'user:bob', body: 'from myself' });
  await eventually(() => a.notifications.some(n => n.content.includes('from myself')), 4000);
  assert.equal(existsSync(markerPath(s, a.project)), false, 'a push from our own user opens nothing');
  topicHeartbeat(f.broker, 'alice', 'a');
  f.broker.send('alice', 'a', { to: 'user:bob', body: 'pushed teammate text' });
  await eventually(() => a.notifications.some(n => n.content.includes('pushed teammate text')), 4000);
  assert.equal(a.notifications.find(n => n.content.includes('pushed teammate text')).markerSeen, true, 'the marker was written before the push');
  // Broken marker path: the push is dropped and inbox refuses while it stays broken.
  rmSync(markerPath(s, a.project)); mkdirSync(markerPath(s, a.project));
  const dropped = f.broker.send('alice', 'a', { to: 'user:bob', body: 'dropped push' });
  await new Promise(resolve => setTimeout(resolve, 600));
  assert.equal(a.notifications.some(n => n.content.includes('dropped push')), false, 'no notification arrives');
  assert.equal((await a.call('inbox')).structuredContent.error, 'SCOPE_WINDOW_UNAVAILABLE');
  rmSync(markerPath(s, a.project), { recursive: true });
  const inbox = await a.call('inbox');
  assert.ok(inbox.structuredContent.messages.some(message => message.id === dropped.id), 'the message stays in the inbox');
});

test('AM-S1 hook: HOOK_INPUT_MAX_BYTES and the CLAUDE_PLUGIN_OPTION_* keys change no decision', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  const noisy = { HOOK_INPUT_MAX_BYTES: '10', CLAUDE_PLUGIN_OPTION_BROKER_URL: 'http://127.0.0.1:9', CLAUDE_PLUGIN_OPTION_TOKEN_FILE: '/dev/null', CLAUDE_PLUGIN_OPTION_WORKSPACE: 'x', CLAUDE_PLUGIN_OPTION_TOPIC: 'x',
    CLAUDE_PLUGIN_OPTION_SESSION_ID: 'x', CLAUDE_PLUGIN_OPTION_MODE: 'push', CLAUDE_PLUGIN_OPTION_ARM: 'off', TM_SCOPE: 'off', TM_ARM: 'off' };
  const inputs = [['Bash', { command: 'ls' }], ['Read', { file_path: join(project, 'README.md') }], ['Read', { file_path: '/etc/passwd' }]];
  const decide = async env => { const out = []; for (const [tool, input] of inputs) out.push(decision(await pre(s, project, tool, input, { env }))); return out; };
  assert.deepEqual(await decide(noisy), await decide({}), 'outside the window');
  openMarker(s, project);
  const quiet = await decide({});
  assert.deepEqual(quiet, [`deny: ${OTHER_REASON}`, 'none', `deny: ${FILE_REASON}`]);
  assert.deepEqual(await decide(noisy), quiet, 'inside the window');
});

test('static imports are node: builtins and ./scope-window.mjs only', () => {
  const source = readFileSync(new URL('../scope-hook.mjs', import.meta.url), 'utf8');
  const specifiers = [...source.matchAll(/^\s*import\s[^;]*?from\s+'([^']+)'/gm)].map(match => match[1]);
  assert.ok(specifiers.length > 0);
  for (const specifier of specifiers) assert.ok(specifier.startsWith('node:') || specifier === './scope-window.mjs', specifier);
  // D-2: the hook reads no option, input-limit or TM_* variable.
  assert.doesNotMatch(source, /CLAUDE_PLUGIN_OPTION_|HOOK_INPUT_MAX_BYTES|env\.TM_|env\[/);
});

test('p99 of 30 fast-path runs is at most 1 s', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k'), times = [];
  for (let i = 0; i < 30; i++) { const result = await pre(s, project, 'Bash', { command: 'ls' }); assert.equal(decision(result), 'none'); times.push(result.ms); }
  times.sort((a, b) => a - b);
  const p99 = times[Math.ceil(0.99 * times.length) - 1];
  t.diagnostic(`scope hook fast path: median ${times[15]} ms, p99 ${p99} ms`);
  assert.ok(p99 <= 1000, `p99 ${p99} ms`);
});

test('D-21 edge inputs: unknown projects and unplaceable paths deny inside a window; no binding means no window', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  const run = (argv, input) => runChannel('scope-hook.mjs', argv, hookEnv(s), JSON.stringify({ session_id: 'agent-k', cwd: project, tool_name: 'Bash', tool_input: {}, ...input })).result;
  // A relative or missing project directory, once the input parsed: deny.
  assert.match(decision(await run(['--event', 'pre', '--project-dir', 'relative', '--data-dir', s.data], {})), /^deny/);
  assert.match(decision(await run(['--event', 'pre', '--project-dir', join(s.dir, 'missing-project'), '--data-dir', s.data], {})), /^deny/);
  // A relative data directory cannot hold a marker: it passes with a code (R-5).
  const relative = await run(['--event', 'pre', '--project-dir', project, '--data-dir', 'relative'], {});
  assert.deepEqual([relative.code, relative.stdout, JSON.parse(relative.stderr).error], [0, '', 'DATA_DIR_REQUIRED']);
  // A project with no binding anywhere up the tree has no window.
  assert.equal(decision(await pre(s, s.unbound(), 'Bash', { command: 'ls' })), 'none');
  for (const [label, input] of [['no tool name', { tool_name: 7 }], ['no file path', { tool_name: 'Read', tool_input: {} }], ['relative path without cwd', { tool_name: 'Read', tool_input: { file_path: 'README.md' }, cwd: undefined }],
    ['a non-string pattern', { tool_name: 'Glob', tool_input: { pattern: 5 } }], ['no tool input', { tool_name: 'Read', tool_input: undefined }],
    ['tool-results without a transcript', { tool_name: 'Read', tool_input: { file_path: join(dirname(transcript(s)), 'agent-k', 'tool-results', 'f.txt') } }]])
    assert.match(decision(await run(args(s, project), input)), /^deny/, label);
  // A path below a directory that cannot be searched is an internal error: deny.
  // Modes are restored inline: t.after hooks run in registration order, after
  // pluginScratch has already removed the directory.
  mkdirSync(join(project, 'locked')); chmodSync(join(project, 'locked'), 0o000);
  try { assert.match(decision(await pre(s, project, 'Read', { file_path: join(project, 'locked', 'x') })), /^deny/); }
  finally { chmodSync(join(project, 'locked'), 0o700); }
  // A missing tool-results file is not readable through the exception.
  mkdirSync(join(dirname(transcript(s)), 'agent-k', 'tool-results'), { recursive: true });
  assert.match(decision(await pre(s, project, 'Read', { file_path: join(dirname(transcript(s)), 'agent-k', 'tool-results', 'gone.txt') })), /^deny/);
  // A binding the plugin refuses still names its root, so its window stays in force.
  const broken = join(s.dir, 'projects', 'broken');
  mkdirSync(broken, { recursive: true });
  writeProjectFile(broken, { version: 1, bad: true });
  openMarker(s, broken);
  assert.match(decision(await pre(s, broken, 'Bash', { command: 'ls' })), /^deny/);
  // An unparsable marker, or an expired one that cannot be removed: deny.
  writeFileSync(markerPath(s, project), 'not json');
  assert.match(decision(await pre(s, project, 'Bash', { command: 'ls' })), /^deny/);
  openMarker(s, project, { opened: Date.now() - 3 * 3600 * 1000 });
  chmodSync(join(s.data, 'state'), 0o500);
  try { assert.match(decision(await pre(s, project, 'Bash', { command: 'ls' })), /^deny/); }
  finally { chmodSync(join(s.data, 'state'), 0o700); }
});

test('input over 64 MiB is denied before it is parsed (D-20)', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  const result = await runChannel('scope-hook.mjs', args(s, project), hookEnv(s), 'x'.repeat(64 * 1024 * 1024 + 1)).result;
  assert.match(decision(result), /^deny/);
  assert.equal(JSON.parse(result.stderr).error, 'HOOK_INPUT_TOO_LARGE');
  const stopped = await runChannel('scope-hook.mjs', args(s, project, 'stop'), hookEnv(s), 'x'.repeat(64 * 1024 * 1024 + 1)).result;
  assert.deepEqual([stopped.code, stopped.stdout], [0, '']);
});

test('Stop keeps the window on doubt and reports a close it cannot make', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = s.project(f, 'agent-k');
  // Nothing to close: no marker, no binding, or a relative project directory.
  assert.deepEqual([(await stop(s, project, s.env(f))).stderr], ['']);
  assert.equal((await stop(s, s.unbound(), s.env(f))).stderr, '');
  const relative = await runChannel('scope-hook.mjs', ['--event', 'stop', '--project-dir', 'relative', '--data-dir', s.data], s.env(f), JSON.stringify({ session_id: 'agent-k' })).result;
  assert.equal(JSON.parse(relative.stderr).error, 'PROJECT_DIR_REQUIRED');
  // An unreachable broker: the pending state is unknown, so the window stays.
  const offline = s.project(OFFLINE, 'agent-k');
  openMarker(s, offline);
  await stop(s, offline, s.env(OFFLINE));
  assert.equal(existsSync(markerPath(s, offline)), true);
  // A broker that never answers: the read gives up within its deadline and the window stays.
  const hanging = http.createServer(() => {});
  await new Promise(resolve => hanging.listen(0, '127.0.0.1', resolve));
  t.after(() => { hanging.closeAllConnections(); return new Promise(resolve => hanging.close(resolve)); });
  const slow = { url: `http://127.0.0.1:${hanging.address().port}` }, slowProject = s.project(slow, 'agent-k');
  openMarker(s, slowProject);
  const waited = await stop(s, slowProject, s.env(slow));
  assert.ok(waited.ms >= 4900 && waited.ms < 9000, `${waited.ms} ms`);
  assert.equal(existsSync(markerPath(s, slowProject)), true);
  // A marker path that is not a file is reported and left alone.
  mkdirSync(markerPath(s, project), { recursive: true });
  const invalid = await stop(s, project, s.env(f));
  assert.deepEqual([invalid.code, invalid.stdout, JSON.parse(invalid.stderr).error], [0, '', 'SCOPE_WINDOW_INVALID']);
  assert.equal(existsSync(markerPath(s, project)), true);
});

test('check_overlap naming another owner and wait returning a teammate message open the window', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), a = await agent(t, f, s, 'scope-plan', { binding: { topic: 'fixture', session: 'plan-b' } });
  const alice = await session(f, 'alice', 'plan-a');
  assert.equal((await alice.call('register_workstream', { name: 'api', paths: ['src/api'] })).isError, undefined);
  assert.equal((await a.call('register_workstream', { name: 'web', paths: ['src/api'] })).isError, undefined);
  assert.equal(existsSync(markerPath(s, a.project)), false, 'registering opens nothing');
  const overlap = await a.call('check_overlap', { name: 'web' });
  assert.equal(overlap.isError, undefined, JSON.stringify(overlap.structuredContent));
  assert.equal(existsSync(markerPath(s, a.project)), true, 'an overlap owned by alice opens');
  rmSync(markerPath(s, a.project));
  const asked = (await a.call('ask', { to: 'user:alice', body: 'which branch?' })).structuredContent;
  assert.equal(existsSync(markerPath(s, a.project)), false);
  const question = (await alice.call('inbox')).structuredContent.messages.find(message => message.thread_id === asked.thread_id);
  await alice.call('ack', { id: question.id });
  const answered = await alice.call('send', { to: 'user:bob', body: 'main', kind: 'answer', thread_id: asked.thread_id });
  assert.equal(answered.isError, undefined, JSON.stringify(answered.structuredContent));
  const waited = await a.call('wait', { thread_id: asked.thread_id, timeout_s: 5 });
  assert.equal(waited.structuredContent.messages.length, 1);
  assert.equal(existsSync(markerPath(s, a.project)), true, 'wait returning a teammate answer opens');
});

test('scope-window edge cases in process', async t => {
  const { anyWindow, closeWindow, openWindow, readWindow, windowPath } = await import('../scope-window.mjs');
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k'), root = realpathSync(project);
  // Review r1 C1: new teammate content moves since and opened to now.
  openWindow(s.data, undefined, project, { home: s.home, now: 1000 });
  assert.deepEqual([marker(s, project).since, marker(s, project).claude_session_id], [1000, null]);
  openWindow(s.data, 'k', project, { home: s.home, now: 5000 });
  assert.deepEqual([marker(s, project).since, marker(s, project).opened], [5000, 5000]);
  assert.equal(readWindow(s.data, root, { now: 5000 + 2 * 3600 * 1000 + 1 }).state, 'closed', '2 h after since');
  assert.equal(anyWindow(s.data), false);
  writeFileSync(windowPath(s.data, root), '{"v":2}');
  assert.equal(anyWindow(s.data), true);
  openWindow(s.data, 'k', project, { home: s.home, now: 7 });
  assert.equal(marker(s, project).since, 7, 'an unreadable old marker is replaced');
  // D-14: a root of HOME or / is recorded as null.
  for (const [where, home] of [[s.home, s.home], ['/', s.home], [s.home, join(s.dir, 'no-such-home')]]) {
    openWindow(s.data, 'k', where, { home, now: 1 });
    const recorded = JSON.parse(readFileSync(windowPath(s.data, realpathSync(where)), 'utf8')).root;
    assert.equal(recorded, where === '/' || home === s.home ? null : realpathSync(where), `${where} with HOME ${home}`);
    closeWindow(s.data, realpathSync(where));
  }
  assert.throws(() => openWindow(s.data, 'k', 'relative', { home: s.home }), { code: 'SCOPE_WINDOW_UNAVAILABLE' });
  // An open onto a path that became a directory fails and leaves no temporary file.
  closeWindow(s.data, root);
  mkdirSync(windowPath(s.data, root));
  assert.throws(() => openWindow(s.data, 'k', project, { home: s.home }), { code: 'SCOPE_WINDOW_UNAVAILABLE' });
  assert.deepEqual(readdirSync(join(s.data, 'state')).filter(name => name.endsWith('.tmp')), []);
  rmSync(windowPath(s.data, root), { recursive: true });
  // S4: compare-and-delete keeps a newer marker and removes the expected one.
  openWindow(s.data, 'k', project, { home: s.home, now: 10 });
  assert.equal(closeWindow(s.data, root, { since: 9, opened: 9 }), false);
  assert.equal(marker(s, project).since, 10, 'a newer marker is linked back');
  assert.equal(closeWindow(s.data, root, { since: 10, opened: 10 }), true);
  assert.equal(existsSync(windowPath(s.data, root)), false);
  assert.equal(closeWindow(s.data, root, { since: 10, opened: 10 }), false, 'nothing to close');
  assert.deepEqual(readdirSync(join(s.data, 'state')).filter(name => name.endsWith('.closing')), []);
  // Closing what is not there is a no-op; a close the directory refuses throws.
  closeWindow(s.data, root);
  openWindow(s.data, 'k', project, { home: s.home });
  chmodSync(join(s.data, 'state'), 0o500);
  try { assert.throws(() => closeWindow(s.data, root), { code: 'EACCES' }); }
  finally { chmodSync(join(s.data, 'state'), 0o700); }
  assert.equal(readWindow(s.data, root).state, 'open');
  // A state path that is not a directory: the close reports it.
  const other = join(s.dir, 'other-data');
  mkdirSync(other, { mode: 0o700 }); writeFileSync(join(other, 'state'), 'x');
  assert.throws(() => closeWindow(other, root), { code: 'ENOTDIR' });
  // An account without a passwd entry still records the root.
  const { default: os } = await import('node:os'), userInfo = os.userInfo;
  os.userInfo = () => { throw new Error('no passwd entry'); };
  try { openWindow(s.data, 'k', project, { home: s.home }); } finally { os.userInfo = userInfo; }
  assert.equal(marker(s, project).root, root);
});

test('S3: in the window ~, $ and % paths and paths with surrounding spaces are denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  for (const [tool, input] of [['Read', { file_path: '~/.bashrc' }], ['Read', { file_path: '~root/.profile' }], ['Read', { file_path: '$HOME/.bashrc' }], ['Read', { file_path: ' /etc/hostname' }],
    ['Read', { file_path: 'docs/%USERPROFILE%/x' }], ['Read', { file_path: 'a/${HOME}/b' }], ['Glob', { pattern: '*', path: '~' }], ['Grep', { pattern: 'root', path: ' /etc' }], ['Grep', { pattern: 'x', path: '$HOME' }]])
    assert.equal(decision(await pre(s, project, tool, input)), `deny: ${FILE_REASON}`, `${tool} ${JSON.stringify(input)}`);
  assert.equal(decision(await pre(s, project, 'Read', { file_path: 'README.md' })), 'none', 'a plain relative path still resolves against cwd');
});

test('S7: in the window brace, parent and absolute-alternative globs are denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  // Review r2 CC8: plain relative brace alternatives (`src/{a,b}`, `*.{md,txt}`) are allowed again.
  for (const [tool, input] of [['Glob', { pattern: '{src,/etc}/*' }], ['Glob', { pattern: 'src/../../x/*' }], ['Glob', { pattern: 'src/../*' }],
    ['Grep', { pattern: 'x', glob: 'a/../b' }]])
    assert.equal(decision(await pre(s, project, tool, input)), `deny: ${FILE_REASON}`, `${tool} ${JSON.stringify(input)}`);
  assert.equal(decision(await pre(s, project, 'Glob', { pattern: '**/*.mjs' })), 'none');
});

test('C2: an unresolvable project root denies only while some window exists', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  const run = projectDir => runChannel('scope-hook.mjs', ['--event', 'pre', '--project-dir', projectDir, '--data-dir', s.data], hookEnv(s), JSON.stringify({ session_id: 'agent-k', tool_name: 'Bash', tool_input: { command: 'ls' } })).result;
  for (const projectDir of [join(s.dir, 'deleted-project'), '${CLAUDE_PROJECT_DIR}', 'relative']) {
    const result = await run(projectDir);
    assert.deepEqual([result.code, result.stdout, JSON.parse(result.stderr).error], [0, '', 'PROJECT_DIR_UNRESOLVED'], projectDir);
  }
  openMarker(s, project);
  for (const projectDir of [join(s.dir, 'deleted-project'), '${CLAUDE_PROJECT_DIR}']) assert.match(decision(await run(projectDir)), /^deny/, projectDir);
});

test('S4: a Stop does not delete a window reopened during its pending check', async t => {
  const s = pluginScratch(t);
  let requests = 0;
  const slow = http.createServer((req, res) => { requests++; req.resume(); setTimeout(() => { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ messages: [] })); }, 1500); });
  await new Promise(resolve => slow.listen(0, '127.0.0.1', resolve));
  t.after(() => { slow.closeAllConnections(); return new Promise(resolve => slow.close(resolve)); });
  const broker = { url: `http://127.0.0.1:${slow.address().port}` }, project = s.project(broker, 'agent-k');
  openMarker(s, project, { since: Date.now() - 1000, opened: Date.now() - 1000 });
  const stopping = stop(s, project, s.env(broker));
  await eventually(() => requests > 0, 4000);
  const reopened = Date.now();
  openMarker(s, project, { since: reopened, opened: reopened });
  const result = await stopping;
  assert.equal(result.code, 0, result.stderr);
  assert.equal(existsSync(markerPath(s, project)), true, 'the reopened window survives');
  assert.equal(marker(s, project).since, reopened);
});

test('S5: the rewake watcher and the Stop hook open the window before waking, and do not wake when it cannot be written', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  topicHeartbeat(f.broker, 'alice', 'a');
  const sent = f.broker.send('alice', 'a', { to: 'user:bob', body: 'wake text' });
  const hookInput = key => JSON.stringify({ hook_event_name: 'Stop', session_id: key, stop_hook_active: false });
  const watcher = key => runChannel('watch.mjs', ['--mode', 'rewake', '--project-dir', s.project(f, key), '--data-dir', s.data, '--poll-s', '1', '--deadline-s', '5'], s.env(f), hookInput(key)).result;
  const stopHook = key => runChannel('stop-hook.mjs', ['--project-dir', s.project(f, key), '--data-dir', s.data], s.env(f), hookInput(key)).result;
  const woke = await watcher('wake-ok');
  assert.equal(woke.code, 2, woke.stderr);
  assert.equal(marker(s, s.project(f, 'wake-ok')).claude_session_id, 'wake-ok', 'the rewake opened the window');
  const blocked = await stopHook('block-ok');
  assert.match(blocked.stdout, /"decision":"block"/);
  assert.equal(marker(s, s.project(f, 'block-ok')).claude_session_id, 'block-ok', 'the Stop block opened the window');
  for (const [key, run] of [['wake-broken', watcher], ['block-broken', stopHook]]) {
    mkdirSync(markerPath(s, s.project(f, key)), { recursive: true });
    const result = await run(key);
    assert.notEqual(result.code, 2, key); assert.equal(result.stdout, '', key);
    assert.ok(!result.stderr.includes('team-message id='), `${key} did not wake`);
    assert.match(result.stderr, /SCOPE_WINDOW_UNAVAILABLE/, key);
    const file = join(s.data, 'state', `${key}.json`);
    assert.deepEqual(existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')).reported : [], [], `${key}: nothing marked reported, so the message still wakes later`);
  }
  assert.equal(f.store.get('SELECT acked_at FROM messages WHERE id=?', sent.id).acked_at, null);
});

test('C7: a push without a teammate sender (a claim notice) is delivered and opens nothing', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  const a = await agent(t, f, s, 'push-n', { binding: { topic: 'fixture', session: 'pushn-b' }, env: { TM_MODE: 'push' } });
  await eventually(() => f.broker.streams.has('pushn-b'), 4000);
  const own = await session(f, 'bob', 'bob-sibling');
  const sent = (await own.call('send', { to: 'user:bob', body: 'self note' })).structuredContent;
  await eventually(() => a.notifications.some(n => n.meta.message_id === String(sent.id) && n.meta.event === 'message'), 4000);
  assert.equal((await own.call('ack', { id: sent.id })).isError, undefined);
  await eventually(() => a.notifications.some(n => n.meta.event === 'claimed' && n.meta.message_id === String(sent.id)), 4000);
  assert.equal(existsSync(markerPath(s, a.project)), false, 'a claim notice opens nothing');
});

test('C8: a Stop whose only pending message is from our own user closes the window', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), project = s.project(f, 'agent-k');
  const own = await session(f, 'bob', 'bob-self');
  assert.equal((await own.call('send', { to: 'user:bob', body: 'note to self' })).isError, undefined);
  openMarker(s, project);
  const result = await stop(s, project, s.env(f));
  assert.equal(result.code, 0, result.stderr);
  assert.equal(existsSync(markerPath(s, project)), false);
});

// Review r2 regressions.
test('SS5: a marker renamed aside by a Stop still counts as an open window', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  const path = markerPath(s, project);
  writeFileSync(`${path}.0123456789abcdef.closing`, readFileSync(path)); rmSync(path);
  assert.equal(decision(await pre(s, project, 'Bash', { command: 'ls' })), `deny: ${OTHER_REASON}`);
});

test('CC3: an expired marker of another project does not make an unresolvable root deny', async t => {
  const s = pluginScratch(t), other = s.project(OFFLINE, 'agent-other');
  openMarker(s, other, { since: Date.now() - 3 * 3600 * 1000, opened: Date.now() - 3 * 3600 * 1000 });
  const result = await runChannel('scope-hook.mjs', ['--event', 'pre', '--project-dir', join(s.dir, 'deleted-project'), '--data-dir', s.data], hookEnv(s), JSON.stringify({ session_id: 'agent-k', tool_name: 'Bash', tool_input: { command: 'ls' } })).result;
  assert.deepEqual([result.code, result.stdout, JSON.parse(result.stderr).error], [0, '', 'PROJECT_DIR_UNRESOLVED']);
  assert.equal(existsSync(markerPath(s, other)), false, 'the expired marker is removed');
});

test('CC4: the printed way out runs from a plain terminal and removes the marker', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  const reason = JSON.parse((await pre(s, project, 'Bash', { command: 'ls' })).stdout).hookSpecificOutput.permissionDecisionReason;
  const command = reason.match(/run `([^`]+)` in a terminal outside Claude/)?.[1];
  assert.ok(command && command.includes('scope-close') && command.includes('--project-dir'), reason);
  assert.ok(reason.includes(`or delete \`${markerPath(s, project)}\``), reason);
  const ran = spawnSync('sh', ['-c', command], { cwd: s.dir, env: { PATH: process.env.PATH, HOME: s.home, TMPDIR: process.env.TMPDIR }, encoding: 'utf8' });
  assert.equal(ran.status, 0, ran.stderr);
  assert.equal(JSON.parse(ran.stdout).closed, true);
  assert.equal(existsSync(markerPath(s, project)), false);
});

test('CC8: plain relative brace alternatives are allowed; others are denied', async t => {
  const s = pluginScratch(t), project = s.project(OFFLINE, 'agent-k');
  openMarker(s, project);
  for (const [tool, input] of [['Glob', { pattern: '**/*.{ts,tsx}' }], ['Glob', { pattern: 'src/{a,b}/*.mjs' }], ['Grep', { pattern: 'x', glob: '*.{md,txt}' }]])
    assert.equal(decision(await pre(s, project, tool, input)), 'none', JSON.stringify(input));
  for (const [tool, input] of [['Glob', { pattern: '{../../..,src}/etc/x' }], ['Glob', { pattern: '{/etc,src}/**' }], ['Glob', { pattern: '{~,src}/x' }], ['Glob', { pattern: '{src,$HOME}/x' }], ['Grep', { pattern: 'x', glob: '{a,%TEMP%}/*' }]])
    assert.equal(decision(await pre(s, project, tool, input)), `deny: ${FILE_REASON}`, JSON.stringify(input));
});

test('CC9: a wake or Stop block for only our own user\'s message opens no window', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  const own = await session(f, 'bob', 'bob-own');
  assert.equal((await own.call('send', { to: 'user:bob', body: 'own note' })).isError, undefined);
  const hookInput = key => JSON.stringify({ hook_event_name: 'Stop', session_id: key, stop_hook_active: false });
  const woke = await runChannel('watch.mjs', ['--mode', 'rewake', '--project-dir', s.project(f, 'own-wake'), '--data-dir', s.data, '--poll-s', '1', '--deadline-s', '5'], s.env(f), hookInput('own-wake')).result;
  assert.equal(woke.code, 2, woke.stderr);
  assert.equal(existsSync(markerPath(s, s.project(f, 'own-wake'))), false, 'the watcher opened no window');
  const blocked = await runChannel('stop-hook.mjs', ['--project-dir', s.project(f, 'own-block'), '--data-dir', s.data], s.env(f), hookInput('own-block')).result;
  assert.match(blocked.stdout, /"decision":"block"/);
  assert.equal(existsSync(markerPath(s, s.project(f, 'own-block'))), false, 'the Stop hook opened no window');
});
