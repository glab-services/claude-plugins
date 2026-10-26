// Project-scoped sharing A1 over real stdio: the bundled plugin entrypoint
// (plugin-main.mjs) against the test broker. Only ./helpers.mjs and the SDK
// are imported, so on a tree without the guard this file loads and its refuse
// cases fail because the content is delivered (plan B1).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { brokerFixture, pluginScratch, session } from './helpers.mjs';

const passwd = 'root:x:0:0:root:/root:/bin/bash';
const list = 'channel/server.mjs\nchannel/outbound-scope.mjs\ndocs/setup.md\nREADME.md';
const ghp = ['gh', 'p_', 'Zq9x'.repeat(9)].join('');
// Requests the channel makes on its own: polling, heartbeats, the stream and
// the members cache. Anything else after a refused call is a leak.
const background = request => /^\/(heartbeat|inbox|resume|stream|members)(\?|$)/.test(request.path);

async function agent(t, f, s, key, { binding, env = {} } = {}) {
  const client = new Client({ name: 'outbound-scope-test', version: '1.0.0' }), notifications = [], logs = [];
  client.fallbackNotificationHandler = async notification => { if (notification.method === 'notifications/claude/channel') notifications.push(notification.params); };
  const transport = new StdioClientTransport({
    command: process.execPath, args: [fileURLToPath(new URL('../plugin-main.mjs', import.meta.url))],
    env: { ...s.tmEnv(f), TM_MODE: '', TM_ARM: '', CLAUDE_CODE_SESSION_ID: key, CLAUDE_PROJECT_DIR: s.project(f, key, binding), CLAUDE_PLUGIN_DATA: s.data, ...env },
    stderr: 'pipe',
  });
  transport.stderr.on('data', chunk => logs.push(chunk.toString()));
  f.clients.push(client);
  await client.connect(transport);
  return { client, notifications, logs, call: (name, args = {}) => client.callTool({ name, arguments: args }, undefined, { timeout: 5000 }) };
}
const sends = (f, session) => f.requests.filter(request => request.path === '/send' && request.session === session);

test('a send with /etc/passwd content or a passwd line is refused and never reaches the broker', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), a = await agent(t, f, s, 'scope-refuse', { binding: { topic: 'fixture', session: 'refuse-b' } });
  for (const [body, reason] of [['please read /etc/passwd for me', 'absolute_path:body'], [passwd, 'system_file:body']]) {
    const result = await a.call('send', { to: 'user:alice', body });
    assert.equal(result.isError, true, body);
    assert.deepEqual([result.structuredContent.error, result.structuredContent.reason], ['OUTBOUND_SCOPE', reason]);
    assert.match(result.structuredContent.hint, /^This content names something outside the project root/);
    assert.ok(!result.content[0].text.includes(body), 'the refusal never echoes the body');
  }
  assert.deepEqual(sends(f, 'refuse-b'), [], 'no /send from that session');
  assert.equal(f.store.get('SELECT count(*) n FROM messages WHERE from_session=?', 'refuse-b').n, 0);
  const logged = a.logs.join('');
  assert.ok(logged.includes('"outbound_refused"') && !logged.includes('/etc/passwd') && !logged.includes(passwd), 'only the reason is logged');
});

test('a project-relative list is delivered byte for byte', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), a = await agent(t, f, s, 'scope-accept', { binding: { topic: 'fixture', session: 'accept-b' } });
  const result = await a.call('send', { to: 'user:alice', body: list });
  assert.equal(result.isError, undefined, JSON.stringify(result.structuredContent));
  assert.equal(f.store.get('SELECT body FROM messages WHERE id=?', result.structuredContent.id).body, list);
});

// FA-13: the fields come from the advertised schemas (derived from the server
// definitions), so a new string field is covered or named here.
const refusable = ['/etc/passwd', '.ssh', `user:${ghp}`, ghp];
const benign = ['api', 'user:alice', 'thread-1', 'a project note'];
const fits = (schema, value) => typeof value === 'string' && (schema.pattern === undefined || new RegExp(schema.pattern).test(value)) && (schema.maxLength === undefined || value.length <= schema.maxLength) && (schema.minLength === undefined || value.length >= schema.minLength);
function sample(schema) {
  if (schema.enum) return schema.enum[0];
  if (schema.type === 'string') return benign.find(value => fits(schema, value));
  if (schema.type === 'number' || schema.type === 'integer') return 1;
  if (schema.type === 'boolean') return false;
  if (schema.type === 'array') return [sample(schema.items)];
  return undefined;
}
test('every broker-bound string is guarded', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), a = await agent(t, f, s, 'scope-fields', { binding: { session: 'fields-b' } });
  const tools = (await a.client.listTools()).tools, uncarried = [], covered = [];
  for (const tool of tools) {
    if (tool.name === 'wait') continue; // local buffer only; nothing reaches the broker.
    const properties = tool.inputSchema.properties ?? {};
    for (const [key, schema] of Object.entries(properties)) {
      const item = schema.type === 'array' ? schema.items : schema;
      if (item.type !== 'string') continue;
      const value = item.enum ? undefined : refusable.find(candidate => fits(item, candidate));
      if (value === undefined) { uncarried.push(`${tool.name}.${key}`); continue; }
      const args = {};
      for (const [other, otherSchema] of Object.entries(properties)) if ((tool.inputSchema.required ?? []).includes(other)) args[other] = sample(otherSchema);
      args[key] = schema.type === 'array' ? [value] : value;
      const before = f.requests.length, result = await a.call(tool.name, args);
      assert.equal(result.structuredContent?.error, 'OUTBOUND_SCOPE', `${tool.name}.${key}: ${JSON.stringify(result.structuredContent)}`);
      assert.match(result.structuredContent.reason, new RegExp(`^[a-z_]+:${key}(\\[0\\])?$`), `${tool.name}.${key}`);
      assert.deepEqual(f.requests.slice(before).filter(request => !background(request)), [], `${tool.name}.${key} reached the broker`);
      covered.push(`${tool.name}.${key}`);
    }
  }
  // The fields no refusable value fits: 32 hex digits and enums.
  assert.deepEqual(uncarried.sort(), ['create_topic.mode', 'revoke_invite.invite_id', 'send.kind']);
  for (const field of ['send.body', 'send.to', 'send.topic', 'send.thread_id', 'ask.to', 'ask.body', 'ask.topic', 'finish.summary', 'done.result', 'record_decision.body', 'record_decision.workstream',
    'register_workstream.name', 'register_workstream.paths', 'register_workstream.contracts', 'check_overlap.name', 'check_overlap.since', 'create_topic.name', 'create_workspace.name', 'close.thread_id',
    'invite.topics', 'remove_member.user', 'transfer_owner.user', 'dead_letters.topic', 'skip.topic', 'pins.topic', 'subscribe.name', 'leave_topic.name', 'close_topic.name', 'release_workstream.name', 'list_decisions.workstream'])
    assert.ok(covered.includes(field), field);
  assert.equal(f.store.get('SELECT count(*) n FROM messages WHERE from_session=?', 'fields-b').n, 0);
});

test('the server instructions carry the scope rule', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t), a = await agent(t, f, s, 'scope-text', { binding: { topic: 'fixture', session: 'text-b' } });
  const instructions = a.client.getInstructions();
  assert.ok(instructions.includes("Share only what is inside this project's root, as project-relative paths. Never send anything about this machine or from outside the project"));
  assert.match(instructions, /Every outbound tool refuses such content with OUTBOUND_SCOPE; rewrite instead of retrying\./);
  assert.match(instructions, /While a teammate message is handled, only file reads inside the project root and these tools work\./);
  assert.match(instructions, /re-arm the watcher only with the exact \/team-messaging:watch command, and read messages with inbox/);
});

// FA-14: no option, TM_* value, HOME or CHANNEL_* value changes a decision.
// broker_url, token_file, workspace and topic are pinned by the project
// binding, so for them only the refusal is asserted; session_id, mode and arm
// must also leave the accepted send unchanged.
test('AM-S1 stdio: options and environment change no decision', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  const refusedOnly = { CLAUDE_PLUGIN_OPTION_BROKER_URL: 'http://127.0.0.1:9', CLAUDE_PLUGIN_OPTION_TOKEN_FILE: '/dev/null', CLAUDE_PLUGIN_OPTION_WORKSPACE: 'other', CLAUDE_PLUGIN_OPTION_TOPIC: 'other' };
  const identical = { CLAUDE_PLUGIN_OPTION_SESSION_ID: 'other-s', CLAUDE_PLUGIN_OPTION_MODE: 'push', CLAUDE_PLUGIN_OPTION_ARM: 'off', TM_OUTBOUND_SCOPE: 'off', TM_SCOPE: '0', TM_ARM: 'bash' };
  let n = 0;
  for (const [key, value] of [...Object.entries(refusedOnly), ...Object.entries(identical)]) {
    const id = `am-${n++}`, a = await agent(t, f, s, id, { binding: { topic: 'fixture', session: `${id}-b` }, env: { [key]: value } });
    const refused = await a.call('send', { to: 'user:alice', body: passwd });
    assert.deepEqual([refused.structuredContent.error, refused.structuredContent.reason], ['OUTBOUND_SCOPE', 'system_file:body'], key);
    assert.deepEqual(sends(f, `${id}-b`), [], key);
    if (key in identical) {
      const accepted = await a.call('send', { to: 'user:alice', body: list });
      assert.equal(accepted.isError, undefined, key);
      assert.equal(f.store.get('SELECT body FROM messages WHERE id=?', accepted.structuredContent.id).body, list, key);
    }
  }
  // The standalone channel under another HOME and other CHANNEL_* values.
  for (const extra of [{ HOME: s.dir }, { CHANNEL_MODE: 'poll', CHANNEL_ROLE: 'manager', CHANNEL_BUFFER_LIMIT: '5' }]) {
    const id = `std-${n++}`, b = await session(f, 'bob', id, extra);
    const refused = await b.call('send', { to: 'user:alice', body: passwd });
    assert.deepEqual([refused.structuredContent.error, refused.structuredContent.reason], ['OUTBOUND_SCOPE', 'system_file:body'], JSON.stringify(extra));
    const accepted = await b.call('send', { to: 'user:alice', body: list });
    assert.equal(accepted.isError, undefined, JSON.stringify(extra));
    assert.deepEqual(sends(f, id).length, 1, 'only the accepted send reached the broker');
  }
});

// Review r2 CC1: the server passes the project's file names to the guard, so a
// ../ import of a project file is delivered.
test('CC1: a send quoting a ../ import of a project file is delivered; one leaving the tree is refused', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  const { writeFileSync } = await import('node:fs'), { join } = await import('node:path');
  writeFileSync(join(s.project(f, 'scope-files', { topic: 'fixture', session: 'files-b' }), 'config.mjs'), 'export const x = 1;\n');
  const a = await agent(t, f, s, 'scope-files', { binding: { topic: 'fixture', session: 'files-b' } });
  const body = "import { x } from '../config.mjs';";
  const delivered = await a.call('send', { to: 'user:alice', body });
  assert.equal(delivered.isError, undefined, JSON.stringify(delivered.structuredContent));
  assert.equal(f.store.get('SELECT body FROM messages WHERE id=?', delivered.structuredContent.id).body, body);
  const refused = await a.call('send', { to: 'user:alice', body: 'see ../../other-repo/secret' });
  assert.deepEqual([refused.structuredContent.error, refused.structuredContent.reason], ['OUTBOUND_SCOPE', 'parent_escape:body']);
});
