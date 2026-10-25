import { topicHeartbeat } from '../../test-support/topic-fixture.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { accessSync, constants, existsSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import * as z from 'zod/v4';
import { testTempDir } from '../../test-support/temp.mjs';
import { brokerFixture, tokens, eventually, pluginScratch } from '../test/helpers.mjs';
import { HOOK_TIMEOUT_S } from '../watch.mjs';

// One bundle for the whole file: npm ci needs the network, which is why these
// tests run only through pnpm plugin:test (decision 133).
const out = testTempDir('plugin-build-'), plugin = join(out, 'plugins', 'team-messaging');
const build = spawnSync(process.execPath, [fileURLToPath(new URL('../../scripts/build-plugin.mjs', import.meta.url)), '--out', out], { encoding: 'utf8', env: { ...process.env, TMPDIR: dirname(out) } });
process.on('exit', () => rmSync(out, { recursive: true, force: true }));
const claudeMissing = spawnSync('claude', ['--version'], { stdio: 'ignore' }).error?.code === 'ENOENT';

test('the build succeeds, validates and refuses unsafe output directories', t => {
  assert.equal(build.status, 0, build.stderr);
  if (claudeMissing) {
    if (process.env.PLUGIN_VALIDATE_SKIP !== '1') assert.fail('claude is not on PATH; install it or set PLUGIN_VALIDATE_SKIP=1');
    t.diagnostic('SKIP: claude plugin validate (claude not on PATH)');
  } else {
    assert.ok(!build.stdout.includes('SKIP:'));
    for (const target of [plugin, out]) {
      const validated = spawnSync('claude', ['plugin', 'validate', target, '--strict'], { encoding: 'utf8' });
      assert.equal(validated.status, 0, `validate ${target}`); assert.match(validated.stdout, /Validation passed/);
    }
  }
  const script = fileURLToPath(new URL('../../scripts/build-plugin.mjs', import.meta.url));
  for (const target of [out, '/tmp/team-messaging-plugin', resolve('/var/team-messaging-plugin')]) {
    const refused = spawnSync(process.execPath, [script, '--out', target], { encoding: 'utf8', env: { ...process.env, TMPDIR: dirname(out) } });
    assert.equal(refused.status, 1, target); assert.match(refused.stderr, /not empty|refusing \/tmp|must be under/);
  }
  const marketplace = JSON.parse(readFileSync(join(out, '.claude-plugin', 'marketplace.json'), 'utf8'));
  assert.deepEqual([marketplace.name, marketplace.plugins[0].source], ['team-messaging-local', './plugins/team-messaging']);
  assert.ok(marketplace.owner && marketplace.description);
  assert.equal(existsSync(join(plugin, '.claude-plugin', 'marketplace.json')), false, 'only one marketplace.json');
});

// Decision 150: SessionStart arms the same rewake watcher as Stop, so a
// session resumed in a new Claude Code process is woken without waiting for
// its first turn to end. The built bundle is checked, not the source.
test('SessionStart arms the rewake watcher with the Stop watcher\'s command and timeout', () => {
  const hooks = JSON.parse(readFileSync(join(plugin, 'hooks', 'hooks.json'), 'utf8')).hooks;
  const watcher = { type: 'command', command: 'node', args: ['--use-system-ca', '${CLAUDE_PLUGIN_ROOT}/runtime/channel/watch.mjs', '--mode', 'rewake', '--data-dir', '${CLAUDE_PLUGIN_DATA}'], asyncRewake: true, timeout: HOOK_TIMEOUT_S };
  assert.equal(hooks.SessionStart.length, 1);
  assert.equal(hooks.SessionStart[0].matcher, 'startup|resume|clear|compact');
  assert.deepEqual(hooks.SessionStart[0].hooks.filter(hook => hook.args.some(arg => arg.endsWith('/watch.mjs'))), [watcher]);
  assert.deepEqual(hooks.Stop[0].hooks.filter(hook => hook.args.some(arg => arg.endsWith('/watch.mjs'))), [watcher], 'same entry as the Stop watcher');
  assert.deepEqual(hooks.SessionStart[0].hooks.map(hook => hook.args[1].split('/').pop()), ['plugin-session-start.mjs', 'watch.mjs']);
});

test('every path the manifests and skills name exists, and no runtime import leaves the plugin root', () => {
  const substitute = value => value.replaceAll('${CLAUDE_PLUGIN_ROOT}', plugin);
  const hooks = JSON.parse(readFileSync(join(plugin, 'hooks', 'hooks.json'), 'utf8')).hooks, mcp = JSON.parse(readFileSync(join(plugin, '.mcp.json'), 'utf8')).mcpServers;
  const args = [...Object.values(hooks).flat().flatMap(entry => entry.hooks).flatMap(hook => hook.args), ...Object.values(mcp).flatMap(server => server.args)];
  const files = args.filter(arg => arg.startsWith('${CLAUDE_PLUGIN_ROOT}')).map(substitute);
  assert.equal(files.length, 5);
  for (const file of files) assert.ok(existsSync(file), file);
  for (const skill of readdirSync(join(plugin, 'skills'))) {
    const text = readFileSync(join(plugin, 'skills', skill, 'SKILL.md'), 'utf8');
    assert.match(text, new RegExp(`^---\\nname: ${skill}\\n`));
    for (const command of ['team-messaging-watch', 'team-messaging']) if (new RegExp(`(^|[\\s'])${command} `, 'm').test(text)) accessSync(join(plugin, 'bin', command), constants.X_OK);
  }
  const runtime = join(plugin, 'runtime');
  for (const dir of ['channel', 'shared']) for (const file of readdirSync(join(runtime, dir))) {
    for (const [, specifier] of readFileSync(join(runtime, dir, file), 'utf8').matchAll(/(?:from|import\()\s*'([^']+)'/g)) {
      if (specifier.startsWith('node:')) continue;
      if (specifier.startsWith('.')) { const target = resolve(runtime, dir, specifier); assert.ok(target.startsWith(plugin + sep) && existsSync(target), `${file}: ${specifier}`); }
      else assert.ok(existsSync(join(plugin, 'node_modules', ...specifier.split('/').slice(0, specifier.startsWith('@') ? 2 : 1))), `${file}: ${specifier}`);
    }
  }
  assert.equal(relative(plugin, join(runtime, 'channel', '..', 'shared', 'address.mjs')).startsWith('..'), false);
});

function bundled(f, s, env) {
  const client = new Client({ name: 'plugin-package-test', version: '1.0.0' }), notifications = [];
  client.setNotificationHandler(z.object({ method: z.literal('notifications/claude/channel'), params: z.object({ content: z.string(), meta: z.record(z.string(), z.string()) }) }), event => notifications.push(event.params));
  const transport = new StdioClientTransport({ command: process.execPath, args: ['--use-system-ca', join(plugin, 'runtime', 'channel', 'plugin-main.mjs')], cwd: plugin, env, stderr: 'pipe' });
  const logs = []; transport.stderr.on('data', chunk => logs.push(chunk.toString()));
  f.clients.push(client);
  return { client, notifications, logs, connect: () => client.connect(transport) };
}

test('the bundled MCP server answers over stdio in poll mode with plugin hints', async t => {
  const f = await brokerFixture(t, { defaultOwner: 'bob' }), s = pluginScratch(t);
  // The .mcp.json shape: TM_* from ${user_config.*}, with an unset topic and mode as "".
  const env = { HOME: s.home, TM_BROKER_URL: f.url, TM_TOKEN_FILE: s.tokenFile, TM_WORKSPACE: 'default', TM_TOPIC: '', TM_SESSION_ID: 'bundled-b', TM_MODE: '', TM_ARM: 'rewake', BROKER_TOKEN: 'tmc_' + 'd'.repeat(32) + '_' + 'P'.repeat(43) };
  const b = bundled(f, s, env);
  await b.connect();
  const tools = (await b.client.listTools()).tools.map(tool => tool.name);
  assert.ok(tools.includes('invite') && tools.includes('ack'));
  const create = await b.client.callTool({ name: 'create_topic', arguments: { name: 'bundle' } });
  assert.equal(create.isError, undefined);
  const invite = (await b.client.callTool({ name: 'invite', arguments: { topics: ['bundle'] } })).structuredContent;
  assert.equal(invite.command, '/team-messaging:join');
  topicHeartbeat(f.broker, 'alice', 'a');
  const sent = f.broker.send('alice', 'a', { to: 'user:bob', body: 'poll only' });
  // The channel polls about once a second; wait for a poll after the send.
  const seen = f.requests.length;
  await eventually(() => f.requests.slice(seen).some(r => r.path.startsWith('/inbox')), 5000);
  const inbox = (await b.client.callTool({ name: 'inbox', arguments: {} })).structuredContent;
  assert.deepEqual(inbox.messages.map(m => m.id), [sent.id]);
  await new Promise(resolve => setTimeout(resolve, 300));
  assert.deepEqual(b.notifications, [], 'poll mode sends no claude/channel notification');
  assert.equal(f.requests.some(r => r.path.startsWith('/stream')), false, 'poll mode never opens /stream');
  assert.ok(!b.logs.join('').includes(env.BROKER_TOKEN) && !b.logs.join('').includes(tokens.bob));
});

test('an unconfigured bundled server exits 1 with PLUGIN_NOT_CONFIGURED and no secret', async t => {
  const sentinel = 'tmc_' + 'c'.repeat(32) + '_' + 'Q'.repeat(43);
  const child = spawn(process.execPath, [join(plugin, 'runtime', 'channel', 'plugin-main.mjs')], { env: { TM_BROKER_URL: '', TM_TOKEN_FILE: '', TM_WORKSPACE: '', TM_TOPIC: '', TM_SESSION_ID: '', TM_MODE: 'poll', TM_ARM: 'rewake', BROKER_TOKEN: sentinel }, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = ''; child.stdout.on('data', c => { output += c; }); child.stderr.on('data', c => { output += c; });
  const code = await new Promise(resolve => child.once('close', resolve));
  assert.equal(code, 1);
  assert.deepEqual(JSON.parse(output), { error: 'PLUGIN_NOT_CONFIGURED', hint: 'Run /team-messaging:join or /team-messaging:bind, then restart Claude Code.' });
  assert.ok(!output.includes(sentinel));
});
