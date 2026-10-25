import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { directoryProblem, localError, pluginEnv, unsafeOption } from '../plugin-env.mjs';
import { normaliseUrl } from '../cli.mjs';
import { pluginAdapter } from '../plugin-config.mjs';
import { ChannelError, config } from '../config.mjs';
import { pluginScratch } from './helpers.mjs';

const all = { CLAUDE_PLUGIN_OPTION_BROKER_URL: 'https://broker.example', CLAUDE_PLUGIN_OPTION_TOKEN_FILE: '/home/u/.config/team-messaging/credentials/x.token',
  CLAUDE_PLUGIN_OPTION_WORKSPACE: 'proj', CLAUDE_PLUGIN_OPTION_TOPIC: 'area/one', CLAUDE_PLUGIN_OPTION_SESSION_ID: 'claude-plugin-abc', CLAUDE_PLUGIN_OPTION_MODE: 'push', CLAUDE_PLUGIN_OPTION_ARM: 'bash', HOME: '/home/u', XDG_CONFIG_HOME: '/home/u/.config' };
const mapped = { BROKER_URL: 'https://broker.example', BROKER_TOKEN_FILE: '/home/u/.config/team-messaging/credentials/x.token', CHANNEL_WORKSPACE: 'proj', CHANNEL_TOPIC: 'area/one', CHANNEL_SESSION_ID: 'claude-plugin-abc', CHANNEL_MODE: 'push', HOME: '/home/u', XDG_CONFIG_HOME: '/home/u/.config' };

test('plugin options map to the exact channel environment', () => {
  assert.deepEqual(pluginEnv(all), { configured: true, arm: 'bash', env: mapped });
  const tm = Object.fromEntries(Object.entries(all).map(([key, value]) => [key.replace('CLAUDE_PLUGIN_OPTION_', 'TM_'), value]));
  assert.deepEqual(pluginEnv(tm), { configured: true, arm: 'bash', env: mapped }, 'the .mcp.json TM_* names map the same way');
});

test('empty options mean unset: workspace-level topic, poll mode and the rewake arm', () => {
  const { CHANNEL_TOPIC: _topic, ...withoutTopic } = mapped;
  assert.deepEqual(pluginEnv({ ...all, CLAUDE_PLUGIN_OPTION_TOPIC: '' }), { configured: true, arm: 'bash', env: withoutTopic });
  assert.deepEqual(pluginEnv({ ...all, CLAUDE_PLUGIN_OPTION_MODE: '' }), { configured: true, arm: 'bash', env: { ...mapped, CHANNEL_MODE: 'poll' } });
  assert.deepEqual(pluginEnv({ ...all, CLAUDE_PLUGIN_OPTION_ARM: '' }), { configured: true, arm: 'rewake', env: mapped });
});

test('an unknown arm and each missing required option are not configured', () => {
  assert.deepEqual(pluginEnv({ ...all, CLAUDE_PLUGIN_OPTION_ARM: 'bogus' }), { configured: false, hint: 'INVALID_ARM' });
  for (const key of ['BROKER_URL', 'TOKEN_FILE', 'WORKSPACE', 'SESSION_ID']) assert.deepEqual(pluginEnv({ ...all, [`CLAUDE_PLUGIN_OPTION_${key}`]: '' }), { configured: false, arm: 'bash' }, key);
  assert.deepEqual(pluginEnv({}), { configured: false, arm: 'rewake' });
});

test('inherited token variables never reach config next to the token file', t => {
  const s = pluginScratch(t), sentinel = 'tmc_' + 'f'.repeat(32) + '_' + 'S'.repeat(43);
  const result = pluginEnv({ ...all, CLAUDE_PLUGIN_OPTION_BROKER_URL: 'http://127.0.0.1:9', CLAUDE_PLUGIN_OPTION_TOKEN_FILE: s.tokenFile, HOME: s.home, XDG_CONFIG_HOME: '', BROKER_TOKEN: sentinel, TEAM_BROKER_TOKEN: sentinel });
  assert.equal(result.configured, true);
  assert.equal('BROKER_TOKEN' in result.env, false); assert.equal('TEAM_BROKER_TOKEN' in result.env, false); assert.equal('XDG_CONFIG_HOME' in result.env, false);
  assert.ok(!JSON.stringify(result).includes(sentinel));
  const settings = config(result.env);
  assert.equal(settings.mode, 'push'); assert.equal(settings.session, 'claude-plugin-abc');
  assert.ok(settings.token !== sentinel, 'the token comes from the credential file');
});

const payload = "https://h.example/p'$(echo$IFS'INJECTED-RAN'>&2)'";
test('a broker URL or option that could leave the skills\' shell quotes is refused at join, bind and load', async t => {
  assert.throws(() => normaliseUrl(payload), { code: 'UNSAFE_BROKER_URL' });
  assert.equal(normaliseUrl('https://h.example/a b'), 'https://h.example/a%20b', 'characters URL parsing percent-encodes are safe');
  for (const url of ['https://h.example/a;b', 'https://h.example/$x', 'https://h.example/a|b', 'https://h.example/a(b)', 'https://h.example/a&b']) assert.throws(() => normaliseUrl(url), { code: 'UNSAFE_BROKER_URL' }, url);
  assert.equal(normaliseUrl('http://[::1]:8280/'), 'http://[::1]:8280');
  for (const [key, value] of [['BROKER_URL', payload], ['TOKEN_FILE', "/home/u/x'.token"], ['WORKSPACE', 'a b'], ['TOPIC', "t'$(x)"], ['SESSION_ID', 's;x'], ['MODE', 'poll;x']])
    assert.deepEqual(pluginEnv({ ...all, [`CLAUDE_PLUGIN_OPTION_${key}`]: value }), { configured: false, arm: 'bash', hint: 'UNSAFE_OPTION' }, key);
  let spawned = 0;
  await assert.rejects(pluginAdapter({ url: payload, workspace: 'w', topic: null, path: '/p.token' }, { spawn: () => { spawned++; return { status: 1 }; }, env: { HOME: '/nonexistent' } }), { code: 'UNSAFE_OPTION' });
  await assert.rejects(pluginAdapter({ url: 'https://h.example', workspace: 'w', topic: null, path: "/x'y.token" }, { spawn: () => { spawned++; return { status: 1 }; }, env: { HOME: '/nonexistent' } }), { code: 'UNSAFE_OPTION' });
  assert.equal(spawned, 0, 'nothing reaches claude plugin configure');
});

test('every skill shell command keeps the widest allowed option values inert', async t => {
  const s = pluginScratch(t), bin = join(s.dir, 'bin'), record = join(s.dir, 'calls.jsonl'), marker = join(s.dir, 'INJECTED-RAN');
  mkdirSync(bin, { mode: 0o700 });
  for (const name of ['team-messaging', 'team-messaging-watch'])
    writeFileSync(join(bin, name), `#!${process.execPath}\nrequire('node:fs').appendFileSync(${JSON.stringify(record)}, JSON.stringify({ name: ${JSON.stringify(name)}, args: process.argv.slice(2), env: Object.fromEntries(Object.entries(process.env).filter(([k]) => k.startsWith('TM_'))) }) + '\\n');\n`, { mode: 0o700 });
  const values = { broker_url: 'https://h.example:8443/a-b_c.d~e%20/[x]', token_file: '/home/u/.config/team-messaging/credentials/a-b_c.d~e%2F.token', workspace: 'w-1.x_', topic: 'a/b-c.d', session_id: 'claude-plugin-0_a.b-c', mode: 'push', arm: 'bash' };
  assert.equal(unsafeOption(Object.fromEntries(Object.entries(values).map(([k, v]) => [k.toUpperCase(), v]))), null, 'these values pass the write and load checks');
  const render = text => text.replace(/\$\{user_config\.([a-z_]+)\}/g, (_, key) => values[key]).replaceAll('${CLAUDE_SESSION_ID}', 'sess-1').replaceAll('${CLAUDE_PLUGIN_DATA}', s.data);
  let commands = 0;
  for (const skill of readdirSync(new URL('../../plugin/skills/', import.meta.url))) {
    const text = readFileSync(new URL(`../../plugin/skills/${skill}/SKILL.md`, import.meta.url), 'utf8');
    for (const [, block] of text.matchAll(/```sh\n([\s\S]*?)```/g)) {
      if (!block.includes('${user_config.')) continue;
      commands++;
      const result = spawnSync('/bin/sh', ['-c', render(block)], { env: { PATH: bin }, encoding: 'utf8', cwd: s.dir });
      assert.equal(result.status, 0, `${skill}: ${result.stderr}`);
    }
  }
  assert.equal(commands, 2, 'status and watch interpolate options into shell commands');
  const calls = readFileSync(record, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  for (const call of calls) assert.deepEqual(call.env, { TM_BROKER_URL: values.broker_url, TM_TOKEN_FILE: values.token_file, TM_WORKSPACE: values.workspace, TM_TOPIC: values.topic, TM_SESSION_ID: values.session_id });
  assert.deepEqual(calls.find(call => call.name === 'team-messaging-watch').args, ['--mode', 'bash', '--session-key', 'sess-1', '--data-dir', s.data]);
  // The reviewer's payload, if it ever reached a skill, would run: this is why it is refused above.
  const unsafe = spawnSync('/bin/sh', ['-c', `TM_BROKER_URL='${payload.replace('>&2', `>${marker}`)}' team-messaging status`], { env: { PATH: bin }, cwd: s.dir });
  assert.equal(unsafe.status, 0); assert.ok(existsSync(marker), 'the control payload executes when not refused');
});

// Decision 148: only the local writable-ancestor error, which carries `dir`,
// passes its hint through. A broker can send any code with any hint text.
test('localError passes through only the local directory hint, and directoryProblem reports only directory codes', () => {
  const local = Object.assign(new ChannelError('INSECURE_CREDENTIAL_DIR', "/h/.claude/plugins is group- or world-writable; run chmod go-w '/h/.claude/plugins'"), { dir: '/h/.claude/plugins' });
  assert.deepEqual(localError(local, 'fallback'), { code: 'INSECURE_CREDENTIAL_DIR', hint: local.hint });
  assert.deepEqual(localError(new ChannelError('INSECURE_CREDENTIAL_DIR', 'broker text'), 'fallback'), { code: 'INSECURE_CREDENTIAL_DIR', hint: 'fallback' });
  assert.deepEqual(localError(Object.assign(new Error('x'), { code: 'INSECURE_CREDENTIAL_DIR', dir: '/x', hint: 'not local' }), 'fallback').hint, 'fallback');
  assert.equal(directoryProblem(localError(local, 'fallback')), `team-messaging: INSECURE_CREDENTIAL_DIR: ${local.hint}`);
  assert.equal(directoryProblem({ code: 'DATA_DIR_REQUIRED', hint: 'h' }), 'team-messaging: DATA_DIR_REQUIRED: h');
  for (const code of ['BROKER_UNAVAILABLE', 'INSECURE_TOKEN_FILE', undefined]) assert.equal(directoryProblem({ code, hint: 'h' }), null);
});
