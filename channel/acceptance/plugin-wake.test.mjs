import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join, resolve, sep } from 'node:path';
import { userInfo } from 'node:os';
import { fileURLToPath } from 'node:url';
import { testTempDir } from '../../test-support/temp.mjs';
import { brokerFixture, tokens } from '../test/helpers.mjs';
import { PLUGIN_ID, writePluginConfig } from '../plugin-config.mjs';

// Scripted part of after-A3 (arm=rewake): an idle interactive Claude Code
// session with the built plugin wakes for a new message and acks it through
// MCP. `-p` cannot run this, because async hooks are killed at teardown in
// -p mode (hooks.md). Driving the TUI through script(1) is best effort;
// logging in to the scratch HOME, A3.4 (desktop app) and A3.5 (timeout
// enforcement) stay operator steps.
const home = process.env.M12_BOB_HOME, idleS = Number(process.env.M12_IDLE_S ?? 60);
function loggedIn() {
  if (!home || !resolve(home).startsWith(resolve(process.env.TMPDIR ?? '/nonexistent') + sep) || resolve(home) === resolve(userInfo().homedir)) return false;
  const status = spawnSync('claude', ['auth', 'status'], { encoding: 'utf8', env: { ...process.env, HOME: home, CLAUDE_CONFIG_DIR: join(home, '.claude') } });
  try { return status.status === 0 && JSON.parse(status.stdout).loggedIn === true; } catch { return false; }
}
const transcriptOf = (configDir, id) => {
  const projects = join(configDir, 'projects');
  for (const dir of existsSync(projects) ? readdirSync(projects) : []) if (existsSync(join(projects, dir, `${id}.jsonl`))) return readFileSync(join(projects, dir, `${id}.jsonl`), 'utf8');
  return '';
};
async function until(predicate, ms) {
  for (const end = Date.now() + ms; Date.now() < end; await new Promise(r => setTimeout(r, 1000))) if (predicate()) return true;
  return false;
}

test('real Claude Code: an idle plugin session wakes through the asyncRewake watcher and acks via MCP', {
  skip: process.env.RUN_CLAUDE_ACCEPTANCE !== '1', timeout: (idleS + 300) * 1000,
}, async t => {
  if (!loggedIn()) { t.diagnostic('UNVERIFIED: plugin wake — set M12_BOB_HOME to a logged-in scratch HOME under $TMPDIR (operator step)'); t.skip('UNVERIFIED: no logged-in scratch config dir'); return; }
  const configDir = join(home, '.claude'), env = { ...process.env, HOME: home, CLAUDE_CONFIG_DIR: configDir };
  for (const key of ['BROKER_TOKEN', 'TEAM_BROKER_TOKEN', 'BROKER_TOKEN_FILE']) delete env[key];
  const dir = testTempDir('plugin-wake-'), out = join(dir, 'bundle'), project = join(dir, 'project'), id = randomUUID(), debugLog = join(dir, `m12-bob-${id}.debug.log`);
  mkdirSync(project, { mode: 0o700 });
  const claude = args => spawnSync('claude', args, { cwd: project, env, encoding: 'utf8' });
  assert.equal(spawnSync(process.execPath, [fileURLToPath(new URL('../../scripts/build-plugin.mjs', import.meta.url)), '--out', out], { stdio: 'ignore', env: { ...process.env, TMPDIR: dir } }).status, 0);
  assert.equal(claude(['plugin', 'marketplace', 'add', out]).status, 0);
  assert.equal(claude(['plugin', 'install', PLUGIN_ID, '--scope', 'local']).status, 0);
  let child;
  t.after(() => {
    child?.kill('SIGTERM');
    claude(['plugin', 'uninstall', PLUGIN_ID, '--scope', 'local']); claude(['plugin', 'marketplace', 'remove', 'team-messaging-local']);
    rmSync(dir, { recursive: true, force: true });
  });
  const fixture = await brokerFixture(t, { defaultOwner: 'bob' });
  const credentials = join(dir, 'credentials'); mkdirSync(credentials, { mode: 0o700 });
  const tokenFile = join(credentials, 'bob.token'); writeFileSync(tokenFile, tokens.bob, { mode: 0o600 });
  assert.equal(writePluginConfig(PLUGIN_ID, { broker_url: fixture.url, token_file: tokenFile, workspace: 'default', topic: '', session_id: `wake-${id}`, mode: 'poll', arm: 'rewake' }, { env }), 'registered');
  const session = randomUUID();
  child = spawn('script', ['-q', '-c', `claude --debug-file ${debugLog} --session-id ${session} 'say ready'`, '/dev/null'], { cwd: project, env, stdio: ['pipe', 'ignore', 'ignore'] });
  // Best effort: accept the workspace trust dialog if it appears.
  for (const ms of [3000, 6000]) setTimeout(() => child.stdin.write('\r'), ms).unref();
  assert.ok(await until(() => /ready/i.test(transcriptOf(configDir, session)), 90000), 'the session answered the first prompt');
  await new Promise(r => setTimeout(r, idleS * 1000));
  fixture.broker.heartbeat('alice', 'a');
  const sent = fixture.broker.send('alice', 'a', { to: 'user:bob', body: 'Reply with the word pong.' });
  assert.equal(fixture.store.get('SELECT acked_at FROM messages WHERE id=?', sent.id).acked_at, null);
  assert.ok(await until(() => fixture.store.get('SELECT acked_at FROM messages WHERE id=?', sent.id).acked_at !== null, 120000), 'the idle session woke and acked within 120 s');
  const transcript = transcriptOf(configDir, session), wake = transcript.indexOf(`team-message id=${sent.id} `), ack = transcript.search(new RegExp(`"name":"mcp__plugin_team-messaging_team-messaging__ack","input":\\{"id":${sent.id}\\}`));
  assert.ok(wake >= 0 && ack > wake, 'the transcript shows the rewake line before the MCP ack tool call');
  assert.equal(fixture.requests.filter(r => r.path.startsWith('/ack')).length, 1, 'only the MCP server acked');
  const log = existsSync(debugLog) ? readFileSync(debugLog, 'utf8') : '';
  assert.ok(/asyncRewake|Stop/.test(log), 'positive control: hook activity reached the debug log');
  assert.equal(log.split(tokens.bob).length - 1, 0, 'no token in the debug log');
  t.diagnostic('Real Claude Code (terminal, script PTY) woke from idle through the asyncRewake Stop hook and acked through MCP.');
});
