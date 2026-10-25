// Real-process coverage for the operator helpers in scripts/ops, run against a
// broker child started from a scratch token file, the way docs/setup.md tells
// an operator to run them. Every check that an invite code or token stays out
// of output looks at the scripts' own stdout and stderr.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, lstatSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { testTempDir } from '../../test-support/temp.mjs';
import { eventually } from '../../test-support/topic-processes.mjs';
import { scratch, cli } from '../../test-support/client-config.mjs';
import { credentialPath } from '../credentials.mjs';

const ops = name => fileURLToPath(new URL(`../../scripts/ops/${name}`, import.meta.url));
// alice is the FIRST key, so with no BROKER_DEFAULT_OWNER she owns `default`
// (broker/workspaces.mjs ensureDefaultWorkspace).
const tokens = { alice: 'alice-ops-scripts-fixture-000000000000', bob: 'bob-ops-scripts-fixture-00000000000000' };
// The helpers are bash + curl + jq; without them there is nothing to run.
const missing = ['bash', 'curl', 'jq'].filter(tool => spawnSync('sh', ['-c', `command -v ${tool}`]).status !== 0);
const skip = missing.length ? `needs ${missing.join(', ')} on PATH` : false;
const inviteCode = /^tmi_[0-9a-f]{32}_[A-Za-z0-9_-]{43}$/;

async function freshBroker(t, extra = {}) {
  const dir = testTempDir('ops-scripts-'), tokenPath = join(dir, 'tokens.json');
  writeFileSync(tokenPath, JSON.stringify(tokens), { mode: 0o600 });
  const env = { ...process.env, BROKER_HOST: '127.0.0.1', BROKER_PORT: '0', BROKER_DB: join(dir, 'broker.sqlite'), BROKER_TOKENS: tokenPath, RETENTION_ENABLED: '0', BROKER_WEBHOOK_URL: '', ...extra };
  delete env.BROKER_DEFAULT_OWNER;
  const child = spawn(process.execPath, ['broker/main.mjs'], { cwd: fileURLToPath(new URL('../../', import.meta.url)), env, stdio: ['ignore', 'ignore', 'pipe'] });
  const exited = new Promise(resolve => child.once('exit', resolve));
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM'); await exited; rmSync(dir, { recursive: true, force: true }); });
  let buffer = '', port;
  child.stderr.on('data', chunk => { buffer += chunk; for (const line of buffer.split('\n')) { try { const event = JSON.parse(line); if (event.event === 'broker_listening') port = event.address.port; } catch {} } });
  await eventually(() => Boolean(port));
  const scripts = join(dir, 'tmp'); mkdirSync(scripts, { mode: 0o700 });
  return { dir, tokenPath, url: `http://127.0.0.1:${port}`, tmp: scripts };
}
// Runs one helper with only the variables an operator would set.
async function run(b, script, args, scope = {}) {
  const env = { PATH: process.env.PATH, HOME: b.dir, TMPDIR: b.tmp, TM_URL: b.url, TM_TOKENS: b.tokenPath, ...scope };
  const child = spawn(ops(script), args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '', stderr = ''; child.stdout.on('data', c => { stdout += c; }); child.stderr.on('data', c => { stderr += c; });
  const code = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
  const lines = stdout.trim().split('\n');
  return { code, stdout, stderr, http: lines.at(-1), body: (() => { try { return JSON.parse(lines[0]); } catch { return null; } })() };
}
const owner = (workspace, session) => ({ TM_WORKSPACE: workspace, TM_SESSION: session });
const noSecret = (result, secrets) => { for (const s of secrets) assert.ok(!(result.stdout + result.stderr).includes(s), 'a secret reached script output'); };
async function ownerSetup(b, workspace, topic, out) {
  if (workspace !== 'default') assert.equal((await run(b, 'tm.sh', ['static:alice', 'POST', '/workspaces', JSON.stringify({ name: workspace })])).http, 'HTTP 201');
  const scope = owner(workspace, `alice-${workspace}`);
  assert.equal((await run(b, 'tm.sh', ['static:alice', 'POST', '/topics', JSON.stringify({ name: topic })], scope)).http, 'HTTP 201');
  assert.equal((await run(b, 'tm.sh', ['static:alice', 'POST', '/topics/subscribe', JSON.stringify({ name: topic })], scope)).http, 'HTTP 200');
  const invite = await run(b, 'tm-invite.sh', ['static:alice', topic, out], scope);
  assert.equal(invite.code, 0, invite.stdout + invite.stderr);
  return invite;
}

test('ops scripts create a workspace, topic and a 0600 invite file without printing the code', { skip }, async t => {
  const b = await freshBroker(t), out = join(b.dir, 'invite.code'), before = Date.now();
  const invite = await ownerSetup(b, 'myproject', 'dev', out);
  assert.equal(statSync(out).mode & 0o777, 0o600);
  const code = readFileSync(out, 'utf8').trim();
  assert.match(code, inviteCode);
  noSecret(invite, [code, tokens.alice]);
  const meta = JSON.parse(invite.stdout.split('\n')[0]);
  assert.match(meta.invite_id, /^[0-9a-f]{32}$/); assert.equal(meta.code, undefined);
  // INVITE_TTL defaults to 3600 s (broker/config.mjs inviteTtl).
  assert.ok(meta.expires >= before + 3600_000 && meta.expires <= Date.now() + 3600_000);
  const listed = await run(b, 'tm.sh', ['static:alice', 'GET', '/workspaces']);
  assert.ok(listed.body.workspaces.some(w => w.name === 'myproject'));
});

test('a static non-owner cannot register itself and gets in only through tm-join-static', { skip }, async t => {
  const b = await freshBroker(t), out = join(b.dir, 'bob.code');
  await ownerSetup(b, 'default', 'main', out);
  const code = readFileSync(out, 'utf8').trim();
  // bob is a static member of default but owns nothing and has no topic.
  const level = await run(b, 'tm.sh', ['static:bob', 'POST', '/heartbeat', '{}'], owner('default', 'bob-default'));
  assert.equal(level.http, 'HTTP 403'); assert.equal(level.body.error, 'TOPIC_REQUIRED'); assert.equal(level.code, 1, 'a non-2xx status exits non-zero');
  const topical = await run(b, 'tm.sh', ['static:bob', 'POST', '/topics/subscribe', JSON.stringify({ name: 'main' })], { ...owner('default', 'bob-main'), TM_TOPIC: 'main' });
  assert.equal(topical.http, 'HTTP 403'); assert.equal(topical.body.error, 'NOT_SUBSCRIBED'); assert.equal(topical.code, 1);
  const joined = await run(b, 'tm-join-static.sh', ['bob', out]);
  assert.equal(joined.code, 0); assert.equal(joined.http, 'HTTP 201');
  assert.deepEqual(joined.body, { user: 'bob', workspace: 'default', topic: 'main' });
  assert.equal(existsSync(out), false, 'the code file must be removed');
  noSecret(joined, [code, tokens.bob]);
  const after = await run(b, 'tm.sh', ['static:bob', 'POST', '/heartbeat', '{}'], { ...owner('default', 'bob-main-2'), TM_TOPIC: 'main' });
  assert.equal(after.http, 'HTTP 200'); assert.equal(after.code, 0);
});

test('an existing minted identity joins a second workspace with its saved credential', { skip }, async t => {
  const b = await freshBroker(t), s = scratch(t), first = join(b.dir, 'carol-1.code'), second = join(b.dir, 'carol-2.code');
  await ownerSetup(b, 'default', 'main', first);
  const code1 = readFileSync(first, 'utf8');
  const minted = await cli(s.env, ['join', b.url, '-', '--name', 'carol', '--client', 'none'], code1);
  assert.equal(minted.code, 0, minted.stderr);
  const path = credentialPath(b.url, 'carol', { env: s.env });
  assert.equal(JSON.parse(minted.stdout).credential_path, path);
  assert.equal(statSync(path).mode & 0o777, 0o600);
  const token = readFileSync(path, 'utf8');
  noSecret(minted, [code1.trim(), token]);

  await ownerSetup(b, 'myproject', 'dev', second);
  const code2 = readFileSync(second, 'utf8');
  const again = await cli(s.env, ['join', b.url, '-', '--name', 'carol', '--client', 'none'], code2);
  assert.equal(again.code, 0, again.stderr);
  // Observed: the CLI finds the saved credential for this URL and name, sends
  // it as the bearer, and the broker adds the membership without minting.
  assert.deepEqual(JSON.parse(again.stdout), { user: 'carol', workspace: 'myproject', topic: 'dev', credential_path: path, adapters: [] });
  assert.equal(readFileSync(path, 'utf8'), token, 'the credential file is reused unchanged');
  noSecret(again, [code2.trim(), token]);
  for (const [workspace, topic] of [['default', 'main'], ['myproject', 'dev']]) {
    const beat = await run(b, 'tm.sh', [`file:${path}`, 'POST', '/heartbeat', '{}'], { ...owner(workspace, `carol-${workspace}`), TM_TOPIC: topic });
    assert.equal(beat.http, 'HTTP 200', `${workspace}/${topic}`);
  }

  // The credential name hashes the broker URL: another spelling of the same
  // broker finds no credential, joins anonymously and hits the reserved name.
  const third = join(b.dir, 'carol-3.code');
  assert.equal((await run(b, 'tm-invite.sh', ['static:alice', 'dev', third], owner('myproject', 'alice-myproject'))).code, 0);
  const other = await cli(s.env, ['join', b.url.replace('127.0.0.1', 'localhost'), '-', '--name', 'carol', '--client', 'none'], readFileSync(third, 'utf8'));
  assert.equal(other.code, 1); assert.equal(JSON.parse(other.stderr).error, 'USER_EXISTS');
});

// Review round 1: an existing out-file kept its own mode under `> "$out"`, and
// a symlink sent the code to its target. Both are now refused before any
// invite is created.
test('tm-invite refuses an existing file or symlink as out-file and creates no invite', { skip }, async t => {
  const b = await freshBroker(t), scope = owner('default', 'alice-default');
  assert.equal((await run(b, 'tm.sh', ['static:alice', 'POST', '/topics', JSON.stringify({ name: 'main' })], scope)).http, 'HTTP 201');
  const existing = join(b.dir, 'existing.code'), target = join(b.dir, 'target'), link = join(b.dir, 'link.code');
  writeFileSync(existing, 'keep me\n', { mode: 0o644 }); symlinkSync(target, link);
  for (const out of [existing, link]) {
    const refused = await run(b, 'tm-invite.sh', ['static:alice', 'main', out], scope);
    assert.equal(refused.code, 2); assert.match(refused.stderr, /already exists/);
    assert.doesNotMatch(refused.stdout + refused.stderr, /tmi_/);
  }
  assert.equal(readFileSync(existing, 'utf8'), 'keep me\n'); assert.equal(statSync(existing).mode & 0o777, 0o644);
  assert.ok(lstatSync(link).isSymbolicLink()); assert.equal(existsSync(target), false);
  const listed = await run(b, 'tm.sh', ['static:alice', 'GET', '/invites'], scope);
  assert.deepEqual(listed.body.invites, []);
  assert.deepEqual(readdirSync(b.dir).filter(n => n.startsWith('.tm-invite')), []);
});

test('tm-invite reports a refused invite and writes no file', { skip }, async t => {
  const b = await freshBroker(t), out = join(b.dir, 'bob.code');
  assert.equal((await run(b, 'tm.sh', ['static:alice', 'POST', '/topics', JSON.stringify({ name: 'main' })], owner('default', 'alice-default'))).http, 'HTTP 201');
  const refused = await run(b, 'tm-invite.sh', ['static:bob', 'main', out], owner('default', 'bob-default'));
  assert.equal(refused.code, 1); assert.equal(refused.http, 'HTTP 403'); assert.equal(refused.body.error, 'TOPIC_REQUIRED');
  assert.equal(existsSync(out), false);
  assert.deepEqual(readdirSync(b.dir).filter(n => n.startsWith('.tm-invite')), []);
});

// Review round 1: the code file used to be removed on any outcome, losing an
// invite that a network error, 4xx or 429 left unused.
test('tm-join-static keeps the code file unless the join returns HTTP 201', { skip }, async t => {
  const b = await freshBroker(t, { JOIN_RATE_PER_MIN: '1' }), file = join(b.dir, 'bad.code');
  const code = 'tmi_' + '0'.repeat(32) + '_' + 'A'.repeat(43);
  writeFileSync(file, code + '\n', { mode: 0o600 });
  const invalid = await run(b, 'tm-join-static.sh', ['bob', file]);
  assert.equal(invalid.code, 1); assert.equal(invalid.http, 'HTTP 403'); assert.equal(invalid.body.error, 'INVITE_INVALID');
  assert.equal(existsSync(file), true, 'kept after a 4xx');
  const limited = await run(b, 'tm-join-static.sh', ['bob', file]);
  assert.equal(limited.code, 1); assert.equal(limited.http, 'HTTP 429');
  assert.equal(existsSync(file), true, 'kept after a 429');
  // Port 1 refuses connections, so curl fails before any HTTP status.
  const offline = await run(b, 'tm-join-static.sh', ['bob', file], { TM_URL: 'http://127.0.0.1:1' });
  assert.notEqual(offline.code, 0); assert.equal(offline.http, 'HTTP 000');
  assert.equal(existsSync(file), true, 'kept after a network error');
  for (const r of [invalid, limited, offline]) noSecret(r, [code, tokens.bob]);
  assert.deepEqual(readdirSync(b.tmp), [], 'the body file is always removed');
});

test('tm.sh refuses a missing or empty token before calling the broker', { skip }, async t => {
  const b = await freshBroker(t), empty = join(b.dir, 'empty.token');
  writeFileSync(empty, '', { mode: 0o600 });
  for (const src of ['static:nobody', `file:${empty}`]) {
    const r = await run(b, 'tm.sh', [src, 'GET', '/workspaces']);
    assert.equal(r.code, 2); assert.equal(r.stdout, ''); assert.equal(r.stderr.trim(), `tm.sh: no token for ${src}`);
  }
});

test('each ops script prints its usage line on wrong arguments', { skip }, async t => {
  const b = await freshBroker(t);
  for (const script of ['tm.sh', 'tm-invite.sh', 'tm-join-static.sh']) {
    const r = await run(b, script, []);
    assert.equal(r.code, 2); assert.match(r.stderr, new RegExp(`^usage: ${script.replace('.', '\\.')} `));
  }
  const bad = await run(b, 'tm.sh', ['bearer:x', 'GET', '/workspaces']);
  assert.equal(bad.code, 2); assert.match(bad.stderr, /bad token source/);
});
