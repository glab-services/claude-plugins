import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync, lstatSync, readFileSync, readdirSync, statSync, symlinkSync, unlinkSync, utimesSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { acquireLock, breakStale, claimUnreported, ended, markEnded, markReported, reported, stateDir, withStateLock } from '../watch-state.mjs';
import { pluginScratch, umaskPluginData } from './helpers.mjs';

const moduleUrl = new URL('../watch-state.mjs', import.meta.url).href;
function child(source, env = {}) {
  const proc = spawn(process.execPath, ['--input-type=module', '--eval', `import * as state from ${JSON.stringify(moduleUrl)};\n${source}`], { env: { ...env, TMPDIR: process.env.TMPDIR }, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '', stderr = ''; proc.stdout.on('data', c => { stdout += c; }); proc.stderr.on('data', c => { stderr += c; });
  return new Promise((resolve, reject) => { proc.once('error', reject); proc.once('close', code => resolve({ code, stdout, stderr, pid: proc.pid })); });
}
const deadPid = async () => (await child('')).pid;

test('invalid session keys are refused before any state file is created', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home });
  for (const key of ['../x', '', 'k'.repeat(129), 'a/b', '..']) {
    for (const call of [() => reported(dir, key), () => ended(dir, key)]) assert.throws(call, { code: 'INVALID_SESSION_KEY' }, key);
    for (const call of [() => markReported(dir, key, [1]), () => claimUnreported(dir, key, [1]), () => markEnded(dir, key, 'X'), () => acquireLock(dir, key, Date.now() + 1000), () => withStateLock(dir, key, () => {})])
      await assert.rejects(call, { code: 'INVALID_SESSION_KEY' }, key);
  }
  assert.deepEqual(readdirSync(dir), []);
  assert.deepEqual(readdirSync(s.data), ['state']);
});

test('the state directory is 0700, state files 0600, and the cap keeps the newest 1000 IDs', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home });
  await markReported(dir, 'k1', Array.from({ length: 1500 }, (_, i) => 1500 - i));
  const ids = reported(dir, 'k1');
  assert.equal(ids.length, 1000); assert.equal(ids[0], 501); assert.equal(ids.at(-1), 1500);
  assert.equal(statSync(dir).mode & 0o777, 0o700);
  for (const file of readdirSync(dir)) assert.equal(statSync(join(dir, file)).mode & 0o777, 0o600, file);
  assert.deepEqual(readdirSync(dir), ['k1.json'], 'locks and temporary files are gone');
  assert.deepEqual(await claimUnreported(dir, 'k1', [1500, 1501, 1501]), [1501]);
  assert.deepEqual(await claimUnreported(dir, 'k1', [1501]), []);
});

test('markEnded reports a terminal state once', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home });
  assert.equal(ended(dir, 'k'), null);
  assert.equal(await markEnded(dir, 'k', 'UNAUTHORIZED'), true);
  assert.equal(await markEnded(dir, 'k', 'NOT_A_MEMBER'), false);
  assert.equal(ended(dir, 'k'), 'UNAUTHORIZED');
  await markReported(dir, 'k', [7]);
  assert.equal(ended(dir, 'k'), 'UNAUTHORIZED', 'marking IDs keeps the terminal state');
});

test('concurrent markReported from several processes loses no ID', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home });
  const runs = Array.from({ length: 8 }, (_, n) => child(`for (let i = 0; i < 25; i++) await state.markReported(${JSON.stringify(dir)}, 'shared', [${n * 100} + i]);`, { HOME: s.home }));
  for (const result of await Promise.all(runs)) assert.equal(result.code, 0, result.stderr);
  assert.equal(reported(dir, 'shared').length, 200);
});

test('a stale state lock is taken over and a live one is waited for', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home }), lock = join(dir, 'k.state.lock');
  writeFileSync(lock, JSON.stringify({ pid: await deadPid(), created: Date.now() }), { mode: 0o600 });
  let started = Date.now();
  assert.equal(await withStateLock(dir, 'k', () => 'dead pid'), 'dead pid');
  assert.ok(Date.now() - started < 500); assert.equal(existsSync(lock), false);
  writeFileSync(lock, JSON.stringify({ pid: process.pid, created: Date.now() - 6000 }), { mode: 0o600 });
  assert.equal(await withStateLock(dir, 'k', () => 'old'), 'old');
  writeFileSync(lock, JSON.stringify({ pid: process.pid, created: Date.now() }), { mode: 0o600 });
  started = Date.now();
  const timer = setTimeout(() => unlinkSync(lock), 300);
  assert.equal(await withStateLock(dir, 'k', () => 'waited'), 'waited');
  assert.ok(Date.now() - started >= 250, 'a live lock is waited for');
  clearTimeout(timer);
  // An unreadable lock may be mid-write: it is waited for while fresh and
  // taken over only once its mtime is older than the stale limit.
  writeFileSync(lock, '', { mode: 0o600 });
  started = Date.now();
  const emptied = setTimeout(() => unlinkSync(lock), 300);
  assert.equal(await withStateLock(dir, 'k', () => 'mid-write'), 'mid-write');
  assert.ok(Date.now() - started >= 250); clearTimeout(emptied);
  writeFileSync(lock, '', { mode: 0o600 }); utimesSync(lock, new Date(Date.now() - 6000), new Date(Date.now() - 6000));
  assert.equal(await withStateLock(dir, 'k', () => 'old empty'), 'old empty');
  writeFileSync(lock, JSON.stringify({ pid: process.pid, created: Date.now() }), { mode: 0o600 });
  started = Date.now();
  await assert.rejects(withStateLock(dir, 'k', () => 'never'), { code: 'STATE_LOCK_TIMEOUT' });
  assert.ok(Date.now() - started >= 1900);
});

test('five concurrent takers of a stale state lock end with exactly one holder, 50 runs', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home }), lock = join(dir, 'many.state.lock'), marker = join(dir, 'many-inside');
  // Watchers re-arm on every Stop, so more than two processes can meet a
  // crash-stale lock. Each holder exits inside the lock without releasing it.
  const body = `import { openSync, closeSync, unlinkSync } from 'node:fs';
    await state.withStateLock(${JSON.stringify(dir)}, 'many', async () => {
      try { closeSync(openSync(${JSON.stringify(marker)}, 'wx')); } catch { process.exit(1); }
      await new Promise(r => setTimeout(r, 10)); unlinkSync(${JSON.stringify(marker)}); process.exit(0);
    });`;
  for (let run = 0; run < 50; run++) {
    writeFileSync(lock, JSON.stringify({ pid: await deadPid(), created: Date.now() }), { mode: 0o600 });
    const results = await Promise.all(Array.from({ length: 5 }, () => child(body, { HOME: s.home })));
    for (const result of results) assert.equal(result.code, 0, `run ${run}: two processes held the state lock at once ${result.stderr}`);
  }
});

test('while one process breaks a stale lock, a second breaker is excluded and cannot delete a fresh lock', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home }), lock = join(dir, 'brk.state.lock');
  const stale = JSON.stringify({ pid: await deadPid(), created: Date.now() }), fresh = JSON.stringify({ pid: process.pid, created: Date.now(), nonce: 'fresh' });
  writeFileSync(lock, stale, { mode: 0o600 });
  let created = false;
  // Runs inside the first breaker's judgment: a second breaker that judged the
  // same stale lock tries to remove it, then an acquirer creates a fresh lock
  // wherever the path is free.
  breakStale(lock, held => {
    breakStale(lock, () => true);
    if (!existsSync(lock)) { writeFileSync(lock, fresh, { mode: 0o600 }); created = true; }
    return held === stale;
  });
  if (created) assert.equal(existsSync(lock) && readFileSync(lock, 'utf8'), fresh, 'a breaker deleted a fresh lock another process created');
  assert.equal(existsSync(lock), created);
  assert.equal(existsSync(lock + '.break'), false, 'the break lock is released');
  writeFileSync(lock + '.break', JSON.stringify({ pid: await deadPid() }), { mode: 0o600 });
  breakStale(lock, () => true);
  assert.equal(existsSync(lock + '.break'), false, 'a dead breaker\'s break lock is removed');
  writeFileSync(lock + '.break', JSON.stringify({ pid: process.pid }), { mode: 0o600 });
  writeFileSync(lock, stale, { mode: 0o600 });
  breakStale(lock, () => true);
  assert.ok(existsSync(lock) && existsSync(lock + '.break'), 'a live breaker is never displaced');
});

test('two concurrent takers of one stale state lock never both hold it', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home }), lock = join(dir, 'race.state.lock'), marker = join(dir, 'inside');
  // A Stop hook and a watcher are the two processes that start together on
  // one key. Each holder exits inside the lock without releasing it, leaving
  // a dead-pid lock for the other.
  const body = `import { openSync, closeSync, unlinkSync } from 'node:fs';
    await state.withStateLock(${JSON.stringify(dir)}, 'race', async () => {
      try { closeSync(openSync(${JSON.stringify(marker)}, 'wx')); } catch { process.exit(1); }
      await new Promise(r => setTimeout(r, 30)); unlinkSync(${JSON.stringify(marker)}); process.exit(0);
    });`;
  for (let round = 0; round < 40; round++) {
    writeFileSync(lock, JSON.stringify({ pid: await deadPid(), created: Date.now() }), { mode: 0o600 });
    const results = await Promise.all(Array.from({ length: 2 }, () => child(body, { HOME: s.home })));
    for (const result of results) assert.equal(result.code, 0, `round ${round}: two processes held the state lock at once ${result.stderr}`);
  }
});

test('a corrupt state file reads as empty and a symlinked one is refused', async t => {
  const s = pluginScratch(t), dir = stateDir(s.data, { home: s.home });
  writeFileSync(join(dir, 'bad.json'), '{not json', { mode: 0o600 });
  assert.deepEqual(reported(dir, 'bad'), []);
  assert.deepEqual(await claimUnreported(dir, 'bad', [3]), [3]);
  writeFileSync(join(s.dir, 'elsewhere.json'), JSON.stringify({ reported: [1] }), { mode: 0o600 });
  symlinkSync(join(s.dir, 'elsewhere.json'), join(dir, 'linked.json'));
  assert.throws(() => reported(dir, 'linked'), { code: 'ELOOP' });
  assert.throws(() => stateDir(''), { code: 'DATA_DIR_REQUIRED' });
});

// Decision 146 (narrowed to watcher state by 149), reproduced from the
// two-node desktop test: Claude Code made the plugin directories 0775 under
// umask 002 and stateDir threw INSECURE_CREDENTIAL_DIR. The 0700 ~/.claude
// above them seals them. No supplementary group is needed, so these run in CI.
for (const mode of [0o775, 0o777])
  test(`stateDir accepts a 0${mode.toString(8)} plugin data dir below a private 0700 ancestor`, t => {
    const s = pluginScratch(t);
    umaskPluginData(s, { mode });
    assert.equal(lstatSync(s.data).mode & 0o777, mode);
    const dir = stateDir(s.data, { home: s.home });
    assert.equal(lstatSync(dir).mode & 0o777, 0o700, 'the state leaf itself stays private');
  });

for (const mode of [0o775, 0o777])
  test(`stateDir refuses a 0${mode.toString(8)} plugin data dir with no sealed ancestor below a 0755 HOME`, t => {
    const s = pluginScratch(t);
    umaskPluginData(s, { mode, claude: 0o755, home: 0o755 });
    assert.throws(() => stateDir(s.data, { home: s.home }), { code: 'INSECURE_CREDENTIAL_DIR', dir: s.data, hint: `${s.data} is group- or world-writable; run chmod go-w '${s.data}'` });
  });
