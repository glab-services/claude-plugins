// 0.11.1 F5 (D4): the serving record. The active MCP server writes
// <session>.serving once the host has listed its tools; the Stop hook, the
// watcher and SessionStart wake or open the team-message window only while
// that record names a live process of this session and this root.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { pluginScratch } from './helpers.mjs';
import { clearServing, markServing, serving } from '../agent-live.mjs';

const recordPath = (s, session) => join(s.data, 'state', `${session}.serving`);
const root = (s, name) => { const dir = join(s.dir, name); mkdirSync(dir, { recursive: true, mode: 0o700 }); return dir; };
// A pid that existed and is gone: a child that has exited and been reaped.
async function deadPid() {
  const child = spawn(process.execPath, ['-e', '0'], { stdio: 'ignore' });
  await new Promise(resolve => child.once('close', resolve));
  return child.pid;
}
const startOf = pid => { const text = readFileSync(`/proc/${pid}/stat`, 'utf8'); return text.slice(text.lastIndexOf(')') + 2).split(' ')[19]; };

test('serving is true only for a live process\'s record of this session and root', async t => {
  const s = pluginScratch(t), home = { home: s.home }, project = root(s, 'project'), other = root(s, 'other');
  assert.throws(() => markServing(s.data, 'bad id', project, home), { code: 'SESSION_ID_UNKNOWN' });
  assert.throws(() => markServing(s.data, 'sess', 'relative/root', home), { code: 'PROJECT_UNKNOWN' });
  assert.equal(serving(s.data, 'sess', project, home), false, 'no record');
  markServing(s.data, 'sess', project, home);
  const record = JSON.parse(readFileSync(recordPath(s, 'sess'), 'utf8'));
  assert.deepEqual(Object.keys(record), ['v', 'pid', 'start', 'root', 'at']);
  assert.deepEqual([record.v, record.pid, record.start, record.root], [1, process.pid, startOf(process.pid), realpathSync(project)]);
  assert.equal(serving(s.data, 'sess', project, home), true, 'positive control: this live process');
  assert.equal(serving(s.data, 'sess', other, home), false, 'another root');
  assert.equal(serving(s.data, 'other-sess', project, home), false, 'another session');
  assert.equal(serving(s.data, 'bad id', project, home), false, 'an invalid session');
  // Another uid: without root no foreign file can be made, so getuid is
  // shifted as in project-agent.test.mjs; readRecord then sees a foreign owner.
  const uid = process.getuid; process.getuid = () => uid() + 1;
  try { assert.equal(serving(s.data, 'sess', project, home), false, 'a record owned by another uid'); } finally { process.getuid = uid; }
  assert.equal(serving(s.data, 'sess', project, home), true);
  const dead = await deadPid();
  markServing(s.data, 'sess', project, { ...home, pid: dead });
  assert.equal(serving(s.data, 'sess', project, home), false, 'a dead pid');
  const write = value => writeFileSync(recordPath(s, 'sess'), typeof value === 'string' ? value : JSON.stringify(value), { mode: 0o600 });
  write({ v: 1, pid: process.pid, start: String(Number(startOf(process.pid)) + 1), root: realpathSync(project), at: Date.now() });
  assert.equal(serving(s.data, 'sess', project, home), false, 'a wrong start time (a reused pid)');
  for (const malformed of ['not json', { v: 2, pid: process.pid, start: null, root: realpathSync(project) }, { v: 1, pid: -1, start: null, root: realpathSync(project) }, { v: 1, pid: '1', start: null, root: realpathSync(project) }, { v: 1, pid: process.pid, start: null }]) {
    write(malformed);
    assert.equal(serving(s.data, 'sess', project, home), false, JSON.stringify(malformed));
  }
  write({ v: 1, pid: process.pid, start: null, root: realpathSync(project), at: Date.now() });
  assert.equal(serving(s.data, 'sess', project, home), true, 'positive control: a null start is a pid check');
  const elsewhere = join(s.dir, 'elsewhere.serving');
  writeFileSync(elsewhere, readFileSync(recordPath(s, 'sess'))); rmSync(recordPath(s, 'sess')); symlinkSync(elsewhere, recordPath(s, 'sess'));
  assert.equal(serving(s.data, 'sess', project, home), false, 'a symlinked record');
  assert.equal(serving(join(s.dir, 'missing-data'), 'sess', project, home), false, 'a missing data directory');
});

test('clearServing removes only its own pid\'s record', async t => {
  const s = pluginScratch(t), home = { home: s.home }, project = root(s, 'project');
  clearServing(s.data, 'sess', home);
  clearServing(s.data, 'bad id', home);
  markServing(s.data, 'sess', project, home);
  clearServing(s.data, 'sess', { ...home, pid: await deadPid() });
  assert.equal(existsSync(recordPath(s, 'sess')), true, 'another pid\'s clear keeps the record');
  clearServing(s.data, 'other-sess', home);
  assert.equal(existsSync(recordPath(s, 'sess')), true, 'another session\'s clear keeps the record');
  clearServing(s.data, 'sess', home);
  assert.equal(existsSync(recordPath(s, 'sess')), false, 'the owner\'s clear removes it');
});
