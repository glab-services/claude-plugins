import { closeSync, constants, existsSync, fsyncSync, linkSync, lstatSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { ChannelError, requireValue } from './config.mjs';
import { ensurePrivateDir } from './credentials.mjs';

// Per-Claude-session watcher state: the IDs already reported to that session,
// a terminal marker, and two O_EXCL locks. Without the reported cursor a
// message the agent leaves unacked would wake every re-armed watcher at once.
const REPORTED_MAX = 1000, LOCK_WAIT_MS = 2000, STATE_LOCK_STALE_MS = 5000;
const flags = constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW;
function checkKey(key) {
  requireValue(typeof key === 'string' && /^[A-Za-z0-9_.-]{1,128}$/.test(key) && !/^\.\.?$/.test(key), 'INVALID_SESSION_KEY', 'Use the host session_id (letters, digits, underscores, dots or hyphens) as the watcher key.');
}
export function stateDir(dataDir, { home } = {}) {
  requireValue(typeof dataDir === 'string' && dataDir.length > 0, 'DATA_DIR_REQUIRED', 'Pass --data-dir ${CLAUDE_PLUGIN_DATA}.');
  const dir = join(dataDir, 'state');
  // Watcher state, not a credential: a sealed ancestor makes the 0775 plugin
  // data directories Claude Code creates under umask 002 acceptable (decision 149).
  ensurePrivateDir(dir, { home, sealedAncestors: true });
  return dir;
}
function read(path) {
  let fd;
  try { fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW); return readFileSync(fd, 'utf8'); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  finally { if (fd !== undefined) closeSync(fd); }
}
// A lock appears with its full content or not at all: the content is
// written to a private temporary file that is then linked into place, and
// link() fails with EEXIST while any lock exists.
function create(path, content) {
  const tmp = `${path}.${randomBytes(8).toString('hex')}.tmp`, fd = openSync(tmp, flags, 0o600);
  try { writeFileSync(fd, content); closeSync(fd); linkSync(tmp, path); }
  finally { try { unlinkSync(tmp); } catch {} }
}
function alive(pid) {
  try { process.kill(pid, 0); return true; } catch (error) { return error.code === 'EPERM'; }
}
function parse(text) { try { return JSON.parse(text); } catch { return null; } }
// Decision 153: a host pid can be reused or linger as a zombie, so the lock
// also records the host's start time, field 22 of /proc/<pid>/stat (clock
// ticks since boot, kept as the decimal string). One parser reads it when the
// lock is written and when it is judged. The comm field can hold spaces or
// parentheses, so fields are counted after the last ')'.
// Returns { state, start }, null when the pid is gone, or undefined when /proc
// is absent (macOS) or unreadable, where the caller falls back to the pid check alone.
function procStat(pid) {
  try { const rest = readFileSync(`/proc/${pid}/stat`, 'utf8'); const fields = rest.slice(rest.lastIndexOf(')') + 2).split(' '); return { state: fields[0], start: fields[19] }; }
  // Any read error other than a missing pid is doubt about /proc, not proof of
  // death, so it also falls back to the pid check.
  catch (error) { return error.code === 'ENOENT' && existsSync('/proc/self/stat') ? null : undefined; }
}
// A host is alive only when its pid exists, it is not a zombie and, when the
// lock recorded one, its start time matches. Without /proc only the pid is checked.
function hostAlive(pid, start) {
  const stat = procStat(pid);
  if (stat === undefined) return alive(pid);
  return stat !== null && stat.state !== 'Z' && (start === undefined || stat.start === start);
}
// Breaking a stale lock is serialized through a separate `<lock>.break`
// lock, so at most one process judges and removes the lock at a time (decision
// 145). Without it, two takers that both judged one stale lock can each remove
// it, and the second removal deletes a fresh lock a third process just created.
// Inside the break lock, the lock is re-read and re-judged: its holder is dead
// or stale, other breakers are excluded, and creation fails while it exists,
// so the unlink removes exactly the lock judged stale. A break lock is stale
// only when its pid is dead; it is moved to a tombstone and linked back if the
// tombstone turns out to be a live breaker's.
// Exported for the deterministic interleaving test in watch-state.test.mjs.
export function breakStale(path, stale) {
  const brk = `${path}.break`, mine = JSON.stringify({ pid: process.pid, nonce: randomBytes(8).toString('hex') });
  try { create(brk, mine); }
  catch (error) {
    if (error.code !== 'EEXIST') throw error;
    const held = read(brk), holder = parse(held);
    if (held !== null && !(holder && alive(holder.pid))) {
      const tombstone = `${brk}.${randomBytes(8).toString('hex')}.stale`;
      try { renameSync(brk, tombstone); } catch (failure) { if (failure.code !== 'ENOENT') throw failure; return; }
      if (read(tombstone) !== held) { try { linkSync(tombstone, brk); } catch {} }
      unlinkSync(tombstone);
    }
    return;
  }
  try { const held = read(path); if (held !== null && stale(held)) unlinkSync(path); }
  finally { if (read(brk) === mine) unlinkSync(brk); }
}
export async function withStateLock(dir, key, fn) {
  checkKey(key);
  const path = join(dir, `${key}.state.lock`), content = JSON.stringify({ pid: process.pid, created: Date.now(), nonce: randomBytes(8).toString('hex') });
  for (const end = Date.now() + LOCK_WAIT_MS; ;) {
    try { create(path, content); break; }
    catch (error) {
      if (error.code !== 'EEXIST') throw error;
      // An unreadable lock (only a damaged file, since creation is atomic) is
      // stale once its mtime is older than the stale limit.
      const stale = held => { const lock = parse(held), age = lock ? Date.now() - lock.created : Date.now() - (lstatSync(path, { throwIfNoEntry: false })?.mtimeMs ?? Date.now()); return age > STATE_LOCK_STALE_MS || Boolean(lock && !alive(lock.pid)); };
      const held = read(path);
      // Retry at once only when the lock is gone; a live breaker or holder is waited for.
      if (held !== null && stale(held)) breakStale(path, stale);
      if (read(path) === null) continue;
      if (Date.now() >= end) throw new ChannelError('STATE_LOCK_TIMEOUT', 'Another watcher or hook holds the session state lock; retry shortly.');
      await sleep(10 + Math.random() * 15);
    }
  }
  try { return await fn(); }
  finally { if (read(path) === content) unlinkSync(path); }
}
function load(dir, key) {
  const state = parse(read(join(dir, `${key}.json`)) ?? '{}') ?? {};
  return { reported: Array.isArray(state.reported) ? state.reported.filter(Number.isSafeInteger) : [], ...(typeof state.ended === 'string' ? { ended: state.ended } : {}) };
}
function save(dir, key, state) {
  const path = join(dir, `${key}.json`), tmp = `${path}.${randomBytes(8).toString('hex')}.tmp`;
  const fd = openSync(tmp, flags, 0o600);
  try { writeFileSync(fd, JSON.stringify(state)); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(tmp, path);
}
export function reported(dir, key) { checkKey(key); return load(dir, key).reported; }
export function markReported(dir, key, ids) {
  return withStateLock(dir, key, () => {
    const state = load(dir, key), merged = [...new Set([...state.reported, ...ids])].sort((a, b) => a - b).slice(-REPORTED_MAX);
    save(dir, key, { ...state, reported: merged });
  });
}
// Returns the IDs not yet reported and marks them in the same locked step, so
// a concurrent Stop hook and watcher never both deliver one message.
export function claimUnreported(dir, key, ids) {
  return withStateLock(dir, key, () => {
    const state = load(dir, key), known = new Set(state.reported), fresh = [...new Set(ids)].filter(id => !known.has(id));
    if (fresh.length) save(dir, key, { ...state, reported: [...new Set([...state.reported, ...fresh])].sort((a, b) => a - b).slice(-REPORTED_MAX) });
    return fresh;
  });
}
// Removes exactly `ids` from the reported set, under the state lock. Used only
// to undo a claim an orphaned watcher made for a host that died meanwhile
// (decision 152), so the resumed session's watcher still wakes for them.
export function unclaim(dir, key, ids) {
  return withStateLock(dir, key, () => {
    const state = load(dir, key), drop = new Set(ids);
    save(dir, key, { ...state, reported: state.reported.filter(id => !drop.has(id)) });
  });
}
// Resolves true only for the first terminal mark, so an ended binding wakes once.
export function markEnded(dir, key, code) {
  return withStateLock(dir, key, () => {
    const state = load(dir, key);
    if (state.ended) return false;
    save(dir, key, { ...state, ended: code });
    return true;
  });
}
export function ended(dir, key) { checkKey(key); return load(dir, key).ended ?? null; }
// One watcher per host session. A live lock means another watcher is armed;
// a lock whose pid is gone or whose deadline passed is taken over.
// Decision 151: the lock also records the watcher's host (its parent, the
// Claude Code process) and is stale once that host is dead. At resume the old
// host is gone but its orphan watcher can live up to one poll or backoff
// interval longer; without this rule the new host's watcher got ALREADY_ARMED,
// the orphan then exited, and nobody was armed. A live host, whether the same
// one (a second watcher of one session) or a different one, keeps its lock.
// Locks written before 151 carry no host and are judged by pid and deadline.
const WATCH_BREAK_TRIES = 3;
export async function acquireLock(dir, key, deadlineMs, host = process.ppid) {
  const hostStart = procStat(host)?.start;
  const path = join(dir, `${key}.watch.lock`), content = JSON.stringify({ pid: process.pid, host, hostStart, deadline: deadlineMs, nonce: randomBytes(8).toString('hex') });
  // Locks from before 153 have no hostStart and are judged by host pid and
  // state; locks from before 151 have no host at all.
  const stale = held => { const lock = parse(held); return !(lock && lock.deadline > Date.now() && alive(lock.pid)) || (Number.isSafeInteger(lock.host) && lock.host > 0 && !hostAlive(lock.host, typeof lock.hostStart === 'string' ? lock.hostStart : undefined)); };
  const owned = await withStateLock(dir, key, () => {
    // Every watcher-lock change happens under the state lock. The takeover
    // itself goes through the decision-145 break lock, which re-reads and
    // re-judges the lock before removing it. A break lock held by a live
    // process means another breaker is mid-way; after a few tries this
    // watcher gives up unarmed rather than spin inside the state lock.
    for (let tries = 0; tries < WATCH_BREAK_TRIES; tries++) {
      try { create(path, content); return true; }
      catch (error) {
        if (error.code !== 'EEXIST') throw error;
        const held = read(path);
        if (held !== null && !stale(held)) return false;
        if (held !== null) breakStale(path, stale);
      }
    }
    return false;
  });
  if (!owned) return null;
  return { release: () => withStateLock(dir, key, () => { if (read(path) === content) unlinkSync(path); }) };
}
