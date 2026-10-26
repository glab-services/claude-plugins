import { closeSync, constants, fchmodSync, fstatSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, parse } from 'node:path';
import { createHash, createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import { ChannelError } from './config.mjs';
import { topicPattern } from '../shared/address.mjs';
import { stateDir } from './watch-state.mjs';

// Plan project-options P-1: a project is bound only by this key in its
// .claude/settings.local.json, under pluginConfigs[PLUGIN_ID].options.
export const PLUGIN_ID = 'team-messaging@team-messaging-local';
const MAX_BYTES = 1024 * 1024;
const NAME = /^[A-Za-z0-9_.-]{1,128}$/, TOPIC = /^[A-Za-z0-9_./-]{1,128}$/;
export const CLAUDE_SESSION = /^[A-Za-z0-9_-]{1,128}$/;
const MAC_LABEL = 'team-messaging project binding v1';
const KEYS = ['version', 'claude_session_id', 'bindings', 'mac'];
const hints = {
  PROJECT_OPTIONS_INVALID: 'This project\'s .claude/settings.local.json holds a team-messaging entry that is not a valid binding; remove that entry, then run /team-messaging:bind or /team-messaging:join.',
  PROJECT_OPTIONS_UNTRUSTED: 'This project\'s team-messaging binding was not written for this checkout by this user (copied, committed, edited, or not private); remove the entry, then run /team-messaging:bind or /team-messaging:join.',
  PROJECT_OPTIONS_UNSUPPORTED: 'This project\'s team-messaging binding was written by a newer plugin; upgrade the plugin.',
  PROJECT_UNKNOWN: 'The project directory is unknown; start Claude Code in the project, or pass --project <dir>.',
  PROJECT_WRITE_FAILED: 'Could not durably write .claude/settings.local.json; check that the project and its .claude directory are yours and writable.',
};
export const projectError = code => new ChannelError(code, hints[code]);
export const projectHint = code => hints[code];

// AM-3: Claude Code validates pluginConfigs values and skips the whole
// settings.local.json for an array of objects, so a binding is one string.
// ":" is outside every name pattern, so the split is unambiguous.
export function encodeBinding({ workspace, topic, session_id }) { return `${workspace}:${topic ?? ''}:${session_id}`; }
function decodeBinding(text) {
  const parts = typeof text === 'string' ? text.split(':') : [];
  if (parts.length !== 3) return null;
  const [workspace, topic, session_id] = parts;
  const valid = NAME.test(workspace) && workspace !== 'broker' && (topic === '' || (TOPIC.test(topic) && topicPattern.test(topic))) && NAME.test(session_id) && session_id !== 'broker';
  return valid ? { workspace, topic: topic === '' ? null : topic, session_id } : null;
}

// P-5 with AM-3. A version or binding count this release cannot read is
// UNSUPPORTED, checked before the key set, so a newer file is never misread.
function validate(options) {
  if (!options || typeof options !== 'object' || Array.isArray(options)) return { code: 'PROJECT_OPTIONS_INVALID' };
  if (Number.isSafeInteger(options.version) && options.version > 1) return { code: 'PROJECT_OPTIONS_UNSUPPORTED' };
  if (Object.keys(options).length !== KEYS.length || !KEYS.every(key => Object.hasOwn(options, key)) || options.version !== 1) return { code: 'PROJECT_OPTIONS_INVALID' };
  const { claude_session_id, bindings, mac } = options;
  if (typeof claude_session_id !== 'string' || !CLAUDE_SESSION.test(claude_session_id) || typeof mac !== 'string' || !/^[0-9a-f]{64}$/.test(mac)) return { code: 'PROJECT_OPTIONS_INVALID' };
  if (!Array.isArray(bindings) || !bindings.every(item => typeof item === 'string') || bindings.length === 0) return { code: 'PROJECT_OPTIONS_INVALID' };
  if (bindings.length > 1) return { code: 'PROJECT_OPTIONS_UNSUPPORTED' };
  const binding = decodeBinding(bindings[0]);
  if (!binding) return { code: 'PROJECT_OPTIONS_INVALID' };
  return { options: { version: 1, claude_session_id, bindings: [...bindings], mac }, binding };
}

// AM-4: the MAC binds the checkout (root realpath), the broker, the agent and
// the binding to the user's credential. A committed or copied file, or one
// edited by hand, does not verify. Token rotation therefore means a rebind.
export function projectMac(token, { root, url, version = 1, claudeSession, bindings }) {
  const key = createHmac('sha256', token).update(MAC_LABEL).digest();
  return createHmac('sha256', key).update(JSON.stringify([root, url, version, claudeSession, bindings])).digest('hex');
}
export function verifyMac(token, found, url) {
  const expected = Buffer.from(projectMac(token, { root: found.root, url, version: found.options.version, claudeSession: found.options.claude_session_id, bindings: found.options.bindings }), 'hex');
  const actual = Buffer.from(found.options.mac, 'hex');
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

// Reads one candidate without blocking on a FIFO. Returns null when there is
// no file, or { text, stat } for a regular file of at most 1 MiB, or
// { unreadable: true } for anything else, which fails closed once keyed.
function readCandidate(path) {
  let fd;
  try { fd = openSync(path, constants.O_RDONLY | constants.O_NONBLOCK); }
  catch (error) { return ['ENOENT', 'ENOTDIR'].includes(error.code) ? null : { unreadable: true }; }
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.size > MAX_BYTES) return { unreadable: true };
    const text = readFileSync(fd, 'utf8');
    return Buffer.byteLength(text) > MAX_BYTES ? { unreadable: true } : { text, stat };
  } catch { return { unreadable: true }; }
  finally { closeSync(fd); }
}
const ours = stat => stat.uid === process.getuid() && (stat.mode & 0o002) === 0;
// P-7 trust checks, run only for a file that holds our key. Group-write is
// allowed (umask 002); other-write, a foreign owner or a symlink is not.
function trusted(root, path, stat) {
  try {
    const claude = lstatSync(join(root, '.claude')), file = lstatSync(path);
    if (claude.isSymbolicLink() || !claude.isDirectory() || file.isSymbolicLink() || !file.isFile()) return false;
    if (file.dev !== stat.dev || file.ino !== stat.ino || !ours(file) || !ours(claude)) return false;
    // Every directory from the root up to / is ours or root's and not
    // other-writable, so nobody else can swap the path below us.
    for (let dir = root; ; dir = dirname(dir)) {
      const entry = lstatSync(dir);
      if (!entry.isDirectory() || !(entry.uid === 0 || entry.uid === process.getuid()) || (entry.mode & 0o002) !== 0) return false;
      if (dir === parse(dir).root) return true;
    }
  } catch { return false; }
}
// P-7 root walk: from realpath(projectDir) up to the first
// .claude/settings.local.json that has our key. The walk stops after the
// first directory holding .git, else at HOME, else at /, and (AM-10) at the
// first directory that is not ours or root's, or is other-writable.
// Returns { state: 'absent' }, { state: 'invalid', code, path? } or
// { state: 'found', root, path, options, binding, identity }. Never throws.
export function findProject(projectDir, { home } = {}) {
  let start;
  try {
    if (typeof projectDir !== 'string' || !isAbsolute(projectDir)) return { state: 'invalid', code: 'PROJECT_UNKNOWN' };
    start = realpathSync(projectDir);
  } catch { return { state: 'invalid', code: 'PROJECT_UNKNOWN' }; }
  const boundary = typeof home === 'string' && isAbsolute(home) ? realpathSafe(home) : null;
  for (let dir = start; ; dir = dirname(dir)) {
    // AM-10: a directory another user could write to, or one that is not
    // ours or root's (a 1777 /tmp, a shared checkout), ends the walk with
    // nothing found. Another user's file there must not make this project
    // untrusted, and is never read.
    let entry;
    try { entry = lstatSync(dir); } catch { return { state: 'absent' }; }
    if (!(entry.uid === 0 || entry.uid === process.getuid()) || (entry.mode & 0o002) !== 0) return { state: 'absent' };
    const path = join(dir, '.claude', 'settings.local.json');
    // Review round 2 (AM-10 widened): a .claude directory or settings file
    // that is not a symlink and is owned by someone else is that user's, not
    // a binding of ours. It is skipped and the walk goes on, so a group member
    // writing into a group-writable directory of ours cannot make the project
    // untrusted. A symlink, other-write or a bad MAC on our own files still fails.
    const read = foreign(join(dir, '.claude')) || foreign(path) ? null : readCandidate(path);
    if (read?.unreadable) {
      // Without the bytes the key cannot be ruled out, so this fails closed.
      return { state: 'invalid', code: 'PROJECT_OPTIONS_INVALID', path };
    }
    if (read) {
      let data;
      try { data = JSON.parse(read.text); } catch {
        // Bad JSON is ours to refuse only if it mentions our id; otherwise it
        // is somebody else's file and the walk goes on.
        if (read.text.includes(PLUGIN_ID)) return { state: 'invalid', code: 'PROJECT_OPTIONS_INVALID', path };
      }
      const configs = data?.pluginConfigs, entry = configs && typeof configs === 'object' && Object.hasOwn(configs, PLUGIN_ID) ? configs[PLUGIN_ID] : undefined;
      if (entry && typeof entry === 'object' && Object.hasOwn(entry, 'options')) {
        if (!trusted(dir, path, read.stat)) return { state: 'invalid', code: 'PROJECT_OPTIONS_UNTRUSTED', path };
        const checked = validate(entry.options);
        if (checked.code) return { state: 'invalid', code: checked.code, path };
        return { state: 'found', root: dir, path, options: checked.options, binding: checked.binding, identity: identity(read.stat) };
      }
    }
    let git = false;
    try { git = Boolean(lstatSync(join(dir, '.git'), { throwIfNoEntry: false })); } catch {}
    if (git || dir === boundary || dir === parse(dir).root) return { state: 'absent' };
  }
}
function foreign(path) {
  try { const stat = lstatSync(path, { throwIfNoEntry: false }); return Boolean(stat && !stat.isSymbolicLink() && stat.uid !== process.getuid()); }
  catch { return false; }
}
const identity = stat => ({ dev: stat.dev, ino: stat.ino, mtimeMs: stat.mtimeMs, size: stat.size });
// The cheap check for a cached read (P-10): the same inode, mtime and size.
export function sameFile(path, known) {
  try { const stat = lstatSync(path); return stat.dev === known.dev && stat.ino === known.ino && stat.mtimeMs === known.mtimeMs && stat.size === known.size; }
  catch { return false; }
}

// Where bind and join write a new file (P-12): the git root when it is
// inside HOME, else the project directory itself.
export function projectTarget(projectDir, { home } = {}) {
  const start = realpathSync(projectDir), boundary = typeof home === 'string' && isAbsolute(home) ? realpathSafe(home) : null;
  if (boundary) for (let dir = start; ; dir = dirname(dir)) {
    if (lstatSync(join(dir, '.git'), { throwIfNoEntry: false })) return dir === boundary || dir.startsWith(boundary + '/') ? dir : start;
    if (dir === boundary || dir === parse(dir).root) break;
  }
  return start;
}
function realpathSafe(path) { try { return realpathSync(path); } catch { return null; } }

// P-12 write: parse the file (invalid JSON is never touched), set only our
// key, keep the mode (0600 when new; a new .claude is 0700), 2-space indent,
// write a temporary file, fsync, check the inode is still the one parsed,
// rename, fsync the directory.
export function writeProjectOptions(root, options) {
  const dir = join(root, '.claude'), path = join(dir, 'settings.local.json');
  try {
    try { mkdirSync(dir, { mode: 0o700 }); } catch (error) { if (error.code !== 'EEXIST') throw error; }
    const claude = lstatSync(dir);
    if (claude.isSymbolicLink() || !claude.isDirectory() || !ours(claude)) throw projectError('PROJECT_OPTIONS_UNTRUSTED');
    let data = {}, mode = 0o600, known = null, fd;
    try { fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK); }
    catch (error) { if (error.code !== 'ENOENT') throw projectError(error.code === 'ELOOP' ? 'PROJECT_OPTIONS_UNTRUSTED' : 'PROJECT_WRITE_FAILED'); }
    if (fd !== undefined) {
      try {
        const stat = fstatSync(fd);
        if (!stat.isFile() || stat.size > MAX_BYTES || !ours(stat)) throw projectError('PROJECT_OPTIONS_UNTRUSTED');
        try { data = JSON.parse(readFileSync(fd, 'utf8')); } catch { throw projectError('PROJECT_OPTIONS_INVALID'); }
        mode = stat.mode & 0o777; known = stat;
      } finally { closeSync(fd); }
    }
    if (!data || typeof data !== 'object' || Array.isArray(data) || (data.pluginConfigs !== undefined && (!data.pluginConfigs || typeof data.pluginConfigs !== 'object' || Array.isArray(data.pluginConfigs)))) throw projectError('PROJECT_OPTIONS_INVALID');
    const entry = data.pluginConfigs?.[PLUGIN_ID];
    data.pluginConfigs = { ...data.pluginConfigs, [PLUGIN_ID]: { ...(entry && typeof entry === 'object' && !Array.isArray(entry) ? entry : {}), options } };
    const tmp = `${path}.${randomBytes(8).toString('hex')}.tmp`;
    const out = openSync(tmp, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, mode);
    try {
      try { writeFileSync(out, JSON.stringify(data, null, 2) + '\n'); fchmodSync(out, mode); fsyncSync(out); } finally { closeSync(out); }
      const now = lstatSync(path, { throwIfNoEntry: false });
      // Another writer replaced or created the file since it was parsed.
      if (known ? !(now && now.dev === known.dev && now.ino === known.ino) : now) throw projectError('PROJECT_WRITE_FAILED');
      renameSync(tmp, path);
    } catch (error) { try { unlinkSync(tmp); } catch {} throw error; }
    const handle = openSync(dir, constants.O_RDONLY | constants.O_DIRECTORY);
    try { fsyncSync(handle); } finally { closeSync(handle); }
    return path;
  } catch (error) {
    if (error instanceof ChannelError) throw error;
    throw projectError('PROJECT_WRITE_FAILED');
  }
}
// Builds the P-5 options for one binding, with the AM-4 MAC.
export function projectOptions(token, { root, url, claudeSession, binding }) {
  const bindings = [encodeBinding(binding)];
  return { version: 1, claude_session_id: claudeSession, bindings, mac: projectMac(token, { root, url, version: 1, claudeSession, bindings }) };
}

// Private records in ${CLAUDE_PLUGIN_DATA}/state, through the watch-state
// directory checks. P-11: SessionStart records each Claude session's project
// directory, so the CLI and the Bash watcher find the project without a path
// in a shell command (U-6). AM-7: the adopt marker.
export function writeRecord(dataDir, name, value, { home } = {}) {
  const dir = stateDir(dataDir, { home }), path = join(dir, name), tmp = `${path}.${randomBytes(8).toString('hex')}.tmp`;
  const fd = openSync(tmp, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
  try { writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd); } finally { closeSync(fd); }
  try { renameSync(tmp, path); } catch (error) { try { unlinkSync(tmp); } catch {} throw error; }
}
export function readRecord(dataDir, name, { home } = {}) {
  const dir = stateDir(dataDir, { home });
  let fd;
  try { fd = openSync(join(dir, name), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK); }
  catch { return null; }
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.uid !== process.getuid() || stat.size > 65536) return null;
    return JSON.parse(readFileSync(fd, 'utf8'));
  } catch { return null; } finally { closeSync(fd); }
}
// Review round 1: one record per (Claude session, project) pair, named
// <session>.<sha256(project)[0:16]>.session.json, so a session id seen in two
// projects never overwrites the other's record; the newest one is used.
// Records older than RECORD_TTL_MS are pruned on each write, never the one
// just written. Claude session ids hold no ".", so the prefix is exact.
const RECORD_TTL_MS = 30 * 24 * 3600 * 1000, RECORD = /^[A-Za-z0-9_-]{1,128}\.[0-9a-f]{16}\.session\.json$/;
export function writeSessionRecord(dataDir, claudeSession, projectDir, options = {}) {
  if (!CLAUDE_SESSION.test(claudeSession ?? '') || typeof projectDir !== 'string' || !isAbsolute(projectDir)) throw projectError('PROJECT_UNKNOWN');
  const name = `${claudeSession}.${createHash('sha256').update(projectDir).digest('hex').slice(0, 16)}.session.json`, now = options.now ?? Date.now();
  writeRecord(dataDir, name, { project_dir: projectDir, at: now }, options);
  const dir = stateDir(dataDir, options);
  for (const entry of readdirSync(dir)) {
    if (entry === name || !RECORD.test(entry)) continue;
    // Review round 2: another SessionStart can rewrite a record while this
    // loop runs. It is removed only when its recorded `at` is stale too and
    // it is still the same inode just before the unlink. `beforeUnlink` is a
    // test-only seam that runs between those checks; nothing in production
    // passes it.
    try {
      const path = join(dir, entry), first = lstatSync(path);
      if (now - first.mtimeMs <= RECORD_TTL_MS) continue;
      const record = readRecord(dataDir, entry, options);
      if (!(Number.isFinite(record?.at) && now - record.at > RECORD_TTL_MS)) continue;
      options.beforeUnlink?.(entry);
      const last = lstatSync(path);
      if (last.dev === first.dev && last.ino === first.ino) unlinkSync(path);
    } catch {}
  }
}
export function readSessionRecord(dataDir, claudeSession, options) {
  if (!CLAUDE_SESSION.test(claudeSession ?? '')) return null;
  let newest = null;
  for (const entry of readdirSync(stateDir(dataDir, options))) {
    if (!entry.startsWith(`${claudeSession}.`) || !RECORD.test(entry)) continue;
    const record = readRecord(dataDir, entry, options);
    if (typeof record?.project_dir === 'string' && isAbsolute(record.project_dir) && Number.isFinite(record.at) && !(newest && newest.at >= record.at)) newest = record;
  }
  return newest?.project_dir ?? null;
}
export function writeAdoptMarker(dataDir, value, options) { writeRecord(dataDir, 'adopted.json', { root: value.root, claude_session_id: value.claudeSession }, options); }
export function readAdoptMarker(dataDir, options) {
  const record = readRecord(dataDir, 'adopted.json', options);
  return typeof record?.root === 'string' && typeof record.claude_session_id === 'string' ? { root: record.root, claudeSession: record.claude_session_id } : null;
}
