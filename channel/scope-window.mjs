// L3 of the project-scoped sharing plan: the team-message window marker.
// While it is open, the scope hook (scope-hook.mjs) allows only file reads
// inside the project root and our own tools. The MCP server opens it before it
// returns teammate content (server.mjs); SessionStart opens it when pending
// messages go into the context (plugin-session-start.mjs), and the watcher
// and Stop hook before they wake a turn for teammate messages. It closes at
// Stop and StopFailure once nothing is pending, through `team-messaging
// scope-close`, or 2 h after the last teammate content opened it.
//
// FD-1: the marker is keyed by the project root, not the Claude session, so a
// /clear or a second session in the same checkout cannot step outside it:
// ${CLAUDE_PLUGIN_DATA}/state/scope-<sha256(realpath root)>.json, mode 0600,
// {v:1, since, opened, root, claude_session_id}. The session id is kept for
// diagnostics only; the hook never uses it to skip a check.
import { closeSync, constants, fsyncSync, linkSync, lstatSync, openSync, readFileSync, readdirSync, realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { basename, dirname, isAbsolute, join } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import os from 'node:os';
import { ChannelError } from './config.mjs';
import { findProject } from './project-options.mjs';
import { stateDir } from './watch-state.mjs';

// Review r1 C1 (replaces D-13's refresh): a window ends this long after
// `since`, the last time teammate content opened it. No hook decision moves
// it, so a stream of denials cannot keep strict mode on forever.
export const WINDOW_TTL_MS = 2 * 3600 * 1000;
const UNAVAILABLE = 'The team-message window could not be recorded, so teammate content is withheld; check the plugin data directory, then retry.';
export const windowUnavailable = () => new ChannelError('SCOPE_WINDOW_UNAVAILABLE', UNAVAILABLE);
const flags = constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW;

// Pure: the marker path for a root that is already a realpath.
export function windowPath(dataDir, root) {
  return join(dataDir, 'state', `scope-${createHash('sha256').update(root).digest('hex')}.json`);
}
// FD-1: the root the window is keyed by, from the same 0.10 root walk the
// binding resolution uses (findProject), so the server, SessionStart and the
// hook find one marker. A binding the plugin refuses still names its root
// (the directory holding .claude/settings.local.json), so a marker written
// before the binding broke stays in force. Returns null when the walk finds
// no binding at all, where no window can have been opened; throws when the
// project directory is unknown, which the hook turns into deny.
export function bindingRoot(projectDir, { home } = {}) {
  const found = findProject(projectDir, { home });
  if (found.state === 'found') return realpathSync(found.root);
  if (found.state === 'invalid' && typeof found.path === 'string') return realpathSync(dirname(dirname(found.path)));
  if (found.state === 'absent') return null;
  throw new ChannelError('PROJECT_UNKNOWN', 'The project directory is unknown.');
}

// Reads the marker without following a symlink. Returns null when there is
// none; any other failure (EISDIR, ENOTDIR, EACCES, ELOOP) throws, because a
// marker that cannot be read may be an open window (D-21).
function readMarker(path) {
  let fd;
  try { fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  try { return readFileSync(fd, 'utf8'); } finally { closeSync(fd); }
}
// Atomic write in the marker's own directory: a private temporary file,
// fsync, rename (the watch-state save pattern).
function writeMarker(path, value) {
  const tmp = `${path}.${randomBytes(8).toString('hex')}.tmp`, fd = openSync(tmp, flags, 0o600);
  try { writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd); } finally { closeSync(fd); }
  try { renameSync(tmp, path); } catch (error) { try { unlinkSync(tmp); } catch {} throw error; }
}
const valid = marker => marker && marker.v === 1 && Number.isFinite(marker.since) && Number.isFinite(marker.opened) && (marker.root === null || (typeof marker.root === 'string' && isAbsolute(marker.root)));
function parse(text) { try { const marker = JSON.parse(text); return valid(marker) ? marker : undefined; } catch { return undefined; } }

// D-14: a root of HOME or / is recorded as null, and every file read is then
// denied, because "inside the root" would mean the whole home or machine.
function recordedRoot(real, home) {
  const homes = new Set();
  for (const value of [home, os.homedir(), (() => { try { return os.userInfo().homedir; } catch { return undefined; } })()]) {
    if (typeof value !== 'string' || !isAbsolute(value)) continue;
    homes.add(value);
    try { homes.add(realpathSync(value)); } catch {}
  }
  return real === '/' || homes.has(real) ? null : real;
}

// Opens the window, or moves it on: new teammate content sets `since` and
// `opened` to now (review r1 C1). The existing marker is still read first, so
// a path that is not a readable file fails here. Every failure is
// SCOPE_WINDOW_UNAVAILABLE, so the caller withholds the teammate content
// (fail closed).
export function openWindow(dataDir, session, root, { home, now = Date.now() } = {}) {
  try {
    if (typeof dataDir !== 'string' || !isAbsolute(dataDir) || typeof root !== 'string' || !isAbsolute(root)) throw windowUnavailable();
    stateDir(dataDir, { home });
    const real = realpathSync(root), path = windowPath(dataDir, real);
    readMarker(path);
    writeMarker(path, { v: 1, since: now, opened: now, root: recordedRoot(real, home), claude_session_id: typeof session === 'string' ? session : null });
  } catch (error) {
    if (error instanceof ChannelError && error.code === 'SCOPE_WINDOW_UNAVAILABLE') throw error;
    throw windowUnavailable();
  }
}
// Returns { state: 'closed' }, { state: 'open', marker } or { state: 'error' }.
// An expired marker is removed and reads as closed; an unparsable one is an
// error (the hook denies).
export function readWindow(dataDir, root, { now = Date.now() } = {}) {
  const path = windowPath(dataDir, root);
  let text;
  try { text = readMarker(path) ?? closingMarker(path); } catch { return { state: 'error' }; }
  if (text === null) return { state: 'closed' };
  const marker = parse(text);
  if (!marker) return { state: 'error' };
  if (now - marker.since > WINDOW_TTL_MS) {
    try { unlinkSync(path); } catch (error) { if (error.code !== 'ENOENT') return { state: 'error' }; }
    return { state: 'closed' };
  }
  return { state: 'open', marker };
}
// Closes the window; an absent marker is already closed. A marker path that
// is not a regular file is left alone and reported, so a failed close leaves
// more denials, never fewer.
// Review r1 S4: with `expected` (the marker read before a slow check), the
// close is compare-and-delete. The marker is first renamed aside, which takes
// exactly the file in place at that instant; if it is not the expected one (a
// reopen happened meanwhile) it is linked back, and link() fails with EEXIST
// when an even newer marker already took its place. A concurrent reader can
// see the window closed only between the rename and the link-back.
export function closeWindow(dataDir, root, expected) {
  const path = windowPath(dataDir, root);
  let stat;
  try { stat = lstatSync(path); } catch (error) { if (error.code === 'ENOENT') return false; throw error; }
  if (!stat.isFile()) throw new ChannelError('SCOPE_WINDOW_INVALID', 'The team-message window marker is not a regular file.');
  if (expected === undefined) {
    try { unlinkSync(path); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    return true;
  }
  const aside = `${path}.${randomBytes(8).toString('hex')}.closing`;
  try { renameSync(path, aside); } catch (error) { if (error.code === 'ENOENT') return false; throw error; }
  const taken = parse(readMarker(aside) ?? '');
  if (taken && taken.since === expected.since && taken.opened === expected.opened) { unlinkSync(aside); return true; }
  // EEXIST: an even newer marker is in place, and this one can go.
  try { linkSync(aside, path); } catch (error) { if (error.code !== 'EEXIST') { renameSync(aside, path); return false; } }
  unlinkSync(aside);
  return false;
}
// Review r2 SS5: while a Stop's compare-and-delete has the marker renamed
// aside (`<marker>.<hex>.closing`), the window still counts as open.
function closingMarker(path) {
  let names;
  try { names = readdirSync(dirname(path)); } catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  const prefix = basename(path) + '.', aside = names.find(name => name.startsWith(prefix) && /^[0-9a-f]{16}\.closing$/.test(name.slice(prefix.length)));
  return aside === undefined ? null : readMarker(join(dirname(path), aside)) ?? null;
}
// Review r1 C2: whether any window marker exists in this data directory, for
// a hook that cannot tell which project it runs in. A state directory that
// cannot be listed, or a marker that cannot be read, counts as yes (fail
// closed). Review r2 CC3: an expired marker does not count, and is removed
// best-effort.
export function anyWindow(dataDir, { now = Date.now() } = {}) {
  const dir = join(dataDir, 'state');
  let names;
  try { names = readdirSync(dir); } catch (error) { return error.code !== 'ENOENT'; }
  let open = false;
  for (const name of names) {
    if (!/^scope-[0-9a-f]{64}\.json(?:\.[0-9a-f]{16}\.closing)?$/.test(name)) continue;
    let marker;
    try { marker = parse(readMarker(join(dir, name)) ?? ''); } catch { return true; }
    if (marker && now - marker.since > WINDOW_TTL_MS) { try { unlinkSync(join(dir, name)); } catch {} continue; }
    open = true;
  }
  return open;
}
