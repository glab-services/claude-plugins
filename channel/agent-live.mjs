import { existsSync, realpathSync, unlinkSync } from 'node:fs';
import { isAbsolute, join } from 'node:path';
import { requireValue } from './config.mjs';
import { CLAUDE_SESSION, projectHint, readRecord, writeRecord } from './project-options.mjs';
import { hostAlive, procStat, stateDir } from './watch-state.mjs';

// 0.11.1 F5 (D4): whether this Claude session's team-messaging MCP server
// serves its tools. Acceptance found a session that had just claimed the
// project, whose MCP server was still inert: the Stop hook opened the strict
// team-message window and blocked, and the session had no tools to read or
// ack the messages, so every human turn stayed strict.
//
// The active server writes <session>.serving once it has answered a
// non-empty tools/list. That answer is the only proof that the host holds
// our tools; a live process alone is not, since after an in-place promotion
// the host may not have listed them again. The hooks then wake or open the
// window only while serving() holds.
//
// The name does not end in .json, so no watch-state key file (${key}.json,
// keys may contain ".") can collide with it (round 1, A6), and it cannot
// match the session-record pattern that writeSessionRecord prunes.
const name = session => `${session}.serving`;

// Throws on an invalid session or root; the server logs that and goes on.
export function markServing(dataDir, session, root, { home, pid = process.pid } = {}) {
  requireValue(CLAUDE_SESSION.test(session ?? ''), 'SESSION_ID_UNKNOWN', 'Claude Code did not pass a valid session id, so this process stays inert.');
  requireValue(typeof root === 'string' && isAbsolute(root), 'PROJECT_UNKNOWN', projectHint('PROJECT_UNKNOWN'));
  // start is null without /proc (macOS): liveness is then pid-only (D18).
  writeRecord(dataDir, name(session), { v: 1, pid, start: procStat(pid)?.start ?? null, root: realpathSync(root), at: Date.now() }, { home });
}

// Never throws. Removes the record only when `pid` wrote it, so a process
// that exits late cannot remove the record of the server that replaced it
// (findings R-c); that server rewrites a missing record within one tick.
// Without a state directory there is nothing to remove; readRecord would
// create it (stateDir), which at exit recreated removed test directories.
export function clearServing(dataDir, session, { home, pid = process.pid } = {}) {
  try {
    if (!CLAUDE_SESSION.test(session ?? '') || !existsSync(join(dataDir, 'state'))) return;
    if (readRecord(dataDir, name(session), { home })?.pid !== pid) return;
    unlinkSync(join(stateDir(dataDir, { home }), name(session)));
  } catch {}
}

// Never throws; any doubt is "not serving", which only removes a wake.
export function serving(dataDir, session, root, { home } = {}) {
  try {
    if (!CLAUDE_SESSION.test(session ?? '')) return false;
    const record = readRecord(dataDir, name(session), { home });
    if (record?.v !== 1 || !Number.isSafeInteger(record.pid) || record.pid <= 0) return false;
    if (record.root !== realpathSync(root)) return false;
    return hostAlive(record.pid, typeof record.start === 'string' ? record.start : undefined);
  } catch { return false; }
}
