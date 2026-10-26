#!/usr/bin/env node
// L3 of the project-scoped sharing plan: a deny-only PreToolUse hook. While a
// team-message window is open (scope-window.mjs), only file reads inside the
// project root, the session's own fresh tool results, our own MCP tools, a
// few local tools and the exact arm=bash watcher command are allowed. Every
// other tool call is denied, in every permission mode: deny is final across
// hooks and permission rules, and this hook never answers `ask` (D-23).
//
// The rule is a constant (AM-S1): no option, input-limit or TM_* variable is
// read here, and the arguments have one fixed form (D-20). Only HOME is read,
// for the project root walk the binding resolution also uses.
//
// Failure (D-21): once the input parsed with a valid session id and data
// directory, any internal error denies. Only arguments in another form,
// unparsable input or a missing session id or data directory pass through
// with a stderr code (residual R-5); a killed or timed-out hook, or a missing
// node, fails open (R-6, D-25: no onFailure "block").
import { lstatSync, realpathSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { anyWindow, bindingRoot, closeWindow, readWindow, windowPath } from './scope-window.mjs';

// D-20: the hook input is at most this many bytes. It is a constant, unlike
// the other hooks' configurable input limit, so the environment cannot change it.
const INPUT_MAX_BYTES = 64 * 1024 * 1024;
// FD-2: the pending check at Stop must end well inside the 10 s hook timeout.
const PENDING_DEADLINE_MS = 5000;
const SESSION = /^[A-Za-z0-9_.-]{1,128}$/;
const EVENTS = new Set(['pre', 'stop', 'stop-failure']);
const FILE_REASON = 'While a team message is being handled, file tools stay inside the project root.';
const OTHER_REASON = 'While a team message is being handled, only file reads inside the project root and team-messaging tools are allowed.';
// Review r1 C1, r2 CC4: every denial names the way out of strict mode as a
// command that runs from a plain terminal: this runtime's own cli.mjs (next
// to this file, so no PATH entry is needed), the project root and the data
// directory, single-quoted for the shell, and the marker file to delete as
// the alternative. Without a resolved root, the marker files are named by pattern.
const CLI = join(dirname(realpathSync(process.argv[1] ?? fileURLToPath(import.meta.url))), 'cli.mjs');
const quote = value => `'${value.replaceAll("'", "'\\''")}'`;
function wayOut(dataDir, root) {
  const command = `node ${quote(CLI)} scope-close --project-dir ${root ? quote(root) : '<project dir>'} --data-dir ${quote(dataDir)}`;
  const file = root ? windowPath(dataDir, root) : join(dataDir, 'state', 'scope-*.json');
  return ` Ack or answer the pending team messages; strict mode ends at the end of the turn once none are pending, or within 2 h. To end it now, run \`${command}\` in a terminal outside Claude, or delete \`${file}\`.`;
}
// D-16: the tool-name prefix Claude Code gives a plugin server's tools, and
// the provenance it reports for ours (findings 1.1, McpServerProvenance).
const OUR_PREFIX = 'mcp__plugin_team-messaging_team-messaging__';
const OUR_SERVER = 'plugin:team-messaging:team-messaging';
// D-17: tools that touch nothing outside the session: tool search loads
// schemas, the to-do list and plan mode are local, and AskUserQuestion talks
// only to the local human.
const LOCAL_TOOLS = new Set(['ToolSearch', 'TodoWrite', 'AskUserQuestion', 'EnterPlanMode', 'ExitPlanMode']);
const FILE_TOOLS = new Set(['Read', 'Glob', 'Grep']);
const GLOB_CHARS = /[*?[{]/;

const code = (error, hint) => process.stderr.write(JSON.stringify({ event: 'scope_hook_unavailable', error, hint }) + '\n');
const deny = (reason, dataDir, root = null) => process.stdout.write(JSON.stringify({ hookSpecificOutput: { hookEventName: 'PreToolUse', permissionDecision: 'deny', permissionDecisionReason: reason + wayOut(dataDir, root) } }) + '\n');

// D-20: exactly --event <event> --project-dir <dir> --data-dir <dir>, in that order.
function parseArgs(argv) {
  if (argv.length !== 6 || argv[0] !== '--event' || argv[2] !== '--project-dir' || argv[4] !== '--data-dir' || !EVENTS.has(argv[1])) return null;
  return { event: argv[1], projectDir: argv[3], dataDir: argv[5] };
}
async function readInput() {
  const chunks = [];
  let size = 0;
  for await (const chunk of process.stdin) {
    size += chunk.length;
    if (size > INPUT_MAX_BYTES) return { tooLarge: true };
    chunks.push(chunk);
  }
  return { text: Buffer.concat(chunks).toString('utf8') };
}

// D-15: the lexically normalised path, with the realpath of its nearest
// existing ancestor, so a symlink cannot lead out of the root and a file that
// does not exist yet is judged by where it would be.
function physical(path) {
  let current = normalize(path), rest = '';
  for (;;) {
    try { return join(realpathSync(current), rest); }
    catch (error) {
      if (!['ENOENT', 'ENOTDIR'].includes(error.code)) throw error;
      const parent = dirname(current);
      if (parent === current) throw error;
      rest = rest ? join(basename(current), rest) : basename(current);
      current = parent;
    }
  }
}
const inside = (path, root) => { const real = physical(path); return real === root || real.startsWith(root + '/'); };
// A relative path is resolved against the input cwd (FA-8); without an
// absolute cwd it cannot be placed, so it is outside. Review r1 S3: the path
// is trimmed first, and one that starts with `~` or holds `$` or `%` may be
// expanded by the tool, so it cannot be placed either.
function place(raw, cwd) {
  if (typeof raw !== 'string') return null;
  const path = raw.trim();
  if (path === '' || path.startsWith('~') || /[$%]/.test(path)) return null;
  if (isAbsolute(path)) return path;
  return typeof cwd === 'string' && isAbsolute(cwd) ? resolve(cwd, path) : null;
}
// D-15: an absolute Glob pattern or Grep glob is joined to the base and
// judged up to its first glob character. Review r1 S7: a pattern with a `..`
// segment is denied outright. Review r2 CC8: a brace alternative is allowed
// only when every alternative is a plain relative name (no leading `/`, no
// `~`, `..` segment, `$` or `%`), so `**/*.{ts,tsx}` works and
// `{/etc,src}/**` does not. Nested braces are denied.
const plainAlternative = alternative => !alternative.startsWith('/') && !/[~$%{]/.test(alternative) && !alternative.split('/').includes('..');
function patternInside(pattern, base, root) {
  if (pattern === undefined) return true;
  if (typeof pattern !== 'string') return false;
  if (/[~$%]/.test(pattern) || pattern.split(/[\\/]/).includes('..')) return false;
  for (const [, group] of pattern.matchAll(/\{([^}]*)\}?/g)) if (!group.split(',').every(plainAlternative)) return false;
  if (!isAbsolute(pattern)) return true;
  const literal = pattern.split(GLOB_CHARS)[0];
  return inside(isAbsolute(literal) ? literal : resolve(base, literal), root);
}
// D-18: a large tool result Claude Code saved for this session, written after
// the window opened. An older file may hold content read before the window.
function freshToolResult(path, input, since) {
  if (typeof input.transcript_path !== 'string' || !isAbsolute(input.transcript_path)) return false;
  let dir;
  try { dir = realpathSync(join(dirname(input.transcript_path), input.session_id, 'tool-results')); } catch { return false; }
  let real;
  try { real = realpathSync(path); } catch { return false; }
  if (!real.startsWith(dir + '/')) return false;
  return lstatSync(real).mtimeMs >= since;
}
const escape = text => text.replace(/[.*+?^${}()|[\]\\/-]/g, '\\$&');
// D-19: the watch skill's command with the plugin's own values. The quoted
// option values cannot contain `'`, so they stay inert; the session id and
// data directory must be exactly this session's.
const watcher = (session, dataDir) => new RegExp(`^TM_BROKER_URL='[A-Za-z0-9:/._~%\\[\\]-]*' TM_TOKEN_FILE='[A-Za-z0-9/._~%-]*' team-messaging-watch --mode bash --session-key '${escape(session)}' --claude-session '${escape(session)}' --data-dir '${escape(dataDir)}'$`);

// D-17: the decision inside an open window. Returns null (allowed) or the
// denial reason.
function judge(input, marker, dataDir) {
  const tool = input.tool_name, args = input.tool_input && typeof input.tool_input === 'object' ? input.tool_input : {};
  if (typeof tool !== 'string') return OTHER_REASON;
  if (tool.startsWith(OUR_PREFIX)) return input.mcp_server?.source === 'plugin' && input.mcp_server?.name === OUR_SERVER ? null : OTHER_REASON;
  if (LOCAL_TOOLS.has(tool)) return null;
  if (tool === 'Bash') return args.run_in_background === true && typeof args.command === 'string' && watcher(input.session_id, dataDir).test(args.command) ? null : OTHER_REASON;
  if (!FILE_TOOLS.has(tool)) return OTHER_REASON;
  // D-14: a root of HOME or / was recorded as null; then no file read is allowed.
  if (marker.root === null) return FILE_REASON;
  if (tool === 'Read') {
    const path = place(args.file_path, input.cwd);
    if (path === null) return FILE_REASON;
    return inside(path, marker.root) || freshToolResult(path, input, marker.since) ? null : FILE_REASON;
  }
  const base = place(args.path ?? input.cwd, input.cwd);
  if (base === null || !inside(base, marker.root)) return FILE_REASON;
  return patternInside(tool === 'Glob' ? args.pattern : args.glob, base, marker.root) ? null : FILE_REASON;
}

// Review r1 C2: the project root cannot be found (a deleted directory, an
// unexpanded variable). Deny only while some window exists in this data
// directory; otherwise pass with a code, so a broken project setting does not
// block every tool in every session.
function unresolved() {
  return { unresolved: true };
}
function projectRoot(projectDir) {
  if (typeof projectDir !== 'string' || !isAbsolute(projectDir) || projectDir.includes('${')) return unresolved();
  try { return { root: bindingRoot(projectDir, { home: process.env.HOME }) }; }
  catch (error) { if (error?.code === 'PROJECT_UNKNOWN') return unresolved(); throw error; }
}

async function pre(input, args, now) {
  const found = projectRoot(args.projectDir);
  if (found.unresolved) {
    if (anyWindow(args.dataDir)) return deny(OTHER_REASON, args.dataDir);
    return code('PROJECT_DIR_UNRESOLVED', 'The scope hook could not resolve --project-dir; no team-message window is open, so nothing was blocked.');
  }
  const root = found.root;
  // No binding anywhere up the tree: no window can have been opened for it.
  if (root === null) return;
  const window = readWindow(args.dataDir, root, { now });
  if (window.state === 'closed') return;
  if (window.state !== 'open') return deny(OTHER_REASON, args.dataDir, root);
  // Review r1 C1: no decision refreshes the window; it ends 2 h after `since`.
  const reason = judge(input, window.marker, args.dataDir);
  if (reason !== null) deny(reason, args.dataDir, root);
}

// FD-2: the window stays open at Stop while this session, as the project's
// agent, still has a teammate message pending, because the next turn will
// handle it. Any doubt (not the agent, no options, a broker error) keeps it:
// a window left open only adds denials until its 2 h expiry.
async function teammatePending(input, args) {
  try {
    const { hookOptions, resolveBinding } = await import('./plugin-env.mjs');
    const { pendingMessages } = await import('./stop-hook.mjs');
    const plugin = resolveBinding({ options: hookOptions(process.env), claudeSession: input.session_id, projectDir: args.projectDir, env: process.env });
    if (plugin.state !== 'active' || typeof plugin.user !== 'string') return true;
    const messages = await pendingMessages(plugin, PENDING_DEADLINE_MS);
    return messages.some(message => message.from_user !== plugin.user);
  } catch { return true; }
}
async function stop(input, args) {
  if (typeof args.projectDir !== 'string' || !isAbsolute(args.projectDir)) return code('PROJECT_DIR_REQUIRED', 'Reinstall the plugin: the scope hook needs --project-dir ${CLAUDE_PROJECT_DIR}.');
  const root = bindingRoot(args.projectDir, { home: process.env.HOME });
  if (root === null) return;
  const window = readWindow(args.dataDir, root);
  if (window.state === 'closed') return;
  if (await teammatePending(input, args)) return;
  // Review r1 S4: delete only the marker read before the pending check; a
  // window reopened meanwhile stays.
  closeWindow(args.dataDir, root, window.state === 'open' ? window.marker : undefined);
}

async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (!args) return code('INVALID_SCOPE_HOOK_ARGS', 'Reinstall the plugin: its scope hook passes --event, --project-dir and --data-dir only.');
  let input;
  try {
    const read = await readInput();
    // D-20: input over the constant limit cannot be judged, so it is denied.
    if (read.tooLarge) { if (args.event === 'pre') deny(OTHER_REASON, args.dataDir); return code('HOOK_INPUT_TOO_LARGE', 'The hook input exceeded 64 MiB.'); }
    input = JSON.parse(read.text);
  } catch { return code('INVALID_HOOK_INPUT', 'The scope hook input was not JSON.'); }
  if (!input || typeof input !== 'object' || typeof input.session_id !== 'string' || !SESSION.test(input.session_id)) return code('SESSION_ID_UNKNOWN', 'Claude Code did not pass a valid session id.');
  if (typeof args.dataDir !== 'string' || !isAbsolute(args.dataDir)) return code('DATA_DIR_REQUIRED', 'Reinstall the plugin: the scope hook needs --data-dir ${CLAUDE_PLUGIN_DATA}.');
  if (args.event === 'pre') {
    try { await pre(input, args, Date.now()); }
    catch { deny(OTHER_REASON, args.dataDir); }
    return;
  }
  try { await stop(input, args); }
  catch (error) { code(/^[A-Z_]{1,64}$/.test(error?.code ?? '') ? error.code : 'SCOPE_CLOSE_FAILED', 'The team-message window could not be closed; it expires 2 h after it opened.'); }
}
if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) { await main(); process.exit(0); }
