import { ChannelError, requireValue } from './config.mjs';
import { credentialName, readCredential } from './credentials.mjs';
import { CLAUDE_SESSION, findProject, projectHint, sameFile, verifyMac } from './project-options.mjs';
// cli.mjs imports this module too; the cycle is safe because normaliseUrl is
// only called at run time, after both modules have loaded.
import { normaliseUrl } from './cli.mjs';

// Plan project-options P-17: resolveBinding (below) replaced pluginEnv, which
// mapped CLAUDE_PLUGIN_OPTION_<KEY> or TM_<KEY> options, binding included,
// to the channel environment.
export const ARMS=['rewake','bash','off'];

// Values that skills paste into single-quoted shell commands must not be able
// to leave the quotes, so every option is held to a fixed charset when it is
// written (plugin-config.mjs) and again when a plugin entrypoint loads it.
// Brackets stay allowed for an IPv6 loopback host; they are inert inside
// single quotes.
export const SAFE_URL = /^[A-Za-z0-9:/._~%[\]-]+$/;
const safe = { BROKER_URL: SAFE_URL, TOKEN_FILE: /^[A-Za-z0-9/._~%-]+$/, WORKSPACE: /^[A-Za-z0-9_.-]{1,128}$/, TOPIC: /^[A-Za-z0-9_./-]{1,128}$/,
  SESSION_ID: /^[A-Za-z0-9_.-]{1,128}$/, MODE: /^(poll|push)$/, ARM: /^(rewake|bash|off)$/ };
// Returns the first option (upper-case key) whose non-empty value is unsafe.
export function unsafeOption(values) {
  return Object.keys(safe).find(key => values[key] !== undefined && values[key] !== '' && !(typeof values[key] === 'string' && safe[key].test(values[key]))) ?? null;
}
// Wake and diagnostic text never carries broker-supplied strings: a code is
// kept only in the stable upper-case form, and hints are fixed local text.
const hints = {
  UNAUTHORIZED: 'The plugin credential was rejected; ask the workspace owner for a new invite, then run /team-messaging:join.',
  NOT_A_MEMBER: 'This workspace membership ended; ask the workspace owner for a new invite, then run /team-messaging:join.',
  BROKER_UNAVAILABLE: 'The broker is unreachable; check the broker URL, TLS and availability.',
  RATE_LIMITED: 'The broker is rate limiting this session; it will retry.',
  INVALID_WATCH_ARGS: 'Use watch.mjs --mode rewake|bash [--session-key id] --data-dir dir [--deadline-s n] [--poll-s n>=1].',
  INVALID_SESSION_KEY: 'Use the host session_id (letters, digits, underscores, dots or hyphens) as the watcher key.',
  HOOK_INPUT_TOO_LARGE: 'Increase HOOK_INPUT_MAX_BYTES if the host requires a larger hook input.',
  STATE_LOCK_TIMEOUT: 'Another watcher or hook holds the session state lock; retry shortly.',
  PROJECT_OPTIONS_INVALID: projectHint('PROJECT_OPTIONS_INVALID'),
  PROJECT_OPTIONS_UNTRUSTED: projectHint('PROJECT_OPTIONS_UNTRUSTED'),
  PROJECT_OPTIONS_UNSUPPORTED: projectHint('PROJECT_OPTIONS_UNSUPPORTED'),
  PROJECT_UNKNOWN: projectHint('PROJECT_UNKNOWN'),
  TOKEN_FILE_MISMATCH: 'The saved token file is not the credential join saved for the saved broker URL; run /team-messaging:join or /team-messaging:bind again.',
  INVALID_HOOK_ARGS: 'Reinstall the plugin: its hooks pass only --project-dir and --data-dir.',
  SESSION_ID_UNKNOWN: 'Claude Code did not pass a valid session id, so this process stays inert.',
  NOT_AGENT_SESSION: 'This Claude session is not the agent session of this project; run /team-messaging:claim here to take over, then reconnect the MCP server.',
  PROJECT_NOT_BOUND: 'This project has no team-messaging binding; run /team-messaging:bind or /team-messaging:join in the session that should be the agent.',
  PLUGIN_NOT_CONFIGURED: 'The plugin has no saved credential; run /team-messaging:join or /team-messaging:bind in the agent session.',
  INVALID_ARM: 'Set the plugin option arm to rewake, bash or off.',
  UNSAFE_OPTION: 'A saved plugin option holds characters the plugin refuses; save it again with claude plugin configure.',
};
export function localError(error, fallback) {
  const code = error?.code === undefined ? undefined : /^[A-Z_]{1,64}$/.test(error.code) ? error.code : 'UNKNOWN_ERROR';
  // The one hint that carries a path: credentials.mjs sets `dir` only on the
  // writable-ancestor error, and a broker-built ChannelError never has it, so
  // the text is still local. It names the directory and its fix (decision 148).
  if (code === 'INSECURE_CREDENTIAL_DIR' && error instanceof ChannelError && typeof error.dir === 'string') return { code, hint: error.hint };
  return { code, hint: hints[code] ?? (/^SESSION_/.test(code ?? '') ? 'This plugin binding ended; run /team-messaging:status, then /team-messaging:join or /team-messaging:bind.' : fallback) };
}
// Decision 148: when a hook cannot use its state or credential directory, it
// says so in the session instead of failing silently at every Stop. Returns
// the user-visible text for those codes, or null for any other failure.
// Plan project-options P-11 and P-18 extend this to the project-file codes:
// a binding the plugin refuses is reported in the session in the same way,
// rather than leaving a bound project silently idle.
const DIRECTORY_CODES = new Set(['INSECURE_CREDENTIAL_DIR', 'DATA_DIR_REQUIRED', 'PROJECT_OPTIONS_INVALID', 'PROJECT_OPTIONS_UNTRUSTED', 'PROJECT_OPTIONS_UNSUPPORTED', 'TOKEN_FILE_MISMATCH']);
export function directoryProblem(safe) {
  return DIRECTORY_CODES.has(safe.code) ? `team-messaging: ${safe.code}: ${safe.hint}` : null;
}

// Plan project-options AM-1. Claude Code leaves an unsaved option absent from
// the hook env and substitutes it as "" in .mcp.json and skill text; an
// unsubstituted ${...} literal also means unset. Hooks read only
// CLAUDE_PLUGIN_OPTION_*, where a saved value beats a repo env value (findings
// 0.2), and never TM_*, which closes J-7. The MCP server, the CLI and the Bash
// watcher read only TM_*, which .mcp.json and the skills set from the saved
// options. Neither source carries a binding: workspace, topic and session come
// from the project file. WORKSPACE is read only to word the 0.9 upgrade hint.
const unset = value => typeof value !== 'string' || value === '' || /^\$\{[^}]*\}$/.test(value);
const pick = (env, prefix, keys) => Object.fromEntries(keys.map(key => [key.toLowerCase(), unset(env[prefix + key]) ? '' : env[prefix + key]]));
export const hookOptions = env => pick(env, 'CLAUDE_PLUGIN_OPTION_', ['BROKER_URL', 'TOKEN_FILE', 'MODE', 'ARM', 'WORKSPACE']);
export const tmOptions = env => pick(env, 'TM_', ['BROKER_URL', 'TOKEN_FILE', 'MODE', 'ARM']);
// The hooks' exec-form args (AM-1): only --project-dir ${CLAUDE_PROJECT_DIR}
// and --data-dir ${CLAUDE_PLUGIN_DATA}, each at most once.
export function hookArgs(argv, env = process.env) {
  const seen = {};
  for (let i = 0; i < argv.length; i += 2) {
    requireValue(['--project-dir', '--data-dir'].includes(argv[i]) && typeof argv[i + 1] === 'string' && !Object.hasOwn(seen, argv[i]), 'INVALID_HOOK_ARGS', hints.INVALID_HOOK_ARGS);
    seen[argv[i]] = argv[i + 1];
  }
  const value = key => unset(seen[key]) ? undefined : seen[key];
  return { projectDir: value('--project-dir'), dataDir: value('--data-dir'), options: hookOptions(env) };
}
const refuse = (code, extra = {}) => ({ state: 'invalid', code, hint: hints[code] ?? 'Check the plugin configuration.', ...extra });
// P-8, P-17: which state this process is in, without throwing.
// - idle: the project has no binding (no file, or a file without our key);
// - inert: the project's agent is another Claude session, or this one's id
//   is unknown; no token is read and nothing is sent;
// - invalid: a binding exists but the plugin refuses it (code and hint);
// - active: the agent session; env is the channel environment for config().
// The token is read only after the agent check, and only for the credential
// AM-2 allows; it is used for the MAC and never returned.
export function resolveBinding({ options = {}, claudeSession, projectDir, env = process.env }) {
  try {
    const arm = options.arm || 'rewake';
    if (!ARMS.includes(arm)) return refuse('INVALID_ARM');
    if (unsafeOption({ BROKER_URL: options.broker_url, TOKEN_FILE: options.token_file, MODE: options.mode, ARM: options.arm })) return refuse('UNSAFE_OPTION', { arm });
    const found = findProject(projectDir, { home: env.HOME });
    if (found.state === 'absent') return { state: 'idle', arm };
    if (found.state === 'invalid') return refuse(found.code, { arm });
    const agent = found.options.claude_session_id, root = found.root;
    // The inert binding is unverified (no token is read); it is shown, never used.
    if (typeof claudeSession !== 'string' || !CLAUDE_SESSION.test(claudeSession)) return { state: 'inert', code: 'SESSION_ID_UNKNOWN', agent, root, arm, binding: found.binding };
    if (claudeSession !== agent) return { state: 'inert', code: 'NOT_AGENT_SESSION', agent, root, arm, binding: found.binding };
    if (!options.broker_url || !options.token_file) return refuse('PLUGIN_NOT_CONFIGURED', { arm });
    const url = normaliseUrl(options.broker_url, env), credentialEnv = { HOME: env.HOME, XDG_CONFIG_HOME: env.XDG_CONFIG_HOME };
    credentialName(url, options.token_file, { env: credentialEnv });
    if (!verifyMac(readCredential(options.token_file, { home: env.HOME }), found, url)) return refuse('PROJECT_OPTIONS_UNTRUSTED', { arm });
    const { workspace, topic, session_id } = found.binding;
    // Only the binding and the credential-path inputs pass through, so an
    // inherited BROKER_TOKEN or TEAM_BROKER_TOKEN cannot reach config() and
    // trip AMBIGUOUS_BROKER_TOKEN next to BROKER_TOKEN_FILE. Poll is the
    // plugin default (decision 118).
    const mapped = { BROKER_URL: url, BROKER_TOKEN_FILE: options.token_file, CHANNEL_WORKSPACE: workspace, ...(topic === null ? {} : { CHANNEL_TOPIC: topic }), CHANNEL_SESSION_ID: session_id, CHANNEL_MODE: options.mode || 'poll' };
    for (const key of ['HOME', 'XDG_CONFIG_HOME']) if (env[key]) mapped[key] = env[key];
    return { state: 'active', arm, env: mapped, root, agent, binding: found.binding, path: found.path, identity: found.identity, mac: found.options.mac, bindings: found.options.bindings };
  } catch (error) {
    const safe = localError(error, 'Check the plugin configuration.');
    return { state: 'invalid', code: safe.code ?? 'PLUGIN_NOT_CONFIGURED', hint: safe.hint };
  }
}
// P-10, P-11 with AM-11: an active process re-checks that it is still the
// agent. The file is re-read only when its inode, mtime or size changed.
// It demotes at once when a valid, trusted file names another agent or
// another binding, or a file that parses fails its MAC. An absent,
// unparsable or partly written file (a writer mid-rename, an editor's
// save) only means "check again": two such reads at least CONFIRM_MS apart,
// with no good read between them, demote. Demotion is final: promotion never
// happens in place, only through a new process.
const CONFIRM_MS = 4000;
export function agentCheck(resolved, { projectDir, env = process.env, now = Date.now } = {}) {
  let known = resolved.identity, mac = resolved.mac, current = true, firstBad = null;
  const doubt = () => {
    firstBad ??= now();
    if (now() - firstBad >= CONFIRM_MS) current = false;
    return current;
  };
  return () => {
    if (!current) return false;
    if (sameFile(resolved.path, known)) { firstBad = null; return true; }
    const found = findProject(projectDir, { home: env.HOME });
    if (found.state !== 'found') return doubt();
    if (found.path !== resolved.path || found.options.claude_session_id !== resolved.agent || found.options.bindings[0] !== resolved.bindings[0]) return (current = false);
    if (found.options.mac !== mac) {
      let token;
      try { token = readCredential(resolved.env.BROKER_TOKEN_FILE, { home: env.HOME }); } catch { return doubt(); }
      if (!verifyMac(token, found, resolved.env.BROKER_URL)) return (current = false);
      mac = found.options.mac;
    }
    known = found.identity; firstBad = null;
    return true;
  };
}
