import { ChannelError } from './config.mjs';

// Maps plugin options to the channel environment that config() reads. Hooks
// receive CLAUDE_PLUGIN_OPTION_<KEY>; the MCP server and the Bash watcher get
// TM_<KEY> from .mcp.json env or the skill's inline ${user_config.*} values.
const options={BROKER_URL:'BROKER_URL',TOKEN_FILE:'BROKER_TOKEN_FILE',WORKSPACE:'CHANNEL_WORKSPACE',TOPIC:'CHANNEL_TOPIC',SESSION_ID:'CHANNEL_SESSION_ID',MODE:'CHANNEL_MODE'};
const required=['BROKER_URL','TOKEN_FILE','WORKSPACE','SESSION_ID'];
export const ARMS=['rewake','bash','off'];
export function pluginEnv(env=process.env) {
  const option=key=>env[`CLAUDE_PLUGIN_OPTION_${key}`]??env[`TM_${key}`]??'';
  // Claude Code substitutes an unset option as "", and config() rejects a set
  // but empty value (an empty CHANNEL_TOPIC fails config.mjs:29-30), so empty
  // means unset here: an empty topic is a workspace-level binding.
  const arm=option('ARM')||'rewake';
  if(!ARMS.includes(arm))return {configured:false,hint:'INVALID_ARM'};
  if(required.some(key=>!option(key)))return {configured:false,arm};
  if(unsafeOption(Object.fromEntries([...Object.keys(options),'ARM'].map(key=>[key,option(key)]))))return {configured:false,arm,hint:'UNSAFE_OPTION'};
  const mapped=Object.fromEntries(Object.entries(options).map(([key,name])=>[name,option(key)]).filter(([,value])=>value));
  // Poll is the plugin default (decision 118) while config.mjs keeps push as
  // the default for existing non-plugin registrations; an empty MODE would
  // otherwise be dropped and fall back to push.
  mapped.CHANNEL_MODE??='poll';
  // Only the binding and the credential-path inputs pass through, so an
  // inherited BROKER_TOKEN or TEAM_BROKER_TOKEN cannot reach config() and
  // trip AMBIGUOUS_BROKER_TOKEN next to BROKER_TOKEN_FILE.
  for(const key of ['HOME','XDG_CONFIG_HOME'])if(env[key])mapped[key]=env[key];
  return {configured:true,arm,env:mapped};
}

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
const DIRECTORY_CODES = new Set(['INSECURE_CREDENTIAL_DIR', 'DATA_DIR_REQUIRED']);
export function directoryProblem(safe) {
  return DIRECTORY_CODES.has(safe.code) ? `team-messaging: ${safe.code}: ${safe.hint}` : null;
}
