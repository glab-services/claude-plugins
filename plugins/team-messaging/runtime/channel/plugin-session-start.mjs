#!/usr/bin/env node
import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { requireValue } from './config.mjs';
import { directoryProblem, localError, pluginEnv } from './plugin-env.mjs';
import { sessionStartOutput } from './session-start.mjs';
import { stateDir } from './watch-state.mjs';

const FETCH_DEADLINE_MS = 10000;
const ARM_TEXT = {
  rewake: 'The plugin re-arms its watcher at every Stop (arm=rewake).',
  bash: 'Team messages arrive through a background watcher (arm=bash): run the /team-messaging:watch skill now and again after each watcher exit.',
  off: '',
};
const NOT_CONFIGURED = 'The team-messaging plugin is not configured. A member with an invite file runs /team-messaging:join; a workspace owner runs /team-messaging:bind. Then restart Claude Code.';

async function pluginSessionStart(env = process.env) {
  let event, context = null, timer;
  try {
    const inputLimit = Number(env.HOOK_INPUT_MAX_BYTES ?? 65536);
    requireValue(Number.isSafeInteger(inputLimit) && inputLimit > 0, 'INVALID_HOOK_INPUT_LIMIT', 'Set HOOK_INPUT_MAX_BYTES to a positive integer.');
    let input = '';
    for await (const chunk of process.stdin) {
      input += chunk;
      requireValue(Buffer.byteLength(input) <= inputLimit, 'HOOK_INPUT_TOO_LARGE', 'Increase HOOK_INPUT_MAX_BYTES if the host requires a larger SessionStart input.');
    }
    event = JSON.parse(input);
  } catch (error) { return fail(error); }
  const plugin = pluginEnv(env);
  if (!plugin.configured) return write(NOT_CONFIGURED);
  try {
    // Called only for its check: an unusable state directory is reported in
    // the context (decision 148). This hook marks nothing reported (decision 154).
    stateDir(env.CLAUDE_PLUGIN_DATA, { home: env.HOME });
    // The deadline bounds only the fetch; past it the hook still starts the
    // session with its arm text and no messages.
    const fetched = await Promise.race([
      sessionStartOutput({ ...plugin.env, CHANNEL_MODE: 'poll' }, event, { limit: env.HOOK_MESSAGE_LIMIT, contextLimit: env.HOOK_CONTEXT_MAX_CHARS }),
      new Promise((_, reject) => { timer = setTimeout(() => reject(Object.assign(new Error('deadline'), { code: 'SESSION_START_DEADLINE', hint: 'The broker did not answer within 10 s; use inbox.' })), FETCH_DEADLINE_MS); }),
    ]);
    clearTimeout(timer);
    // Decision 154: the context lists pending messages but never marks them
    // reported. Context alone does not start a turn in an idle session, and the
    // SessionStart watcher starts at the same moment: a claim here made it see
    // the message as already reported, so it never woke (0.9.2 test, #29).
    // The watcher is the only path that claims and wakes, so a message pending
    // at startup is in the context and also wakes once; ack dedupes that.
    context = fetched.additionalContext;
  } catch (error) { clearTimeout(timer); context = directoryProblem(fail(error)); }
  write([context, ARM_TEXT[plugin.arm]].filter(Boolean).join('\n\n'));
}
function write(text) {
  if (text) process.stdout.write(JSON.stringify({ hookSpecificOutput: { hookEventName: 'SessionStart', additionalContext: text } }) + '\n');
}
function fail(error) {
  // A broker outage must not prevent the host from starting. Never echo input or credentials.
  const safe = localError(error, 'Check the SessionStart hook input, plugin binding and broker configuration.');
  process.stderr.write(JSON.stringify({ event: 'session_start_unavailable', error: safe.code ?? 'INVALID_HOOK_INPUT', hint: safe.hint }) + '\n');
  // Returned so the context can report an unusable directory (decision 148).
  return safe;
}
if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) { await pluginSessionStart(); process.exit(0); }
