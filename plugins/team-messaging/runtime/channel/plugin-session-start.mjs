#!/usr/bin/env node
import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { requireValue } from './config.mjs';
import { directoryProblem, hookArgs, localError, resolveBinding } from './plugin-env.mjs';
import { CLAUDE_SESSION, writeSessionRecord } from './project-options.mjs';
import { sessionStartOutput } from './session-start.mjs';

const FETCH_DEADLINE_MS = 10000;
const ARM_TEXT = {
  rewake: 'The plugin re-arms its watcher at every Stop (arm=rewake).',
  bash: 'Team messages arrive through a background watcher (arm=bash): run the /team-messaging:watch skill now and again after each watcher exit.',
  off: '',
};
// Plan project-options P-11: at most one line about the project's binding.
// Only names that passed their patterns are echoed.
const IDLE = 'team-messaging is idle in this project; run /team-messaging:bind or /team-messaging:join in the session that should be the agent';
// P-15: a 0.9 global binding is ignored; its workspace only words the hint.
const ADOPT = workspace => ` (the plugin still holds a 0.9 binding to workspace ${workspace}, which 0.10 ignores; to keep it, run /team-messaging:bind --adopt-global in the agent session)`;
const INERT = agent => `team-messaging: this project's agent is Claude session ${CLAUDE_SESSION.test(agent ?? '') ? agent : '(unknown)'}; run /team-messaging:claim here to take over`;
const ACTIVE = ({ binding, root }) => `team-messaging: agent session for workspace ${binding.workspace}${binding.topic === null ? '' : `, topic ${binding.topic}`}, project root ${root}`;

async function pluginSessionStart(env = process.env, argv = process.argv.slice(2)) {
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
  let args;
  try { args = hookArgs(argv, env); } catch (error) { const safe = fail(error); return write(`team-messaging: ${safe.code}: ${safe.hint}`); }
  const plugin = resolveBinding({ options: args.options, claudeSession: event?.session_id, projectDir: args.projectDir, env });
  // P-11: the record is written in every state, so the CLI and the Bash
  // watcher of this session can find the project later. Writing it also
  // checks the state directory: an unusable one is reported in the context
  // (decision 148). This hook marks nothing reported (decision 154).
  let problem = null;
  try { writeSessionRecord(args.dataDir, event?.session_id, args.projectDir, { home: env.HOME }); }
  catch (error) { problem = directoryProblem(fail(error)); }
  if (plugin.state === 'idle') return write([problem, IDLE + (/^[A-Za-z0-9_.-]{1,128}$/.test(args.options.workspace) ? ADOPT(args.options.workspace) : '')].filter(Boolean).join('\n\n'));
  if (plugin.state === 'inert') return write([problem, INERT(plugin.agent)].filter(Boolean).join('\n\n'));
  if (plugin.state === 'invalid') return write([problem, `team-messaging: ${plugin.code}: ${plugin.hint}`].filter(Boolean).join('\n\n'));
  if (problem) return write([problem, ACTIVE(plugin), ARM_TEXT[plugin.arm]].filter(Boolean).join('\n\n'));
  try {
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
  write([context, ACTIVE(plugin), ARM_TEXT[plugin.arm]].filter(Boolean).join('\n\n'));
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
