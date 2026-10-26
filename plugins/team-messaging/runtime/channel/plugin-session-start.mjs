#!/usr/bin/env node
import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { requireValue } from './config.mjs';
import { directoryProblem, hookArgs, localError, resolveBinding } from './plugin-env.mjs';
import { CLAUDE_SESSION, writeSessionRecord } from './project-options.mjs';
import { sessionStartOutput } from './session-start.mjs';
import { openWindow } from './scope-window.mjs';
import { serving } from './agent-live.mjs';

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
// 0.11.1 F1 (D2): a fork or /clear gives a session a new id, so the agent
// can silently become inert. INERT reaches only the model; INERT_NOTICE is a
// systemMessage, which the host shows to the human. It shows at every
// SessionStart of a non-agent session, a deliberate second session included:
// a fork cannot be told from one, and a wrong silence is the bug.
const agentId = agent => CLAUDE_SESSION.test(agent ?? '') ? agent : '(unknown)';
const INERT = agent => `team-messaging: this session is not this project's team-messaging agent (the agent is Claude session ${agentId(agent)}), so it has no team-messaging tools and is not woken for team messages. A fork or /clear gives a session a new id; if the user expects this session to be the agent, tell them to run /team-messaging:claim here.`;
const INERT_NOTICE = agent => `team-messaging: this session is not the project's agent (agent: Claude session ${agentId(agent)}). If it replaced the agent after a fork or /clear, run /team-messaging:claim here.`;
const ACTIVE = ({ binding, root }) => `team-messaging: agent session for workspace ${binding.workspace}${binding.topic === null ? '' : `, topic ${binding.topic}`}, project root ${root}`;
// Project-scoped sharing P4 (L1): follows ACTIVE in every active state. At
// most 120 characters (FA-16), so the largest context stays under the host's
// 10,000-character limit.
const SCOPE = 'team-messaging: share only project content and project-relative paths, nothing about this machine or outside the root.';
const SCOPE_UNAVAILABLE = 'team-messaging: SCOPE_WINDOW_UNAVAILABLE: the team-message window could not be recorded, so pending messages are not shown here; check the plugin data directory, then use inbox.';
// 0.11.1 F5 (D5): context only, no systemMessage, because a startup race with
// the MCP server is normal; the SessionStart watcher wakes once it serves.
const TOOLS_NOT_LOADED = "team-messaging: TOOLS_NOT_LOADED: team messages are pending but not shown, because this session's team-messaging tools are not loaded yet. Once they load, call inbox; if they do not appear, ask the user to reconnect the team-messaging MCP server.";

async function pluginSessionStart(env = process.env, argv = process.argv.slice(2)) {
  let event, context = null, timer, fetched = null;
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
  if (plugin.state === 'inert') return write([problem, INERT(plugin.agent)].filter(Boolean).join('\n\n'), INERT_NOTICE(plugin.agent));
  if (plugin.state === 'invalid') return write([problem, `team-messaging: ${plugin.code}: ${plugin.hint}`].filter(Boolean).join('\n\n'));
  if (problem) return write([problem, ACTIVE(plugin), SCOPE, ARM_TEXT[plugin.arm]].filter(Boolean).join('\n\n'));
  try {
    // The deadline bounds only the fetch; past it the hook still starts the
    // session with its arm text and no messages.
    fetched = await Promise.race([
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
  } catch (error) { clearTimeout(timer); context = directoryProblem(fail(error)); fetched = null; }
  // D-11: pending teammate messages in the context open the team-message
  // window first. Only a fetched list opens it, never a fetch failure or a
  // directory problem. FA-5: if the window cannot be written, the messages
  // are left out and the session is told why.
  // F5 (D4): only while this session's MCP server serves its tools; otherwise
  // the messages are left out and no window opens.
  if (fetched?.additionalContext != null) {
    if (!serving(args.dataDir, event.session_id, plugin.root, { home: env.HOME })) context = TOOLS_NOT_LOADED;
    else {
      try { openWindow(args.dataDir, event.session_id, plugin.root, { home: env.HOME }); }
      catch (error) { fail(error); context = SCOPE_UNAVAILABLE; }
    }
  }
  write([context, ACTIVE(plugin), SCOPE, ARM_TEXT[plugin.arm]].filter(Boolean).join('\n\n'));
}
// systemMessage (optional) is shown to the user; additionalContext reaches only the model.
function write(text, systemMessage) {
  if (text) process.stdout.write(JSON.stringify({ ...(systemMessage ? { systemMessage } : {}), hookSpecificOutput: { hookEventName: 'SessionStart', additionalContext: text } }) + '\n');
}
function fail(error) {
  // A broker outage must not prevent the host from starting. Never echo input or credentials.
  const safe = localError(error, 'Check the SessionStart hook input, plugin binding and broker configuration.');
  process.stderr.write(JSON.stringify({ event: 'session_start_unavailable', error: safe.code ?? 'INVALID_HOOK_INPUT', hint: safe.hint }) + '\n');
  // Returned so the context can report an unusable directory (decision 148).
  return safe;
}
if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) { await pluginSessionStart(); process.exit(0); }
