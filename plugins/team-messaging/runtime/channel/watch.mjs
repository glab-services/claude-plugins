#!/usr/bin/env node
import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';
import { config, requireValue } from './config.mjs';
import { Channel } from './channel.mjs';
import { agentCheck, hookOptions, localError, resolveBinding, tmOptions } from './plugin-env.mjs';
import { readSessionRecord } from './project-options.mjs';
import { acquireLock, claimUnreported, markEnded, reported, stateDir, unclaim } from './watch-state.mjs';
import { openWindow } from './scope-window.mjs';

// The Stop hook entry in plugin/hooks/hooks.json uses HOOK_TIMEOUT_S. Its
// watcher must exit first: hooks.md says "Claude Code still enforces
// `timeout` on a hook you run with `asyncRewake`", and a killed watcher would
// leave its lock to the stale-lock path. The margin also covers one in-flight
// inbox page after the deadline check (decision 137).
export const HOOK_TIMEOUT_S = 28800;
export const REWAKE_DEADLINE_S = HOOK_TIMEOUT_S - 300;
const BASH_DEADLINE_S = 1800, MAX_BACKOFF_MS = 60000;
const terminal = code => ['UNAUTHORIZED', 'NOT_A_MEMBER'].includes(code) || /^SESSION_/.test(code);
const quiet = new Set(['BROKER_UNAVAILABLE', 'BROKER_ERROR', 'RATE_LIMITED']);
const diagnostic = (event, error) => { const safe = localError(error, 'Check the plugin binding and broker availability.'); process.stderr.write(JSON.stringify({ event, error: safe.code ?? 'WATCH_FAILED', hint: safe.hint }) + '\n'); };
const alive = pid => { try { process.kill(pid, 0); return true; } catch (error) { return error.code === 'EPERM'; } };

function parseArgs(argv) {
  const options = {};
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i]?.slice(2);
    requireValue(['mode', 'session-key', 'data-dir', 'deadline-s', 'poll-s', 'project-dir', 'claude-session'].includes(name) && argv[i].startsWith('--') && argv[i + 1] !== undefined && !Object.hasOwn(options, name), 'INVALID_WATCH_ARGS', 'Use watch.mjs --mode rewake|bash [--session-key id] [--claude-session id] [--project-dir dir] --data-dir dir [--deadline-s n] [--poll-s n].');
    options[name] = argv[i + 1];
  }
  requireValue(['rewake', 'bash'].includes(options.mode), 'INVALID_WATCH_ARGS', 'Pass --mode rewake or --mode bash.');
  const number = (key, fallback, min) => {
    const value = Number(options[key] ?? fallback);
    requireValue(Number.isFinite(value) && value >= min, 'INVALID_WATCH_ARGS', `Set --${key} to a number of at least ${min}.`);
    return value;
  };
  return { ...options, deadlineS: number('deadline-s', options.mode === 'rewake' ? REWAKE_DEADLINE_S : BASH_DEADLINE_S, 0), pollS: number('poll-s', 5, 1) };
}
async function hookSessionId(env) {
  const inputLimit = Number(env.HOOK_INPUT_MAX_BYTES ?? 65536);
  requireValue(Number.isSafeInteger(inputLimit) && inputLimit > 0, 'INVALID_HOOK_INPUT_LIMIT', 'Set HOOK_INPUT_MAX_BYTES to a positive integer.');
  let input = '';
  for await (const chunk of process.stdin) {
    input += chunk;
    requireValue(Buffer.byteLength(input) <= inputLimit, 'HOOK_INPUT_TOO_LARGE', 'Increase HOOK_INPUT_MAX_BYTES if the host requires a larger hook input.');
  }
  return JSON.parse(input).session_id;
}
// One metadata line per message: no body, and only fields whose format the
// channel already validated, plus a checked thread ID.
function line(message) {
  const thread = /^[A-Za-z0-9_.:-]{1,128}$/.test(message.thread_id) ? ` thread=${message.thread_id}` : '';
  return `team-message id=${message.id}${thread} from=${message.from_user} kind=${message.kind}${message.topic == null ? '' : ` topic=${message.topic}`}`;
}

// `afterClaim` is a test-only dependency (decision 153): tests run watch()
// in-process and pass a function that runs between the claim and the parent
// re-check. The CLI entrypoint never passes it, and no environment variable
// can change the watcher's behaviour.
export async function watch(argv = process.argv.slice(2), env = process.env, { afterClaim } = {}) {
  let options;
  try { options = parseArgs(argv); }
  catch (error) { diagnostic('watch_invalid', error); return 5; }
  // The deadline starts here so that every wait below, including the hook
  // input read, is bounded by it.
  const deadline = Date.now() + options.deadlineS * 1000;
  const rewake = options.mode === 'rewake', out = text => (rewake ? process.stderr : process.stdout).write(text + '\n');
  let channel, lock, key, dir;
  const controller = new AbortController(), stopOn = () => { controller.abort(); void channel?.stop(); };
  process.once('SIGTERM', stopOn); process.once('SIGINT', stopOn);
  // Every wait is bounded by the deadline and ends on SIGTERM/SIGINT (decision
  // 153). Resolves { done: true, value } or { done: false } when cut short.
  const bounded = async promise => {
    let timer, onAbort;
    const cut = new Promise(resolve => {
      timer = setTimeout(resolve, Math.max(0, deadline - Date.now())); onAbort = resolve;
      controller.signal.addEventListener('abort', onAbort, { once: true }); if (controller.signal.aborted) resolve();
    });
    try { return await Promise.race([promise.then(value => ({ done: true, value })), cut.then(() => ({ done: false }))]); }
    finally { clearTimeout(timer); controller.signal.removeEventListener('abort', onAbort); }
  };
  try {
    let hookKey;
    if (rewake && options['session-key'] === undefined) {
      const read = await bounded(hookSessionId(env));
      if (!read.done) return 0;
      hookKey = read.value;
    }
    key = rewake ? (options['session-key'] ?? hookKey) : options['session-key'];
    // Plan project-options P-8, AM-1. The rewake watcher is a hook: its options
    // come from CLAUDE_PLUGIN_OPTION_*, its Claude session from the hook input
    // (or --claude-session when started with --session-key) and its project
    // from --project-dir. The Bash watcher has TM_* from the skill, its Claude
    // session from --claude-session '${CLAUDE_SESSION_ID}', and its project
    // from the SessionStart record, since the Bash env has no project path (U-6).
    const claudeSession = options['claude-session'] ?? (rewake ? hookKey : undefined);
    const projectDir = options['project-dir'] ?? (options['data-dir'] && claudeSession ? readSessionRecord(options['data-dir'], claudeSession, { home: env.HOME }) : undefined);
    const plugin = resolveBinding({ options: rewake ? hookOptions(env) : tmOptions(env), claudeSession, projectDir, env });
    // A rewake watcher that is off, idle, inert or refused must stay silent:
    // exit 2 would wake the session, and any other output is noise at every Stop.
    if (rewake && (plugin.state !== 'active' || plugin.arm === 'off')) return 0;
    if (plugin.state !== 'active') { diagnostic('watch_not_configured', { code: plugin.state === 'idle' ? 'PROJECT_NOT_BOUND' : plugin.code, hint: plugin.hint }); return 5; }
    const stillAgent = agentCheck(plugin, { projectDir, env });
    dir = stateDir(options['data-dir'], { home: env.HOME });
    const settings = config({ ...plugin.env, CHANNEL_MODE: 'poll' });
    // The host is the parent at start (decision 151): the lock names it, and
    // the loop below stops once it is gone.
    const parent = process.ppid;
    lock = await acquireLock(dir, key, deadline, parent);
    if (!lock) { if (!rewake) out('ALREADY_ARMED'); return 0; }
    // Channel state logs stay out of stderr: in rewake mode stderr is the wake text.
    channel = new Channel(settings, async () => {}, () => {});
    const pollMs = options.pollS * 1000, pause = ms => sleep(Math.max(0, Math.min(ms, deadline - Date.now())), undefined, { signal: controller.signal }).catch(() => {});
    let backoff = pollMs;
    // A watcher whose host exited would keep the lock and mark messages
    // reported for a session that no longer exists: it stops silently instead.
    const orphaned = () => process.ppid !== parent || !alive(parent);
    while (Date.now() < deadline && !controller.signal.aborted) {
      // P-11: a watcher whose session stopped being the agent (claim) exits
      // silently at its next poll.
      if (orphaned() || !stillAgent()) return 0;
      try {
        // inbox() has its own per-request timeout; bounding it also keeps a
        // slow page from outliving the deadline or a SIGTERM.
        const page = await bounded(channel.inbox(0));
        if (!page.done) break;
        const { messages } = page.value;
        // The host can die while inbox() is in flight. An orphan must not
        // claim: the IDs would be marked reported for the dead host and the
        // new host's watcher would never wake for them (decision 151).
        if (orphaned() || !stillAgent()) return 0;
        backoff = pollMs;
        // Review r1 S5: a wake for teammate messages opens the team-message
        // window first. If it cannot be written, nothing is claimed and the
        // watcher keeps retrying without waking (the messages stay pending).
        const known = new Set(reported(dir, key));
        if (messages.some(message => !known.has(message.id) && message.from_user !== plugin.user)) openWindow(options['data-dir'], claudeSession, plugin.root, { home: env.HOME });
        const claimed = await claimUnreported(dir, key, messages.map(message => message.id)), fresh = new Set(claimed);
        if (claimed.length && afterClaim) await bounded(Promise.resolve().then(afterClaim));
        // Stopped by a signal after claiming: nothing will be printed, so the
        // claim is undone to keep "claimed means delivered".
        if (claimed.length && controller.signal.aborted) { await unclaim(dir, key, claimed); break; }
        // The host can also die between the check above and the claim. The
        // claim is then undone, exactly these IDs, so the resumed session's
        // watcher wakes for them instead of finding them silently reported
        // (decision 152). A host dying after this check, before the wake is
        // delivered, is the accepted residual (follow-ups).
        if (claimed.length && orphaned()) { await unclaim(dir, key, claimed); return 0; }
        if (fresh.size) {
          for (const message of messages) if (fresh.has(message.id)) out(line(message));
          return rewake ? 2 : 0;
        }
      } catch (error) {
        if (controller.signal.aborted) break;
        const safe = localError(error, 'Check the plugin binding and broker availability.');
        if (terminal(safe.code)) {
          if (!rewake) { out(`WATCH_ENDED ${safe.code}`); return 4; }
          if (!await markEnded(dir, key, safe.code)) return 0;
          out(`team-messaging ended ${safe.code}: ${safe.hint}`);
          return 2;
        }
        if (!quiet.has(safe.code)) diagnostic('watch_retry', error);
        // Broker errors back off: the first retry waits one poll interval and
        // each later one doubles, up to MAX_BACKOFF_MS; a success resets it.
        await pause(backoff);
        backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
        continue;
      }
      await pause(pollMs);
    }
    if (!rewake && !controller.signal.aborted) out('WATCH_TIMEOUT re-arm');
    return controller.signal.aborted && !rewake ? 143 : 0;
  } catch (error) {
    diagnostic('watch_unavailable', error);
    return rewake ? 0 : 5;
  } finally {
    process.off('SIGTERM', stopOn); process.off('SIGINT', stopOn);
    await channel?.stop();
    await lock?.release().catch(() => {});
  }
}
if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) process.exit(await watch());
