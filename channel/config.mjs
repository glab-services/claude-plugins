import { topicPattern } from '../shared/address.mjs';
import { randomUUID } from 'node:crypto';
import { readCredential } from './credentials.mjs';

export class ChannelError extends Error {
  constructor(code, hint,reason) { super(code); this.code = code; this.hint = hint; if(code==='SESSION_ENDED'&&['topic_left','membership_ended'].includes(reason))this.reason=reason; }
}
export function requireValue(condition, code, hint,reason) {
  if (!condition) throw new ChannelError(code, hint,reason);
}
function integer(env, key, fallback, min = 1) {
  const value = Number(env[key] ?? fallback);
  requireValue(Number.isSafeInteger(value) && value >= min, `INVALID_${key}`, `Set ${key} to an integer of at least ${min}.`);
  return value;
}
export function config(env = process.env) {
  let url;
  try { url = new URL(env.BROKER_URL ?? 'http://127.0.0.1:8091'); } catch {}
  requireValue(url && ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password && !url.search && !url.hash && !url.href.includes('?') && !url.href.includes('#'),
    'INVALID_BROKER_URL', 'Set BROKER_URL to the broker HTTP(S) address without credentials, query, or fragment.');
  requireValue(!(env.BROKER_TOKEN !== undefined && env.BROKER_TOKEN_FILE !== undefined), 'AMBIGUOUS_BROKER_TOKEN', 'Set only BROKER_TOKEN or BROKER_TOKEN_FILE.');
  const token = env.BROKER_TOKEN ?? (env.BROKER_TOKEN_FILE !== undefined ? readCredential(env.BROKER_TOKEN_FILE,{home:env.HOME}) : undefined);
  requireValue(typeof token === 'string' && token.length >= 24 && !/\s/.test(token) && !/^replace-with/i.test(token),
    'INVALID_BROKER_TOKEN', 'Set BROKER_TOKEN to your developer secret from the broker token configuration.');
  const session = env.CHANNEL_SESSION_ID ?? randomUUID();
  requireValue(/^[a-zA-Z0-9_.-]{1,128}$/.test(session) && session !== 'broker', 'INVALID_SESSION', 'Use a unique CHANNEL_SESSION_ID other than broker, or omit it to generate one.');
  const workspace = env.CHANNEL_WORKSPACE ?? 'default';
  requireValue(/^[a-zA-Z0-9_.-]{1,128}$/.test(workspace) && workspace !== 'broker', 'INVALID_WORKSPACE', 'Set CHANNEL_WORKSPACE to 1-128 letters, digits, underscores, dots or hyphens, other than broker.');
  const topic=env.CHANNEL_TOPIC??null;
  requireValue(topic===null || (typeof topic==='string'&&topicPattern.test(topic)),'INVALID_TOPIC','Set CHANNEL_TOPIC to a valid topic name without empty or dot path segments.');
  const role = env.CHANNEL_ROLE ?? 'worker';
  requireValue(['manager', 'worker'].includes(role), 'INVALID_ROLE', 'Set CHANNEL_ROLE to manager or worker.');
  const mode = env.CHANNEL_MODE ?? 'push';
  requireValue(['push', 'poll'].includes(mode), 'INVALID_MODE', 'Set CHANNEL_MODE to push or poll.');
  return {
    url: url.href.replace(/\/$/, ''), token, session, workspace, topic, role, mode,
    idleExitMs: integer(env, 'CHANNEL_IDLE_EXIT_MS', 0, 0),
    heartbeatMs: integer(env, 'CHANNEL_HEARTBEAT_MS', 30000),
    pollMs: integer(env, 'CHANNEL_POLL_MS', 1000),
    retryMaxMs: integer(env, 'CHANNEL_RETRY_MAX_MS', 30000),
    requestTimeoutMs: integer(env, 'CHANNEL_REQUEST_TIMEOUT_MS', 10000),
    streamIdleMs: integer(env, 'CHANNEL_STREAM_IDLE_MS', 45000),
    bufferLimit: integer(env, 'CHANNEL_BUFFER_LIMIT', 1000),
    maxFrameBytes: integer(env, 'CHANNEL_MAX_FRAME_BYTES', 1024 * 1024),
  };
}
