import { readFileSync } from 'node:fs';

const hints = {
  INVALID_RETENTION_LIMIT: 'Use a positive safe integer candidate limit.',
  INVALID_RETENTION_EPOCH: 'Inspect the retention epoch in an offline database copy; retention cannot proceed without a valid safety epoch.',
  ITEM_NOT_DEAD: 'Requeue only a dead-letter item; inspect its current queue state first.',
  NOT_QUEUE_TOPIC: 'Choose a queue topic in this workspace.',
  QUEUE_BLOCKING_UNSUPPORTED: 'Queue threads are read-only; send nonblocking work and publish its result with done.',
  DONE_RESULT_MISMATCH: 'This claim already completed with a different result; read queue_item and preserve the original result.',
  ITEM_CLAIMED: 'Another session holds this item; wait for release or lease expiry.',
  ITEM_DONE: 'This queue item is terminal; inspect it with queue_item.',
  CLAIM_STALE: 'Use a current, unexpired claim held by this session and its returned generation.',
  USE_CLAIM: 'Queue items use claim, renew and done; acknowledgments and broadcast skips do not process them.',
  QUEUE_THREAD_READ_ONLY: 'Queue threads are read-only; publish results with done and read them with queue_item.',
  INVALID_QUEUE_FIELDS: 'Use only id, generation and optional result as documented for this queue operation.',
  INVALID_GENERATION: 'Use the positive integer generation returned by claim.',
  INVALID_RESULT: 'Use a result string within MAX_MESSAGE_CHARS, or omit it.',
  INVALID_SKIP_THROUGH: 'Use a nonnegative safe integer message ID for through, or omit it for the current topic maximum.',
  PIN_LIMIT: 'Unkeep a pinned message before keeping another, or ask the operator to raise MAX_PINS.',
  INVALID_SESSION_FIELDS: 'Use the registered session and workspace/topic headers without query parameters.',
  SESSION_ENDED: 'This session ended permanently; rejoin with a fresh session ID.',
  TOPIC_REQUIRED: 'Register a topic for a member session; only the workspace owner can use a workspace-level session.',
  SESSION_TOPIC_MISMATCH: 'Use a distinct session ID for this topic; existing session bindings cannot change.',
  TOPIC_SCOPE_MISMATCH: 'Use a session bound to this topic; global workspace administration requires an owner workspace-level session.',
  NOT_SUBSCRIBED: 'Join this topic before starting a bound session; use a fresh registration after a session ends.',
  TOPIC_NOT_SUBSCRIBED: 'Subscribe to this topic before reading, sending or replying.',
  TOPIC_THREAD_MISMATCH: 'Reply to a topic thread using its same topic address; ordinary and topic threads cannot be mixed.',

  INVALID_TOPIC_NAME: 'Use slash-separated 1-64 character ASCII name segments, no empty/dot/dot-dot segments, at most 128 characters total.',
  INVALID_TOPIC_FIELDS: 'Use only documented topic operation fields: name/mode, topic/through, id, or topic filter.',
  INVALID_TOPIC_MODE: 'Use broadcast or queue; modes cannot be changed after creation.',
  TOPIC_EXISTS: 'Choose another topic name in this workspace.',
  TOPIC_NOT_FOUND: 'Choose a topic in this workspace.',
  TOPIC_CLOSED: 'This topic is closed; use another open topic.',
  NOT_TOPIC_OWNER: 'A topic owner or this workspace owner can moderate; a subscribed starter can close their own topic thread.',
  TOPIC_DELIVERY_UNAVAILABLE: 'Topic delivery is not available in this release; use user: or ws: messaging.',
  INVALID_INVITE_TOPICS: 'Name exactly one open topic in this workspace.',

  JOIN_NAME_MISMATCH: 'The saved credential belongs to another user; remove it or choose that user.',
  USER_CONFLICT: 'A configured static user collides with a reserved self-onboarded identity; change the static configuration before starting.',
  USER_EXISTS: 'This identity is reserved; choose another name, or join using your existing bearer credential.',
  ALREADY_MEMBER: 'You already belong to this workspace and topic; the invite was not consumed.',
  INVITE_INVALID: 'Use a valid invite code from the workspace owner.',
  INVITE_REVOKED: 'Ask the owner for a new invite; this invite was revoked.',
  INVITE_EXPIRED: 'Ask the owner for a new invite; this invite expired.',
  INVITE_USED: 'Ask the owner for a new invite; this single-use invite was consumed.',
  INVITE_LIMIT: 'Revoke unused invites or wait for expiry before creating more.',
  INVALID_INVITE_FIELDS: 'Use only the documented invite fields; create and credential revoke require an empty object.',
  NOT_MINTED_CREDENTIAL: 'Only self-onboarded credentials can be revoked through this recovery endpoint.',
  RATE_LIMITED: 'Wait for the global join rate limit to refill, then retry.',

  OWNER_TRANSFER_SELF: 'Choose a different current member to receive workspace ownership.',
  NOT_WORKSPACE_OWNER: 'Only the workspace owner can manage invites, remove members or transfer ownership.',
  OWNER_CANNOT_BE_REMOVED: 'Transfer ownership before removing the current owner.',
  OWNER_MUST_TRANSFER: 'Transfer ownership to another current member before leaving or finishing.',
  MEMBER_NOT_FOUND: 'Choose a current workspace member from members.',
  INVALID_MEMBERSHIP_FIELDS: 'Use only the documented membership fields: user for remove/transfer, summary for finish; leave requires an empty object.',
  WORKSPACE_PROTECTED: 'The default workspace cannot be deleted.',
  WORKSPACE_NOT_FOUND: 'Choose an existing workspace name.',

  INVALID_MIGRATION_TARGET: 'Choose a supported migration target version at least as new as the database and no newer than this release.',
  INVALID_WORKSPACE_FIELDS: 'Provide only the workspace name field: {"name":"project-a"}.',
  RESERVED_WORKSPACE: 'Choose a workspace name other than broker, which is reserved for system use.',
  NOT_A_MEMBER: 'Use a workspace where your authenticated user is a current member.',
  SESSION_WORKSPACE_MISMATCH: 'Use a distinct session ID for each workspace; a bound session cannot switch workspaces.',
  WORKSPACE_EXISTS: 'Choose another workspace name.',
  WORKSPACE_LIMIT: 'You reached WORKSPACE_LIMIT owned workspaces; use an existing workspace or ask the operator to raise the limit.',

  BROKER_RUNNING: 'Stop every broker and close all database connections before retrying the offline migration.',
  SCHEMA_UPGRADE_REQUIRED: 'Stop all brokers and run broker:cli migrate --backup <new-file> --owner <user> before restarting.',
  SCHEMA_TOO_NEW: 'Use the matching newer broker release, or stop all brokers and restore the pre-upgrade backup.',
  MIGRATION_BACKUP_REQUIRED: 'Provide --backup <new-file> for a consistent pre-upgrade copy.',
  MIGRATION_BACKUP_FAILED: 'Choose a new writable backup path that does not exist; the database was not migrated.',
  MIGRATION_OWNER_UNKNOWN: 'Choose an owner already present in database history or the configured static users.',
  MIGRATION_OWNER_REQUIRED: 'Provide --owner <user> for the default workspace when upgrading a pre-M5 database.',
  MIGRATION_FK_VIOLATION: 'The migration failed its foreign key check and was rolled back; inspect a copy of the database.',
  MIGRATION_INTEGRITY: 'The migration failed its integrity check and was rolled back; inspect a copy of the database.',

  INVALID_PATHS: 'Provide a bounded array of repository-relative literal file/directory paths; omit absolute paths, traversal, and glob patterns.',
  INVALID_CONTRACTS: 'Provide a bounded array of nonempty contract identifiers without control characters.',
  INVALID_PLANNING_FIELDS: 'Use only documented planning fields; authorship is derived from the token.',
  INVALID_DECISION_BODY: 'Provide nonempty decision text within DECISION_MAX_CHARS.',
  DECISION_LIMIT_REACHED: 'This workstream reached DECISION_MAX_PER_WORKSTREAM; use a new workstream for further planning history.',
  DECISION_PAGE_TOO_SMALL: 'Shorten a new decision or raise DECISION_PAGE_BYTES to fit the complete record; existing history is never silently skipped or truncated.',
  INVALID_DECISION_ID: 'Use a nonnegative integer decision ID for since.',
  INVALID_NAME: 'Use 1-128 letters, digits, underscores, dots, or hyphens for the name.',
  RESERVED_USER: 'Choose a developer name other than broker.',
  RESERVED_SESSION: 'Choose a session ID other than broker.',
  INVALID_ROLE: 'Set X-Agent-Role to manager or worker.',
  SESSION_OWNED: 'Choose a new unique session ID; this one belongs to another user.',
  WORKSTREAM_OWNED: 'Use the owning session, or wait for lease expiry before same-user takeover.',
  WORKSTREAM_NOT_FOUND: 'Register the workstream first or address user:<name>.',
  THREAD_NOT_FOUND: 'Use a thread_id returned by send, or omit it to start a new thread.',
  NOT_PARTICIPANT: 'Use a thread shared by sender and recipient, or start a separate thread.',
  ACK_REQUIRED: 'Ack the latest incoming message before replying or closing this thread.',
  MESSAGE_CLAIMED: 'Stop handling this message; another session claimed it.',
  INVALID_RECIPIENT: 'Address ws:<name>, user:<name> or topic:<slash-safe-name>; sessions cannot be addressed directly.',
  INVALID_KIND: 'Use question, answer, finding, blocker, or decision.',
  INVALID_BODY: 'Provide nonempty text within the configured MAX_MESSAGE_CHARS limit.',
  INVALID_BLOCKING: 'Set blocking to true or false, or omit it.',
  BLOCKING_REQUIRES_QUESTION: 'blocking is allowed on: question, blocker.',
  INVALID_THREAD: 'Provide a thread_id string of at most 128 characters.',
  ANSWER_REQUIRES_THREAD: 'Include the thread_id of the message you are answering.',
  USER_NOT_FOUND: 'Address a member of this workspace.',
  THREAD_ESCALATED: 'Stop this thread and ask a human to resolve or reopen it with the CLI.',
  THREAD_RESOLVED: 'Start a new thread or ask a human to reopen this one with the CLI.',
  THREAD_NOT_REOPENABLE: 'Reopen is allowed only for escalated or resolved threads; send a follow-up on an open or answered thread.',
  INVALID_SINCE: 'Use a nonnegative integer message ID for since.',
  INVALID_MESSAGE_ID: 'Use a positive integer message id returned by inbox or stream.',
  MESSAGE_NOT_FOUND: 'Ack a message delivered to your user/session; refresh inbox first.',
  INVALID_TOKENS: 'Provide a JSON object mapping developer names to unique secret tokens.',
  INVALID_TOKEN: 'Replace placeholders with a unique secret of at least 24 characters and no whitespace.',
  DUPLICATE_TOKEN: 'Generate a different secret token for each developer.',
  EMPTY_TOKENS: 'Configure at least one developer token before starting the broker.',
  BODY_TOO_LARGE: 'Shorten the JSON request to fit MAX_BODY_BYTES.',
  INVALID_JSON: 'Send one valid JSON object as the request body.',
  UNAUTHORIZED: 'Supply Authorization: Bearer <developer-token> from the broker token file.',
  JSON_REQUIRED: 'Set Content-Type: application/json for POST requests.',
  SESSION_STREAM_EXISTS: 'Reuse the existing stream or disconnect it before reconnecting this session.',
  NOT_FOUND: 'Use a documented broker HTTP endpoint and method.',
  REASON_REQUIRED: 'Provide --reason "..." when reopening a thread or deleting a workspace.',
  DATABASE_NOT_FOUND: 'Set BROKER_DB to the existing database file used by the broker; start the broker first to initialize it.',
  REOPEN_LIMIT: 'The reopen limit is reached; use resolve <thread_id> instead.',
  INVALID_CLI_ARGS: 'Use retention --dry-run [--workspace W] or retention --run, list, resolve <thread_id>, reopen <thread_id> --reason "...", migrate --backup <new-file> [--owner <user>] [--to <version>], delete-workspace <name> --reason "...", or audit <workspace>.',
  INTERNAL_ERROR: 'Retry the request; if it persists, ask the broker operator to check its logs.',
};

export class BrokerError extends Error {
  constructor(code, status = 400, hint = hints[code]) {
    super(code);
    this.code = code;
    this.status = status;
    this.hint = hint ?? 'Check the broker configuration and documented request format.';
  }
}

export function check(condition, code, status = 400, hint) {
  if (!condition) throw new BrokerError(code, status, hint);
}

export function rejection(error) {
  const known = error instanceof BrokerError ? error : new BrokerError('INTERNAL_ERROR', 500);
  return { error: known.code, hint: known.hint,...(known.code==='ALREADY_MEMBER'&&known.binding?{user:known.binding.user,workspace:known.binding.workspace,topic:known.binding.topic}:{}),...(known.code==='SESSION_ENDED'&&['topic_left','membership_ended'].includes(known.reason)?{reason:known.reason}:{}) };
}

export function name(value) {
  check(typeof value === 'string' && /^[a-zA-Z0-9_.-]{1,128}$/.test(value), 'INVALID_NAME');
  return value;
}

function integer(env, key, fallback, min = 1, max = Number.MAX_SAFE_INTEGER) {
  const value = Number(env[key] ?? fallback);
  check(Number.isSafeInteger(value) && value >= min && value <= max, `INVALID_${key}`, 400,
    `Set ${key} to an integer from ${min} through ${max}.`);
  return value;
}

export function config(env = process.env) {
  const webhookUrl = env.BROKER_WEBHOOK_URL || null;
  if (webhookUrl) {
    let url;
    try { url = new URL(webhookUrl); } catch {}
    check(url && ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password, 'INVALID_WEBHOOK_URL', 400, 'Set BROKER_WEBHOOK_URL to an HTTP(S) receiver URL without embedded credentials.');
  }
  const defaultOwner = env.BROKER_DEFAULT_OWNER || null;
  if (defaultOwner) { name(defaultOwner); check(defaultOwner !== 'broker', 'RESERVED_USER'); }
  return {
    defaultOwner,
    retentionEnabled: integer(env, 'RETENTION_ENABLED', 1, 0, 1) === 1,
    retentionInterval: integer(env, 'RETENTION_INTERVAL', 3600, 1, Math.floor(Number.MAX_SAFE_INTEGER / 1000)) * 1000,
    retentionBatch: integer(env, 'RETENTION_BATCH', 500, 1, 10000),
    retentionMaxBatches: integer(env, 'RETENTION_MAX_BATCHES', 20, 1, 10000),
    retentionBlockerLimit: integer(env, 'RETENTION_BLOCKER_LIMIT', 100, 1, 10000),
    topicRetention: integer(env, 'TOPIC_RETENTION', 7 * 86400, 1, Math.floor(Number.MAX_SAFE_INTEGER / 1000)) * 1000,
    closedRetention: integer(env, 'CLOSED_RETENTION', 3 * 86400, 1, Math.floor(Number.MAX_SAFE_INTEGER / 1000)) * 1000,
    maxAttempts: integer(env, 'MAX_ATTEMPTS', 3),
    queueExpiryBatch: integer(env, 'QUEUE_EXPIRY_BATCH', 100, 1, 10000),
    claimTtl: integer(env, 'CLAIM_TTL', 300) * 1000,
    maxPins: integer(env, 'MAX_PINS', 100),
    inviteTtl: integer(env, 'INVITE_TTL', 3600) * 1000,
    inviteLimit: integer(env, 'INVITE_LIMIT', 20),
    joinRatePerMin: integer(env, 'JOIN_RATE_PER_MIN', 10),
    joinMaxBodyBytes: integer(env, 'JOIN_MAX_BODY_BYTES', 2048),
    workspaceLimit: integer(env, 'WORKSPACE_LIMIT', 20),
    host: env.BROKER_HOST ?? '127.0.0.1',
    port: integer(env, 'BROKER_PORT', 8091, 0, 65535),
    dbPath: env.BROKER_DB ?? 'data/team-messaging.sqlite',
    tokensPath: env.BROKER_TOKENS ?? 'broker/tokens.json',
    leaseTtl: integer(env, 'LEASE_TTL', 600) * 1000,
    threadCap: integer(env, 'THREAD_CAP', 20),
    maxReopens: integer(env, 'MAX_REOPENS', 3),
    sseHeartbeat: integer(env, 'SSE_HEARTBEAT', 15) * 1000,
    sseBufferBytes: integer(env, 'SSE_BUFFER_BYTES', 4 * 1024 * 1024),
    sseDrainTimeout: integer(env, 'SSE_DRAIN_TIMEOUT', 30) * 1000,
    maxBodyBytes: integer(env, 'MAX_BODY_BYTES', 65536),
    maxMessageChars: integer(env, 'MAX_MESSAGE_CHARS', 16000),
    inboxLimit: integer(env, 'INBOX_LIMIT', 100),
    planningMaxItems: integer(env, 'PLANNING_MAX_ITEMS', 128),
    planningMaxItemChars: integer(env, 'PLANNING_MAX_ITEM_CHARS', 512),
    decisionLimit: integer(env, 'DECISION_LIMIT', 100),
    decisionMaxChars: integer(env, 'DECISION_MAX_CHARS', 2000),
    decisionPageBytes: integer(env, 'DECISION_PAGE_BYTES', 16000, 512),
    decisionMaxPerWorkstream: integer(env, 'DECISION_MAX_PER_WORKSTREAM', 1000),
    overlapLimit: integer(env, 'OVERLAP_LIMIT', 10),
    overlapPairLimit: integer(env, 'OVERLAP_PAIR_LIMIT', 10),
    overlapScanLimit: integer(env, 'OVERLAP_SCAN_LIMIT', 100),
    maintenanceMs: integer(env, 'MAINTENANCE_INTERVAL_MS', 1000),
    blockingTimeout: integer(env, 'BLOCKING_TIMEOUT', 1800) * 1000,
    idleClose: integer(env, 'IDLE_CLOSE', 86400) * 1000,
    webhookUrl,
    webhookTimeoutMs: integer(env, 'WEBHOOK_TIMEOUT_MS', 10000),
    webhookRetryMs: integer(env, 'WEBHOOK_RETRY_MS', 1000),
    webhookRetryMaxMs: integer(env, 'WEBHOOK_RETRY_MAX_MS', 30000),
    webhookBatch: integer(env, 'WEBHOOK_BATCH', 10),
  };
}

// File format: {"alice":"<secret>","bob":"<different-secret>"}.
export function loadTokens(file) {
  const entries = JSON.parse(readFileSync(file, 'utf8'));
  check(entries && typeof entries === 'object' && !Array.isArray(entries), 'INVALID_TOKENS');
  const tokens = new Map();
  for (const [user, token] of Object.entries(entries)) {
    name(user);
    check(user !== 'broker', 'RESERVED_USER');
    check(typeof token === 'string' && token.length >= 24 && !/\s/.test(token) && !/^replace-with/i.test(token), 'INVALID_TOKEN');
    check(!tokens.has(token), 'DUPLICATE_TOKEN');
    tokens.set(token, user);
  }
  check(tokens.size > 0, 'EMPTY_TOKENS');
  return tokens;
}
