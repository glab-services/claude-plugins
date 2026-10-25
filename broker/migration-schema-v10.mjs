// Frozen schema from main 2fa8cf9; historical target 10.
// Version 10 adds per-user topic pins.
export const SCHEMA_VERSION = 10;
export const DEFAULT_WORKSPACE = 'default';
// No workspace DEFAULT: legacy or forgotten inserts must fail loudly.
// Widen CHECKs now to avoid rebuilding the largest FK target again; input validation stays unchanged.
export const TABLES = {
  topic_pins: `CREATE TABLE topic_pins(message_id INTEGER NOT NULL REFERENCES messages(id), user TEXT NOT NULL, workspace TEXT NOT NULL, pinned_at INTEGER NOT NULL, PRIMARY KEY(message_id,user));`,
  topic_cursors: `CREATE TABLE topic_cursors(workspace TEXT NOT NULL, topic TEXT NOT NULL, user TEXT NOT NULL, acked_through INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(workspace,topic,user));`,
  topic_acks: `CREATE TABLE topic_acks(message_id INTEGER NOT NULL REFERENCES messages(id), user TEXT NOT NULL, session TEXT NOT NULL REFERENCES agents(session_id), acked_at INTEGER NOT NULL, PRIMARY KEY(message_id,user));`,
  topics: `CREATE TABLE topics(workspace TEXT NOT NULL REFERENCES workspaces(name), name TEXT NOT NULL, mode TEXT NOT NULL CHECK(mode IN ('broadcast','queue')), owner_user TEXT NOT NULL, created INTEGER NOT NULL, closed_at INTEGER, closed_by_user TEXT, PRIMARY KEY(workspace,name));`,
  topic_subscriptions: `CREATE TABLE topic_subscriptions(workspace TEXT NOT NULL, topic TEXT NOT NULL, user TEXT NOT NULL, subscribed_at INTEGER NOT NULL, PRIMARY KEY(workspace,topic,user), FOREIGN KEY(workspace,topic) REFERENCES topics(workspace,name));`,
  broker_users: `CREATE TABLE broker_users(name TEXT PRIMARY KEY, origin TEXT NOT NULL CHECK(origin='self'), created INTEGER NOT NULL, deactivated_at INTEGER);`,
  credentials: `CREATE TABLE credentials(id TEXT PRIMARY KEY, user TEXT NOT NULL REFERENCES broker_users(name), secret_sha256 TEXT NOT NULL, created INTEGER NOT NULL, invite_id TEXT, deactivated_at INTEGER);`,
  invites: `CREATE TABLE invites(id TEXT PRIMARY KEY, workspace TEXT NOT NULL REFERENCES workspaces(name), secret_sha256 TEXT NOT NULL, created_by_user TEXT NOT NULL, created_by_session TEXT NOT NULL, created INTEGER NOT NULL, expires INTEGER NOT NULL, redeemed_at INTEGER, redeemed_by_user TEXT, revoked_at INTEGER, revoked_by_user TEXT);`,
  workspaces: `CREATE TABLE workspaces(name TEXT PRIMARY KEY, created INTEGER NOT NULL, created_by_user TEXT NOT NULL, deleted_at INTEGER, deleted_reason TEXT);`,
  workspace_members: `CREATE TABLE workspace_members(workspace TEXT NOT NULL REFERENCES workspaces(name), user TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('owner','member')), joined INTEGER NOT NULL, origin TEXT NOT NULL CHECK(origin IN ('static','migrated','creator','invite')), PRIMARY KEY(workspace,user));`,
  workspace_audit: `CREATE TABLE workspace_audit(id INTEGER PRIMARY KEY AUTOINCREMENT, workspace TEXT NOT NULL REFERENCES workspaces(name), action TEXT NOT NULL, actor_user TEXT, actor_session TEXT, subject_user TEXT, detail TEXT, created INTEGER NOT NULL);`,
  agents: `CREATE TABLE agents (
        session_id TEXT PRIMARY KEY, user TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('manager','worker')), last_seen INTEGER NOT NULL,
        workspace TEXT NOT NULL REFERENCES workspaces(name)
      );`,
  workstreams: `CREATE TABLE workstreams (
        name TEXT NOT NULL, owner_session TEXT NOT NULL REFERENCES agents(session_id),
        owner_user TEXT NOT NULL, lease_until INTEGER NOT NULL, paths TEXT, contracts TEXT,
        workspace TEXT NOT NULL REFERENCES workspaces(name), PRIMARY KEY(workspace,name)
      );`,
  decisions: `CREATE TABLE decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, workstream TEXT NOT NULL,
        from_user TEXT NOT NULL, from_session TEXT NOT NULL REFERENCES agents(session_id),
        body TEXT NOT NULL, created INTEGER NOT NULL,
        workspace TEXT NOT NULL REFERENCES workspaces(name)
      );`,
  threads: `CREATE TABLE threads (
        id TEXT PRIMARY KEY,
        status TEXT NOT NULL CHECK(status IN ('open','answered','escalated','resolved')),
        msg_count INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL, last_activity INTEGER NOT NULL,
        reopen_count INTEGER NOT NULL DEFAULT 0, cycle_start_id INTEGER NOT NULL DEFAULT 0,
        workspace TEXT NOT NULL REFERENCES workspaces(name)
      );`,
  messages: `CREATE TABLE messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL REFERENCES threads(id),
        from_user TEXT NOT NULL, from_session TEXT NOT NULL,
        to_kind TEXT NOT NULL CHECK(to_kind IN ('ws','user','topic')), to_name TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('question','answer','finding','blocker','decision','escalation','notice')),
        blocking INTEGER NOT NULL CHECK(blocking IN (0,1)), body TEXT NOT NULL,
        created INTEGER NOT NULL, acked_at INTEGER, acked_by_session TEXT REFERENCES agents(session_id),
        workspace TEXT NOT NULL REFERENCES workspaces(name)
      );`,
  delivery_targets: `CREATE TABLE delivery_targets (
        message_id INTEGER PRIMARY KEY REFERENCES messages(id), user TEXT NOT NULL
      );`,
  participants: `CREATE TABLE participants (
        thread_id TEXT NOT NULL REFERENCES threads(id), user TEXT NOT NULL,
        PRIMARY KEY(thread_id, user)
      );`,
  escalation_targets: `CREATE TABLE escalation_targets (
        message_id INTEGER NOT NULL REFERENCES messages(id), user TEXT NOT NULL,
        acked_at INTEGER, acked_by_session TEXT REFERENCES agents(session_id),
        PRIMARY KEY(message_id, user)
      );`,
  audit: `CREATE TABLE audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL REFERENCES threads(id),
        action TEXT NOT NULL, created INTEGER NOT NULL, previous_msg_count INTEGER, reason TEXT
      );`,
  session_cursors: `CREATE TABLE session_cursors (
        session_id TEXT PRIMARY KEY REFERENCES agents(session_id), acked_id INTEGER NOT NULL DEFAULT 0
      );`,
  webhook_outbox: `CREATE TABLE webhook_outbox (
        message_id INTEGER PRIMARY KEY REFERENCES messages(id), payload TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL, sent_at INTEGER
      );`,
};
// Preserve indexes named by M4 queries; workspace indexes are used starting in M5b.
export const INDEXES = [
  "CREATE INDEX topic_pins_user ON topic_pins(workspace,user);",
  "CREATE INDEX messages_topic ON messages(workspace,to_name,id) WHERE to_kind='topic';",
  "CREATE INDEX invites_ws ON invites(workspace,expires);",
  "CREATE INDEX workstreams_lease ON workstreams(lease_until,name);",
  "CREATE INDEX decisions_workstream ON decisions(workstream,id);",
  "CREATE INDEX messages_thread_id ON messages(thread_id, id);",
  "CREATE INDEX targets_user ON delivery_targets(user, message_id);",
  "CREATE INDEX threads_idle ON threads(status,last_activity);",
  "CREATE INDEX webhook_due ON webhook_outbox(sent_at,next_attempt);",
  "CREATE INDEX messages_pending ON messages(id) WHERE acked_at IS NULL AND kind NOT IN ('escalation','notice') AND to_kind<>'topic';",
  "CREATE INDEX escalation_targets_pending ON escalation_targets(user,message_id) WHERE acked_at IS NULL;",
  "CREATE UNIQUE INDEX workspace_one_owner ON workspace_members(workspace) WHERE role='owner';",
  "CREATE INDEX workspace_members_user ON workspace_members(user,workspace);",
  "CREATE INDEX workspace_audit_ws ON workspace_audit(workspace,id);",
  "CREATE INDEX messages_ws ON messages(workspace,id);",
  "CREATE INDEX agents_ws_user ON agents(workspace,user);",
  "CREATE INDEX workstreams_ws_lease ON workstreams(workspace,lease_until,name);",
  "CREATE INDEX decisions_ws ON decisions(workspace,workstream,id);",
  "CREATE INDEX decisions_ws_id ON decisions(workspace,id);",
  "CREATE INDEX threads_ws ON threads(workspace,last_activity);"
];
// A view occupying the old index name makes old binaries refuse before writing.
export const GUARD = "CREATE VIEW messages_thread AS SELECT 'team-messaging schema 5+: restore the pre-upgrade backup to run an older release' AS reason";
export function createSchema(db) {
  db.exec(Object.values(TABLES).join('\n'));
  // Use the same SQLite ALTER representation as an upgrade, keeping strict schema equality.
  db.exec('ALTER TABLE threads ADD COLUMN topic TEXT; ALTER TABLE invites ADD COLUMN topics TEXT');
  db.exec(INDEXES.join('\n'));
  db.exec(GUARD);
  db.exec(`PRAGMA user_version=${SCHEMA_VERSION}`);
}
