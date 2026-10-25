import { v13ToV14 } from './migration-v14.mjs';
import { DatabaseSync } from 'node:sqlite';
import { statSync } from 'node:fs';
import { check, name, BrokerError } from './config.mjs';
import { SCHEMA_VERSION } from './schema.mjs';
import { TABLES as V8_TABLES } from './migration-schema-v8.mjs';
import { TABLES as V7_TABLES, INDEXES as V7_INDEXES, GUARD as V7_GUARD } from './migration-schema-v7.mjs';

const M4_DDL = `      CREATE TABLE IF NOT EXISTS agents (
        session_id TEXT PRIMARY KEY, user TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('manager','worker')), last_seen INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS workstreams (
        name TEXT PRIMARY KEY, owner_session TEXT NOT NULL REFERENCES agents(session_id),
        owner_user TEXT NOT NULL, lease_until INTEGER NOT NULL, paths TEXT, contracts TEXT
      );
      CREATE INDEX IF NOT EXISTS workstreams_lease ON workstreams(lease_until,name);
      -- Keep planning history even after a workstream is released or registered again.
      CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, workstream TEXT NOT NULL,
        from_user TEXT NOT NULL, from_session TEXT NOT NULL REFERENCES agents(session_id),
        body TEXT NOT NULL, created INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS decisions_workstream ON decisions(workstream,id);
      CREATE TABLE IF NOT EXISTS threads (
        id TEXT PRIMARY KEY,
        status TEXT NOT NULL CHECK(status IN ('open','answered','escalated','resolved')),
        msg_count INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL, last_activity INTEGER NOT NULL,
        reopen_count INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL REFERENCES threads(id),
        from_user TEXT NOT NULL, from_session TEXT NOT NULL,
        to_kind TEXT NOT NULL CHECK(to_kind IN ('ws','user')), to_name TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('question','answer','finding','blocker','decision','escalation')),
        blocking INTEGER NOT NULL CHECK(blocking IN (0,1)), body TEXT NOT NULL,
        created INTEGER NOT NULL, acked_at INTEGER, acked_by_session TEXT REFERENCES agents(session_id)
      );
      -- Snapshot the user at send time: a later workstream release/rebind cannot leak history.
      CREATE TABLE IF NOT EXISTS delivery_targets (
        message_id INTEGER PRIMARY KEY REFERENCES messages(id), user TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS participants (
        thread_id TEXT NOT NULL REFERENCES threads(id), user TEXT NOT NULL,
        PRIMARY KEY(thread_id, user)
      );
      -- Escalation messages fan out to participants rather than one normal recipient.
      CREATE TABLE IF NOT EXISTS escalation_targets (
        message_id INTEGER NOT NULL REFERENCES messages(id), user TEXT NOT NULL,
        acked_at INTEGER, acked_by_session TEXT REFERENCES agents(session_id),
        PRIMARY KEY(message_id, user)
      );
      CREATE TABLE IF NOT EXISTS audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL REFERENCES threads(id),
        action TEXT NOT NULL, created INTEGER NOT NULL, previous_msg_count INTEGER, reason TEXT
      );
      CREATE INDEX IF NOT EXISTS messages_thread ON messages(thread_id, id);
      CREATE INDEX IF NOT EXISTS targets_user ON delivery_targets(user, message_id);
      CREATE TABLE IF NOT EXISTS session_cursors (
        session_id TEXT PRIMARY KEY REFERENCES agents(session_id), acked_id INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS webhook_outbox (
        message_id INTEGER PRIMARY KEY REFERENCES messages(id), payload TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL, sent_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS threads_idle ON threads(status,last_activity);
      CREATE INDEX IF NOT EXISTS webhook_due ON webhook_outbox(sent_at,next_attempt);
      CREATE INDEX IF NOT EXISTS messages_pending ON messages(id) WHERE acked_at IS NULL AND kind<>'escalation';
    `;
function legacyToM4(db) {
  const store = { db, all: (sql, ...args) => db.prepare(sql).all(...args) };
  (function () {
      const threadColumns = this.all('PRAGMA table_info(threads)').map(c => c.name);
      if (!threadColumns.includes('reopen_count')) {
        this.db.exec(`ALTER TABLE threads ADD COLUMN reopen_count INTEGER NOT NULL DEFAULT 0;
          UPDATE threads SET reopen_count=(SELECT count(*) FROM audit WHERE thread_id=threads.id AND action='human:reopen');`);
      }
      if (!threadColumns.includes('cycle_start_id')) {
        this.db.exec(`ALTER TABLE threads ADD COLUMN cycle_start_id INTEGER NOT NULL DEFAULT 0;
          UPDATE threads SET cycle_start_id=coalesce((SELECT max(m.id) FROM messages m
            WHERE m.thread_id=threads.id),0);`); // Only asks created after upgrade start M3 timers.
      }
      const auditColumns = this.all('PRAGMA table_info(audit)').map(c => c.name);
      if (!auditColumns.includes('previous_msg_count')) this.db.exec('ALTER TABLE audit ADD COLUMN previous_msg_count INTEGER');
      if (!auditColumns.includes('reason')) this.db.exec('ALTER TABLE audit ADD COLUMN reason TEXT');
      const columns = this.all('PRAGMA table_info(escalation_targets)').map(c => c.name);
      if (!columns.includes('acked_at')) this.db.exec('ALTER TABLE escalation_targets ADD COLUMN acked_at INTEGER');
      if (!columns.includes('acked_by_session')) this.db.exec('ALTER TABLE escalation_targets ADD COLUMN acked_by_session TEXT REFERENCES agents(session_id)');
      this.db.exec('CREATE INDEX IF NOT EXISTS escalation_targets_pending ON escalation_targets(user,message_id) WHERE acked_at IS NULL');
      // A legacy global escalation ack applies only to the acknowledging user's target.
      this.db.exec(`UPDATE escalation_targets SET
        acked_at=(SELECT m.acked_at FROM messages m WHERE m.id=message_id),
        acked_by_session=(SELECT m.acked_by_session FROM messages m WHERE m.id=message_id)
        WHERE EXISTS(SELECT 1 FROM messages m JOIN agents a ON a.session_id=m.acked_by_session
          WHERE m.id=message_id AND m.kind='escalation' AND m.acked_at IS NOT NULL AND a.user=escalation_targets.user);
        UPDATE messages SET acked_at=NULL,acked_by_session=NULL WHERE kind='escalation' AND acked_at IS NOT NULL;`);
      this.db.exec(`INSERT INTO session_cursors(session_id,acked_id)
        SELECT session_id,max(id) FROM (
          SELECT acked_by_session AS session_id,id FROM messages WHERE acked_by_session IS NOT NULL
          UNION ALL SELECT acked_by_session AS session_id,message_id AS id FROM escalation_targets WHERE acked_by_session IS NOT NULL
        ) GROUP BY session_id
        ON CONFLICT(session_id) DO UPDATE SET acked_id=max(acked_id,excluded.acked_id);`);
  }).call(store);
}

function historyUsers(db) {
  const users = new Set();
  // Pre-M4 files can omit tables introduced by later milestones.
  for (const [table, column] of [['agents','user'], ['workstreams','owner_user'], ['participants','user'],
    ['delivery_targets','user'], ['escalation_targets','user'], ['decisions','from_user'], ['messages','from_user']]) {
    if (!db.prepare("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?").get(table)) continue;
    for (const row of db.prepare(`SELECT ${column} AS user FROM ${table}`).all()) users.add(row.user);
  }
  users.delete('broker');
  return users;
}

function validateOwner(db, owner, staticUsers) {
  check(owner, 'MIGRATION_OWNER_REQUIRED'); name(owner); check(owner !== 'broker', 'RESERVED_USER');
  check(historyUsers(db).has(owner) || staticUsers.includes(owner), 'MIGRATION_OWNER_UNKNOWN');
}

function v4ToV5(db, { owner, now }) {
  const run = (sql, ...args) => db.prepare(sql).run(...args);
  for (const table of ['workspaces', 'workspace_members', 'workspace_audit']) db.exec(V7_TABLES[table]);
  const created = now();
  run("INSERT INTO workspaces(name,created,created_by_user) VALUES ('default',?,?)", created, owner);
  const users = historyUsers(db);
  users.add(owner);
  run("INSERT INTO workspace_audit(workspace,action,actor_user,created) VALUES ('default','workspace:create',?,?)", owner, created);
  for (const user of users) {
    run("INSERT INTO workspace_members VALUES ('default',?,?,?,'migrated')", user, user === owner ? 'owner' : 'member', created);
    run("INSERT INTO workspace_audit(workspace,action,actor_user,subject_user,detail,created) VALUES ('default','member:add',?,?,?,?)", owner, user, '{"origin":"migrated"}', created);
  }
  const rebuilt = ['agents', 'workstreams', 'decisions', 'threads', 'messages'];
  // M1's additive upgrades never supplied these original foreign keys.
  for (const [table, column, target] of [['audit', 'thread_id', 'threads'], ['escalation_targets', 'message_id', 'messages']]) {
    if (!db.prepare(`PRAGMA foreign_key_list(${table})`).all().some(fk => fk.from === column && fk.table === target)) rebuilt.push(table);
  }
  for (const table of rebuilt) {
    const saved = db.prepare('SELECT seq FROM sqlite_sequence WHERE name=?').get(table)?.seq;
    const columns = db.prepare(`PRAGMA table_info(${table})`).all().map(row => row.name).join(',');
    db.exec(V7_TABLES[table].replace(`CREATE TABLE ${table}`, `CREATE TABLE ${table}_v5`));
    const workspace = ['audit', 'escalation_targets'].includes(table) ? '' : ',workspace';
    const value = workspace ? ",'default'" : '';
    db.exec(`INSERT INTO ${table}_v5(${columns}${workspace}) SELECT ${columns}${value} FROM ${table};
      DROP TABLE ${table}; ALTER TABLE ${table}_v5 RENAME TO ${table};`);
    if (saved !== undefined) run('UPDATE sqlite_sequence SET seq=max(seq,?) WHERE name=?', saved, table);
  }
  // Preserve schema 5's predicate when a caller stops the migration chain there.
  for (const sql of V7_INDEXES.filter(sql => !sql.includes(' invites('))) db.exec(sql.replace("kind NOT IN ('escalation','notice')", "kind<>'escalation'").replace('INDEX ', 'INDEX IF NOT EXISTS '));
  db.exec(V7_GUARD);
  return { owner, members: users.size };
}
function v5ToV6(db) {
  db.exec('DROP INDEX messages_pending');
  db.exec(V7_INDEXES.find(sql => sql.startsWith('CREATE INDEX messages_pending ')));
}
function v6ToV7(db) {
  for (const table of ['broker_users', 'credentials', 'invites']) db.exec(V7_TABLES[table]);
  db.exec(V7_INDEXES.find(sql => sql.startsWith('CREATE INDEX invites_ws ')));
}
function v7ToV8(db) {
  for (const table of ['topics','topic_subscriptions']) db.exec(V8_TABLES[table]);
  db.exec('ALTER TABLE threads ADD COLUMN topic TEXT; ALTER TABLE invites ADD COLUMN topics TEXT');
}
// Literal versioned DDL: later current-table/index changes must not change target 9.
function v8ToV9(db) {
  db.exec(`CREATE TABLE topic_cursors(workspace TEXT NOT NULL, topic TEXT NOT NULL, user TEXT NOT NULL, acked_through INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(workspace,topic,user));
    CREATE TABLE topic_acks(message_id INTEGER NOT NULL REFERENCES messages(id), user TEXT NOT NULL, session TEXT NOT NULL REFERENCES agents(session_id), acked_at INTEGER NOT NULL, PRIMARY KEY(message_id,user));
    CREATE INDEX messages_topic ON messages(workspace,to_name,id) WHERE to_kind='topic';
    DROP INDEX messages_pending;
    CREATE INDEX messages_pending ON messages(id) WHERE acked_at IS NULL AND kind NOT IN ('escalation','notice') AND to_kind<>'topic';
    INSERT INTO topic_cursors(workspace,topic,user,acked_through) SELECT workspace,topic,user,0 FROM topic_subscriptions;`);
}
function v9ToV10(db) {
  db.exec(`CREATE TABLE topic_pins(message_id INTEGER NOT NULL REFERENCES messages(id), user TEXT NOT NULL, workspace TEXT NOT NULL, pinned_at INTEGER NOT NULL, PRIMARY KEY(message_id,user));
    CREATE INDEX topic_pins_user ON topic_pins(workspace,user);`);
}
function v10ToV11(db) {
  db.exec(`CREATE TABLE queue_items(message_id INTEGER PRIMARY KEY REFERENCES messages(id), workspace TEXT NOT NULL, topic TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('ready','claimed','done','dead')), attempts INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0, holder_user TEXT, holder_session TEXT, lease_until INTEGER, done_at INTEGER, done_generation INTEGER, result TEXT);
    CREATE INDEX queue_ready ON queue_items(workspace,topic,state,message_id);`);
}
function v11ToV12(db, { now }) {
  db.exec(`ALTER TABLE messages ADD COLUMN purged_at INTEGER;
    ALTER TABLE messages ADD COLUMN purge_run_id INTEGER;
    CREATE TABLE broker_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);`);
  db.prepare("INSERT INTO broker_meta(key,value) VALUES ('retention_epoch',?)").run(String(now()));
}
function v12ToV13(db) {
  db.exec('CREATE TABLE retention_runs(id INTEGER PRIMARY KEY AUTOINCREMENT, started INTEGER NOT NULL, finished INTEGER, purged INTEGER NOT NULL DEFAULT 0, config TEXT NOT NULL);');
}
export const STEPS = [{ to: 5, run: v4ToV5 }, { to: 6, run: v5ToV6 }, { to: 7, run: v6ToV7 }, { to: 8, run: v7ToV8 }, { to: 9, run: v8ToV9 }, { to: 10, run: v9ToV10 }, { to: 11, run: v10ToV11 }, { to: 12, run: v11ToV12 }, { to: 13, run: v12ToV13 }, { to: 14, run: v13ToV14 }];

// afterBackup is a test-only hook to prove the held exclusive lock excludes writers.
export function migrate(file, { backup, owner, now = Date.now, afterBackup, staticUsers = [], to = SCHEMA_VERSION } = {}) {
  check(file !== ':memory:' && statSync(file, { throwIfNoEntry: false })?.isFile(), 'DATABASE_NOT_FOUND');
  const db = new DatabaseSync(file);
  try {
    db.exec('PRAGMA busy_timeout=5000');
    const version = () => db.prepare('PRAGMA user_version').get().user_version;
    const from = version();
    check(from <= SCHEMA_VERSION, 'SCHEMA_TOO_NEW', 500);
    check(STEPS.some(step => step.to === to) && to <= SCHEMA_VERSION && to >= from, 'INVALID_MIGRATION_TARGET');
    if (from === to) return { changed: false };
    check(backup, 'MIGRATION_BACKUP_REQUIRED');
    // Keep the exclusive lock through VACUUM and migration, including their gap.
    // WAL connections that have touched the database must be closed first.
    try { db.exec('PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE; COMMIT'); }
    catch (error) {
      if (db.isTransaction) db.exec('ROLLBACK');
      if ((error.errcode & 255) === 5 || (error.errcode & 255) === 6) throw new BrokerError('BROKER_RUNNING', 500);
      throw error;
    }
    if (from === 0) validateOwner(db, owner, [...staticUsers]);
    const previousUmask = process.umask(0o077);
    try { db.prepare('VACUUM INTO ?').run(backup); }
    catch { throw new BrokerError('MIGRATION_BACKUP_FAILED', 500); }
    finally { process.umask(previousUmask); }
    afterBackup?.();
    db.exec('PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE');
    const current = version();
    check(current <= SCHEMA_VERSION, 'SCHEMA_TOO_NEW', 500);
    check(current <= to, 'INVALID_MIGRATION_TARGET');
    if (current === to) { db.exec('ROLLBACK'); return { changed: false }; }
    if (current === 0) { db.exec(M4_DDL); legacyToM4(db); }
    let result = {};
    let applied = current;
    for (const step of STEPS) if (step.to > current && step.to <= to) {
      result = { ...result, ...step.run(db, { owner, now }) }; applied = step.to;
    }
    check(db.prepare('PRAGMA foreign_key_check').all().length === 0, 'MIGRATION_FK_VIOLATION', 500);
    check(db.prepare('PRAGMA integrity_check').get().integrity_check === 'ok', 'MIGRATION_INTEGRITY', 500);
    db.exec(`PRAGMA user_version=${applied}; COMMIT`);
    return { changed: true, from, to: applied, backup, ...result };
  } finally {
    try { if (db.isTransaction) db.exec('ROLLBACK'); }
    finally {
      try { db.exec('PRAGMA foreign_keys=ON'); }
      finally { db.close(); }
    }
  }
}
