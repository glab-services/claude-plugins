// Verbatim Store from M4 merge 14e9889a647482d6e50a456dd56c27bd8ec8eaf6.
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export class Store {
  constructor(file) {
    if (file !== ':memory:') mkdirSync(dirname(file), { recursive: true });
    this.db = new DatabaseSync(file);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      -- Synchronize each WAL commit before acknowledging messages or claims.
      -- SIGKILL tests cover process crashes; they do not simulate power loss.
      PRAGMA synchronous = FULL;
      PRAGMA foreign_keys = ON;
      PRAGMA busy_timeout = 5000;
      CREATE TABLE IF NOT EXISTS agents (
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
    `);
    // Upgrade an existing M1 database without discarding messages or claims.
    this.transaction(() => {
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
    });
  }

  get(sql, ...args) { return this.db.prepare(sql).get(...args); }
  all(sql, ...args) { return this.db.prepare(sql).all(...args); }
  iterate(sql, ...args) { return this.db.prepare(sql).iterate(...args); }
  run(sql, ...args) { return this.db.prepare(sql).run(...args); }
  transaction(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const value = fn();
      this.db.exec('COMMIT');
      return value;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }
  close() { this.db.close(); }
}
