import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export class Store {
  constructor(file) {
    if (file !== ':memory:') mkdirSync(dirname(file), { recursive: true });
    this.db = new DatabaseSync(file);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
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
    `);
    // Upgrade an existing M1 database without discarding messages or claims.
    this.transaction(() => {
      const threadColumns = this.all('PRAGMA table_info(threads)').map(c => c.name);
      if (!threadColumns.includes('reopen_count')) {
        this.db.exec(`ALTER TABLE threads ADD COLUMN reopen_count INTEGER NOT NULL DEFAULT 0;
          UPDATE threads SET reopen_count=(SELECT count(*) FROM audit WHERE thread_id=threads.id AND action='human:reopen');`);
      }
      const auditColumns = this.all('PRAGMA table_info(audit)').map(c => c.name);
      if (!auditColumns.includes('previous_msg_count')) this.db.exec('ALTER TABLE audit ADD COLUMN previous_msg_count INTEGER');
      if (!auditColumns.includes('reason')) this.db.exec('ALTER TABLE audit ADD COLUMN reason TEXT');
      const columns = this.all('PRAGMA table_info(escalation_targets)').map(c => c.name);
      if (!columns.includes('acked_at')) this.db.exec('ALTER TABLE escalation_targets ADD COLUMN acked_at INTEGER');
      if (!columns.includes('acked_by_session')) this.db.exec('ALTER TABLE escalation_targets ADD COLUMN acked_by_session TEXT REFERENCES agents(session_id)');
      // A legacy global escalation ack applies only to the acknowledging user's target.
      this.db.exec(`UPDATE escalation_targets SET
        acked_at=(SELECT m.acked_at FROM messages m WHERE m.id=message_id),
        acked_by_session=(SELECT m.acked_by_session FROM messages m WHERE m.id=message_id)
        WHERE EXISTS(SELECT 1 FROM messages m JOIN agents a ON a.session_id=m.acked_by_session
          WHERE m.id=message_id AND m.kind='escalation' AND m.acked_at IS NOT NULL AND a.user=escalation_targets.user);
        UPDATE messages SET acked_at=NULL,acked_by_session=NULL WHERE kind='escalation' AND acked_at IS NOT NULL;`);
    });
  }

  get(sql, ...args) { return this.db.prepare(sql).get(...args); }
  all(sql, ...args) { return this.db.prepare(sql).all(...args); }
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
