// Frozen from main e0a4d7598c13542343ccc34b6cd26a8458132b57; only schema/config imports redirected.
import { createSchema, SCHEMA_VERSION } from '../../migration-schema-v11.mjs';
import { check } from '../../config.mjs';
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export class Store {
  constructor(file) {
    if (file !== ':memory:') mkdirSync(dirname(file), { recursive: true });
    this.db = new DatabaseSync(file);
    try {
      this.db.exec(`
        PRAGMA journal_mode = WAL;
        -- Synchronize each WAL commit before acknowledging messages or claims.
        -- SIGKILL tests cover process crashes; they do not simulate power loss.
        PRAGMA synchronous = FULL;
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 5000;
      `);
      this.transaction(() => {
        const version = this.get('PRAGMA user_version').user_version;
        const empty = !this.get("SELECT 1 FROM sqlite_schema LIMIT 1");
        if (version === 0 && empty) return createSchema(this.db);
        check(version <= SCHEMA_VERSION, 'SCHEMA_TOO_NEW', 500);
        check(version === SCHEMA_VERSION, 'SCHEMA_UPGRADE_REQUIRED', 500);
      });
    } catch (error) { this.db.close(); throw error; }
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
