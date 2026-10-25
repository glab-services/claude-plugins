import { testTempDir } from '../../test-support/temp.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { migrate } from '../migrate.mjs';
import { Store } from '../store.mjs';
import { Store as MainStore } from './fixtures/main-store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';

test('an original M1 database upgrade preserves history, participant acks, and previous reopens', t => {
  const dir = testTempDir('broker-upgrade-');
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const file = join(dir, 'db.sqlite');
  // Build the original schema shape, before acknowledgment/audit/reopen columns existed.
  const original = new DatabaseSync(file);
  original.exec(`
    CREATE TABLE agents(session_id TEXT PRIMARY KEY,user TEXT NOT NULL,role TEXT NOT NULL,last_seen INTEGER NOT NULL);
    INSERT INTO agents VALUES ('a','alice','worker',1000);
    CREATE TABLE threads(id TEXT PRIMARY KEY,status TEXT NOT NULL,msg_count INTEGER NOT NULL,created INTEGER NOT NULL,last_activity INTEGER NOT NULL);
    INSERT INTO threads VALUES ('legacy','escalated',20,1000,2000);
    CREATE TABLE messages(id INTEGER PRIMARY KEY AUTOINCREMENT,thread_id TEXT NOT NULL,from_user TEXT NOT NULL,from_session TEXT NOT NULL,
      to_kind TEXT NOT NULL,to_name TEXT NOT NULL,kind TEXT NOT NULL,blocking INTEGER NOT NULL,body TEXT NOT NULL,created INTEGER NOT NULL,acked_at INTEGER,acked_by_session TEXT);
    INSERT INTO messages VALUES (1,'legacy','alice','a','user','bob','question',1,'original body',1000,NULL,NULL);
    INSERT INTO messages VALUES (2,'legacy','broker','broker','user','broker','escalation',0,'original escalation',2000,2001,'a');
    CREATE TABLE escalation_targets(message_id INTEGER NOT NULL,user TEXT NOT NULL,PRIMARY KEY(message_id,user));
    INSERT INTO escalation_targets VALUES (2,'alice'),(2,'bob');
    CREATE TABLE audit(id INTEGER PRIMARY KEY AUTOINCREMENT,thread_id TEXT NOT NULL,action TEXT NOT NULL,created INTEGER NOT NULL);
    INSERT INTO audit(thread_id,action,created) VALUES ('legacy','human:reopen',1200),('legacy','human:reopen',1500);
  `);
  original.close();
  migrate(file, { backup: join(dir, 'pre-upgrade.sqlite'), owner: 'alice' });
  let store = new Store(file);
  try {
    assert.equal(store.get("SELECT reopen_count FROM threads WHERE id='legacy'").reopen_count, 2);
    assert.equal(store.get('SELECT body FROM messages WHERE id=1').body, 'original body');
    assert.equal(store.get('SELECT acked_at FROM messages WHERE id=2').acked_at, null);
    assert.equal(store.get("SELECT acked_by_session FROM escalation_targets WHERE user='alice'").acked_by_session, 'a');
    assert.equal(store.get("SELECT acked_at FROM escalation_targets WHERE user='bob'").acked_at, null);
    assert.equal(store.get('SELECT count(*) n FROM audit').n, 2);
  } finally { store.close(); }
  store = new Store(file); // Upgrades are idempotent, including already-migrated claims.
  try {
    assert.equal(store.get("SELECT reopen_count FROM threads WHERE id='legacy'").reopen_count, 2);
    assert.equal(store.get("SELECT acked_by_session FROM escalation_targets WHERE user='alice'").acked_by_session, 'a');
  } finally { store.close(); }
});

test('upgrading real pre-M3 main quarantines every existing ask and arms only new asks', t => {
  const dir = testTempDir('migration-cutoff-');
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const file = join(dir, 'broker.sqlite');
  const legacy = new MainStore(file); // Exact Store snapshot from main at 17ba1f0.
  legacy.run("INSERT INTO agents VALUES ('a','alice','worker',1000),('b','bob','worker',1000)");
  for (const [id, status] of [['closed','answered'], ['reopened','open']]) {
    legacy.run('INSERT INTO threads(id,status,msg_count,created,last_activity,reopen_count) VALUES (?,?,1,1000,1000,?)', id, status, id === 'reopened' ? 1 : 0);
    const row = legacy.run(`INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,acked_at,acked_by_session)
      VALUES (?,'alice','a','user','bob','question',1,'old question',1000,1000,'b')`, id);
    legacy.run('INSERT INTO delivery_targets VALUES (?,?)', row.lastInsertRowid, 'bob');
    legacy.run('INSERT INTO participants VALUES (?,?),(?,?)', id, 'alice', id, 'bob');
    if (id === 'reopened') legacy.run("INSERT INTO audit(thread_id,action,created,previous_msg_count,reason) VALUES (?,'human:reopen',1000,20,'review')", id);
  }
  legacy.close();
  migrate(file, { backup: join(dir, 'pre-upgrade.sqlite'), owner: 'alice' });
  let store = new Store(file);
  t.after(() => store.close());
  let now = 10000;
  const settings = { ...config({}), blockingTimeout: 100, idleClose: 100000, webhookUrl: 'http://127.0.0.1:1' };
  let broker = new Broker(store, settings, ['alice','bob'], { now: () => now, log: () => {} });
  broker.expireThreads();
  assert.equal(store.get("SELECT status FROM threads WHERE id='closed'").status, 'answered');
  assert.equal(store.get("SELECT status FROM threads WHERE id='reopened'").status, 'open');
  assert.equal(store.get('SELECT count(*) n FROM webhook_outbox').n, 0);
  for (const row of store.all('SELECT id,cycle_start_id FROM threads')) {
    assert.equal(row.cycle_start_id, store.get('SELECT max(id) id FROM messages WHERE thread_id=?', row.id).id);
  }
  broker.send('alice','a',{to:'user:bob',thread_id:'reopened',kind:'question',blocking:true,body:'new after upgrade'});
  store.close();
  store = new Store(file); // Reopening must not advance the migration cutoff again.
  broker = new Broker(store, settings, ['alice','bob'], { now: () => now, log: () => {} });
  now += 100;
  broker.expireThreads();
  assert.equal(store.get("SELECT status FROM threads WHERE id='reopened'").status, 'escalated');
  assert.equal(store.get('SELECT count(*) n FROM webhook_outbox').n, 1);
});
