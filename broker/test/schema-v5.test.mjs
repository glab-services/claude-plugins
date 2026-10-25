import { test } from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { rmSync, existsSync, statSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { testTempDir } from '../../test-support/temp.mjs';
import { Store as V11Store } from './fixtures/v11-store.mjs';
import { Store as V12Store } from './fixtures/v12-store.mjs';
import { createSchema as createV12Schema } from '../migration-schema-v12.mjs';
import { createSchema as createV11Schema } from '../migration-schema-v11.mjs';
import { Store as V10Store } from './fixtures/v10-store.mjs';
import { createSchema as createV10Schema } from '../migration-schema-v10.mjs';
import { Store as V9Store } from './fixtures/v9-store.mjs';
import { createSchema as createV9Schema } from '../migration-schema-v9.mjs';
import { Store as V8Store } from './fixtures/v8-store.mjs';
import { Store as V7Store } from './fixtures/v7-store.mjs';
import { Store as M4Store } from './fixtures/m4-store.mjs';
import { Store as MainStore } from './fixtures/main-store.mjs';
import { Store } from '../store.mjs';
import { migrate } from '../migrate.mjs';
import { SCHEMA_VERSION } from '../schema.mjs';
import { createSchema } from '../schema.mjs';
import { createSchema as createV8Schema } from '../migration-schema-v8.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { httpFixture } from './http.mjs';

function fixture(t) {
  const dir = testTempDir('schema-v5-');
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const file = join(dir, 'broker.sqlite'), backup = join(dir, 'backup.sqlite');
  const legacy = new M4Store(file);
  legacy.db.exec(`
    INSERT INTO agents VALUES ('a','u_agents','worker',100);
    INSERT INTO workstreams VALUES ('api','a','u_ws_owner',200,'["src"]','["api:v1"]');
    INSERT INTO decisions(workstream,from_user,from_session,body,created) VALUES ('api','u_decision','a','retained decision',100);
    INSERT INTO threads(id,status,msg_count,created,last_activity) VALUES ('thread','escalated',2,100,101);
    INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,acked_at,acked_by_session)
      VALUES ('thread','u_sender','a','user','u_target','question',1,'retained body',100,101,'a');
    INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created)
      VALUES ('thread','broker','broker','user','broker','escalation',0,'retained escalation',101);
    INSERT INTO delivery_targets VALUES (1,'u_target');
    INSERT INTO participants VALUES ('thread','u_participant');
    INSERT INTO escalation_targets VALUES (2,'u_escalation',102,'a');
    INSERT INTO audit(thread_id,action,created,previous_msg_count,reason) VALUES ('thread','human:reopen',100,20,'reviewed');
    INSERT INTO session_cursors VALUES ('a',2);
    INSERT INTO webhook_outbox VALUES (2,'{"event_id":"fixture-event"}',1,150,NULL);
    UPDATE sqlite_sequence SET seq=80 WHERE name='messages';
  `);
  legacy.close();
  return { dir, file, backup, upgrade: options => migrate(file, { backup, owner: 'alice', staticUsers: ['alice'], ...options }) };
}
function snapshot(db) {
  return Object.fromEntries(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all()
    .map(({ name }) => [name, db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all().map(row => ({ ...row }))]));
}
// v14 adds nullable scope; historical topic messages retain their existing topic.
function topicBindingRows(table,rows) {
  return rows.map(row=>{
    const copy={...row};
    if(table==='agents')for(const key of ['ended_at','ended_reason'])if(Object.hasOwn(copy,key)){assert.equal(copy[key],null);delete copy[key];}
    if(table==='messages'&&Object.hasOwn(copy,'notice_scope')){assert.equal(copy.notice_scope,null);delete copy.notice_scope;}
    if(['agents','messages','decisions','workstreams','invites'].includes(table)&&Object.hasOwn(copy,'topic')) {
      assert.equal(copy.topic,table==='messages'&&copy.to_kind==='topic'?copy.to_name:null);
      delete copy.topic;
    }
    return copy;
  });
}
// Assert nullable tombstone backfill without weakening historical value/DDL checks.
function retentionRows(table,rows) {
  return topicBindingRows(table,rows).map(row=>{
    const copy={...row};
    if(table==='messages')for(const key of ['purged_at','purge_run_id']) {
      if(Object.hasOwn(copy,key)) {assert.equal(copy[key],null);delete copy[key];}
    }
    return copy;
  });
}
// Historical fixtures predate only these nullable v8 columns. Verify their
// null backfill, then compare every pre-existing value without relaxing DDL equality.
function previousRows(table, rows) {
  const column = table === 'threads' ? 'topic' : table === 'invites' ? 'topics' : null;
  return retentionRows(table,rows).map(row => {
    const copy = { ...row };
    if (column && Object.hasOwn(copy, column)) { assert.equal(copy[column], null); delete copy[column]; }
    return copy;
  });
}
function schema(db) {
  return db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").all()
    .map(row => ({ ...row, sql: row.sql.replaceAll('"','').replace(/\s+/g,' ').trim() }));
}
function open(t, file) { const s = new Store(file); t.after(() => s.close()); return s; }

test('an M4 database migrates preserving every row, id, claim, sequence and history identity', t => {
  const f = fixture(t), old = new DatabaseSync(f.file), before = snapshot(old);
  const sequences = old.prepare('SELECT * FROM sqlite_sequence ORDER BY name').all(); old.close();
  assert.deepEqual(f.upgrade(), { changed: true, from: 0, to: SCHEMA_VERSION, backup: f.backup, owner: 'alice', members: 8, revoked_invites: 0 });
  const s = open(t, f.file), after = snapshot(s.db);
  for (const [table, rows] of Object.entries(before)) {
    assert.deepEqual(previousRows(table,after[table]).map(({ workspace, ...row }) => row), rows, table);
    if (['agents','workstreams','decisions','threads','messages'].includes(table)) assert.ok(after[table].every(row => row.workspace === 'default'));
  }
  assert.deepEqual(s.all("SELECT * FROM sqlite_sequence WHERE name<>'workspace_audit' ORDER BY name"), sequences);
  assert.deepEqual(s.all('PRAGMA foreign_key_check'), []);
  assert.equal(s.get('PRAGMA integrity_check').integrity_check, 'ok');
  assert.equal(s.get("SELECT count(*) n FROM workspace_members WHERE role='owner'").n, 1);
  assert.deepEqual(s.all('SELECT user FROM workspace_members ORDER BY user').map(row => row.user),
    ['alice','u_agents','u_decision','u_escalation','u_participant','u_sender','u_target','u_ws_owner']);
  assert.equal(s.get("SELECT count(*) n FROM workspace_audit WHERE action='member:add'").n, 8);
  assert.equal(s.get("SELECT count(*) n FROM workspace_audit WHERE action='workspace:create'").n, 1);
});

test('a migrated M4 file and a fresh file have identical schemas', t => {
  const f = fixture(t); f.upgrade();
  const migrated = open(t, f.file), fresh = open(t, ':memory:');
  assert.deepEqual(schema(migrated.db), schema(fresh.db));
  assert.equal(migrated.get('PRAGMA user_version').user_version, SCHEMA_VERSION);
  assert.equal(fresh.get('PRAGMA user_version').user_version, SCHEMA_VERSION);
});

test('the rebuilt CHECKs admit topic and notice rows and reject an invalid address kind', t => {
  const f = fixture(t); f.upgrade();
  for (const s of [open(t, f.file), open(t, ':memory:')]) {
    const broker = new Broker(s, config({}), ['alice']); broker.heartbeat('alice','test');
    s.run("INSERT INTO threads(id,status,created,last_activity,workspace) VALUES ('checks','open',1,1,'default')");
    const sql = "INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace) VALUES ('checks','alice','test',?,'x',?,0,'check',1,'default')";
    s.run(sql, 'topic', 'finding'); s.run(sql, 'user', 'notice');
    assert.throws(() => s.run(sql, 'bogus', 'finding'), /CHECK/);
  }
});

test('the pre-upgrade backup is exact and opens under the M4 store', t => {
  const f = fixture(t), db = new DatabaseSync(f.file), before = snapshot(db), originalSchema = schema(db);
  const sequences = db.prepare('SELECT * FROM sqlite_sequence ORDER BY name').all(); db.close();
  f.upgrade(); const backup = new M4Store(f.backup); t.after(() => backup.close());
  assert.deepEqual(snapshot(backup.db), before); assert.deepEqual(schema(backup.db), originalSchema);
  assert.deepEqual(backup.all('SELECT * FROM sqlite_sequence ORDER BY name'), sequences);
  assert.equal(backup.get('PRAGMA user_version').user_version, 0);
});

test('M4 and main stores refuse an upgraded file before writing', t => {
  const f = fixture(t); f.upgrade(); const db = new DatabaseSync(f.file); t.after(() => db.close());
  const before = snapshot(db), ddl = schema(db);
  for (const Legacy of [M4Store, MainStore]) {
    assert.throws(() => new Legacy(f.file), /already a table named messages_thread/);
    assert.deepEqual(snapshot(db), before); assert.deepEqual(schema(db), ddl);
  }
});

test('migration refuses an idle open M4 connection before taking a backup', t => {
  const f = fixture(t), legacy = new M4Store(f.file);
  const before = snapshot(legacy.db);
  try {
    assert.throws(() => f.upgrade(), e => e.code === 'BROKER_RUNNING' && /Stop/.test(e.hint));
    assert.equal(existsSync(f.backup), false);
    assert.deepEqual(snapshot(legacy.db), before);
  } finally { legacy.close(); }
  assert.equal(f.upgrade().changed, true);
});

test('the exclusive migration lock prevents writes after backup until completion', t => {
  const f = fixture(t);
  f.upgrade({ afterBackup() {
    const writer = new DatabaseSync(f.file);
    try { assert.throws(() => writer.exec("INSERT INTO agents VALUES ('late','late-user','worker',100)"), /locked|busy/); }
    finally { writer.close(); }
  } });
  const db = new DatabaseSync(f.file), backup = new DatabaseSync(f.backup); t.after(() => { db.close(); backup.close(); });
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, SCHEMA_VERSION);
  assert.equal(db.prepare("SELECT 1 FROM agents WHERE session_id='late'").get(), undefined);
  assert.equal(backup.prepare("SELECT 1 FROM agents WHERE session_id='late'").get(), undefined);
});

test('migration is idempotent and refuses missing owner, existing backup and newer schema', t => {
  const f = fixture(t);
  assert.throws(() => f.upgrade({ owner: undefined }), { code: 'MIGRATION_OWNER_REQUIRED' });
  assert.equal(existsSync(f.backup), false);
  f.upgrade();
  const other = fixture(t);
  assert.throws(() => other.upgrade({ backup: f.backup }), { code: 'MIGRATION_BACKUP_FAILED' });
  assert.deepEqual(f.upgrade(), { changed: false });
  const db = new DatabaseSync(f.file); db.exec(`PRAGMA user_version=${SCHEMA_VERSION + 1}`); db.close();
  assert.throws(() => f.upgrade(), { code: 'SCHEMA_TOO_NEW' });
});

test('the broker refuses an unmigrated or newer database with an actionable hint', t => {
  const f = fixture(t);
  assert.throws(() => new Store(f.file), e => e.code === 'SCHEMA_UPGRADE_REQUIRED' && /migrate/.test(e.hint));
  f.upgrade();
  const db = new DatabaseSync(f.file); db.exec(`PRAGMA user_version=${SCHEMA_VERSION + 1}`); db.close();
  assert.throws(() => new Store(f.file), e => e.code === 'SCHEMA_TOO_NEW' && /backup/.test(e.hint));
});

test('HTTP send still refuses kind notice (regression guard: reserved system kind stays unreachable)', async t => {
  const s = open(t, ':memory:'), broker = new Broker(s, config({}), ['alice','bob']);
  const http = await httpFixture(t, broker);
  const response = await http.request('/send', { method: 'POST', body: JSON.stringify({ to: 'user:bob', kind: 'notice', body: 'not allowed' }) });
  assert.equal(response.status, 400); assert.equal((await response.json()).error, 'INVALID_KIND');
});

test('static users join default once and an audited departure is respected on restart', t => {
  const s = open(t, ':memory:');
  new Broker(s, config({ BROKER_DEFAULT_OWNER: 'bob' }), ['alice','bob']);
  const before = snapshot(s.db); new Broker(s, config({}), ['alice','bob']); assert.deepEqual(snapshot(s.db), before);
  assert.equal(s.get("SELECT user FROM workspace_members WHERE role='owner'").user, 'bob');
  for (const action of ['member:remove','member:leave','member:finish']) {
    s.run("DELETE FROM workspace_members WHERE user='alice'");
    s.run("INSERT INTO workspace_audit(workspace,action,subject_user,created) VALUES ('default',?,'alice',100)", action);
    new Broker(s, config({}), ['alice','bob']);
    assert.equal(s.get("SELECT 1 FROM workspace_members WHERE user='alice'"), undefined);
  }
});

test('broker:cli migrate upgrades a file and prints no row data', t => {
  const f = fixture(t);
  const result = spawnSync(process.execPath, ['broker/cli.mjs','migrate','--backup',f.backup,'--owner','u_agents'],
    { env: { ...process.env, BROKER_DB: f.file }, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(Object.keys(JSON.parse(result.stdout)).sort(), ['backup','changed','from','members','owner','revoked_invites','to']);
  assert.equal((result.stdout + result.stderr).includes('retained body'), false);
  assert.equal(open(t, f.file).get('PRAGMA user_version').user_version, SCHEMA_VERSION);
});


test('a failed foreign-key check rolls back rebuilt rows, schema and version together', t => {
  const f = fixture(t), db = new DatabaseSync(f.file);
  db.exec("PRAGMA foreign_keys=OFF; UPDATE workstreams SET owner_session='missing-agent'");
  const before = snapshot(db), ddl = schema(db); db.close();
  assert.throws(() => f.upgrade(), { code: 'MIGRATION_FK_VIOLATION' });
  const restored = new DatabaseSync(f.file); t.after(() => restored.close());
  assert.deepEqual(snapshot(restored), before); assert.deepEqual(schema(restored), ddl);
  assert.equal(restored.prepare('PRAGMA user_version').get().user_version, 0);
});


test('migration rejects an unknown owner and accepts historical or configured static identities', t => {
  const bad = fixture(t);
  assert.throws(() => bad.upgrade({ owner: 'owner-typo' }), error =>
    error.code === 'MIGRATION_OWNER_UNKNOWN' && /history|static/.test(error.hint));
  assert.equal(existsSync(bad.backup), false);
  const db = new DatabaseSync(bad.file); t.after(() => db.close());
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, 0);
  assert.equal(db.prepare("SELECT 1 FROM sqlite_schema WHERE name='workspaces'").get(), undefined);
  const historical = fixture(t);
  assert.equal(historical.upgrade({ owner: 'u_sender', staticUsers: [] }).owner, 'u_sender');
  const configured = fixture(t);
  assert.equal(configured.upgrade({ owner: 'static-owner', staticUsers: ['static-owner'] }).owner, 'static-owner');
});

test('an M1-origin database reaches the same schema as fresh and M4-created files', t => {
  const f = fixture(t), db = new DatabaseSync(f.file);
  // Original M1 tables had neither these references nor the later ack/audit columns.
  db.exec(`PRAGMA foreign_keys=OFF;
    CREATE TABLE audit_m1(id INTEGER PRIMARY KEY AUTOINCREMENT,thread_id TEXT NOT NULL,action TEXT NOT NULL,created INTEGER NOT NULL);
    INSERT INTO audit_m1(id,thread_id,action,created) SELECT id,thread_id,action,created FROM audit;
    DROP TABLE audit; ALTER TABLE audit_m1 RENAME TO audit;
    CREATE TABLE escalation_m1(message_id INTEGER NOT NULL,user TEXT NOT NULL,PRIMARY KEY(message_id,user));
    INSERT INTO escalation_m1(message_id,user) SELECT message_id,user FROM escalation_targets;
    DROP TABLE escalation_targets; ALTER TABLE escalation_m1 RENAME TO escalation_targets;
  `);
  db.close(); f.upgrade();
  const migrated = open(t, f.file), fresh = open(t, ':memory:');
  assert.deepEqual(schema(migrated.db), schema(fresh.db));
  assert.equal(migrated.get('SELECT count(*) n FROM audit').n, 1);
  assert.equal(migrated.get('SELECT count(*) n FROM escalation_targets').n, 1);
  assert.deepEqual(migrated.all('PRAGMA foreign_key_check'), []);
});


test('migration backups are private even with a permissive process umask', t => {
  const f = fixture(t), previous = process.umask(0o022);
  try {
    f.upgrade();
    assert.equal(statSync(f.backup).mode & 0o777, 0o600);
    assert.equal(process.umask(), 0o022);
    const failed = fixture(t);
    assert.throws(() => failed.upgrade({ backup: f.backup }), { code: 'MIGRATION_BACKUP_FAILED' });
    assert.equal(process.umask(), 0o022);
  } finally { process.umask(previous); }
});

test('an SQLite automatic rollback preserves its original error and closes the migration connection', t => {
  const f = fixture(t), exec = DatabaseSync.prototype.exec, close = DatabaseSync.prototype.close;
  let connection, closed = 0;
  t.mock.method(DatabaseSync.prototype, 'exec', function(sql) {
    const result = exec.call(this, sql);
    if (sql === 'CREATE TABLE workspaces(name TEXT PRIMARY KEY, created INTEGER NOT NULL, created_by_user TEXT NOT NULL, deleted_at INTEGER, deleted_reason TEXT);') {
      connection = this;
      exec.call(this, "CREATE TEMP TRIGGER auto_rollback BEFORE INSERT ON workspaces BEGIN SELECT RAISE(ROLLBACK,'fixture-auto-rollback'); END");
    }
    return result;
  });
  t.mock.method(DatabaseSync.prototype, 'close', function() { closed++; return close.call(this); });
  try {
    assert.throws(() => f.upgrade(), /fixture-auto-rollback/);
    assert.equal(closed, 1);
  } finally { if (connection?.isOpen) close.call(connection); }
});


test('broker:cli migrate accepts an owner found only in its configured static tokens file', t => {
  const f = fixture(t), tokensFile = join(f.dir, 'tokens.json');
  writeFileSync(tokensFile, JSON.stringify({ 'static-cli-owner': 'synthetic-cli-token-0000000000000000' }), { mode: 0o600 });
  const result = spawnSync(process.execPath, ['broker/cli.mjs','migrate','--backup',f.backup,'--owner','static-cli-owner'],
    { env: { ...process.env, BROKER_DB: f.file, BROKER_TOKENS: tokensFile }, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).owner, 'static-cli-owner');
  const s = open(t, f.file);
  assert.equal(s.get("SELECT user FROM workspace_members WHERE role='owner'").user, 'static-cli-owner');
  assert.equal((result.stdout + result.stderr).includes('synthetic-cli-token'), false);
});

test('a schema 5 file migrates to the current version with its own exact backup', t => {
  const f = fixture(t);
  const first = f.upgrade({ to: 5 });
  assert.equal(first.to, 5);
  const v5 = new DatabaseSync(f.file);
  assert.equal(v5.prepare('PRAGMA user_version').get().user_version, 5);
  assert.match(v5.prepare("SELECT sql FROM sqlite_schema WHERE name='messages_pending'").get().sql, /kind<>'escalation'/);
  const before = snapshot(v5); v5.close();
  const nextBackup = join(f.dir,'v5-backup.sqlite');
  const second = migrate(f.file,{ backup:nextBackup });
  assert.equal(second.changed,true); assert.equal(second.from,5); assert.equal(second.to,SCHEMA_VERSION);
  const saved = new DatabaseSync(nextBackup);
  assert.equal(saved.prepare('PRAGMA user_version').get().user_version,5);
  assert.deepEqual(snapshot(saved),before); saved.close();
  const upgraded = open(t,f.file), fresh = open(t,':memory:');
  assert.deepEqual(schema(upgraded.db),schema(fresh.db));
  const after = snapshot(upgraded.db);
  for (const [table, rows] of Object.entries(before)) assert.deepEqual(previousRows(table,after[table]), rows, table);
  for (const table of ['broker_users','credentials','invites']) assert.deepEqual(after[table], []);
  assert.throws(()=>migrate(f.file,{to:5}),{code:'INVALID_MIGRATION_TARGET'});
});


test('a schema 6 file migrates to current with an exact backup, matching fresh and direct schemas', t => {
  const f = fixture(t);
  assert.equal(f.upgrade({ to: 6 }).to, 6);
  const v6 = new DatabaseSync(f.file);
  assert.equal(v6.prepare('PRAGMA user_version').get().user_version, 6);
  assert.equal(v6.prepare("SELECT 1 FROM sqlite_schema WHERE name='invites'").get(), undefined);
  const before = snapshot(v6); v6.close();
  const backup = join(f.dir, 'v6-backup.sqlite');
  assert.equal(migrate(f.file, { backup }).to, SCHEMA_VERSION);
  const saved = new DatabaseSync(backup);
  assert.deepEqual(snapshot(saved), before); saved.close();
  const upgraded = open(t, f.file), fresh = open(t, ':memory:');
  assert.deepEqual(schema(upgraded.db), schema(fresh.db));
  for (const [table, rows] of Object.entries(before)) assert.deepEqual(previousRows(table,snapshot(upgraded.db)[table]), rows, table);
});


test('a v7 file upgrades with exact private backup, null topic backfill and refusal by the v7 binary', t => {
  const f=fixture(t);assert.equal(f.upgrade({to:7}).to,7);
  const old=new V7Store(f.file);
  assert.equal(old.get("SELECT 1 FROM sqlite_schema WHERE name='topics'"),undefined);
  assert.equal(old.all('PRAGMA table_info(threads)').some(c=>c.name==='topic'),false);
  assert.equal(old.all('PRAGMA table_info(invites)').some(c=>c.name==='topics'),false);
  old.run("INSERT INTO invites(id,workspace,secret_sha256,created_by_user,created_by_session,created,expires) VALUES ('public-fixture','default',?,'alice','a',1,1000)",'0'.repeat(64));
  const before=snapshot(old.db);old.close();
  const backup=join(f.dir,'v7-backup.sqlite');
  assert.equal(migrate(f.file,{backup}).to,SCHEMA_VERSION);assert.equal(statSync(backup).mode&0o777,0o600);
  const saved=new V7Store(backup);assert.deepEqual(snapshot(saved.db),before);saved.close();
  const current=open(t,f.file),fresh=open(t,':memory:');assert.deepEqual(schema(current.db),schema(fresh.db));
  const after=snapshot(current.db);
  for(const [table,rows] of Object.entries(before))assert.deepEqual(previousRows(table,after[table]),rows,table);
  for(const table of ['topics','topic_subscriptions'])assert.deepEqual(after[table],[]);
  assert.throws(()=>new V7Store(f.file),{code:'SCHEMA_TOO_NEW'});
});


test('F4: an explicit target-8 upgrade equals the frozen v8 schema',t=>{
  const f=fixture(t);assert.equal(f.upgrade({to:8}).to,8);
  const target=new DatabaseSync(f.file),frozen=new DatabaseSync(':memory:');
  try {
    createV8Schema(frozen);
    assert.equal(target.prepare('PRAGMA user_version').get().user_version,8);
    assert.deepEqual(schema(target),schema(frozen));
  } finally {target.close();frozen.close();}
});


test('a v8 upgrade backfills zero cursors, preserves rows and private backup, and is refused by v8',t=>{
  const f=fixture(t);assert.equal(f.upgrade({to:8}).to,8);
  const old=new V8Store(f.file);
  old.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('default','flow/API','broadcast','alice',1)");
  old.run("INSERT INTO topic_subscriptions VALUES ('default','flow/API','u_agents',1)");
  const before=snapshot(old.db);old.close();const backup=join(f.dir,'v8-backup.sqlite');
  assert.equal(migrate(f.file,{backup}).to,SCHEMA_VERSION);assert.equal(statSync(backup).mode&0o777,0o600);
  const saved=new V8Store(backup);assert.deepEqual(snapshot(saved.db),before);saved.close();
  const current=open(t,f.file),fresh=open(t,':memory:');assert.deepEqual(schema(current.db),schema(fresh.db));
  const after=snapshot(current.db);for(const [table,rows] of Object.entries(before))assert.deepEqual(retentionRows(table,after[table]),rows,table);
  assert.deepEqual(current.all('SELECT * FROM topic_cursors').map(r=>({...r})),[{workspace:'default',topic:'flow/API',user:'u_agents',acked_through:0}]);
  assert.deepEqual(after.topic_acks,[]);assert.throws(()=>new V8Store(f.file),{code:'SCHEMA_TOO_NEW'});
});


test('target 9 remains frozen and a v9 upgrade preserves sparse claims, cursors and its private backup',t=>{
  const f=fixture(t);assert.equal(f.upgrade({to:9}).to,9);
  const old=new V9Store(f.file),frozen=new DatabaseSync(':memory:');
  try {createV9Schema(frozen);assert.deepEqual(schema(old.db),schema(frozen));}finally{frozen.close();}
  old.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('default','flow/API','broadcast','alice',1)");
  old.run("INSERT INTO topic_subscriptions VALUES ('default','flow/API','u_agents',1)");
  old.run("INSERT INTO topic_cursors VALUES ('default','flow/API','u_agents',0)");
  old.run("INSERT INTO threads(id,status,created,last_activity,workspace,topic) VALUES ('topic-v9','open',1,1,'default','flow/API')");
  old.run("INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace) VALUES ('topic-v9','alice','a','topic','flow/API','finding',0,'preserve',1,'default')");
  const id=old.get("SELECT max(id) id FROM messages").id;
  old.run("INSERT INTO topic_acks VALUES (?,'u_agents','a',1)",id);
  const before=snapshot(old.db),sequences=old.all('SELECT * FROM sqlite_sequence ORDER BY name');old.close();
  const backup=join(f.dir,'v9-backup.sqlite');assert.equal(migrate(f.file,{backup}).to,SCHEMA_VERSION);
  assert.equal(statSync(backup).mode&0o777,0o600);
  const saved=new V9Store(backup);assert.deepEqual(snapshot(saved.db),before);saved.close();
  const current=open(t,f.file),fresh=open(t,':memory:');assert.deepEqual(schema(current.db),schema(fresh.db));
  const after=snapshot(current.db);for(const [table,rows] of Object.entries(before))assert.deepEqual(retentionRows(table,after[table]),rows,table);
  assert.deepEqual(after.topic_pins,[]);assert.deepEqual(current.all('SELECT * FROM sqlite_sequence ORDER BY name'),sequences);
  assert.throws(()=>new V9Store(f.file),{code:'SCHEMA_TOO_NEW'});
});


test('target 10 stays frozen and v11 preserves its pins, sparse claims and private backup while v10 refuses it',t=>{
  const f=fixture(t);assert.equal(f.upgrade({to:10}).to,10);
  const old=new V10Store(f.file),frozen=new DatabaseSync(':memory:');
  try{createV10Schema(frozen);assert.deepEqual(schema(old.db),schema(frozen));}finally{frozen.close();}
  old.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('default','historic/topic','broadcast','alice',1)");
  old.run("INSERT INTO topic_subscriptions VALUES ('default','historic/topic','u_agents',1)");
  old.run("INSERT INTO topic_cursors VALUES ('default','historic/topic','u_agents',0)");
  old.run("INSERT INTO threads(id,status,created,last_activity,workspace,topic) VALUES ('v10-thread','open',1,1,'default','historic/topic')");
  old.run("INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace) VALUES ('v10-thread','alice','a','topic','historic/topic','finding',0,'historical topic',1,'default')");
  const id=old.get('SELECT max(id) id FROM messages').id;
  old.run("INSERT INTO topic_acks VALUES (?,'u_agents','a',1)",id);old.run("INSERT INTO topic_pins VALUES (?,'u_agents','default',1)",id);
  const before=snapshot(old.db),sequences=old.all('SELECT * FROM sqlite_sequence ORDER BY name');old.close();
  const backup=join(f.dir,'v10-backup.sqlite');assert.equal(migrate(f.file,{backup}).to,SCHEMA_VERSION);assert.equal(statSync(backup).mode&0o777,0o600);
  const saved=new V10Store(backup);assert.deepEqual(snapshot(saved.db),before);saved.close();
  const current=open(t,f.file),fresh=open(t,':memory:');assert.deepEqual(schema(current.db),schema(fresh.db));
  const after=snapshot(current.db);for(const [table,rows] of Object.entries(before))assert.deepEqual(retentionRows(table,after[table]),rows,table);
  assert.deepEqual(after.queue_items,[]);assert.deepEqual(current.all('SELECT * FROM sqlite_sequence ORDER BY name'),sequences);
  assert.throws(()=>new V10Store(f.file),{code:'SCHEMA_TOO_NEW'});
});

test('target 11 stays frozen; v11 to v12 preserves every row, fence, claim and private backup, and establishes its epoch',t=>{
  const f=fixture(t);assert.equal(f.upgrade({to:11}).to,11);
  const old=new V11Store(f.file),frozen=new DatabaseSync(':memory:');
  try{createV11Schema(frozen);assert.deepEqual(schema(old.db),schema(frozen));}finally{frozen.close();}
  old.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('default','historic/Queue','queue','alice',1)");
  old.run("INSERT INTO topic_subscriptions VALUES ('default','historic/Queue','u_agents',1)");
  old.run("INSERT INTO topic_cursors VALUES ('default','historic/Queue','u_agents',17)");
  old.run("INSERT INTO threads(id,status,created,last_activity,workspace,topic) VALUES ('v11-thread','open',1,1,'default','historic/Queue')");
  old.run("INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace) VALUES ('v11-thread','alice','a','topic','historic/Queue','finding',0,'historical queue body',1,'default')");
  const id=old.get('SELECT max(id) id FROM messages').id;
  old.run("INSERT INTO queue_items(message_id,workspace,topic,state,attempts,generation,holder_user,holder_session,lease_until,done_at,done_generation,result) VALUES (?,'default','historic/Queue','done',3,7,'u_agents','a',100,90,7,'retained result')",id);
  old.run("INSERT INTO topic_pins VALUES (?,'u_agents','default',1)",id);
  old.run("INSERT INTO webhook_outbox(message_id,payload,next_attempt) VALUES (?,'{}',1)",id);
  const before=snapshot(old.db),sequences=old.all('SELECT * FROM sqlite_sequence ORDER BY name');old.close();
  const backup=join(f.dir,'v11-backup.sqlite');assert.equal(migrate(f.file,{backup,now:()=>123456}).to,SCHEMA_VERSION);
  assert.equal(statSync(backup).mode&0o777,0o600);
  const saved=new V11Store(backup);assert.deepEqual(snapshot(saved.db),before);saved.close();
  const current=open(t,f.file),fresh=open(t,':memory:');assert.deepEqual(schema(current.db),schema(fresh.db));
  const after=snapshot(current.db);
  for(const [table,rows] of Object.entries(before))assert.deepEqual(retentionRows(table,after[table]),rows,table);
  assert.deepEqual(current.all('SELECT * FROM sqlite_sequence ORDER BY name'),sequences);
  assert.deepEqual(after.broker_meta,[{key:'retention_epoch',value:'123456'}]);
  assert.throws(()=>new V11Store(f.file),{code:'SCHEMA_TOO_NEW'});
  assert.deepEqual(migrate(f.file,{now:()=>999999}),{changed:false});
  assert.equal(current.get("SELECT value FROM broker_meta WHERE key='retention_epoch'").value,'123456');
});

test('fresh and older-store migrations set the safety epoch once at creation or upgrade, with no purge path',t=>{
  const fresh=new DatabaseSync(':memory:');t.after(()=>fresh.close());createSchema(fresh,()=>456789);
  assert.equal(fresh.prepare("SELECT value FROM broker_meta WHERE key='retention_epoch'").get().value,'456789');
  assert.equal(fresh.prepare('PRAGMA user_version').get().user_version,SCHEMA_VERSION);
  const f=fixture(t);f.upgrade({now:()=>987654});
  const current=new Store(f.file);
  let before,ddl;
  try {
    assert.equal(current.get("SELECT value FROM broker_meta WHERE key='retention_epoch'").value,'987654');
    assert.deepEqual(current.all('SELECT DISTINCT purged_at,purge_run_id FROM messages').map(r=>({...r})),[{purged_at:null,purge_run_id:null}]);
    before=snapshot(current.db);ddl=schema(current.db);
  } finally {current.close();}
  const reopened=new Store(f.file);try{assert.deepEqual(snapshot(reopened.db),before);assert.deepEqual(schema(reopened.db),ddl);}finally{reopened.close();}
});

test('target 12 stays frozen and v13 preserves epoch, tombstones, history and exact private backup while v12 refuses it',t=>{
  const f=fixture(t);assert.equal(f.upgrade({to:12,now:()=>123456}).to,12);
  const old=new V12Store(f.file),frozen=new DatabaseSync(':memory:');
  try{createV12Schema(frozen);assert.deepEqual(schema(old.db),schema(frozen));}finally{frozen.close();}
  old.run("UPDATE messages SET body='',purged_at=123,purge_run_id=7 WHERE id=1");
  old.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('default','v12/Queue','queue','alice',1)");
  old.run("INSERT INTO topic_subscriptions VALUES ('default','v12/Queue','u_agents',1)");
  old.run("INSERT INTO topic_cursors VALUES ('default','v12/Queue','u_agents',17)");
  old.run("INSERT INTO threads(id,status,created,last_activity,workspace,topic) VALUES ('v12-thread','open',1,1,'default','v12/Queue')");
  old.run("INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace) VALUES ('v12-thread','alice','a','topic','v12/Queue','finding',0,'preserve v12 body',1,'default')");
  const id=old.get('SELECT max(id) id FROM messages').id;
  old.run("INSERT INTO queue_items(message_id,workspace,topic,state,attempts,generation,holder_user,holder_session,lease_until,done_at,done_generation,result) VALUES (?,'default','v12/Queue','done',3,7,'u_agents','a',100,90,7,'preserve result')",id);
  old.run("INSERT INTO topic_pins VALUES (?,'u_agents','default',1)",id);
  const before=snapshot(old.db),sequences=old.all('SELECT * FROM sqlite_sequence ORDER BY name');old.close();
  const backup=join(f.dir,'v12-backup.sqlite');assert.equal(migrate(f.file,{backup,now:()=>999999}).to,SCHEMA_VERSION);
  assert.equal(statSync(backup).mode&0o777,0o600);
  const saved=new V12Store(backup);assert.deepEqual(snapshot(saved.db),before);saved.close();
  const current=open(t,f.file),fresh=open(t,':memory:');assert.deepEqual(schema(current.db),schema(fresh.db));
  const after=snapshot(current.db);for(const [table,rows] of Object.entries(before))assert.deepEqual(topicBindingRows(table,after[table]),rows,table);
  assert.deepEqual(after.retention_runs,[]);assert.equal(current.get("SELECT value FROM broker_meta WHERE key='retention_epoch'").value,'123456');
  assert.deepEqual(current.all('SELECT * FROM sqlite_sequence ORDER BY name'),sequences);
  assert.throws(()=>new V12Store(f.file),{code:'SCHEMA_TOO_NEW'});
});
