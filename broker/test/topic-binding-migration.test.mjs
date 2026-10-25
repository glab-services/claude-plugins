import {test} from 'node:test';
import {DatabaseSync} from 'node:sqlite';
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {rmSync,statSync} from 'node:fs';
import {Store as V13Store} from './fixtures/v13-store.mjs';
import {Store} from '../store.mjs';
import {migrate} from '../migrate.mjs';
import {testTempDir} from '../../test-support/temp.mjs';
const snapshot=db=>Object.fromEntries(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map(({name})=>[name,db.prepare('SELECT * FROM '+name+' ORDER BY rowid').all()]));
const schema=db=>db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").all().map(r=>({...r,sql:r.sql.replaceAll('"','').replace(/\s+/g,' ').trim()}));
test('v13 to v14 preserves scoped history and revokes only incompatible active invites with audits',t=>{
 const dir=testTempDir('topic-binding-migration-');t.after(()=>rmSync(dir,{recursive:true,force:true}));
 const file=join(dir,'state.sqlite'),backup=join(dir,'backup.sqlite'),old=new V13Store(file);
 old.run("INSERT INTO workspaces VALUES ('default',1,'alice',NULL,NULL)");
 old.run("INSERT INTO workspace_members VALUES ('default','alice','owner',1,'creator')");
 old.run("INSERT INTO agents VALUES ('alice-session','alice','manager',1,'default')");
 old.run("INSERT INTO workstreams VALUES ('flow','alice-session','alice',100,NULL,NULL,'default')");
 old.run("INSERT INTO topics VALUES ('default','open','broadcast','alice',1,NULL,NULL),('default','closed','broadcast','alice',1,2,'alice')");
 old.run("INSERT INTO threads(id,status,created,last_activity,workspace,topic) VALUES ('direct','open',1,1,'default',NULL),('topic','open',1,1,'default','open')");
 for(const [thread,kind,target] of [['direct','user','alice'],['topic','topic','open']]) old.run("INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace) VALUES (?,'alice','alice-session',?,?,'finding',0,'preserved body',1,'default')",thread,kind,target);
 const rows=[['null',null,100,null],['empty','[]',100,null],['multi','["open","closed"]',100,null],['missing','["missing"]',100,null],['closed','["closed"]',100,null],['single','["open"]',100,null],['expired',null,5,null],['redeemed',null,100,3]];
 for(const [id,topics,expires,redeemed] of rows)old.run("INSERT INTO invites(id,workspace,secret_sha256,created_by_user,created_by_session,created,expires,topics,redeemed_at) VALUES (?,'default','fixture-hash','alice','alice-session',1,?,?,?)",id,expires,topics,redeemed);
 const before=snapshot(old.db),beforeSchema=schema(old.db);old.close();
 const result=migrate(file,{backup,now:()=>10});assert.equal(result.to,14);assert.equal(result.revoked_invites,5);
 assert.equal(statSync(backup).mode&0o777,0o600);const saved=new DatabaseSync(backup,{readOnly:true});try{assert.deepEqual(snapshot(saved),before);assert.deepEqual(schema(saved),beforeSchema);}finally{saved.close();}
 const current=new Store(file),fresh=new Store(':memory:');t.after(()=>{current.close();fresh.close();});
 assert.deepEqual(schema(current.db),schema(fresh.db));assert.equal(current.get('PRAGMA foreign_key_check'),undefined);
 assert.equal(current.get("SELECT topic FROM agents").topic,null);
 assert.equal(current.get('SELECT ended_at,ended_reason FROM agents').ended_at,null);assert.equal(current.get('SELECT ended_at,ended_reason FROM agents').ended_reason,null);
 assert.equal(current.get("SELECT topic FROM workstreams").topic,null);
 assert.deepEqual(current.all('SELECT topic,body FROM messages ORDER BY id').map(r=>({...r})),[{topic:null,body:'preserved body'},{topic:'open',body:'preserved body'}]);
 assert.equal(current.get("SELECT count(*) n FROM invites WHERE revoked_at=10").n,5);
 assert.equal(current.get("SELECT count(*) n FROM workspace_audit WHERE action='invite:migration_revoke'").n,5);
 for(const id of ['single','expired','redeemed'])assert.equal(current.get('SELECT revoked_at FROM invites WHERE id=?',id).revoked_at,null);
 assert.equal(current.get("SELECT topic FROM invites WHERE id='single'").topic,'open');
 assert.throws(()=>new V13Store(file),e=>e.code==='SCHEMA_TOO_NEW');
 current.run("INSERT INTO workstreams(name,owner_session,owner_user,lease_until,workspace,topic) VALUES ('flow','alice-session','alice',100,'default','open')");
 assert.throws(()=>current.run("INSERT INTO workstreams(name,owner_session,owner_user,lease_until,workspace,topic) VALUES ('flow','alice-session','alice',100,'default',NULL)"));
});

test('v14 invite-audit failure rolls schema and state back to v13',t=>{
 const dir=testTempDir('topic-binding-rollback-');t.after(()=>rmSync(dir,{recursive:true,force:true}));
 const file=join(dir,'state.sqlite'),old=new V13Store(file);
 old.run("INSERT INTO workspaces VALUES ('default',1,'alice',NULL,NULL)");
 old.run("INSERT INTO invites(id,workspace,secret_sha256,created_by_user,created_by_session,created,expires) VALUES ('invalid','default','fixture-hash','alice','alice-session',1,100)");
 old.db.exec("CREATE TRIGGER fail_migration_audit BEFORE INSERT ON workspace_audit WHEN new.action='invite:migration_revoke' BEGIN SELECT RAISE(ABORT,'fixture audit failure'); END");
 const before=snapshot(old.db),beforeSchema=schema(old.db);old.close();
 assert.throws(()=>migrate(file,{backup:join(dir,'backup.sqlite'),now:()=>10}),/fixture audit failure/);
 const saved=new V13Store(file);try{assert.deepEqual(snapshot(saved.db),before);assert.deepEqual(schema(saved.db),beforeSchema);}finally{saved.close();}
});

test('migrated v13 topic-close notices stay quarantined while new workspace notices reach bound targets',async t=>{
 const {Broker}=await import('../broker.mjs'),{config}=await import('../config.mjs'),{createServer}=await import('../server.mjs');
 const dir=testTempDir('legacy-notice-scope-'),file=join(dir,'state.sqlite'),old=new V13Store(file);
 old.run("INSERT INTO workspaces VALUES ('default',1,'alice',NULL,NULL)");
 for(const user of ['alice','bob'])old.run("INSERT INTO workspace_members VALUES ('default',?,?,1,'static')",user,user==='alice'?'owner':'member');
 old.run("INSERT INTO topics VALUES ('default','x','broadcast','alice',1,NULL,NULL)");
 for(const user of ['alice','bob'])old.run("INSERT INTO topic_subscriptions VALUES ('default','x',?,1)",user);
 old.run("INSERT INTO threads(id,status,created,last_activity,workspace) VALUES ('legacy-notice','resolved',1,1,'default')");
 const legacy=Number(old.run("INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace) VALUES ('legacy-notice','broker','broker','user','broker','notice',0,'bob closed topic y.',1,'default')").lastInsertRowid);
 old.run("INSERT INTO threads(id,status,created,last_activity,workspace,topic) VALUES ('legacy-tagged','resolved',1,1,'default','x')");
 const tagged=Number(old.run("INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace) VALUES ('legacy-tagged','broker','broker','user','broker','notice',0,'legacy topic x metadata',1,'default')").lastInsertRowid);
 for(const user of ['alice','bob'])for(const [id,thread] of [[legacy,'legacy-notice'],[tagged,'legacy-tagged']]){old.run('INSERT INTO escalation_targets(message_id,user) VALUES (?,?)',id,user);old.run('INSERT INTO participants VALUES (?,?)',thread,user);}
 old.close();migrate(file,{backup:join(dir,'backup.sqlite')});
 const store=new Store(file),broker=new Broker(store,config({}),['alice','bob'],{log:()=>{}}),tokens=new Map([['alice-legacy-notice-fixture-token-0000','alice'],['bob-legacy-notice-fixture-token-00000','bob']]),server=createServer(broker,tokens);
 await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(async()=>{server.closeAllConnections();await new Promise(r=>server.close(r));store.close();rmSync(dir,{recursive:true,force:true});});
 const call=async(user,session,topic,path,args)=>{const response=await fetch('http://127.0.0.1:'+server.address().port+path,{method:args===undefined?'GET':'POST',headers:{Authorization:'Bearer '+[...tokens].find(([,u])=>u===user)[0],'X-Session-Id':session,'Content-Type':'application/json',...(topic?{'X-Topic':topic}:{})},...(args===undefined?{}:{body:JSON.stringify(args)})});return {status:response.status,data:await response.json()};};
 const member=await call('bob','new-x','x','/inbox');assert.equal(member.status,200);assert.equal(member.data.messages.some(m=>[legacy,tagged].includes(m.id)),false);
 const owner=await call('alice','workspace-owner',null,'/inbox');assert.ok([legacy,tagged].every(id=>owner.data.messages.some(m=>m.id===id)));
 assert.equal(broker.eligible(store.get('SELECT * FROM messages WHERE id=?',tagged),'bob','new-x'),false);
 assert.equal(store.get('SELECT notice_scope FROM messages WHERE id=?',legacy).notice_scope,null);
 store.run("UPDATE messages SET notice_scope='topic' WHERE id=?",legacy);
 assert.equal((await call('bob','new-x','x','/inbox')).data.messages.some(m=>m.id===legacy),false);
 assert.equal(broker.eligible(store.get('SELECT * FROM messages WHERE id=?',legacy),'bob','new-x'),false);
 store.run('UPDATE messages SET notice_scope=NULL WHERE id=?',legacy);
 assert.equal((await call('bob','new-x','x','/close',{thread_id:'legacy-notice'})).data.error,'THREAD_NOT_FOUND');
 assert.equal((await call('alice','workspace-owner',null,'/members/transfer',{user:'bob'})).status,200);
 const fresh=await call('bob','new-x','x','/inbox');assert.ok(fresh.data.messages.some(m=>m.kind==='notice'&&m.notice_scope==='workspace'));
 assert.equal(fresh.data.messages.some(m=>[legacy,tagged].includes(m.id)),false);
});
