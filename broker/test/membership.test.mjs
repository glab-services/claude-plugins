import {prepareTopicFixture,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { rmSync } from 'node:fs';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { testTempDir } from '../../test-support/temp.mjs';
import { httpFixture, eventually } from './http.mjs';

async function fixture(t, file = ':memory:') {
  const store = new Store(file); t.after(() => store.close());
  const settings = { ...config({}), sseHeartbeat: 20 };
  const broker = new Broker(store, settings, ['alice','bob','eve'], { log: () => {} });
  store.run("INSERT INTO workspaces(name,created,created_by_user) VALUES ('proj-b',1,'alice')");
  store.run("INSERT INTO workspace_members VALUES ('proj-b','alice','owner',1,'creator'),('proj-b','bob','member',1,'static')");
  for(const workspace of ['default','proj-b'])prepareTopicFixture(broker,workspace);
  const wire = await httpFixture(t, broker,{workspaceAdminSessions:true});
  async function call(path, args, user = 'alice', workspace = 'default') {
    const response = await wire.request(path, { method: args === undefined ? 'GET' : 'POST', body: args === undefined ? undefined : JSON.stringify(args),
      user, session: `${user}-${workspace}`, headers: { 'X-Workspace': workspace } });
    return { status: response.status, data: await response.json() };
  }
  return { store, broker, settings, wire, call };
}

const audit = (f, action) => f.store.all('SELECT * FROM workspace_audit WHERE action=? ORDER BY id', action);

test("owner removal ends the member's stream and later requests get NOT_A_MEMBER", async t => {
  const f = await fixture(t), stream = await f.wire.stream('bob','bob-default');
  assert.equal((await f.call('/members')).data.members.find(m => m.user === 'alice').role, 'owner');
  assert.equal((await f.call('/members/remove',{user:'bob'})).status,200);
  await stream.wait(events => events.some(e => e.event === 'removed'));
  assert.deepEqual(stream.events.find(e => e.event === 'removed').data, {workspace:'default',reason:'remove'});
  await eventually(() => !f.broker.streams.has('bob-default'));
  assert.equal((await f.call('/members',undefined,'bob')).data.error,'NOT_A_MEMBER');
  const notices = (await f.call('/inbox')).data.messages;
  assert.ok(notices.some(m=>m.kind==='notice'&&m.body.includes('reason: remove')));
});

test("removal releases the member's leases only in that workspace and retains decisions", async t => {
  const f = await fixture(t);
  for (const workspace of ['default','proj-b']) {
    await f.call('/workstreams/register',{name:'api'},'bob',workspace);
    await f.call('/decisions/record',{workstream:'api',body:workspace},'bob',workspace);
  }
  await f.call('/send',{to:'user:bob',body:'pending'});
  await f.call('/members/remove',{user:'bob'});
  assert.deepEqual(f.store.all("SELECT workspace FROM workstreams WHERE name='api'").map(r=>r.workspace),['proj-b']);
  assert.equal(f.store.get('SELECT count(*) n FROM decisions').n,2);
  assert.deepEqual(JSON.parse(audit(f,'member:remove')[0].detail),{pending_messages:1,released_workstreams:1});
  assert.equal((await f.call('/workstreams/register',{name:'api'},'bob','proj-b')).status,200);
});

test('leave and finish refuse the owner until ownership is transferred', async t => {
  const f = await fixture(t);
  for (const [path,args] of [['leave',{}],['finish',{summary:'done'}]]) {
    const response=await f.call('/members/'+path,args);
    assert.equal(response.status,409);assert.equal(response.data.error,'OWNER_MUST_TRANSFER');
  }
  assert.equal((await f.call('/members/remove',{user:'alice'})).data.error,'OWNER_CANNOT_BE_REMOVED');
  assert.equal((await f.call('/members/transfer',{user:'nobody'})).data.error,'MEMBER_NOT_FOUND');
  assert.equal((await f.call('/members/transfer',{user:'bob'})).status,200);
  assert.equal((await f.call('/members/leave',{})).status,200);
  assert.equal(f.store.get("SELECT user FROM workspace_members WHERE workspace='default' AND role='owner'").user,'bob');
  assert.equal(f.store.get("SELECT user FROM workspace_members WHERE workspace='proj-b' AND role='owner'").user,'alice');
});

test('non-owners cannot remove members or transfer ownership', async t => {
  const f = await fixture(t);
  for(const path of ['remove','transfer']) {
    const response=await f.call('/members/'+path,{user:'eve'},'bob');
    assert.equal(response.status,403);assert.equal(response.data.error,'NOT_WORKSPACE_OWNER');assert.ok(response.data.hint);
  }
  assert.equal(audit(f,'owner:transfer').length,0);
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_members WHERE workspace='default'").n,3);
});

test('finish atomically stores a summary for the owner and removes the member', async t => {
  const f = await fixture(t); await f.call('/workstreams/register',{name:'api'},'bob');
  const response=await f.call('/members/finish',{summary:'finished contract'},'bob');
  assert.equal(response.status,200);assert.equal(response.data.removed,true);
  const summary=f.store.get('SELECT * FROM messages WHERE thread_id=?',response.data.thread_id);
  assert.equal(summary.kind,'finding');assert.equal(summary.body,'finished contract');assert.equal(summary.from_user,'bob');
  assert.equal(f.store.get('SELECT user FROM delivery_targets WHERE message_id=?',summary.id).user,'alice');
  assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE workspace='default' AND user='bob'"),undefined);
  assert.equal(f.store.get("SELECT 1 FROM workstreams WHERE workspace='default' AND name='api'"),undefined);
  assert.equal(audit(f,'member:finish').length,1);
});

test('a real failure after summary insertion rolls back the whole finish transaction', async t => {
  const f = await fixture(t); await f.call('/workstreams/register',{name:'api'},'bob');
  f.store.db.exec("CREATE TEMP TRIGGER fail_finish BEFORE INSERT ON workspace_audit WHEN NEW.action='member:finish' BEGIN SELECT RAISE(ABORT,'injected-finish-failure'); END");
  const before=f.store.get('SELECT count(*) n FROM workspace_audit').n;
  assert.equal((await f.call('/members/finish',{summary:'must roll back'},'bob')).status,500);
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM threads').n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM workspace_audit').n,before);
  assert.ok(f.store.get("SELECT 1 FROM workspace_members WHERE workspace='default' AND user='bob'"));
  assert.ok(f.store.get("SELECT 1 FROM workstreams WHERE workspace='default' AND name='api'"));
});

test('transfer notices are delivered and acked per user and cannot be replied to', async t => {
  const f = await fixture(t), a=await f.wire.stream('alice','alice-default'), b=await f.wire.stream('bob','bob-default');
  await f.call('/members/transfer',{user:'bob'});
  await Promise.all([a,b].map(s=>s.wait(events=>events.some(e=>e.data.kind==='notice'))));
  const message=a.events.find(e=>e.data.kind==='notice').data;
  assert.ok(b.events.some(e=>e.data.id===message.id));
  assert.equal((await f.call('/ack',{id:message.id})).status,200);
  assert.equal(f.store.get('SELECT acked_at FROM messages WHERE id=?',message.id).acked_at,null);
  assert.equal(f.store.get("SELECT acked_at FROM escalation_targets WHERE message_id=? AND user='bob'",message.id).acked_at,null);
  assert.equal((await f.call('/inbox',undefined,'alice')).data.messages.some(m=>m.id===message.id),false);
  assert.equal((await f.call('/inbox',undefined,'bob')).data.messages.some(m=>m.id===message.id),true);
  assert.equal((await f.call('/ack',{id:message.id},'bob','proj-b')).data.error,'MESSAGE_NOT_FOUND');
  assert.equal((await f.call('/ack',{id:message.id},'bob')).status,200);
  assert.equal(f.store.get('SELECT count(*) n FROM escalation_targets WHERE message_id=? AND acked_at IS NOT NULL',message.id).n,2);
  const reply=await f.call('/send',{to:'user:bob',kind:'answer',thread_id:message.thread_id,body:'reply'});
  assert.equal(reply.data.error,'THREAD_RESOLVED');
  assert.equal((await f.call('/close',{thread_id:message.thread_id})).data.error,'THREAD_RESOLVED');
});

test('a send whose partial body arrives after committed removal is refused without storing it', async t => {
  const f=await fixture(t), body=JSON.stringify({to:'user:alice',body:'late after removal'});
  const request=http.request({hostname:'127.0.0.1',port:f.wire.server.address().port,path:'/send',method:'POST',headers:{
    Authorization:'Bearer bob-test-token','X-Session-Id':'delayed-bob','X-Topic':FIXTURE_TOPIC,'Content-Type':'application/json','Content-Length':Buffer.byteLength(body),
  }});
  t.after(()=>request.destroy());
  const response=new Promise((resolve,reject)=>{request.once('response',resolve);request.once('error',reject);});
  request.write(body.slice(0,1));
  await eventually(()=>f.store.get("SELECT 1 FROM agents WHERE session_id='delayed-bob'"));
  await f.call('/members/remove',{user:'bob'});
  const count=f.store.get('SELECT count(*) n FROM messages').n;
  request.end(body.slice(1));
  const result=await response;let text='';for await(const chunk of result)text+=chunk;
  assert.equal(result.statusCode,403);assert.equal(JSON.parse(text).error,'NOT_A_MEMBER');
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n,count);
});

test('a removed static user is not re-added to default by a separate broker bootstrap process', async t => {
  const dir=testTempDir('membership-restart-');t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const file=join(dir,'broker.sqlite'),f=await fixture(t,file);
  await f.call('/members/remove',{user:'bob'});
  const result=spawnSync(process.execPath,['--input-type=module','-e',`
    import { Store } from './broker/store.mjs';
    import { Broker } from './broker/broker.mjs';
    import { config } from './broker/config.mjs';
    const store=new Store(process.env.BROKER_DB);
    try {
      new Broker(store,config({}),['alice','bob','eve'],{log:()=>{}});
      console.log(JSON.stringify(store.get("SELECT count(*) n FROM workspace_members WHERE workspace='default' AND user='bob'")));
    } finally { store.close(); }
  `],{env:{...process.env,BROKER_DB:file},encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);assert.equal(JSON.parse(result.stdout).n,0);
  assert.equal((await f.call('/members',undefined,'bob')).data.error,'NOT_A_MEMBER');
  assert.ok(f.store.get("SELECT 1 FROM workspace_members WHERE workspace='proj-b' AND user='bob'"));
});

test('human CLI delete-workspace archives membership but preserves message/decision history and audit', async t => {
  const dir=testTempDir('membership-cli-');t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const file=join(dir,'broker.sqlite'),f=await fixture(t,file);
  await f.call('/workstreams/register',{name:'api'},'bob','proj-b');
  await f.call('/decisions/record',{workstream:'api',body:'retained decision'},'bob','proj-b');
  await f.call('/send',{to:'user:bob',body:'retained message'},'alice','proj-b');
  const before=f.store.all("SELECT * FROM messages WHERE workspace='proj-b'");
  const run=(args)=>spawnSync(process.execPath,['broker/cli.mjs',...args],{env:{...process.env,BROKER_DB:file},encoding:'utf8'});
  const protectedResult=run(['delete-workspace','default','--reason','reviewed']);
  assert.equal(JSON.parse(protectedResult.stderr).error,'WORKSPACE_PROTECTED');
  assert.equal(run(['delete-workspace','proj-b']).status,1);
  const result=run(['delete-workspace','proj-b','--reason','project completed']);
  assert.equal(result.status,0,result.stderr);assert.equal(JSON.parse(result.stdout).changed,true);
  assert.equal(f.store.get("SELECT deleted_reason FROM workspaces WHERE name='proj-b'").deleted_reason,'project completed');
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_members WHERE workspace='proj-b'").n,0);
  assert.deepEqual(f.store.all("SELECT * FROM messages WHERE workspace='proj-b'"),before);
  assert.equal(f.store.get("SELECT count(*) n FROM decisions WHERE workspace='proj-b'").n,1);
  const records=JSON.parse(run(['audit','proj-b']).stdout);
  assert.equal(records.filter(row=>row.action==='member:workspace_deleted').length,2);
  assert.equal(records.filter(row=>row.action==='workspace:delete').length,1);
  assert.equal((await f.call('/inbox',undefined,'bob','proj-b')).data.error,'NOT_A_MEMBER');
  assert.equal(run(['delete-workspace','proj-b','--reason','again']).status,0);
  assert.equal(audit(f,'workspace:delete').length,1);
  assert.equal((await f.call('/workspaces/delete',{name:'proj-b'})).status,404);
});

test('owners cannot transfer ownership to themselves or create a spurious notice and audit', async t => {
  const f=await fixture(t);
  const result=await f.call('/members/transfer',{user:'alice'});
  assert.equal(result.status,409);assert.equal(result.data.error,'OWNER_TRANSFER_SELF');
  assert.match(result.data.hint,/different/);
  assert.equal(audit(f,'owner:transfer').length,0);
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n,0);
  assert.equal(f.store.get("SELECT user FROM workspace_members WHERE workspace='default' AND role='owner'").user,'alice');
});

test('leave rejects a non-empty body without changing membership, leases or audit', async t => {
  const f=await fixture(t);
  await f.call('/workstreams/register',{name:'api'},'bob');
  const result=await f.call('/members/leave',{user:'eve'},'bob');
  assert.equal(result.status,400);assert.equal(result.data.error,'INVALID_MEMBERSHIP_FIELDS');
  assert.match(result.data.hint,/empty/);
  assert.ok(f.store.get("SELECT 1 FROM workspace_members WHERE workspace='default' AND user='bob'"));
  assert.ok(f.store.get("SELECT 1 FROM workstreams WHERE workspace='default' AND name='api'"));
  assert.equal(audit(f,'member:leave').length,0);
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n,0);
});
