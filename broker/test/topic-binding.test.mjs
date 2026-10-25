import {test} from 'node:test';
import assert from 'node:assert/strict';
import {Store} from '../store.mjs';
import {Broker} from '../broker.mjs';
import {config} from '../config.mjs';
import {createServer} from '../server.mjs';
async function fixture(t){
 const store=new Store(':memory:'),broker=new Broker(store,{...config({}),defaultOwner:'alice',sseHeartbeat:20},['alice','bob','eve'],{log:()=>{}});
 const tokens=new Map(['alice','bob','eve'].map(u=>[u+'-binding-fixture-token-00000000',u]));
 const server=createServer(broker,tokens);await new Promise(r=>server.listen(0,'127.0.0.1',r));
 t.after(async()=>{server.closeAllConnections();await new Promise(r=>server.close(r));store.close();});
 const call=async(user,session,topic,path,args)=>{
  const response=await fetch(`http://127.0.0.1:${server.address().port}`+path,{method:args===undefined?'GET':'POST',headers:{Authorization:'Bearer '+user+'-binding-fixture-token-00000000','X-Session-Id':session,'Content-Type':'application/json',...(topic===null?{}:{'X-Topic':topic})},...(args===undefined?{}:{body:JSON.stringify(args)})});return {status:response.status,data:await response.json()};
 };
 for(const topic of ['x','y']){
  assert.equal((await call('alice','owner',null,'/topics',{name:topic})).status,201);
  for(const user of ['alice','bob','eve'])store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,1)','default',topic,user);
 }
 return {store,broker,call};
}
test('member heartbeat requires an immutable subscribed topic; owner may coordinate without a topic',async t=>{
 const f=await fixture(t);
 assert.equal((await f.call('bob','missing',null,'/heartbeat',{})).data.error,'TOPIC_REQUIRED');
 assert.equal(f.store.get("SELECT 1 FROM agents WHERE session_id='missing'"),undefined);
 assert.equal((await f.call('bob','bound','x','/heartbeat',{})).status,200);
 assert.equal((await f.call('bob','bound','y','/heartbeat',{})).data.error,'SESSION_TOPIC_MISMATCH');
 assert.equal((await f.call('bob','bound',null,'/heartbeat',{})).data.error,'SESSION_TOPIC_MISMATCH');
 assert.equal((await f.call('alice','owner',null,'/heartbeat',{})).status,200);
});
test('bound topic authorization applies to listing, subscription and topic creation',async t=>{
 const f=await fixture(t);
 const topics=await f.call('bob','bx','x','/topics');assert.deepEqual(topics.data.topics.map(r=>r.name),['x']);
 assert.equal((await f.call('bob','bx','x','/topics/subscribe',{name:'y'})).data.error,'TOPIC_SCOPE_MISMATCH');
 assert.equal((await f.call('bob','bx','x','/topics',{name:'member-created'})).data.error,'NOT_WORKSPACE_OWNER');
 assert.equal((await f.call('alice','ax','x','/topics',{name:'owner-created'})).status,201);
});
test('closed binding can read but cannot send; departure ends the bound session',async t=>{
 const f=await fixture(t);
 assert.equal((await f.call('bob','bx','x','/heartbeat',{})).status,200);
 assert.equal((await f.call('alice','owner',null,'/topics/close',{name:'x'})).status,200);
 assert.equal((await f.call('bob','bx','x','/inbox')).status,200);
 assert.equal((await f.call('bob','bx','x','/send',{to:'user:alice',body:'closed'})).data.error,'TOPIC_CLOSED');
 assert.equal((await f.call('bob','bx','x','/topics/leave',{name:'x'})).status,200);
 assert.equal((await f.call('bob','bx','x','/inbox')).data.error,'SESSION_ENDED');
});

test('direct questions carry their topic and reach only matching sessions plus the workspace owner',async t=>{
 const f=await fixture(t);
 const x=await f.call('bob','bx','x','/send',{to:'user:alice',body:'question x',kind:'question'});
 const y=await f.call('eve','ey','y','/send',{to:'user:alice',body:'question y',kind:'question'});
 assert.equal(x.status,201);assert.equal(y.status,201);
 const owner=await f.call('alice','owner',null,'/inbox');
 assert.deepEqual(owner.data.messages.filter(m=>[x.data.id,y.data.id].includes(m.id)).map(m=>m.topic),['x','y']);
 const local=await f.call('alice','ax','x','/inbox');
 assert.deepEqual(local.data.messages.filter(m=>[x.data.id,y.data.id].includes(m.id)).map(m=>m.id),[x.data.id]);
 assert.equal((await f.call('alice','ax','x','/ack',{id:y.data.id})).data.error,'MESSAGE_NOT_FOUND');
 assert.equal((await f.call('alice','owner',null,'/ack',{id:x.data.id})).status,200);
 const answer=await f.call('alice','owner',null,'/send',{to:'user:bob',kind:'answer',thread_id:x.data.thread_id,body:'answer x'});assert.equal(answer.status,201);
 assert.ok((await f.call('bob','bx','x','/inbox')).data.messages.some(m=>m.id===answer.data.id));
 assert.equal((await f.call('bob','by','y','/inbox')).data.messages.some(m=>m.id===answer.data.id),false);
 assert.equal((await f.call('bob','by','y','/close',{thread_id:x.data.thread_id})).data.error,'THREAD_NOT_FOUND');
 assert.equal((await f.call('alice','owner',null,'/send',{to:'user:bob',body:'requires topic'})).data.error,'TOPIC_REQUIRED');
 assert.equal((await f.call('alice','owner',null,'/send',{to:'user:bob',body:'owner starts x',topic:'x'})).status,201);
});

test('workstream names, overlaps and decision pages stay in the bound topic',async t=>{
 const f=await fixture(t);
 for(const topic of ['x','y']){
  assert.equal((await f.call('bob','b'+topic,topic,'/workstreams/register',{name:'same',paths:['src']})).status,200);
  assert.equal((await f.call('bob','b'+topic,topic,'/decisions/record',{workstream:'same',body:'decision '+topic})).status,201);
 }
 assert.equal(f.store.get("SELECT count(*) n FROM workstreams WHERE name='same'").n,2);
 assert.equal((await f.call('eve','ey','y','/workstreams/register',{name:'hidden-y',paths:['src']})).status,200);
 const overlap=await f.call('bob','bx','x','/workstreams/check-overlap',{name:'same'});
 assert.equal(overlap.status,200);assert.deepEqual(overlap.data.overlaps,[]);
 const own=await f.call('bob','bx','x','/decisions');assert.deepEqual(own.data.decisions.map(r=>r.body),['decision x']);
 const all=await f.call('alice','owner',null,'/decisions');assert.deepEqual(all.data.decisions.map(r=>r.body),['decision x','decision y']);
 assert.equal((await f.call('bob','bx','x','/workstreams/release',{name:'hidden-y'})).data.error,'WORKSTREAM_NOT_FOUND');
});

test('invites name one topic, respect owner binding and reuse membership for another topic',async t=>{
 const f=await fixture(t);
 for(const topics of [undefined,[],['x','y']])assert.equal((await f.call('alice','owner',null,'/invites',topics===undefined?{}:{topics})).data.error,'INVALID_INVITE_TOPICS');
 assert.equal((await f.call('alice','ax','x','/invites',{topics:['y']})).data.error,'TOPIC_SCOPE_MISMATCH');
 assert.equal((await f.call('bob','bx','x','/invites',{topics:['x']})).data.error,'NOT_WORKSPACE_OWNER');
 f.store.run("DELETE FROM topic_subscriptions WHERE user='bob' AND topic='y'");
 const invite=await f.call('alice','owner',null,'/invites',{topics:['y']});assert.equal(invite.status,201);
 const joined=await f.call('bob','bx','x','/join',{code:invite.data.code,name:'bob'});assert.equal(joined.status,201);
 assert.equal(joined.data.topic,'y');assert.equal(Object.hasOwn(joined.data,'token'),false);
 assert.ok(f.store.get("SELECT 1 FROM topic_subscriptions WHERE user='bob' AND topic='y'"));
 assert.equal(f.store.get("SELECT count(*) n FROM workspace_members WHERE user='bob'").n,1);
});

test('topic notices retain their scope and purged queue inspection cannot cross the binding',async t=>{
 const f=await fixture(t);
 assert.equal((await f.call('bob','by','y','/heartbeat',{})).status,200);
 assert.equal((await f.call('alice','owner',null,'/topics/close',{name:'y'})).status,200);
 const local=await f.call('bob','by','y','/inbox');assert.ok(local.data.messages.some(m=>m.kind==='notice'&&m.topic==='y'));
 assert.equal((await f.call('bob','bx','x','/inbox')).data.messages.some(m=>m.kind==='notice'&&m.topic==='y'),false);
 assert.equal((await f.call('alice','owner',null,'/topics',{name:'queue-y',mode:'queue'})).status,201);
 f.store.run("INSERT INTO topic_subscriptions VALUES ('default','queue-y','bob',1)");
 const sent=await f.call('alice','owner',null,'/send',{to:'topic:queue-y',body:'queue tombstone'});assert.equal(sent.status,201);
 f.store.run('UPDATE messages SET purged_at=1,purge_run_id=1 WHERE id=?',sent.data.id);
 assert.equal((await f.call('bob','bx','x','/queue/item?id='+sent.data.id)).data.error,'MESSAGE_NOT_FOUND');
});

test('workspace owner inspects unsubscribed topics but pending delivery and ack still require subscription',async t=>{
 const f=await fixture(t);f.store.run("DELETE FROM topic_subscriptions WHERE user='alice' AND topic='x'");
 const sent=await f.call('bob','bx','x','/send',{to:'topic:x',body:'subscriber-only delivery'});assert.equal(sent.status,201);
 const message=f.store.get('SELECT * FROM messages WHERE id=?',sent.data.id);
 assert.equal(f.broker.visibleTopic(message,'alice','owner'),true);
 assert.equal((await f.call('alice','owner',null,'/inbox')).data.messages.some(m=>m.id===sent.data.id),false);
 assert.equal((await f.call('alice','owner',null,'/ack',{id:sent.data.id})).data.error,'MESSAGE_NOT_FOUND');
 assert.equal((await f.call('alice','owner',null,'/send',{to:'topic:x',body:'owner may post without subscribing'})).status,201);
});

test('broker workspace notices cross bindings only for targets; ordinary NULL history remains owner-level',async t=>{
 const f=await fixture(t);
 const ordinary=await f.call('alice','owner',null,'/send',{to:'user:bob',topic:'x',body:'historical ordinary'});assert.equal(ordinary.status,201);
 f.store.run('UPDATE messages SET topic=NULL WHERE id=?',ordinary.data.id);f.store.run('UPDATE threads SET topic=NULL WHERE id=?',ordinary.data.thread_id);
 const changed=await f.call('alice','owner',null,'/members/transfer',{user:'bob'});assert.equal(changed.status,200);
 const notice=f.store.get("SELECT id FROM messages WHERE kind='notice' ORDER BY id DESC").id;
 for(const [user,session,topic] of [['alice','former-owner-x','x'],['bob','new-owner-y','y']]) {
  const inbox=await f.call(user,session,topic,'/inbox');assert.equal(inbox.status,200);
  assert.ok(inbox.data.messages.some(m=>m.id===notice));assert.equal(inbox.data.messages.some(m=>m.id===ordinary.data.id),false);
 }
 assert.ok((await f.call('bob','new-owner-workspace',null,'/inbox')).data.messages.some(m=>m.id===ordinary.data.id));
 assert.equal((await f.call('eve','not-target','x','/inbox')).data.messages.some(m=>m.id===notice),false);
});

test('global administration requires owner workspace scope with a fixed refusal order',async t=>{
 const f=await fixture(t),before=f.store.all('SELECT * FROM workspace_members');
 for(const path of ['/members/remove','/members/transfer']) {
  const member=await f.call('bob','bx','x',path,{user:'eve'});assert.equal(member.data.error,'NOT_WORKSPACE_OWNER');
  const owner=await f.call('alice','ax','x',path,{user:'eve'});assert.equal(owner.status,403);assert.equal(owner.data.error,'TOPIC_SCOPE_MISMATCH');
 }
 assert.deepEqual(f.store.all('SELECT * FROM workspace_members'),before);
 assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action IN ('member:remove','owner:transfer')").n,0);
 assert.equal((await f.call('bob','missing-binding',null,'/members/remove',{user:'eve'})).data.error,'TOPIC_REQUIRED');
 assert.equal((await f.call('bob','bx','x','/members')).data.members.length,3);
 assert.equal((await f.call('alice','owner',null,'/members/remove',{user:'eve'})).status,200);
});

test('an owner topic workstream also reaches its owner workspace-level session',async t=>{
 const f=await fixture(t);
 assert.equal((await f.call('alice','owner-x','x','/workstreams/register',{name:'owner-flow'})).status,200);
 const sent=await f.call('bob','bx','x','/send',{to:'ws:owner-flow',kind:'question',body:'owner workstream question'});assert.equal(sent.status,201);
 assert.ok((await f.call('alice','owner-x','x','/inbox')).data.messages.some(m=>m.id===sent.data.id));
 assert.ok((await f.call('alice','owner',null,'/inbox')).data.messages.some(m=>m.id===sent.data.id));
 assert.equal((await f.call('alice','owner-y','y','/inbox')).data.messages.some(m=>m.id===sent.data.id),false);
});

test('owner replies preserve a historical NULL thread while bound sessions cannot read its new ordinary messages',async t=>{
 const f=await fixture(t),sent=await f.call('bob','bx','x','/send',{to:'user:alice',body:'legacy question',kind:'question'});
 f.store.run('UPDATE messages SET topic=NULL WHERE id=?',sent.data.id);f.store.run('UPDATE threads SET topic=NULL WHERE id=?',sent.data.thread_id);
 assert.equal((await f.call('alice','owner',null,'/ack',{id:sent.data.id})).status,200);
 const answer=await f.call('alice','owner',null,'/send',{to:'user:bob',thread_id:sent.data.thread_id,kind:'answer',body:'owner legacy reply'});assert.equal(answer.status,201);
 assert.equal(f.store.get('SELECT topic FROM messages WHERE id=?',answer.data.id).topic,null);
 assert.equal((await f.call('bob','bx','x','/inbox')).data.messages.some(m=>m.id===answer.data.id),false);
});

for(const action of ['remove','leave','finish'])test(`workspace ${action} permanently ends the old session after a successful rejoin`,async t=>{
 const f=await fixture(t);assert.equal((await f.call('bob','old-member','x','/heartbeat',{})).status,200);
 const ended=action==='remove'?await f.call('alice','owner',null,'/members/remove',{user:'bob'}):await f.call('bob','old-member','x','/members/'+action,action==='finish'?{summary:'completed'}:{});assert.equal(ended.status,200);
 assert.equal((await f.call('bob','old-member','x','/heartbeat',{})).data.error,'NOT_A_MEMBER');
 const invite=await f.call('alice','owner',null,'/invites',{topics:['x']});assert.equal(invite.status,201);
 assert.equal((await f.call('bob','rejoin-identity','x','/join',{name:'bob',code:invite.data.code})).status,201);
 const refused=await f.call('bob','old-member','x','/heartbeat',{});assert.equal(refused.status,409);assert.equal(refused.data.error,'SESSION_ENDED');assert.equal(refused.data.reason,'membership_ended');
 assert.equal((await f.call('bob','fresh-member','x','/heartbeat',{})).status,200);
});

test('topic-bound owners cannot inspect or mutate another topic through any topic-aware endpoint',async t=>{
 const f=await fixture(t),owner=(path,args)=>f.call('alice','owner',null,path,args),bound=(path,args)=>f.call('alice','audit-x','x',path,args);
 const topic=await f.call('bob','publisher-y','y','/send',{to:'topic:y',body:'TOPIC_Y_BODY'});assert.equal(topic.status,201);
 const direct=await f.call('bob','direct-y','y','/send',{to:'user:alice',body:'TOPIC_Y_BODY',kind:'question'});assert.equal(direct.status,201);
 assert.equal((await owner('/topics/keep',{id:topic.data.id})).status,200);
 assert.equal((await owner('/topics',{name:'queue-y',mode:'queue'})).status,201);f.store.run("INSERT INTO topic_subscriptions VALUES ('default','queue-y','alice',1)");
 const queue=await owner('/send',{to:'topic:queue-y',body:'TOPIC_Y_BODY'});assert.equal(queue.status,201);f.store.run("UPDATE queue_items SET state='dead',attempts=3,generation=7 WHERE message_id=?",queue.data.id);
 const invite=await owner('/invites',{topics:['y']});assert.equal(invite.status,201);
 assert.equal((await f.call('alice','plan-y','y','/workstreams/register',{name:'only-y'})).status,200);
 assert.equal((await f.call('alice','plan-y','y','/decisions/record',{workstream:'only-y',body:'TOPIC_Y_BODY'})).status,201);
 const snapshot=()=>JSON.stringify([
  f.store.all("SELECT * FROM topics WHERE name IN ('y','queue-y')"),f.store.all("SELECT * FROM topic_subscriptions WHERE topic IN ('y','queue-y')"),f.store.all("SELECT * FROM topic_cursors WHERE topic IN ('y','queue-y')"),f.store.all('SELECT * FROM topic_pins'),f.store.all('SELECT * FROM queue_items'),f.store.all('SELECT * FROM invites'),f.store.all("SELECT * FROM workstreams WHERE topic='y'"),f.store.all("SELECT * FROM decisions WHERE topic='y'"),f.store.all("SELECT * FROM agents WHERE topic IN ('y','queue-y')"),
 ]),before=snapshot();
 const cases=[
  ['/queue/claim',{id:queue.data.id},'MESSAGE_NOT_FOUND'],['/queue/renew',{id:queue.data.id,generation:7},'MESSAGE_NOT_FOUND'],['/queue/done',{id:queue.data.id,generation:7},'MESSAGE_NOT_FOUND'],['/queue/item?id='+queue.data.id,undefined,'MESSAGE_NOT_FOUND'],['/queue/requeue',{id:queue.data.id},'TOPIC_SCOPE_MISMATCH'],['/queue/dead-letters?topic=queue-y',undefined,'TOPIC_SCOPE_MISMATCH'],
  ['/topics/skip',{topic:'y'},'TOPIC_SCOPE_MISMATCH'],['/topics/keep',{id:topic.data.id},'MESSAGE_NOT_FOUND'],['/topics/unkeep',{id:topic.data.id},'MESSAGE_NOT_FOUND'],['/topics/pins?topic=y',undefined,'TOPIC_SCOPE_MISMATCH'],['/topics/subscribe',{name:'y'},'TOPIC_SCOPE_MISMATCH'],['/topics/leave',{name:'y'},'TOPIC_SCOPE_MISMATCH'],['/topics/close',{name:'y'},'TOPIC_SCOPE_MISMATCH'],
  ['/invites',{topics:['y']},'TOPIC_SCOPE_MISMATCH'],['/invites/revoke',{invite_id:invite.data.invite_id},'TOPIC_SCOPE_MISMATCH'],
  ['/workstreams/check-overlap',{name:'only-y'},'WORKSTREAM_NOT_FOUND'],['/workstreams/release',{name:'only-y'},'WORKSTREAM_NOT_FOUND'],['/decisions/record',{workstream:'only-y',body:'cannot cross'},'WORKSTREAM_NOT_FOUND'],
  ['/send',{to:'user:bob',topic:'y',body:'cannot cross'},'TOPIC_SCOPE_MISMATCH'],['/send',{to:'ws:only-y',body:'cannot cross'},'WORKSTREAM_NOT_FOUND'],['/send',{to:'user:bob',kind:'answer',thread_id:direct.data.thread_id,body:'cannot cross'},'TOPIC_SCOPE_MISMATCH'],['/ack',{id:direct.data.id},'MESSAGE_NOT_FOUND'],['/close',{thread_id:direct.data.thread_id},'TOPIC_SCOPE_MISMATCH'],
 ];
 for(const [path,args,expected] of cases){const result=await bound(path,args);assert.equal(result.data.error,expected,path);assert.ok(snapshot()===before,path+' leaves the other topic untouched');}
 assert.deepEqual((await bound('/topics')).data.topics.map(r=>r.name),['x']);
 assert.deepEqual((await bound('/topics/pins')).data.messages,[]);assert.deepEqual((await bound('/invites')).data.invites,[]);
 assert.deepEqual((await bound('/decisions')).data.decisions,[]);assert.equal((await bound('/inbox')).data.messages.some(m=>m.body==='TOPIC_Y_BODY'),false);
});

test('workspace owner inspection does not bypass queue processing or pin subscription requirements',async t=>{
 const f=await fixture(t),owner=(path,args)=>f.call('alice','owner',null,path,args);
 assert.equal((await owner('/topics',{name:'unsubscribed-queue',mode:'queue'})).status,201);
 const sent=await owner('/send',{to:'topic:unsubscribed-queue',body:'owner inspection only'});assert.equal(sent.status,201);
 assert.equal((await owner('/queue/item?id='+sent.data.id)).status,200);
 assert.equal((await owner('/queue/claim',{id:sent.data.id})).data.error,'MESSAGE_NOT_FOUND');
 assert.equal((await owner('/topics/keep',{id:sent.data.id})).data.error,'MESSAGE_NOT_FOUND');
 assert.equal((await owner('/topics/subscribe',{name:'unsubscribed-queue'})).status,200);
 assert.equal((await owner('/queue/claim',{id:sent.data.id})).status,200);
 assert.equal((await owner('/topics/keep',{id:sent.data.id})).status,200);
});

test('closed-topic finish is a narrow lifecycle exception and reaches only the owner in its topic',async t=>{
 const f=await fixture(t),sent=await f.call('bob','finisher','x','/send',{to:'user:alice',body:'before close'});assert.equal(sent.status,201);
 assert.equal((await f.call('alice','owner',null,'/topics/close',{name:'x'})).status,200);
 for(const kind of ['finding','question'])assert.equal((await f.call('bob','finisher','x','/send',{to:'user:alice',body:'still closed',kind,lifecycleFinish:true})).data.error,'TOPIC_CLOSED');
 assert.equal((await f.call('bob','finisher','x','/close',{thread_id:sent.data.thread_id})).data.error,'TOPIC_CLOSED');
 const result=await f.call('bob','finisher','x','/members/finish',{summary:'closed completion'});assert.equal(result.status,200);
 assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE user='bob'"),undefined);
 assert.equal(f.store.get("SELECT ended_reason FROM agents WHERE session_id='finisher'").ended_reason,'membership_ended');
 const inbox=await f.call('alice','owner',null,'/inbox');assert.ok(inbox.data.messages.some(m=>m.body==='closed completion'&&m.topic==='x'&&m.kind==='finding'));
 assert.equal((await f.call('alice','other-owner-binding','y','/inbox')).data.messages.some(m=>m.body==='closed completion'),false);
});

test('closed-topic finish rolls its summary and session end back when membership removal fails',async t=>{
 const f=await fixture(t);await f.call('bob','rollback-finisher','x','/heartbeat',{});await f.call('alice','owner',null,'/topics/close',{name:'x'});
 const before=f.store.get('SELECT count(*) n FROM messages').n;
 f.store.db.exec("CREATE TEMP TRIGGER fail_closed_finish BEFORE DELETE ON workspace_members WHEN OLD.user='bob' BEGIN SELECT RAISE(ABORT,'fixture finish failure'); END");
 const failed=await f.call('bob','rollback-finisher','x','/members/finish',{summary:'must roll back'});assert.equal(failed.status,500);
 assert.equal(f.store.get('SELECT count(*) n FROM messages').n,before);assert.ok(f.store.get("SELECT 1 FROM workspace_members WHERE user='bob'"));
 assert.equal(f.store.get("SELECT ended_at FROM agents WHERE session_id='rollback-finisher'").ended_at,null);assert.ok(f.store.get("SELECT 1 FROM topic_subscriptions WHERE user='bob' AND topic='x'"));
 assert.equal((await f.call('bob','rollback-finisher','x','/send',{to:'user:alice',body:'still closed'})).data.error,'TOPIC_CLOSED');
 f.store.db.exec('DROP TRIGGER fail_closed_finish');
 assert.equal((await f.call('bob','rollback-finisher','x','/members/finish',{summary:'successful retry'})).status,200);
});

test('NULL and X workstream namespaces stay separate in both directions without fallback',async t=>{
 const f=await fixture(t),root=(path,args)=>f.call('alice','root-lease',null,path,args),x=(path,args)=>f.call('bob','x-lease','x',path,args);
 for(const name of ['same','root-only'])assert.equal((await root('/workstreams/register',{name})).status,200);
 for(const name of ['same','x-only'])assert.equal((await x('/workstreams/register',{name})).status,200);
 const a=await f.call('alice','root-sender',null,'/send',{to:'ws:same',body:'root namespace'});assert.equal(a.status,201);assert.equal(f.store.get('SELECT topic FROM messages WHERE id=?',a.data.id).topic,null);
 const b=await f.call('alice','root-sender',null,'/send',{to:'ws:same',topic:'x',body:'x namespace'});assert.equal(b.status,201);
 assert.equal(f.store.get('SELECT user FROM delivery_targets WHERE message_id=?',a.data.id).user,'alice');assert.equal(f.store.get('SELECT user FROM delivery_targets WHERE message_id=?',b.data.id).user,'bob');
 assert.ok((await root('/inbox')).data.messages.some(m=>m.id===a.data.id));assert.equal((await x('/inbox')).data.messages.some(m=>m.id===a.data.id),false);
 assert.ok((await x('/inbox')).data.messages.some(m=>m.id===b.data.id));assert.equal((await root('/inbox')).data.messages.some(m=>m.id===b.data.id),false);
 assert.equal((await x('/send',{to:'ws:root-only',body:'no fallback'})).data.error,'WORKSTREAM_NOT_FOUND');
 assert.equal((await root('/send',{to:'ws:x-only',body:'no fallback'})).data.error,'WORKSTREAM_NOT_FOUND');
 assert.equal((await root('/send',{to:'user:bob',body:'direct user still needs a topic'})).data.error,'TOPIC_REQUIRED');
});

test('ownership transfer releases only the old owner NULL leases with audit and transaction rollback',async t=>{
 const f=await fixture(t);f.broker.now=()=>1000;
 for(const name of ['root-one','root-two']){
  assert.equal((await f.call('alice','old-root',null,'/workstreams/register',{name})).status,200);
  assert.equal((await f.call('alice','old-root',null,'/decisions/record',{workstream:name,body:'retained root decision'})).status,201);
 }
 assert.equal((await f.call('alice','old-x','x','/workstreams/register',{name:'root-one',paths:['src']})).status,200);
 f.broker.createWorkspace('alice',{name:'other'});f.broker.heartbeat('alice','other-root','worker','other',null);f.broker.register('alice','other-root','root-one');
 const before=f.store.all('SELECT * FROM workstreams'),decisions=f.store.all('SELECT * FROM decisions');
 f.store.db.exec("CREATE TEMP TRIGGER fail_transfer BEFORE UPDATE OF role ON workspace_members WHEN NEW.user='bob' AND NEW.role='owner' BEGIN SELECT RAISE(ABORT,'fixture transfer failure'); END");
 assert.equal((await f.call('alice','owner',null,'/members/transfer',{user:'bob'})).status,500);
 assert.deepEqual(f.store.all('SELECT * FROM workstreams'),before);assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='workstream:release'").n,0);
 f.store.db.exec('DROP TRIGGER fail_transfer');
 assert.equal((await f.call('alice','owner',null,'/members/transfer',{user:'bob'})).status,200);
 assert.equal(f.store.get("SELECT count(*) n FROM workstreams WHERE workspace='default' AND topic IS NULL AND owner_user='alice'").n,0);
 assert.deepEqual(f.store.all("SELECT * FROM workstreams WHERE workspace='other'"),before.filter(r=>r.workspace==='other'));
 assert.deepEqual(f.store.all("SELECT * FROM workstreams WHERE topic='x'"),before.filter(r=>r.topic==='x'));assert.deepEqual(f.store.all('SELECT * FROM decisions'),decisions);
 const audit=f.store.all("SELECT * FROM workspace_audit WHERE action='workstream:release' ORDER BY id");assert.equal(audit.length,2);
 assert.deepEqual(audit.map(r=>JSON.parse(r.detail).name).sort(),['root-one','root-two']);assert.ok(audit.every(r=>r.actor_user==='alice'&&JSON.parse(r.detail).reason==='owner_transfer'&&JSON.parse(r.detail).topic===null));
 for(const name of ['root-one','root-two'])assert.equal((await f.call('bob','new-root',null,'/workstreams/register',{name})).status,200);
});

test('fresh member bindings do not reveal whether an unsubscribed topic exists or is closed',async t=>{
 const f=await fixture(t);f.store.run("DELETE FROM topic_subscriptions WHERE user='bob' AND topic='y'");
 await f.call('alice','owner',null,'/topics',{name:'closed-hidden'});await f.call('alice','owner',null,'/topics/close',{name:'closed-hidden'});
 const refused=[];
 for(const topic of ['y','absent','closed-hidden']){const response=await f.call('bob','probe-'+topic,topic,'/heartbeat',{});assert.equal(response.status,403);assert.equal(response.data.error,'NOT_SUBSCRIBED');refused.push(response.data);}
 assert.deepEqual(refused,[refused[0],refused[0],refused[0]]);assert.equal(f.store.get("SELECT count(*) n FROM agents WHERE session_id LIKE 'probe-%'").n,0);
 assert.equal((await f.call('bob','allowed-x','x','/heartbeat',{})).status,200);
});

test('member id probes have identical out-of-topic and nonexistent responses without writes',async t=>{
 const f=await fixture(t),member=(path,args)=>f.call('bob','probe-x','x',path,args);
 const own=await f.call('alice','owner',null,'/send',{to:'topic:x',body:'own positive control'});assert.equal(own.status,201);
 await f.call('alice','owner',null,'/topics',{name:'probe-queue',mode:'queue'});
 const foreign=await f.call('alice','owner',null,'/send',{to:'topic:probe-queue',body:'hidden queue'});assert.equal(foreign.status,201);
 const thread=await f.call('bob','probe-y','y','/send',{to:'user:alice',kind:'question',body:'hidden thread'});assert.equal(thread.status,201);
 assert.equal((await f.call('bob','work-y','y','/workstreams/register',{name:'hidden-workstream'})).status,200);
 const invitation=await f.call('alice','owner',null,'/invites',{topics:['y']});assert.equal(invitation.status,201);
 const legacy=await f.call('alice','owner',null,'/send',{to:'user:bob',topic:'x',body:'legacy hidden context'});assert.equal(legacy.status,201);
 f.store.run("UPDATE messages SET kind='notice',notice_scope=NULL,from_user='broker',from_session='broker' WHERE id=?",legacy.data.id);f.store.run("UPDATE threads SET status='resolved' WHERE id=?",legacy.data.thread_id);f.store.run("INSERT INTO escalation_targets(message_id,user) VALUES (?,'bob')",legacy.data.id);
 const snapshot=()=>JSON.stringify([f.store.all('SELECT * FROM messages'),f.store.all('SELECT * FROM threads'),f.store.all('SELECT * FROM queue_items'),f.store.all('SELECT * FROM topic_pins'),f.store.all('SELECT * FROM topic_cursors'),f.store.all('SELECT * FROM workstreams'),f.store.all('SELECT * FROM invites'),f.store.all('SELECT * FROM workspace_audit')]),before=snapshot(),missing=999999;
 const routes=[
  ['/queue/requeue',id=>({id})],['/queue/item',id=>undefined,true],['/queue/claim',id=>({id})],['/queue/renew',id=>({id,generation:1})],['/queue/done',id=>({id,generation:1})],['/topics/keep',id=>({id})],['/topics/unkeep',id=>({id})],['/ack',id=>({id})],
 ];
 for(const [path,args,query] of routes){const a=await member(path+(query?'?id='+foreign.data.id:''),args(foreign.data.id)),b=await member(path+(query?'?id='+missing:''),args(missing));assert.equal(a.status,404,path);assert.deepEqual(a,b,path);assert.ok(snapshot()===before,path+' has no business writes');}
 for(const [path,args] of [['/close',id=>({thread_id:id})],['/send',id=>({to:'user:alice',kind:'answer',thread_id:id,body:'cannot cross'})]]){
  for(const id of [thread.data.thread_id,legacy.data.thread_id]){const a=await member(path,args(id)),b=await member(path,args('absent-thread-id'));assert.equal(a.status,404,path);assert.deepEqual(a,b,path);assert.ok(snapshot()===before,path+' has no business writes');}
 }
 const a=await member('/workstreams/release',{name:'hidden-workstream'}),b=await member('/workstreams/release',{name:'absent-workstream'});assert.equal(a.status,404);assert.deepEqual(a,b);
 // Owner-only id routes refuse before inspecting any identifier (decision98).
 const c=await member('/invites/revoke',{invite_id:invitation.data.invite_id}),d=await member('/invites/revoke',{invite_id:'f'.repeat(32)});assert.equal(c.status,403);assert.deepEqual(c,d);assert.ok(snapshot()===before);
 // Skip consumes a numeric bound, not an addressable id; a foreign id above
 // the local maximum and a nonexistent high id must cap to identical progress.
 const first=await member('/topics/skip',{topic:'x',through:foreign.data.id});assert.equal(first.status,200);
 f.store.run("DELETE FROM topic_cursors WHERE workspace='default' AND topic='x' AND user='bob'");
 const second=await member('/topics/skip',{topic:'x',through:missing});assert.deepEqual(first,second);assert.equal(second.data.acked_through,own.data.id);
});

test('second-topic redemption names the topic in a topic-scoped owner notice',async t=>{
 const f=await fixture(t);f.store.run("DELETE FROM topic_subscriptions WHERE user='bob' AND topic='y'");
 const before=f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='member:add' AND subject_user='bob'").n;
 const invite=await f.call('alice','owner',null,'/invites',{topics:['y']});assert.equal(invite.status,201);
 assert.equal((await f.call('bob','bob-x','x','/join',{code:invite.data.code,name:'bob'})).status,201);
 const notice=f.store.get("SELECT * FROM messages WHERE kind='notice' ORDER BY id DESC");assert.match(notice.body,/bob joined topic y/);assert.equal(notice.topic,'y');assert.equal(notice.notice_scope,'topic');
 assert.ok((await f.call('alice','owner',null,'/inbox')).data.messages.some(m=>m.id===notice.id));
 assert.ok((await f.call('alice','owner-y','y','/inbox')).data.messages.some(m=>m.id===notice.id));
 assert.equal((await f.call('alice','owner-x','x','/inbox')).data.messages.some(m=>m.id===notice.id),false);
 assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='member:add' AND subject_user='bob'").n,before);
});
