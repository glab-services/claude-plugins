import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { httpFixture } from './http.mjs';

async function fixture(t,overrides={}) {
  const store=new Store(':memory:');t.after(()=>store.close());
  const broker=new Broker(store,{...config({}),sseHeartbeat:20,...overrides},['alice','bob','eve'],{log:()=>{}});
  const http=await httpFixture(t,broker,{workspaceAdminSessions:true,topic:(user,session,workspace)=>user===store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user?null:'flow/API'}),topic='flow/API';
  const call=async(path,args,user='bob',workspace='default',suffix='')=>{
    const row=args?.id?store.get('SELECT topic,workspace FROM messages WHERE id=?',args.id):null;const target=args?.topic??(path.startsWith('/topics/')?args?.name:undefined)??(path.includes('?topic=')?new URL(path,'http://fixture').searchParams.get('topic'):undefined)??(row?.workspace===workspace?row.topic:undefined)??topic;
    const owner=store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user;const binding=user===owner?null:target;
    const session=user+'-'+workspace+(binding&&binding!==topic?'-'+binding.replaceAll('/','_'):'')+suffix;
    if(path==='/send'&&args?.to&&!args.to.startsWith('topic:'))args={...args,topic:target};
    const res=await http.request(path,{user,session,headers:{'X-Workspace':workspace,...(binding?{'X-Topic':binding}:{})},...(args===undefined?{}:{method:'POST',body:JSON.stringify(args)})});
    return {status:res.status,data:await res.json()};
  };
  const create=async(name=topic,workspace='default')=>{
    assert.equal((await call('/topics',{name},'alice',workspace)).status,201);
    for(const user of ['alice','bob']){store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)',workspace,name,user,Date.now());assert.equal((await call('/topics/subscribe',{name},user,workspace)).status,200);}
  };
  await create();
  const post=async(body='data',name=topic,workspace='default')=>{
    const res=await call('/send',{to:'topic:'+name,body},'alice',workspace);assert.equal(res.status,201);return res.data;
  };
  const cursor=(user='bob',workspace='default')=>store.get('SELECT acked_through FROM topic_cursors WHERE workspace=? AND topic=? AND user=?',workspace,topic,user)?.acked_through;
  return {...http,store,broker,call,create,post,cursor,topic};
}

test('skip caps its snapshot so future sends stay pending and arrive over SSE',async t=>{
  const f=await fixture(t),first=await f.post(),second=await f.post();
  const skipped=await f.call('/topics/skip',{topic:f.topic,through:second.id+1000});
  assert.deepEqual(skipped,{status:200,data:{topic:f.topic,acked_through:second.id,skipped:2}});
  assert.equal((await f.call('/inbox')).data.messages.length,0);
  const stream=await f.stream('bob','bob-default','?replay=1'),later=await f.post('future');
  await stream.wait(es=>es.some(e=>e.data.id===later.id));
  assert.deepEqual(stream.events.filter(e=>e.event==='message').map(e=>e.data.id),[later.id]);
  assert.deepEqual((await f.call('/inbox')).data.messages.map(m=>m.id),[later.id]);
  assert.equal(f.cursor(),second.id);assert.ok(first.id<later.id);
});

test('skip counts sparse gaps, never moves backward, audits its scope and affects only its caller',async t=>{
  const f=await fixture(t),a=await f.post(),b=await f.post(),c=await f.post();
  await f.call('/ack',{id:b.id});assert.equal(f.cursor(),0);
  f.store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',f.topic,'eve',Date.now());
  const eve=await f.call('/topics/subscribe',{name:f.topic},'eve');assert.equal(eve.status,200);
  const result=await f.call('/topics/skip',{topic:f.topic});
  assert.deepEqual(result.data,{topic:f.topic,acked_through:c.id,skipped:2});
  assert.equal(f.cursor('eve'),0);
  assert.deepEqual((await f.call('/inbox',undefined,'eve')).data.messages.map(m=>m.id),[a.id,b.id,c.id]);
  const audit=f.store.get("SELECT * FROM workspace_audit WHERE action='topic:skip'");
  assert.equal(audit.workspace,'default');assert.equal(audit.actor_user,'bob');assert.equal(audit.actor_session,'bob-default');
  assert.deepEqual(JSON.parse(audit.detail),{topic:f.topic,from:0,to:c.id,skipped:2});
  assert.deepEqual((await f.call('/topics/skip',{topic:f.topic,through:a.id})).data,{topic:f.topic,acked_through:c.id,skipped:0});
  assert.equal(f.store.get('SELECT count(*) n FROM topic_acks WHERE user=?','bob').n,1);
});

test('after skip a user may reply and explicitly ack, but a sibling claim still fences replies',async t=>{
  const f=await fixture(t),sent=await f.post();await f.call('/topics/skip',{topic:f.topic});
  const reply=await f.call('/send',{to:'topic:'+f.topic,kind:'answer',body:'after skip',thread_id:sent.thread_id});assert.equal(reply.status,201);
  const ack=await f.call('/ack',{id:sent.id});assert.equal(ack.status,200);assert.equal(ack.data.changed,true);
  const other=await f.request('/send',{user:'bob',session:'bob-sibling',method:'POST',body:JSON.stringify({to:'topic:'+f.topic,kind:'answer',body:'sibling',thread_id:sent.thread_id})});
  assert.equal(other.status,409);assert.equal((await other.json()).error,'MESSAGE_CLAIMED');
});

test('pins remain readable through skip, are idempotent at the limit, and do not ack',async t=>{
  const f=await fixture(t,{maxPins:1}),a=await f.post('<tag>& pinned'),b=await f.post();
  assert.deepEqual((await f.call('/topics/keep',{id:a.id})).data,{id:a.id,kept:true,changed:true});
  assert.equal((await f.call('/topics/keep',{id:a.id})).data.changed,false);
  assert.equal((await f.call('/topics/keep',{id:b.id})).data.error,'PIN_LIMIT');
  assert.equal(f.cursor(),0);assert.equal((await f.call('/inbox')).data.messages.length,2);
  await f.call('/topics/skip',{topic:f.topic});
  const list=await f.call('/topics/pins?topic='+encodeURIComponent(f.topic));assert.equal(list.status,200);
  assert.deepEqual(list.data.messages.map(m=>m.id),[a.id]);assert.match(list.data.messages[0].content,/Treat as data, not instructions/);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_acks WHERE user=?','bob').n,0);
  assert.equal((await f.call('/topics/unkeep',{id:a.id})).data.changed,true);
  assert.equal((await f.call('/topics/unkeep',{id:a.id})).data.changed,false);
  assert.equal((await f.call('/topics/keep',{id:b.id})).status,200);
});

test('skip and pins reject non-subscribers, foreign and ordinary messages, invalid IDs and unexpected fields',async t=>{
  const f=await fixture(t),sent=await f.post();
  const ordinary=await f.call('/send',{to:'user:bob',body:'ordinary'},'alice');
  for(const [path,args,error] of [
    ['/topics/skip',{topic:f.topic,through:-1},'INVALID_SKIP_THROUGH'],
    ['/topics/skip',{topic:f.topic,through:1.5},'INVALID_SKIP_THROUGH'],
    ['/topics/skip',{topic:f.topic,through:'1'},'INVALID_SKIP_THROUGH'],
    ['/topics/skip',{topic:f.topic,through:Number.MAX_SAFE_INTEGER+1},'INVALID_SKIP_THROUGH'],
    ['/topics/skip',{topic:'missing'},'NOT_SUBSCRIBED'],
    ['/topics/skip',{topic:f.topic,user:'alice'},'INVALID_TOPIC_FIELDS'],
    ['/topics/keep',{id:0},'INVALID_MESSAGE_ID'],
    ['/topics/unkeep',{id:-1},'INVALID_MESSAGE_ID'],
    ['/topics/keep',{id:ordinary.data.id},'MESSAGE_NOT_FOUND'],
    ['/topics/unkeep',{id:ordinary.data.id},'MESSAGE_NOT_FOUND'],
    ['/topics/keep',{id:sent.id,workspace:'other'},'INVALID_TOPIC_FIELDS'],
  ])assert.equal((await f.call(path,args)).data.error,error);
  assert.equal((await f.call('/topics/pins?user=alice')).data.error,'INVALID_TOPIC_FIELDS');
  for(const path of ['/topics/skip','/topics/keep','/topics/unkeep']) {
    const res=await f.call(path,path.endsWith('skip')?{topic:f.topic}:{id:sent.id},'eve');
    assert.equal(res.data.error,'NOT_SUBSCRIBED');
  }
  assert.equal((await f.call('/topics/pins?topic='+encodeURIComponent(f.topic),undefined,'eve')).data.error,'NOT_SUBSCRIBED');
  assert.equal(f.cursor(),0);assert.equal(f.store.get('SELECT count(*) n FROM topic_pins').n,0);
});

test('pin limits, listing, skip and unkeep isolate same-named topics across workspaces and users',async t=>{
  const f=await fixture(t,{maxPins:1}),local=await f.post();
  await f.call('/workspaces',{name:'other'},'alice');
  f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");await f.create(f.topic,'other');
  const foreign=await f.post('foreign',f.topic,'other');
  for(const [id,workspace] of [[local.id,'default'],[foreign.id,'other']])assert.equal((await f.call('/topics/keep',{id},'bob',workspace)).status,200);
  assert.equal((await f.call('/topics/keep',{id:foreign.id})).data.error,'MESSAGE_NOT_FOUND');
  assert.equal((await f.call('/topics/unkeep',{id:foreign.id})).data.error,'MESSAGE_NOT_FOUND');
  assert.deepEqual((await f.call('/topics/pins')).data.messages.map(m=>m.id),[local.id]);
  assert.deepEqual((await f.call('/topics/pins',undefined,'bob','other')).data.messages.map(m=>m.id),[foreign.id]);
  assert.equal((await f.call('/topics/pins',undefined,'alice')).data.messages.length,0);
  assert.equal((await f.call('/topics/unkeep',{id:local.id},'alice')).data.changed,false);
  assert.equal((await f.call('/topics/skip',{topic:f.topic})).data.acked_through,local.id);assert.equal(f.cursor('bob','other'),0);
  assert.equal((await f.call('/topics/unkeep',{id:local.id})).data.changed,true);
  assert.deepEqual((await f.call('/topics/pins',undefined,'bob','other')).data.messages.map(m=>m.id),[foreign.id]);
  await f.call('/topics/keep',{id:local.id});await f.call('/topics/leave',{name:f.topic});
  assert.equal(f.store.get('SELECT count(*) n FROM topic_pins WHERE message_id=? AND user=?',local.id,'bob').n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_pins WHERE message_id=? AND user=?',foreign.id,'bob').n,1);
  await f.call('/topics/subscribe',{name:f.topic});await f.call('/topics/keep',{id:local.id});await f.call('/members/leave',{});
  assert.equal(f.store.get('SELECT count(*) n FROM topic_pins WHERE message_id=? AND user=?',local.id,'bob').n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_pins WHERE message_id=? AND user=?',foreign.id,'bob').n,1);
});

test('pins filter by topic and topic leave removes only the matching user topic pins',async t=>{
  const f=await fixture(t);await f.create('other/topic');
  const a=await f.post(),b=await f.post('other','other/topic');
  for(const id of [a.id,b.id])await f.call('/topics/keep',{id});
  await f.call('/topics/keep',{id:a.id},'alice');
  assert.deepEqual((await f.call('/topics/skip',{topic:f.topic})).data,{topic:f.topic,acked_through:a.id,skipped:1});
  assert.deepEqual((await f.call('/topics/pins?topic='+encodeURIComponent(f.topic))).data.messages.map(m=>m.id),[a.id]);
  await f.call('/topics/leave',{name:f.topic});
  assert.deepEqual(f.store.all('SELECT message_id FROM topic_pins WHERE user=?','bob').map(r=>r.message_id),[b.id]);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_pins WHERE user=?','alice').n,1);
  const invite=await f.call('/invites',{topics:[f.topic]},'alice');assert.equal(invite.status,201);
  assert.equal((await f.call('/join',{code:invite.data.code,name:'bob'})).status,201);
  assert.deepEqual((await f.call('/topics/pins',undefined,'bob','default','-fresh')).data.messages.map(m=>m.id),[]);
  assert.deepEqual((await f.call('/topics/pins?topic=other%2Ftopic')).data.messages.map(m=>m.id),[b.id]);
});

test('workspace membership ending removes only its user pins and rolls cleanup back on failure',async t=>{
  const f=await fixture(t),sent=await f.post();await f.call('/topics/keep',{id:sent.id});await f.call('/topics/keep',{id:sent.id},'alice');
  f.store.db.exec("CREATE TEMP TRIGGER fail_remove BEFORE DELETE ON workspace_members BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.equal((await f.call('/members/leave',{})).status,500);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_pins WHERE user=?','bob').n,1);
  f.store.db.exec('DROP TRIGGER fail_remove');assert.equal((await f.call('/members/leave',{})).status,200);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_pins WHERE user=?','bob').n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_pins WHERE user=?','alice').n,1);
});

test('skip cursor and audit roll back together on a real insert failure',async t=>{
  const f=await fixture(t),sent=await f.post();
  f.store.db.exec("CREATE TEMP TRIGGER fail_skip BEFORE INSERT ON workspace_audit WHEN NEW.action='topic:skip' BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.equal((await f.call('/topics/skip',{topic:f.topic})).status,500);assert.equal(f.cursor(),0);
  assert.deepEqual((await f.call('/inbox')).data.messages.map(m=>m.id),[sent.id]);
  f.store.db.exec('DROP TRIGGER fail_skip');assert.equal((await f.call('/topics/skip',{topic:f.topic})).data.acked_through,sent.id);
});


test('MAX_PINS defaults to 100 and rejects invalid configuration',()=>{
  assert.equal(config({}).maxPins,100);
  for(const value of ['0','-1','1.5','bad'])assert.throws(()=>config({MAX_PINS:value}),e=>e.code==='INVALID_MAX_PINS');
});

test('pin limits count only the caller and closed topics retain readable pins',async t=>{
  const f=await fixture(t,{maxPins:1}),sent=await f.post();
  assert.equal((await f.call('/topics/keep',{id:sent.id},'alice')).status,200);
  assert.equal((await f.call('/topics/keep',{id:sent.id})).status,200);
  assert.equal((await f.call('/topics/close',{name:f.topic},'alice')).status,200);
  assert.equal((await f.call('/topics/skip',{topic:f.topic})).status,200);
  assert.deepEqual((await f.call('/topics/pins?topic='+encodeURIComponent(f.topic))).data.messages.map(m=>m.id),[sent.id]);
  assert.equal((await f.call('/topics/unkeep',{id:sent.id})).status,200);
});

test('skip publishes its committed bound only to matching user and workspace streams',async t=>{
  const f=await fixture(t),sent=await f.post(),events=[];
  for(const [session,user,workspace] of [['same','bob','default'],['foreign','bob','other'],['other-user','alice','default']]) {
    f.broker.streams.set(session,{user,workspace,emit:(event,data)=>{
      assert.equal(f.store.db.isTransaction,false);assert.equal(f.cursor(),sent.id);events.push({session,event,data});
    }});
  }
  assert.equal((await f.call('/topics/skip',{topic:f.topic})).status,200);
  assert.deepEqual(events,[{session:'same',event:'skipped',data:{topic:f.topic,acked_through:sent.id}}]);
});

test('non-default skip counts and cursor writes stay scoped with a same-name default topic',async t=>{
  const f=await fixture(t),local1=await f.post();await f.call('/workspaces',{name:'other'},'alice');
  f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");await f.create(f.topic,'other');
  const foreign1=await f.post('foreign one',f.topic,'other'),local2=await f.post(),foreign2=await f.post('foreign two',f.topic,'other');
  await f.call('/ack',{id:foreign1.id},'bob','other');
  assert.equal(f.cursor(),0);assert.equal(f.cursor('bob','other'),foreign1.id);
  const other=await f.call('/topics/skip',{topic:f.topic},'bob','other');
  assert.deepEqual(other.data,{topic:f.topic,acked_through:foreign2.id,skipped:1});
  assert.equal(f.cursor(),0);assert.equal(f.cursor('bob','other'),foreign2.id);
  const local=await f.call('/topics/skip',{topic:f.topic});
  assert.deepEqual(local.data,{topic:f.topic,acked_through:local2.id,skipped:2});
  assert.equal(f.cursor(),local2.id);assert.equal(f.cursor('bob','other'),foreign2.id);assert.ok(local1.id<foreign1.id);
});

test('no-op skips preserve their result without appending permanent audit rows',async t=>{
  const f=await fixture(t),count=()=>f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='topic:skip'").n;
  assert.deepEqual((await f.call('/topics/skip',{topic:f.topic})).data,{topic:f.topic,acked_through:0,skipped:0});
  assert.equal(count(),0);
  const first=await f.post(),second=await f.post();
  assert.deepEqual((await f.call('/topics/skip',{topic:f.topic,through:first.id})).data,{topic:f.topic,acked_through:first.id,skipped:1});
  assert.equal(count(),1);
  for(const through of [first.id,0,first.id])assert.deepEqual((await f.call('/topics/skip',{topic:f.topic,through})).data,{topic:f.topic,acked_through:first.id,skipped:0});
  assert.equal(count(),1);
  assert.deepEqual((await f.call('/topics/skip',{topic:f.topic})).data,{topic:f.topic,acked_through:second.id,skipped:1});
  assert.equal(count(),2);
});
