import http from 'node:http';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { httpFixture, eventually } from './http.mjs';
import { testTempDir } from '../../test-support/temp.mjs';

async function fixture(t,overrides={}) {
  const store=new Store(':memory:');t.after(()=>store.close());let now=1000;
  const broker=new Broker(store,{...config({}),sseHeartbeat:20,claimTtl:100,...overrides},['alice','bob','eve'],{now:()=>now,log:()=>{}});
  const wire=await httpFixture(t,broker,{workspaceAdminSessions:true,topic:(user,session,workspace)=>user===store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user?null:'jobs/build'}),topic='jobs/build';
  const call=async(path,args,user='bob',session=user,workspace='default',bindingTopic)=>{
    const row=args?.id?store.get('SELECT topic,workspace FROM messages WHERE id=?',args.id):null;const target=bindingTopic??args?.topic??(path.startsWith('/topics/')?args?.name:undefined)??(path.includes('?topic=')?new URL(path,'http://fixture').searchParams.get('topic'):undefined)??(row?.workspace===workspace?row.topic:undefined)??topic;
    const owner=store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user;const binding=user===owner?null:target;
    if(binding&&binding!==topic)session+='-'+binding.replaceAll('/','_');
    if(path==='/send'&&args?.to&&!args.to.startsWith('topic:'))args={...args,topic:target};
    const res=await wire.request(path,{user,session,headers:{'X-Workspace':workspace,...(binding?{'X-Topic':binding}:{})},...(args===undefined?{}:{method:'POST',body:JSON.stringify(args)})});
    return {status:res.status,data:await res.json()};
  };
  const create=async(workspace='default')=>{
    assert.equal((await call('/topics',{name:topic,mode:'queue'},'alice','alice-'+workspace,workspace)).status,201);
    for(const user of ['alice','bob','eve']){store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)',workspace,topic,user,now);assert.equal((await call('/topics/subscribe',{name:topic},user,user+'-'+workspace,workspace)).status,200);}
  };await create();
  const post=async(body='work',workspace='default')=>{
    const r=await call('/send',{to:'topic:'+topic,body},'alice','alice-'+workspace,workspace);assert.equal(r.status,201);return r.data;
  };
  const item=id=>store.get('SELECT * FROM queue_items WHERE message_id=?',id);
  return {...wire,store,broker,topic,call,post,item,create,advance:ms=>{now+=ms;}};
}
const messageIds=stream=>stream.events.filter(e=>e.event==='message').map(e=>e.data.id);

test('queue sends create atomic offers for every subscriber including their sender, without broadcast acks',async t=>{
  const f=await fixture(t),stream=await f.stream('bob','bob','?replay=1'),sent=await f.post();
  await stream.wait(es=>es.some(e=>e.data.id===sent.id));
  assert.equal(stream.events.find(e=>e.event==='message').data.claim_generation,0);
  for(const user of ['alice','bob','eve'])assert.ok((await f.call('/inbox',undefined,user,user+'-default')).data.messages.some(m=>m.id===sent.id&&m.claim_generation===0));
  assert.equal(f.item(sent.id).state,'ready');assert.equal(f.item(sent.id).generation,0);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_acks').n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM delivery_targets').n,0);
  f.store.db.exec("CREATE TEMP TRIGGER fail_enqueue BEFORE INSERT ON queue_items BEGIN SELECT RAISE(ABORT,'injected'); END");
  const before=f.store.get('SELECT count(*) n FROM messages').n;
  assert.equal((await f.call('/send',{to:'topic:'+f.topic,body:'rollback'},'alice')).status,500);
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n,before);
  assert.equal(f.store.get('SELECT count(*) n FROM threads').n,1);
});

test('regression guard: three competing consumers record one accepted claim and completion for each of twenty items',async t=>{
  // One connection serializes requests. BEGIN IMMEDIATE guards the read/transition;
  // the separate-process test below also exercises independent SQLite connections.
  const f=await fixture(t),consumers=[['bob','b1'],['bob','b2'],['eve','e']];
  for(let n=0;n<20;n++) {
    const sent=await f.post('item '+n),results=await Promise.all(consumers.map(([u,s])=>f.call('/queue/claim',{id:sent.id},u,s)));
    assert.deepEqual(results.map(r=>r.status).sort(),[200,409,409]);
    for(const r of results.filter(r=>r.status===409))assert.equal(r.data.error,'ITEM_CLAIMED');
    const winner=results.findIndex(r=>r.status===200),generation=results[winner].data.generation;assert.equal(generation,1);
    const completed=await Promise.all(consumers.map(([u,s])=>f.call('/queue/done',{id:sent.id,generation,result:'winner '+winner},u,s)));
    assert.deepEqual(completed.map(r=>r.status).sort(),[200,409,409]);assert.equal(completed[winner].data.changed,true);
    for(const r of completed.filter(r=>r.status===409))assert.equal(r.data.error,'CLAIM_STALE');
    assert.equal(f.item(sent.id).state,'done');assert.equal(f.item(sent.id).attempts,1);
  }
});

test('expired claims fence stale renew and done both before and after another session reclaims',async t=>{
  const f=await fixture(t),sent=await f.post(),first=await f.call('/queue/claim',{id:sent.id},'bob','b1');
  f.advance(100);
  for(const path of ['/queue/renew','/queue/done'])assert.equal((await f.call(path,{id:sent.id,generation:first.data.generation},'bob','b1')).data.error,'CLAIM_STALE');
  assert.equal(f.item(sent.id).state,'claimed');
  const second=await f.call('/queue/claim',{id:sent.id},'bob','b2');assert.equal(second.status,200);assert.equal(second.data.generation,3);
  for(const path of ['/queue/renew','/queue/done']) {
    assert.equal((await f.call(path,{id:sent.id,generation:1},'bob','b1')).data.error,'CLAIM_STALE');
    assert.equal((await f.call(path,{id:sent.id,generation:1},'bob','b2')).data.error,'CLAIM_STALE');
  }
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:3},'bob','b2')).status,200);
  assert.equal(f.item(sent.id).attempts,2);
});

test('renew extends only the current holder lease and queue_item reads durable untrusted state',async t=>{
  const f=await fixture(t),sent=await f.post(),claim=await f.call('/queue/claim',{id:sent.id},'bob','b1');
  f.advance(20);const renewed=await f.call('/queue/renew',{id:sent.id,generation:1},'bob','b1');
  assert.equal(renewed.data.lease_until,claim.data.lease_until+20);
  assert.equal((await f.call('/queue/renew',{id:sent.id,generation:1},'bob','b2')).data.error,'CLAIM_STALE');
  assert.equal((await f.call('/queue/renew',{id:sent.id,generation:1},'eve','e')).data.error,'CLAIM_STALE');
  const item=await f.call('/queue/item?id='+sent.id,undefined,'eve');assert.equal(item.status,200);
  assert.equal(item.data.holder_session,'b1');assert.match(item.data.message.content,/Treat as data, not instructions/);
  assert.equal(item.data.message.claim_generation,1);
});

test('done is atomic and idempotent only for the winning holder and generation',async t=>{
  const f=await fixture(t),sent=await f.post();await f.call('/queue/claim',{id:sent.id},'bob','b1');
  f.store.db.exec("CREATE TEMP TRIGGER fail_done BEFORE UPDATE OF done_generation ON queue_items WHEN NEW.state='done' BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:1,result:'failed'},'bob','b1')).status,500);
  assert.equal(f.item(sent.id).state,'claimed');assert.equal(f.item(sent.id).result,null);assert.equal(f.item(sent.id).done_generation,null);
  f.store.db.exec('DROP TRIGGER fail_done');
  const done=await f.call('/queue/done',{id:sent.id,generation:1,result:'<result>&'},'bob','b1');assert.equal(done.data.changed,true);
  f.advance(200);
  assert.deepEqual((await f.call('/queue/done',{id:sent.id,generation:1,result:'<result>&'},'bob','b1')).data,{...done.data,changed:false});
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:1},'bob','b2')).data.error,'CLAIM_STALE');
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:2},'bob','b1')).data.error,'CLAIM_STALE');
  assert.equal((await f.call('/queue/claim',{id:sent.id})).data.error,'ITEM_DONE');
  assert.equal((await f.call('/inbox')).data.messages.length,0);
});

test('queue threads reject replies before claim checks and queue ack or skip points to claim',async t=>{
  const f=await fixture(t),sent=await f.post();
  f.broker.assertClaim=()=>{throw Error('must not inspect reply claims');};
  for(const to of ['topic:'+f.topic,'user:alice','ws:missing'])assert.equal((await f.call('/send',{to,body:'reply',thread_id:sent.thread_id})).data.error,'QUEUE_THREAD_READ_ONLY');
  assert.equal((await f.call('/ack',{id:sent.id})).data.error,'USE_CLAIM');
  assert.equal((await f.call('/topics/skip',{topic:f.topic})).data.error,'USE_CLAIM');
  assert.equal(f.item(sent.id).state,'ready');
});

test('claims notify other subscriber sessions and only the holder receives a claimed item on replay',async t=>{
  const f=await fixture(t),stream=await f.stream('eve','e','?replay=1'),sent=await f.post();await stream.wait(es=>es.some(e=>e.data.id===sent.id));
  const claimed=await f.call('/queue/claim',{id:sent.id},'bob','b1');assert.equal(claimed.status,200);
  await stream.wait(es=>es.some(e=>e.event==='claimed'&&e.data.id===sent.id&&e.data.claim_generation===1));
  assert.equal((await f.call('/inbox',undefined,'eve','e')).data.messages.length,0);
  assert.equal((await f.call('/inbox',undefined,'bob','b2')).data.messages.length,0);
  assert.deepEqual((await f.call('/inbox',undefined,'bob','b1')).data.messages.map(m=>[m.id,m.claim_generation]),[[sent.id,1]]);
  const message=f.store.get('SELECT * FROM messages WHERE id=?',sent.id);
  assert.equal(f.broker.pendingTopic(message,'eve','e'),false);
  assert.equal(f.broker.pendingTopic(message,'bob','b2'),false);
  assert.equal(f.broker.pendingTopic(message,'bob','b1'),true);
  const replay=await f.stream('bob','b1','?replay=1');await replay.wait(es=>es.some(e=>e.data.id===sent.id&&e.data.claim_generation===1));
});

test('topic leave releases and re-offers an older item on a live stream with a newer fence',async t=>{
  const f=await fixture(t,{sseHeartbeat:60000}),stream=await f.stream('eve','e','?replay=1'),sent=await f.post();await stream.wait(es=>es.some(e=>e.data.id===sent.id));
  await f.call('/queue/claim',{id:sent.id},'bob','b1');
  const marker=await f.post('newer ID');await stream.wait(es=>es.some(e=>e.data.id===marker.id));
  assert.equal((await f.call('/topics/leave',{name:f.topic},'bob','b1')).status,200);
  await stream.wait(es=>es.filter(e=>e.event==='message'&&e.data.id===sent.id).length===2);
  assert.equal(f.item(sent.id).state,'ready');assert.equal(f.item(sent.id).generation,2);
  await f.call('/topics/subscribe',{name:f.topic},'bob','b1');
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:1},'bob','b1')).data.error,'SESSION_ENDED');
  assert.equal((await f.call('/queue/claim',{id:sent.id},'eve','e')).data.generation,3);
  assert.equal(messageIds(stream).filter(id=>id===sent.id).length,2);
});

for(const operation of ['leave','finish','remove'])test('workspace '+operation+' releases claims atomically and re-offers to remaining subscribers',async t=>{
  const f=await fixture(t),stream=await f.stream('eve','e','?replay=1'),sent=await f.post();await stream.wait(es=>es.some(e=>e.data.id===sent.id));
  await f.call('/queue/claim',{id:sent.id},'bob','b1');
  const path=operation==='remove'?'/members/remove':'/members/'+operation,args=operation==='finish'?{summary:'finished'}:operation==='remove'?{user:'bob'}:{};
  f.store.db.exec("CREATE TEMP TRIGGER fail_leave BEFORE DELETE ON workspace_members WHEN OLD.user='bob' BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.equal((await f.call(path,args,operation==='remove'?'alice':'bob',operation==='remove'?'a':'b1')).status,500);
  assert.equal(f.item(sent.id).state,'claimed');assert.equal(f.item(sent.id).generation,1);
  f.store.db.exec('DROP TRIGGER fail_leave');assert.equal((await f.call(path,args,operation==='remove'?'alice':'bob',operation==='remove'?'a':'b1')).status,200);
  await stream.wait(es=>es.filter(e=>e.event==='message'&&e.data.id===sent.id).length===2);
  assert.equal(f.item(sent.id).generation,2);assert.equal(f.item(sent.id).holder_session,null);
});

test('queue access, count and release remain isolated for same-user same-topic workspaces',async t=>{
  const f=await fixture(t),local=await f.post();
  await f.call('/workspaces',{name:'other'},'alice');for(const user of ['bob','eve'])f.store.run("INSERT INTO workspace_members VALUES ('other',?,'member',1,'invite')",user);
  await f.create('other');const foreign=await f.post('other','other');
  assert.equal((await f.call('/queue/claim',{id:foreign.id})).data.error,'MESSAGE_NOT_FOUND');
  assert.equal((await f.call('/queue/item?id='+foreign.id)).data.error,'MESSAGE_NOT_FOUND');
  await f.call('/queue/claim',{id:local.id},'bob','bob-default');await f.call('/queue/claim',{id:foreign.id},'bob','bob-other','other');
  assert.deepEqual((await f.call('/inbox',undefined,'bob','bob-other','other')).data.messages.map(m=>m.id),[foreign.id]);
  assert.deepEqual((await f.call('/inbox',undefined,'bob','bob-default')).data.messages.map(m=>m.id),[local.id]);
  await f.call('/topics/leave',{name:f.topic},'bob','bob-other','other');
  assert.equal(f.item(foreign.id).state,'ready');assert.equal(f.item(local.id).state,'claimed');
});

test('queue operations reject invisible/non-queue IDs, invalid generations/results and unknown fields',async t=>{
  const f=await fixture(t,{maxMessageChars:10}),sent=await f.post(),ordinary=await f.call('/send',{to:'user:bob',body:'ordinary'},'alice');
  for(const [path,args,error] of [
    ['/queue/claim',{id:0},'INVALID_MESSAGE_ID'],['/queue/claim',{id:ordinary.data.id},'MESSAGE_NOT_FOUND'],
    ['/queue/claim',{id:sent.id,generation:1},'INVALID_QUEUE_FIELDS'],['/queue/renew',{id:sent.id,generation:0},'INVALID_GENERATION'],
    ['/queue/done',{id:sent.id,generation:'1'},'INVALID_GENERATION'],['/queue/done',{id:sent.id,generation:1.5},'INVALID_GENERATION'],
  ])assert.equal((await f.call(path,args)).data.error,error);
  await f.call('/queue/claim',{id:sent.id});
  for(const result of [123,null,'x'.repeat(11)])assert.equal((await f.call('/queue/done',{id:sent.id,generation:1,result})).data.error,'INVALID_RESULT');
  assert.equal(f.item(sent.id).state,'claimed');
  await f.call('/topics/leave',{name:f.topic},'eve');
  for(const path of ['/queue/claim','/queue/renew','/queue/done'])assert.equal((await f.call(path,{id:sent.id,...(path.endsWith('claim')?{}:{generation:1})},'eve')).data.error,'SESSION_ENDED');
  assert.equal((await f.call('/queue/item?id='+sent.id,undefined,'eve')).data.error,'SESSION_ENDED');
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:1,result:'x'.repeat(10)})).status,200);
  assert.equal(config({}).claimTtl,300000);assert.throws(()=>config({CLAIM_TTL:'0'}),e=>e.code==='INVALID_CLAIM_TTL');
});

async function processes(t) {
  const dir=testTempDir('queue-process-'),file=join(dir,'broker.sqlite'),tokenPath=join(dir,'tokens.json');
  const tokens=Object.fromEntries(['alice','bob','eve'].map(user=>[user,user+'-queue-fixture-token-00000000']));
  writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});new Store(file).close();const children=[];
  t.after(async()=>{for(const p of children){if(p.child.exitCode===null&&p.child.signalCode===null)p.child.kill('SIGTERM');await p.exited;}rmSync(dir,{recursive:true,force:true});});
  const start=async()=>{
    const child=spawn(process.execPath,['broker/main.mjs'],{env:{...process.env,BROKER_HOST:'127.0.0.1',BROKER_PORT:'0',BROKER_DB:file,BROKER_TOKENS:tokenPath,SSE_HEARTBEAT:'1'},stdio:['ignore','ignore','pipe']});
    const exited=new Promise(resolve=>child.once('exit',resolve));children.push({child,exited});let log='',port;child.stderr.on('data',chunk=>{log+=chunk;});
    await eventually(()=>{for(const line of log.split('\n')){try{const row=JSON.parse(line);if(row.event==='broker_listening')port=row.address.port;}catch{}}return Boolean(port);});
    return `http://127.0.0.1:${port}`;
  };
  const a=await start(),b=await start();
  let processTopic;
  const call=async(url,path,args,user='alice',session=user)=>{
    if(path==='/topics')processTopic=args.name;
    if(path==='/topics/subscribe'){const setup=new Store(file);try{setup.run('INSERT OR IGNORE INTO topic_subscriptions VALUES (?,?,?,?)','default',args.name,user,Date.now());}finally{setup.close();}}
    const res=await fetch(url+path,{method:'POST',headers:{Authorization:`Bearer ${tokens[user]}`,'X-Session-Id':session,...(user==='alice'?{}:{'X-Topic':processTopic}),'Content-Type':'application/json'},body:JSON.stringify(args)});return {status:res.status,data:await res.json()};
  };
  return {a,b,call,file,tokens};
}

test('regression guard: two broker processes serialize claims and accept one terminal result per item',async t=>{
  // Independent connections exercise SQLite's BEGIN IMMEDIATE exclusion of a competing writer.
  const f=await processes(t),topic='two/process';
  await f.call(f.a,'/topics',{name:topic,mode:'queue'});
  for(const user of ['alice','bob','eve'])await f.call(f.a,'/topics/subscribe',{name:topic},user);
  for(let n=0;n<10;n++) {
    const sent=await f.call(f.a,'/send',{to:'topic:'+topic,body:'process item '+n});assert.equal(sent.status,201);
    const actors=[[f.a,'bob','b1'],[f.b,'bob','b2'],[f.b,'eve','e']],claims=await Promise.all(actors.map(([url,u,s])=>f.call(url,'/queue/claim',{id:sent.data.id},u,s)));
    assert.deepEqual(claims.map(r=>r.status).sort(),[200,409,409]);
    const winner=claims.findIndex(r=>r.status===200),done=await Promise.all(actors.map(([url,u,s])=>f.call(url,'/queue/done',{id:sent.data.id,generation:1,result:'accepted '+winner},u,s)));
    assert.deepEqual(done.map(r=>r.status).sort(),[200,409,409]);assert.equal(done[winner].data.changed,true);
    const [url,u,s]=actors[winner];assert.equal((await f.call(url===f.a?f.b:f.a,'/queue/done',{id:sent.data.id,generation:1,result:'accepted '+winner},u,s)).data.changed,false);
  }
  const store=new Store(f.file);try{assert.equal(store.get("SELECT count(*) n FROM queue_items WHERE state='done' AND attempts=1 AND done_generation=1").n,10);}finally{store.close();}
});

test('release scopes preserve other topics, users and workspaces while fencing the departed holder',async t=>{
  const f=await fixture(t),first=await f.post(),other=await f.post('other holder');
  await f.call('/queue/claim',{id:first.id},'bob','b1');await f.call('/queue/claim',{id:other.id},'eve','e');
  const topic='jobs/second';await f.call('/topics',{name:topic,mode:'queue'},'alice');
  for(const user of ['alice','bob']){f.store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',topic,user,Date.now());await f.call('/topics/subscribe',{name:topic},user);}
  const second=(await f.call('/send',{to:'topic:'+topic,body:'second topic'},'alice')).data;
  await f.call('/queue/claim',{id:second.id},'bob','b1');
  await f.call('/topics/leave',{name:f.topic},'bob','b1');
  assert.equal(f.item(first.id).state,'ready');assert.equal(f.item(first.id).generation,2);
  assert.equal(f.item(other.id).state,'claimed');assert.equal(f.item(second.id).state,'claimed');
  await f.call('/members/leave',{},'bob','b1','default',topic);
  assert.equal(f.item(second.id).state,'ready');assert.equal(f.item(other.id).state,'claimed');
});

test('holder user and terminal done_generation are independently required by the completion contract',async t=>{
  const f=await fixture(t),sent=await f.post();await f.call('/queue/claim',{id:sent.id},'bob','b1');
  // A direct stored-row contract check: public identity binding normally makes
  // mismatched user/session ownership unreachable, masking a removed user guard.
  f.store.run("UPDATE queue_items SET holder_user='eve' WHERE message_id=?",sent.id);
  assert.equal(f.broker.pendingTopic(f.store.get('SELECT * FROM messages WHERE id=?',sent.id),'bob','b1'),false);
  assert.equal((await f.call('/inbox',undefined,'bob','b1')).data.messages.some(m=>m.id===sent.id),false);
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:1},'bob','b1')).data.error,'CLAIM_STALE');
  f.store.run("UPDATE queue_items SET holder_user='bob' WHERE message_id=?",sent.id);
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:1},'bob','b1')).status,200);
  f.store.run('UPDATE queue_items SET done_generation=0 WHERE message_id=?',sent.id);
  assert.equal((await f.call('/queue/done',{id:sent.id,generation:1},'bob','b1')).data.error,'CLAIM_STALE');
});


test('a release through another broker process re-offers the newer fence on an existing replay stream',async t=>{
  const f=await processes(t),topic='cross/release';
  await f.call(f.a,'/topics',{name:topic,mode:'queue'});for(const user of ['alice','bob','eve'])await f.call(f.a,'/topics/subscribe',{name:topic},user);
  const req=http.get(f.b+'/stream?replay=1',{headers:{Authorization:`Bearer ${f.tokens.eve}`,'X-Session-Id':'eve-stream','X-Topic':f.topic??topic}});t.after(()=>req.destroy());
  const res=await new Promise((resolve,reject)=>{req.once('response',resolve);req.once('error',reject);});assert.equal(res.statusCode,200);
  const events=[];let buffer='';res.setEncoding('utf8');res.on('error',()=>{});res.on('data',chunk=>{
    buffer+=chunk;let end;while((end=buffer.indexOf('\n\n'))>=0){const frame=buffer.slice(0,end);buffer=buffer.slice(end+2);const data=frame.split('\n').find(l=>l.startsWith('data: '));if(data&&frame.includes('event: message'))events.push(JSON.parse(data.slice(6)));}
  });
  const sent=await f.call(f.a,'/send',{to:'topic:'+topic,body:'old offer'});await eventually(()=>events.some(e=>e.id===sent.data.id));
  await f.call(f.a,'/queue/claim',{id:sent.data.id},'bob','b1');
  const marker=await f.call(f.a,'/send',{to:'topic:'+topic,body:'newer ID'});await eventually(()=>events.some(e=>e.id===marker.data.id));
  await f.call(f.a,'/topics/leave',{name:topic},'bob','b1');
  await eventually(()=>events.some(e=>e.id===sent.data.id&&e.claim_generation===2));
  assert.deepEqual(events.filter(e=>e.id===sent.data.id).map(e=>e.claim_generation),[0,2]);
});

test('queue schema reserves dead state for M8b without offering it or implementing a retry job',async t=>{
  const f=await fixture(t),sent=await f.post();
  f.store.run("UPDATE queue_items SET state='dead' WHERE message_id=?",sent.id);
  assert.equal((await f.call('/queue/claim',{id:sent.id})).data.error,'ITEM_DONE');
  assert.equal((await f.call('/inbox')).data.messages.length,0);
  assert.throws(()=>f.store.run("UPDATE queue_items SET state='unknown' WHERE message_id=?",sent.id),/CHECK/);
  f.store.run("UPDATE queue_items SET state='ready' WHERE message_id=?",sent.id);
  assert.equal((await f.call('/queue/claim',{id:sent.id})).status,200);
});

for(const replay of [false,true])test(`finish re-offers to the pumping owner's ${replay?'replay':'non-replay'} stream before a long heartbeat`,async t=>{
  const f=await fixture(t,{sseHeartbeat:60000}),stream=await f.stream('alice','alice-default',replay?'?replay=1':'');
  const sent=await f.post('owner offer'),other=await f.post('second owner offer');
  for(const id of [sent.id,other.id]) {
    await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===id));
    await f.call('/queue/claim',{id},'bob','b1');await stream.wait(es=>es.some(e=>e.event==='claimed'&&e.data.id===id));
  }
  assert.equal((await f.call('/members/finish',{summary:'summary starts owner pump'},'bob','b1')).status,200);
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.from_user==='bob') && [sent.id,other.id].every(id=>es.filter(e=>e.event==='message'&&e.data.id===id).length===2),1000);
  for(const id of [sent.id,other.id])assert.deepEqual(stream.events.filter(e=>e.event==='message'&&e.data.id===id).map(e=>e.data.claim_generation),[0,2]);
  assert.equal(stream.heartbeats,0);assert.equal(f.item(sent.id).state,'ready');
});

test('decision 72: repeated completion requires an identical result or omission both times',async t=>{
  const f=await fixture(t);
  for(const value of [undefined,'stored result','']) {
    const sent=await f.post(),args={id:sent.id,generation:1,...(value===undefined?{}:{result:value})};
    await f.call('/queue/claim',{id:sent.id});assert.equal((await f.call('/queue/done',args)).data.changed,true);
    const before={...f.item(sent.id)};f.advance(200);
    assert.deepEqual((await f.call('/queue/done',args)).data,{id:sent.id,generation:1,changed:false,result:value??null});
    for(const result of ['different',...(value===undefined?['']:[undefined])]) {
      const repeated=await f.call('/queue/done',{id:sent.id,generation:1,...(result===undefined?{}:{result})});
      assert.equal(repeated.status,409);assert.equal(repeated.data.error,'DONE_RESULT_MISMATCH');
      assert.deepEqual({...f.item(sent.id)},before);
    }
  }
});

test('decision 73: blocking queue sends are refused without creating work, while nonblocking and broadcast sends work',async t=>{
  const f=await fixture(t),counts=()=>Object.fromEntries(['messages','threads','queue_items','workspace_audit','webhook_outbox'].map(table=>[table,f.store.get(`SELECT count(*) n FROM ${table}`).n]));
  const before=counts();
  for(const kind of ['question','blocker']) {
    const blocked=await f.call('/send',{to:'topic:'+f.topic,body:'cannot answer a read-only thread',kind,blocking:true},'alice');
    assert.equal(blocked.status,409);assert.equal(blocked.data.error,'QUEUE_BLOCKING_UNSUPPORTED');assert.deepEqual(counts(),before);
  }
  assert.equal((await f.call('/send',{to:'topic:'+f.topic,body:'queue work',kind:'question',blocking:false},'alice')).status,201);
  await f.call('/topics',{name:'broadcast/control'},'alice');await f.call('/topics/subscribe',{name:'broadcast/control'},'alice');
  assert.equal((await f.call('/send',{to:'topic:broadcast/control',body:'answerable',kind:'question',blocking:true},'alice')).status,201);
});
