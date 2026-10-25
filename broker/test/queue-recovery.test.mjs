import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { fork } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { Jobs } from '../jobs.mjs';
import { expireClaims } from '../queue.mjs';
import { config } from '../config.mjs';
import { httpFixture, eventually } from './http.mjs';
import { testTempDir } from '../../test-support/temp.mjs';

async function fixture(t,overrides={}) {
  const store=new Store(':memory:');let clock=1000;const logs=[],received=[];
  const hook=http.createServer((req,res)=>{let data='';req.on('data',chunk=>{data+=chunk;});req.on('end',()=>{received.push({payload:JSON.parse(data),key:req.headers['idempotency-key']});res.end();});});
  await new Promise(resolve=>hook.listen(0,'127.0.0.1',resolve));
  const broker=new Broker(store,{...config({}),claimTtl:100,maxAttempts:2,sseHeartbeat:60000,webhookUrl:`http://127.0.0.1:${hook.address().port}`,...overrides},['alice','bob','eve'],{now:()=>clock,log:value=>logs.push(JSON.parse(value))});
  const jobs=new Jobs(broker),wire=await httpFixture(t,broker,{workspaceAdminSessions:true,topic:(user,session,workspace)=>user===store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user?null:'recover/Build'}),topic='recover/Build';
  t.after(async()=>{await jobs.stop();hook.closeAllConnections();await new Promise(resolve=>hook.close(resolve));store.close();});
  const call=async(path,args,user='eve',session=user,workspace='default')=>{
    const row=args?.id?store.get('SELECT topic,workspace FROM messages WHERE id=?',args.id):null;const target=args?.topic??(path.startsWith('/topics/')?args?.name:undefined)??(path.includes('?topic=')?new URL(path,'http://fixture').searchParams.get('topic'):undefined)??(row?.workspace===workspace?row.topic:undefined)??topic;
    const owner=store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user;const binding=user===owner?null:target;
    if(binding&&binding!==topic)session+='-'+binding.replaceAll('/','_');
    if(path==='/send'&&args?.to&&!args.to.startsWith('topic:'))args={...args,topic:target};
    const res=await wire.request(path,{user,session,headers:{'X-Workspace':workspace,...(binding?{'X-Topic':binding}:{})},...(args===undefined?{}:{method:'POST',body:JSON.stringify(args)})});return {status:res.status,data:await res.json()};
  };
  await call('/topics',{name:topic,mode:'queue'},'alice');store.run('UPDATE topics SET owner_user=? WHERE workspace=? AND name=?','bob','default',topic);
  for(const user of ['alice','bob','eve']){store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',topic,user,clock);await call('/topics/subscribe',{name:topic},user);}
  const send=async()=>{const sent=await call('/send',{to:'topic:'+topic,body:'retained work'},'alice');assert.equal(sent.status,201);return sent.data.id;};
  const row=id=>store.get('SELECT * FROM queue_items WHERE message_id=?',id);
  const advance=()=>{clock+=100;};
  const exhaust=async(id,{lazy=false}={})=>{
    for(let n=0;n<broker.config.maxAttempts;n++) {
      const claimed=await call('/queue/claim',{id});assert.equal(claimed.status,200);advance();
      if(n<broker.config.maxAttempts-1){jobs.tick();await jobs.sending;assert.equal(row(id).state,'ready');}
    }
    if(lazy){const rejected=await call('/queue/claim',{id},'alice');assert.equal(rejected.status,409);assert.equal(rejected.data.error,'ITEM_DONE');}
    else{jobs.tick();await jobs.sending;}
    assert.equal(row(id).state,'dead');
  };
  return {...wire,broker,store,jobs,logs,received,topic,call,send,row,advance,exhaust};
}

test('the expiry job alone re-offers a newer generation before a long heartbeat and fences the expired holder',async t=>{
  const f=await fixture(t),id=await f.send(),stream=await f.stream('alice','a','?replay=1');
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===id));
  const claim=await f.call('/queue/claim',{id});assert.equal(claim.data.generation,1);f.advance();
  f.jobs.tick();await f.jobs.sending;
  assert.equal(f.row(id).state,'ready');assert.equal(f.row(id).generation,2);
  await stream.wait(es=>es.filter(e=>e.event==='message'&&e.data.id===id).length===2,1000);assert.equal(stream.heartbeats,0);
  for(const path of ['/queue/done','/queue/renew'])assert.equal((await f.call(path,{id,generation:1})).data.error,'CLAIM_STALE');
  assert.equal((await f.call('/queue/claim',{id},'bob')).data.generation,3);
});

test('MAX_ATTEMPTS expiries dead-letter through both the job and a committed lazy claim',async t=>{
  const f=await fixture(t),job=await f.send(),lazy=await f.send(),owner=await f.stream('bob','owner','?replay=1');
  await owner.wait(es=>es.some(e=>e.event==='message'&&e.data.id===lazy));
  for(const id of [job,lazy]) {
    await f.exhaust(id,{lazy:id===lazy});
    const outbox=f.store.all('SELECT * FROM webhook_outbox').find(r=>JSON.parse(r.payload).queue_message_id===id);assert.ok(outbox);
    const payload=JSON.parse(outbox.payload);assert.equal(payload.event,'queue_dead_letter');assert.equal(payload.workspace,'default');assert.equal(payload.topic,f.topic);assert.equal(payload.attempts,2);assert.deepEqual(Object.keys(payload).sort(),['attempts','event','event_id','message_id','queue_message_id','topic','workspace']);
    assert.equal(payload.message_id,outbox.message_id);assert.notEqual(outbox.message_id,id);
    const message=f.store.get('SELECT * FROM messages WHERE id=?',outbox.message_id);assert.equal(message.kind,'notice');
    // Wait before another tick can replay a missed publish and mask either path.
    await owner.wait(es=>es.some(e=>e.event==='message'&&e.data.id===message.id&&e.data.kind==='notice'),1000);
    assert.equal(owner.heartbeats,0);
    assert.deepEqual(f.store.all('SELECT user FROM escalation_targets WHERE message_id=? ORDER BY user',outbox.message_id).map(r=>r.user),['alice','bob']);
    assert.equal(f.row(id).attempts,2);
  }
  const audits=f.store.all("SELECT * FROM workspace_audit WHERE action='queue:dead_letter'");assert.equal(audits.length,2);
  f.jobs.tick();await f.jobs.sending;assert.equal(f.received.length,2);
  for(const event of f.received)assert.equal(event.key,event.payload.event_id);
  for(const log of f.logs.filter(l=>l.event==='queue_dead_letter'))assert.deepEqual(Object.keys(log).sort(),['attempts','event','message_id']);
  f.jobs.tick();await f.jobs.sending;assert.equal(f.store.all("SELECT * FROM workspace_audit WHERE action='queue:dead_letter'").length,2);
});

for(const mode of ['ready','dead'])test(`expiry ${mode} transitions are bounded per pass and continue on later ticks`,async t=>{
  const f=await fixture(t,{maxAttempts:mode==='dead'?1:2,queueExpiryBatch:2}),ids=[];
  for(let n=0;n<5;n++){const id=await f.send();ids.push(id);assert.equal((await f.call('/queue/claim',{id})).status,200);}
  f.advance();
  for(const expected of [2,4,5]) {
    f.jobs.tick();await f.jobs.sending;
    assert.equal(ids.filter(id=>f.row(id).state===mode).length,expected);
    assert.equal(ids.filter(id=>f.row(id).state==='claimed').length,5-expected);
  }
  if(mode==='dead')assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:dead_letter'").n,5);
});

test('owner requeue lazily expires a final lease and commits dead-letter plus reset without waiting for a sweep',async t=>{
  const f=await fixture(t,{maxAttempts:1}),id=await f.send(),stream=await f.stream('alice','lazy-owner','?replay=1');
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===id));await f.call('/queue/claim',{id});f.advance();
  const before=f.row(id);assert.equal((await f.call('/queue/requeue',{id})).data.error,'NOT_TOPIC_OWNER');assert.deepEqual(f.row(id),before);
  const response=await f.call('/queue/requeue',{id},'bob');assert.equal(response.status,200);
  assert.equal(response.data.generation,2);assert.equal(f.row(id).attempts,0);assert.equal(f.row(id).state,'ready');
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:dead_letter'").n,1);
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:requeue'").n,1);
  assert.equal(f.store.get('SELECT count(*) n FROM webhook_outbox').n,1);
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===id&&e.data.claim_generation===2),1000);assert.equal(stream.heartbeats,0);
  for(const path of ['/queue/done','/queue/renew'])assert.equal((await f.call(path,{id,generation:1})).data.error,'CLAIM_STALE');
  assert.equal((await f.call('/queue/requeue',{id},'bob')).data.error,'ITEM_NOT_DEAD');
  const claim=await f.call('/queue/claim',{id});assert.equal(claim.data.generation,3);assert.equal((await f.call('/queue/done',{id,generation:3})).status,200);
});

test('requeue never resets a live lease or an expired claim still below its attempt budget',async t=>{
  const f=await fixture(t),id=await f.send();await f.call('/queue/claim',{id});const before=f.row(id);
  assert.equal((await f.call('/queue/requeue',{id},'bob')).data.error,'ITEM_NOT_DEAD');assert.deepEqual(f.row(id),before);
  f.advance();assert.equal((await f.call('/queue/requeue',{id},'bob')).data.error,'ITEM_NOT_DEAD');assert.deepEqual(f.row(id),before);
  assert.equal(config({}).queueExpiryBatch,100);assert.throws(()=>config({QUEUE_EXPIRY_BATCH:'0'}),{code:'INVALID_QUEUE_EXPIRY_BATCH'});
});

test('an owner cannot requeue a live final attempt and its holder can still complete it',async t=>{
  const f=await fixture(t,{maxAttempts:1}),id=await f.send(),claim=await f.call('/queue/claim',{id});const before=f.row(id);
  const refused=await f.call('/queue/requeue',{id},'bob');assert.equal(refused.status,409);assert.equal(refused.data.error,'ITEM_NOT_DEAD');
  assert.deepEqual(f.row(id),before);assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:requeue'").n,0);
  const done=await f.call('/queue/done',{id,generation:claim.data.generation});assert.equal(done.status,200);assert.equal(done.data.changed,true);assert.equal(f.row(id).state,'done');
});

test('lazy expiry and owner requeue roll back dead state, notice and outbox if the reset audit fails',async t=>{
  const f=await fixture(t,{maxAttempts:1}),id=await f.send();await f.call('/queue/claim',{id});f.advance();const before=f.row(id),messages=f.store.get('SELECT count(*) n FROM messages').n;
  f.store.db.exec("CREATE TEMP TRIGGER fail_lazy_reset BEFORE INSERT ON workspace_audit WHEN NEW.action='queue:requeue' BEGIN SELECT RAISE(ABORT,'lazy-reset-fault'); END");
  assert.equal((await f.call('/queue/requeue',{id},'bob')).status,500);
  assert.deepEqual(f.row(id),before);assert.equal(f.store.get('SELECT count(*) n FROM messages').n,messages);assert.equal(f.store.get('SELECT count(*) n FROM webhook_outbox').n,0);
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:dead_letter'").n,0);
});

test('a second dead-letter after requeue has a distinct notice/outbox key, fresh budget and newer fence',async t=>{
  const f=await fixture(t,{maxAttempts:1}),id=await f.send(),stream=await f.stream('eve','e-stream','?replay=1');
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===id));await f.exhaust(id);
  const first=f.store.get('SELECT * FROM webhook_outbox'),oldGeneration=f.row(id).generation;
  const requeued=await f.call('/queue/requeue',{id},'bob');assert.equal(requeued.status,200);assert.equal(requeued.data.generation,oldGeneration+1);assert.equal(requeued.data.attempts,0);
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===id&&e.data.claim_generation===oldGeneration+1),1000);assert.equal(stream.heartbeats,0);
  await f.exhaust(id);const rows=f.store.all('SELECT * FROM webhook_outbox ORDER BY message_id');assert.equal(rows.length,2);assert.notEqual(rows[1].message_id,first.message_id);
  assert.equal(f.row(id).state,'dead');assert.equal(f.row(id).attempts,1);assert.equal(f.row(id).generation,oldGeneration+2);
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:requeue'").n,1);
});

test('dead-letter operations allow unsubscribed current topic/workspace owners only and preserve scoped queues',async t=>{
  const f=await fixture(t,{maxAttempts:1}),id=await f.send();await f.exhaust(id);
  await f.call('/topics/leave',{name:f.topic},'bob');await f.call('/topics/leave',{name:f.topic},'alice');
  assert.deepEqual((await f.call('/queue/dead-letters?topic='+encodeURIComponent(f.topic),undefined,'alice')).data.items.map(r=>r.id),[id]);
  assert.equal((await f.call('/queue/dead-letters?topic='+encodeURIComponent(f.topic),undefined,'bob')).data.error,'SESSION_ENDED');
  for(const path of ['/queue/dead-letters?topic='+encodeURIComponent(f.topic),'/queue/requeue'])assert.equal((await f.call(path,path.includes('requeue')?{id}:undefined,'eve')).data.error,'NOT_TOPIC_OWNER');
  await f.call('/workspaces',{name:'other'},'eve');
  assert.equal((await f.call('/queue/requeue',{id},'eve')).data.error,'NOT_TOPIC_OWNER');
  f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
  await f.call('/topics',{name:f.topic,mode:'queue'},'eve','eve-other','other');
  f.store.run("UPDATE topics SET owner_user='bob' WHERE workspace='other'");f.store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','other',f.topic,'bob',Date.now());
  assert.deepEqual((await f.call('/queue/dead-letters?topic='+encodeURIComponent(f.topic),undefined,'bob','bob-other','other')).data.items,[]);
  assert.equal((await f.call('/queue/requeue',{id},'bob','bob-other','other')).data.error,'MESSAGE_NOT_FOUND');
  const result=await f.call('/queue/requeue',{id},'alice');assert.equal(result.status,200);
  const audit=f.store.get("SELECT * FROM workspace_audit WHERE action='queue:requeue'");assert.equal(audit.actor_user,'alice');assert.equal(audit.workspace,'default');assert.equal(JSON.parse(audit.detail).message_id,id);
  assert.equal((await f.call('/queue/requeue',{id},'alice')).data.error,'ITEM_NOT_DEAD');
});

test('a failed dead-letter outbox insert rolls back the state, audit and notice; a failed requeue audit rolls back its reset',async t=>{
  const f=await fixture(t,{maxAttempts:1}),id=await f.send();await f.call('/queue/claim',{id});f.advance();
  const messages=f.store.get('SELECT count(*) n FROM messages').n;
  f.store.db.exec("CREATE TEMP TRIGGER fail_outbox BEFORE INSERT ON webhook_outbox BEGIN SELECT RAISE(ABORT,'injected'); END");
  f.jobs.tick();await f.jobs.sending;assert.equal(f.row(id).state,'claimed');assert.equal(f.store.get('SELECT count(*) n FROM messages').n,messages);assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:dead_letter'").n,0);
  assert.ok(f.logs.some(l=>l.event==='queue_maintenance_failed'));
  f.store.db.exec('DROP TRIGGER fail_outbox');f.jobs.tick();await f.jobs.sending;assert.equal(f.row(id).state,'dead');
  const before={...f.row(id)};f.store.db.exec("CREATE TEMP TRIGGER fail_requeue BEFORE INSERT ON workspace_audit WHEN NEW.action='queue:requeue' BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.equal((await f.call('/queue/requeue',{id},'bob')).status,500);assert.deepEqual({...f.row(id)},before);
});

test('thread and queue maintenance fail independently in either direction',async t=>{
  const f=await fixture(t),id=await f.send();await f.call('/queue/claim',{id});f.advance();
  const expire=f.broker.expireThreads.bind(f.broker);f.broker.expireThreads=()=>{throw Error('injected');};f.jobs.tick();await f.jobs.sending;
  assert.equal(f.row(id).state,'ready');assert.ok(f.logs.some(l=>l.event==='maintenance_failed'));f.broker.expireThreads=expire;
  await f.call('/queue/claim',{id});f.advance();
  const ordinary=await f.call('/send',{to:'user:bob',kind:'question',blocking:true,body:'thread expiry'},'alice');
  f.store.run('UPDATE messages SET created=0 WHERE id=?',ordinary.data.id);f.broker.config.blockingTimeout=1;
  f.store.db.exec("CREATE TEMP TRIGGER fail_queue BEFORE UPDATE ON queue_items BEGIN SELECT RAISE(ABORT,'injected'); END");
  f.jobs.tick();await f.jobs.sending;
  assert.equal(f.store.get('SELECT status FROM threads WHERE id=?',ordinary.data.thread_id).status,'escalated');assert.equal(f.row(id).state,'claimed');assert.ok(f.logs.some(l=>l.event==='queue_maintenance_failed'));
});

test('recovery ignores live, ready, completed and dead rows and validates budgets and owner inputs',async t=>{
  const f=await fixture(t),id=await f.send();assert.deepEqual(expireClaims(f.broker),{ready:0,dead:0});
  await f.call('/queue/claim',{id});assert.deepEqual(expireClaims(f.broker),{ready:0,dead:0});
  await f.call('/queue/done',{id,generation:1});f.advance();assert.deepEqual(expireClaims(f.broker),{ready:0,dead:0});
  assert.equal((await f.call('/queue/requeue',{id},'bob')).data.error,'ITEM_NOT_DEAD');
  assert.deepEqual((await f.call('/queue/dead-letters?topic='+encodeURIComponent(f.topic),undefined,'bob')).data.items,[]);
  await f.call('/topics',{name:'broadcast/control'},'alice');
  assert.equal((await f.call('/queue/dead-letters?topic=broadcast%2Fcontrol',undefined,'alice')).data.error,'NOT_QUEUE_TOPIC');
  assert.equal((await f.call('/queue/requeue',{id:0},'bob')).data.error,'INVALID_MESSAGE_ID');
  assert.equal((await f.call('/queue/requeue',{id,user:'bob'},'bob')).data.error,'INVALID_QUEUE_FIELDS');
  assert.equal((await f.call('/queue/dead-letters?topic='+encodeURIComponent(f.topic)+'&user=bob',undefined,'bob')).data.error,'INVALID_QUEUE_FIELDS');
  assert.equal(config({}).maxAttempts,3);assert.throws(()=>config({MAX_ATTEMPTS:'0'}),e=>e.code==='INVALID_MAX_ATTEMPTS');
});

async function processes(t) {
  const dir=testTempDir('queue-expiry-process-'),file=join(dir,'broker.sqlite'),tokenPath=join(dir,'tokens.json');
  const tokens=Object.fromEntries(['alice','bob','eve'].map(user=>[user,user+'-recovery-fixture-token-00000000']));writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});
  const children=[];t.after(async()=>{for(const p of children){if(p.child.exitCode===null&&p.child.signalCode===null)p.child.kill('SIGKILL');await p.exited;}rmSync(dir,{recursive:true,force:true});});
  const start=async()=>{
    // Without TMPDIR the child falls back to this test's own scratch dir, so its
    // temp files stay inside the test directory on any machine.
    const child=fork(fileURLToPath(new URL('../main.mjs',import.meta.url)),[],{execArgv:[],env:{TMPDIR:process.env.TMPDIR??dir,BROKER_HOST:'127.0.0.1',BROKER_PORT:'0',BROKER_DB:file,BROKER_TOKENS:tokenPath,
      BROKER_WEBHOOK_URL:'http://127.0.0.1:1',CLAIM_TTL:'60',MAX_ATTEMPTS:'1',MAINTENANCE_INTERVAL_MS:'60000',SSE_HEARTBEAT:'1'},stdio:['ignore','ignore','pipe','ipc']});
    const exited=new Promise(resolve=>child.once('exit',resolve));children.push({child,exited});let log='',port;child.stderr.on('data',chunk=>{log+=chunk;});
    await eventually(()=>{for(const line of log.split('\n')){try{const value=JSON.parse(line);if(value.event==='broker_listening')port=value.address.port;}catch{}}return Boolean(port);});return `http://127.0.0.1:${port}`;
  };
  const a=await start(),b=await start(),topic='multi/Recover';
  let processTopic;
  const call=async(url,path,args,user='alice',session=user)=>{
    if(path==='/topics')processTopic=args.name;
    if(path==='/topics/subscribe'){const setup=new Store(file);try{setup.run('INSERT OR IGNORE INTO topic_subscriptions VALUES (?,?,?,?)','default',args.name,user,Date.now());}finally{setup.close();}}
    const res=await fetch(url+path,{method:args===undefined?'GET':'POST',headers:{Authorization:`Bearer ${tokens[user]}`,'X-Session-Id':session,...(user==='alice'?{}:{'X-Topic':processTopic}),'Content-Type':'application/json'},...(args===undefined?{}:{body:JSON.stringify(args)})});return {status:res.status,data:await res.json()};
  };
  await call(a,'/topics',{name:topic,mode:'queue'},'alice');const setup=new Store(file);try{setup.run('UPDATE topics SET owner_user=? WHERE name=?','bob',topic);}finally{setup.close();}for(const user of ['alice','bob','eve'])await call(a,'/topics/subscribe',{name:topic},user);
  return {a,b,file,tokens,topic,call};
}

test('regression guard: two broker processes lazily exhaust one item into exactly one committed notice and outbox',async t=>{
  // BEGIN IMMEDIATE excludes an independent writer between expiry and commit.
  const f=await processes(t),id=(await f.call(f.a,'/send',{to:'topic:'+f.topic,body:'exhaust once'})).data.id;
  await f.call(f.a,'/queue/claim',{id},'eve');
  const store=new Store(f.file);t.after(()=>store.close());store.run('UPDATE queue_items SET lease_until=0 WHERE message_id=?',id);
  const results=await Promise.all([f.call(f.a,'/queue/claim',{id},'alice','a1'),f.call(f.b,'/queue/claim',{id},'bob','b1')]);
  for(const result of results){assert.equal(result.status,409);assert.equal(result.data.error,'ITEM_DONE');}
  assert.equal(store.get('SELECT state FROM queue_items WHERE message_id=?',id).state,'dead');
  assert.equal(store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:dead_letter'").n,1);
  assert.equal(store.get("SELECT count(*) n FROM messages WHERE kind='notice'").n,1);
  const outbox=store.all('SELECT * FROM webhook_outbox');assert.equal(outbox.length,1);
  assert.equal(JSON.parse(outbox[0].payload).queue_message_id,id);assert.notEqual(outbox[0].message_id,id);
  assert.deepEqual(store.all('SELECT user FROM escalation_targets WHERE message_id=? ORDER BY user',outbox[0].message_id).map(r=>r.user),['alice','bob']);
});

test('requeue re-pushes its newer generation to an existing remote-process replay stream',async t=>{
  const f=await processes(t),req=http.get(f.b+'/stream?replay=1',{headers:{Authorization:`Bearer ${f.tokens.eve}`,'X-Session-Id':'eve-stream','X-Topic':f.topic??topic}});t.after(()=>req.destroy());
  const res=await new Promise((resolve,reject)=>{req.once('response',resolve);req.once('error',reject);});assert.equal(res.statusCode,200);
  const frames=[];let buffer='';res.setEncoding('utf8');res.on('error',()=>{});res.on('data',chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf('\n\n'))>=0){const frame=buffer.slice(0,end);buffer=buffer.slice(end+2);const data=frame.split('\n').find(l=>l.startsWith('data: '));if(data&&frame.includes('event: message'))frames.push(JSON.parse(data.slice(6)));}});
  const id=(await f.call(f.a,'/send',{to:'topic:'+f.topic,body:'remote requeue'})).data.id;await eventually(()=>frames.some(m=>m.id===id));
  await f.call(f.a,'/queue/claim',{id},'bob');
  const store=new Store(f.file);try{store.run('UPDATE queue_items SET lease_until=0 WHERE message_id=?',id);}finally{store.close();}
  assert.equal((await f.call(f.a,'/queue/claim',{id},'alice')).data.error,'ITEM_DONE');
  assert.equal((await f.call(f.a,'/queue/requeue',{id},'alice')).data.generation,2);
  await eventually(()=>frames.some(m=>m.id===id&&m.claim_generation===2));
  assert.deepEqual(frames.filter(m=>m.id===id).map(m=>m.claim_generation),[0,2]);
});

test('dead-letter listing filters the requested topic independently of its workspace',async t=>{
  const f=await fixture(t,{maxAttempts:1}),first=await f.send();await f.exhaust(first);
  const topic='recover/Other';await f.call('/topics',{name:topic,mode:'queue'},'alice');f.store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',topic,'eve',Date.now());for(const user of ['alice','eve'])await f.call('/topics/subscribe',{name:topic},user);
  const second=(await f.call('/send',{to:'topic:'+topic,body:'other work'},'alice')).data.id;await f.exhaust(second);
  assert.deepEqual((await f.call('/queue/dead-letters?topic='+encodeURIComponent(f.topic),undefined,'bob')).data.items.map(r=>r.id),[first]);
  assert.deepEqual((await f.call('/queue/dead-letters?topic='+encodeURIComponent(topic),undefined,'alice')).data.items.map(r=>r.id),[second]);
  await f.call('/queue/requeue',{id:first},'bob');assert.equal(f.row(second).state,'dead');
});
