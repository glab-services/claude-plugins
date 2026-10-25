import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { EventEmitter } from 'node:events';
import { spawn } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { createServer } from '../server.mjs';
import { testTempDir } from '../../test-support/temp.mjs';
import { eventually } from './http.mjs';

const bindings=new Map();
const tokens=Object.fromEntries(['alice','bob','eve','dan'].map(user=>[user,user+'-broadcast-fixture-token-00000000']));
function headers(user,session,workspace='default',url) {const binding=bindings.get(url);return {...(user===(binding?.owners.get(workspace)??'alice')?{}:{'X-Topic':binding?.topic??'flow/API'}),Authorization:`Bearer ${tokens[user]}`,'X-Session-Id':session,'X-Workspace':workspace,'Content-Type':'application/json'};}
async function api(url,path,args,user='alice',session=`${user}-default`,workspace='default') {
  const res=await fetch(url+path,{method:args===undefined?'GET':'POST',headers:headers(user,session,workspace,url),...(args===undefined?{}:{body:JSON.stringify(args)})});
  return {status:res.status,data:await res.json()};
}
async function observe(t,url,user,session,workspace='default',query='?replay=1') {
  const req=http.get(url+'/stream'+query,{headers:headers(user,session,workspace,url)});t.after(()=>req.destroy());
  const response=await new Promise((resolve,reject)=>{req.once('response',resolve);req.once('error',reject);});assert.equal(response.statusCode,200);
  const events=[],updates=new EventEmitter();let buffer='',heartbeats=0,closed=false;
  response.setEncoding('utf8');response.on('error',()=>{});response.on('data',chunk=>{
    buffer+=chunk;let end;
    while((end=buffer.indexOf('\n\n'))>=0){const frame=buffer.slice(0,end);buffer=buffer.slice(end+2);const lines=frame.split('\n');
      if(lines.some(l=>l.startsWith(': heartbeat')))heartbeats++;
      const data=lines.find(l=>l.startsWith('data: '));if(data)events.push({event:lines.find(l=>l.startsWith('event: '))?.slice(7),data:JSON.parse(data.slice(6))});
    }updates.emit('update');
  });
  response.on('close',()=>{closed=true;updates.emit('update');});
  return {events,get heartbeats(){return heartbeats;},get closed(){return closed;},close:()=>req.destroy(),wait(predicate,timeout=4000){
    return new Promise((resolve,reject)=>{
      const finish=error=>{clearTimeout(timer);updates.off('update',changed);error?reject(error):resolve(events);};
      const changed=()=>{if(predicate(events))finish();else if(closed)finish(Error('Stream closed before expected event'));};
      const timer=setTimeout(()=>finish(Error('Timed out waiting for topic stream')),timeout);updates.on('update',changed);changed();
    });
  }};
}
async function fixture(t,{subscribers=['alice','bob','eve','dan'],owner='alice',workspaceOwner='alice',...overrides}={}) {
  const store=new Store(':memory:'),broker=new Broker(store,{...config({}),sseHeartbeat:20,defaultOwner:workspaceOwner,...overrides},Object.keys(tokens),{log:()=>{}});
  const server=createServer(broker,new Map(Object.entries(tokens).map(([user,token])=>[token,user])));
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  t.after(async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));store.close();});
  const url=`http://127.0.0.1:${server.address().port}`,topic='flow/API';
  const meta={topic,owners:new Map([['default',workspaceOwner]])};bindings.set(url,meta);t.after(()=>bindings.delete(url));
  const call=async(path,args,user='alice',session=`${user}-default`,workspace='default')=>{const result=await api(url,path,args,user,session,workspace);if(path==='/workspaces'&&result.status===201)meta.owners.set(args.name,user);return result;};
  assert.equal((await call('/topics',{name:topic},workspaceOwner)).status,201);
  store.run('UPDATE topics SET owner_user=? WHERE workspace=? AND name=?',owner,'default',topic);
  for(const user of subscribers){store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',topic,user,Date.now());assert.equal((await call('/topics/subscribe',{name:topic},user)).status,200);}
  return {url,store,broker,topic,call,stream:(user,session=`${user}-default`,workspace='default')=>observe(t,url,user,session,workspace),
    post:async(body,args={})=>{const r=await call('/send',{to:'topic:'+topic,body,...args});assert.equal(r.status,201);return r.data;},
    cursor:(user,workspace='default')=>store.get('SELECT acked_through FROM topic_cursors WHERE workspace=? AND topic=? AND user=?',workspace,topic,user)?.acked_through};
}
const messages=stream=>stream.events.filter(e=>e.event==='message');

for(const replay of [true,false])test(`local leave/resubscribe applies one marker on a ${replay?'replay':'non-replay'} stream`,async t=>{
  const f=await fixture(t,{workspaceOwner:'bob'}),stream=await observe(t,f.url,'bob','bob-default','default',replay?'?replay=1':''),sent=await f.post('local repeated offer');
  await stream.wait(es=>es.some(e=>e.data.id===sent.id));
  await f.call('/topics/leave',{name:f.topic},'bob');await f.call('/topics/subscribe',{name:f.topic},'bob');
  await stream.wait(es=>es.filter(e=>e.event==='message'&&e.data.id===sent.id).length>=2);
  const before=stream.heartbeats;await stream.wait(()=>stream.heartbeats>=before+3);
  assert.equal(messages(stream).filter(e=>e.data.id===sent.id).length,2);
});

test('first subscription on a non-replay stream does not rewind to historical topic messages',async t=>{
  const f=await fixture(t,{subscribers:['alice'],workspaceOwner:'bob'}),sent=await f.post('history before subscription');
  const stream=await observe(t,f.url,'bob','first-non-replay','default','');
  await f.call('/topics/subscribe',{name:f.topic},'bob');const before=stream.heartbeats;
  await stream.wait(()=>stream.heartbeats>=before+3);
  assert.equal(messages(stream).filter(e=>e.data.id===sent.id).length,0);
});

test('a broadcast reaches three non-sender subscribers once and each user acknowledges independently',async t=>{
  const f=await fixture(t),streams=await Promise.all(['bob','eve','dan'].map(user=>f.stream(user)));
  const sent=await f.post('broadcast to all');
  for(const stream of streams)await stream.wait(es=>es.some(e=>e.data.id===sent.id));
  await streams[0].wait(()=>streams[0].heartbeats>=4);
  for(const stream of streams)assert.equal(messages(stream).filter(e=>e.data.id===sent.id).length,1);
  assert.equal((await f.call('/inbox')).data.messages.length,0);
  for(const [index,user] of ['bob','eve','dan'].entries()) {
    const ack=await f.call('/ack',{id:sent.id},user);assert.equal(ack.status,200);assert.equal(ack.data.claimed_by_session,`${user}-default`);
    for(const pending of ['bob','eve','dan'].slice(index+1))assert.ok((await f.call('/inbox',undefined,pending)).data.messages.some(m=>m.id===sent.id));
  }
  assert.equal(f.store.get('SELECT acked_at FROM messages WHERE id=?',sent.id).acked_at,null);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_acks WHERE message_id=?',sent.id).n,4);
  assert.equal(f.store.get('SELECT count(*) n FROM participants WHERE thread_id=?',sent.thread_id).n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM delivery_targets WHERE message_id=?',sent.id).n,0);
});

test('a sibling receives the claim, loses ack and reply ownership, and the claimer repeats idempotently',async t=>{
  const f=await fixture(t),a=await f.stream('bob','bob-one'),b=await f.stream('bob','bob-two');
  const sent=await f.post('claim once per user');await Promise.all([a.wait(es=>es.some(e=>e.data.id===sent.id)),b.wait(es=>es.some(e=>e.data.id===sent.id))]);
  assert.equal((await f.call('/ack',{id:sent.id},'bob','bob-one')).data.changed,true);
  await b.wait(es=>es.some(e=>e.event==='claimed'&&e.data.id===sent.id));
  assert.equal((await f.call('/ack',{id:sent.id},'bob','bob-two')).data.error,'MESSAGE_CLAIMED');
  assert.equal((await f.call('/ack',{id:sent.id},'bob','bob-one')).data.changed,false);
  assert.equal((await f.call('/send',{to:'topic:'+f.topic,body:'wrong session',kind:'answer',thread_id:sent.thread_id},'bob','bob-two')).data.error,'MESSAGE_CLAIMED');
  assert.equal((await f.call('/send',{to:'topic:'+f.topic,body:'owned answer',kind:'answer',thread_id:sent.thread_id},'bob','bob-one')).status,201);
  assert.equal(b.events.filter(e=>e.event==='claimed'&&e.data.id===sent.id).length,1);
});

test('sparse acknowledgments preserve earlier gaps for replay and advance only over acknowledged topic IDs',async t=>{
  const f=await fixture(t),sent=[];for(let i=0;i<3;i++)sent.push(await f.post('message '+i));
  const ids=sent.map(m=>m.id);
  await f.call('/ack',{id:ids[2]},'bob');assert.equal(f.cursor('bob'),0);
  const replay=await f.stream('bob','bob-replay');await replay.wait(es=>es.filter(e=>e.event==='message').length===2);
  assert.deepEqual(messages(replay).map(e=>e.data.id),ids.slice(0,2));
  const reply=await f.call('/send',{to:'topic:'+f.topic,body:'sparse ack permits reply',kind:'answer',thread_id:sent[2].thread_id},'bob');
  assert.equal(reply.status,201,reply.data.error);assert.equal(f.cursor('bob'),0);
  await f.call('/ack',{id:ids[0]},'bob');assert.equal(f.cursor('bob'),ids[0]);
  await f.call('/ack',{id:ids[1]},'bob');assert.equal(f.cursor('bob'),reply.data.id);
});

test('cursor advancement is bounded by INBOX_LIMIT and idempotent repeats continue it',async t=>{
  const f=await fixture(t,{inboxLimit:3}),ids=[];for(let i=0;i<8;i++)ids.push((await f.post('bounded '+i)).id);
  for(const id of ids.slice(1))await f.call('/ack',{id},'bob');assert.equal(f.cursor('bob'),0);
  await f.call('/ack',{id:ids[0]},'bob');assert.equal(f.cursor('bob'),ids[2]);
  assert.equal((await f.call('/ack',{id:ids[0]},'bob')).data.changed,false);assert.equal(f.cursor('bob'),ids[5]);
  await f.call('/ack',{id:ids[0]},'bob');assert.equal(f.cursor('bob'),ids[7]);
});

test('a late subscriber reads retained messages and resubscription replays only its unacknowledged history',async t=>{
  const f=await fixture(t,{subscribers:['alice'],workspaceOwner:'bob'}),first=await f.post('old one'),second=await f.post('old two');
  assert.equal((await f.call('/inbox',undefined,'bob')).data.messages.length,0);
  await f.call('/topics/subscribe',{name:f.topic},'bob');assert.equal(f.cursor('bob'),0);
  assert.deepEqual((await f.call('/inbox',undefined,'bob')).data.messages.map(m=>m.id),[first.id,second.id]);
  await f.call('/ack',{id:first.id},'bob');
  await f.call('/topics/leave',{name:f.topic},'bob');assert.equal(f.cursor('bob'),undefined);
  await f.call('/topics/subscribe',{name:f.topic},'bob');
  assert.deepEqual((await f.call('/inbox',undefined,'bob')).data.messages.map(m=>m.id),[second.id]);
  assert.equal(f.store.get('SELECT count(*) n FROM topic_acks WHERE message_id=? AND user=?',first.id,'bob').n,1);
});

test('subscribe immediately rescans an open replay stream before a long heartbeat',async t=>{
  const f=await fixture(t,{subscribers:['alice'],workspaceOwner:'bob',sseHeartbeat:60_000});
  const retained=await f.post('retained before subscription');
  const stream=await f.stream('bob','bob-live');
  assert.equal(messages(stream).length,0);
  assert.equal((await f.call('/topics/subscribe',{name:f.topic},'bob')).status,200);
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===retained.id),1000);
  assert.equal(stream.heartbeats,0);
  assert.deepEqual(messages(stream).map(e=>e.data.id),[retained.id]);
});

for(const replay of [true,false])test(`member leave terminates its ${replay?'replay':'non-replay'} stream and a fresh bound session replays once`,async t=>{
  const f=await fixture(t),query=replay?'?replay=1':'',stream=await observe(t,f.url,'bob','bob-live','default',query),first=await f.post('before leave');
  await stream.wait(es=>es.some(e=>e.data.id===first.id));await f.call('/topics/leave',{name:f.topic},'bob');
  await eventually(()=>stream.closed);assert.equal((await f.call('/inbox',undefined,'bob','bob-live')).data.error,'SESSION_ENDED');
  const second=await f.post('after topic leave');
  const invite=await f.call('/invites',{topics:[f.topic]});assert.equal(invite.status,201);
  assert.equal((await f.call('/join',{code:invite.data.code,name:'bob'},'bob')).status,201);
  const fresh=await observe(t,f.url,'bob','bob-rejoined','default',query);
  await fresh.wait(es=>es.filter(e=>e.event==='message').length>=2);
  const before=fresh.heartbeats;await fresh.wait(()=>fresh.heartbeats>=before+3);
  assert.deepEqual(messages(fresh).map(e=>e.data.id),[first.id,second.id]);
  await f.call('/members/remove',{user:'bob'});await eventually(()=>fresh.closed);
  const third=await f.post('after workspace removal');assert.equal(messages(fresh).some(e=>e.data.id===third.id),false);
  assert.equal((await f.call('/inbox',undefined,'bob')).data.error,'NOT_A_MEMBER');assert.equal(f.cursor('bob'),undefined);
});

test('topic replies keep their address and escalation targets only current subscribers',async t=>{
  const f=await fixture(t,{threadCap:2}),sent=await f.post('blocking',{kind:'question',blocking:true});
  await f.call('/topics',{name:'other/topic'});await f.call('/topics/subscribe',{name:'other/topic'});
  for(const to of ['topic:other/topic','user:alice','ws:missing'])assert.equal((await f.call('/send',{to,body:'wrong destination',thread_id:sent.thread_id},'bob')).data.error,'TOPIC_THREAD_MISMATCH');
  const ordinary=await f.call('/send',{to:'user:bob',topic:f.topic,body:'ordinary'});
  assert.equal((await f.call('/send',{to:'topic:'+f.topic,body:'wrong thread',thread_id:ordinary.data.thread_id})).data.error,'TOPIC_THREAD_MISMATCH');
  await f.call('/topics/leave',{name:f.topic});await f.call('/topics/leave',{name:f.topic},'dan');
  assert.equal((await f.call('/send',{to:'topic:'+f.topic,kind:'answer',body:'ack first',thread_id:sent.thread_id},'bob')).data.error,'ACK_REQUIRED');
  await f.call('/ack',{id:sent.id},'bob');
  const answer=await f.call('/send',{to:'topic:'+f.topic,kind:'answer',body:'answer',thread_id:sent.thread_id},'bob');assert.equal(answer.data.status,'escalated');
  const escalation=f.store.get("SELECT id FROM messages WHERE thread_id=? AND kind='escalation'",sent.thread_id);
  assert.deepEqual(f.store.all('SELECT user FROM escalation_targets WHERE message_id=? ORDER BY user',escalation.id).map(r=>r.user),['bob','eve']);
  assert.equal((await f.call('/close',{thread_id:sent.thread_id})).data.error,'THREAD_ESCALATED');
  assert.equal((await f.call('/send',{to:'topic:'+f.topic,body:'over cap',thread_id:sent.thread_id},'bob')).data.error,'THREAD_ESCALATED');
  await f.call('/topics/leave',{name:f.topic},'eve');
  assert.equal((await f.call('/inbox',undefined,'eve')).data.error,'SESSION_ENDED');
  assert.equal(f.broker.eligible(f.store.get('SELECT * FROM messages WHERE id=?',escalation.id),'eve','eve-default'),false);
  const ack=await f.call('/ack',{id:escalation.id},'eve');
  assert.equal(ack.status,409);assert.equal(ack.data.error,'SESSION_ENDED');
});

test('decision 71: topic moderation does not create subscriptions and other close callers are refused',async t=>{
  const f=await fixture(t,{owner:'bob',subscribers:['bob','eve','dan']});
  const start=async()=>{const r=await f.call('/send',{to:'topic:'+f.topic,body:'starter'},'eve');assert.equal(r.status,201);return r.data.thread_id;};
  const first=await start();assert.equal((await f.call('/close',{thread_id:first},'bob')).status,200);assert.equal(f.cursor('bob'),0);
  const second=await start();assert.equal((await f.call('/close',{thread_id:second},'alice')).status,200);assert.equal(f.cursor('alice'),undefined);
  const own=await start();assert.equal((await f.call('/close',{thread_id:own},'eve')).status,200);
  const third=await start();await f.call('/topics/leave',{name:f.topic},'eve');
  assert.equal((await f.call('/close',{thread_id:third},'eve')).data.error,'SESSION_ENDED');
  assert.equal((await f.call('/close',{thread_id:third},'dan')).data.error,'NOT_TOPIC_OWNER');
  await f.call('/workspaces',{name:'elsewhere'},'dan');
  assert.equal((await f.call('/close',{thread_id:third},'dan')).data.error,'NOT_TOPIC_OWNER');
  assert.equal(f.store.get('SELECT status FROM threads WHERE id=?',third).status,'open');
  for(const user of ['alice','bob','eve'])f.store.run("INSERT INTO workspace_members VALUES ('elsewhere',?,'member',1,'invite')",user);
  await f.call('/topics',{name:f.topic},'dan','dan-elsewhere','elsewhere');
  f.store.run("UPDATE topics SET owner_user='bob' WHERE workspace='elsewhere' AND name=?",f.topic);
  for(const user of ['alice','eve'])f.store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','elsewhere',f.topic,user,Date.now());
  await f.call('/topics/subscribe',{name:f.topic},'eve','eve-elsewhere','elsewhere');
  const foreign=await f.call('/send',{to:'topic:'+f.topic,body:'other workspace'},'eve','eve-elsewhere','elsewhere');
  assert.equal((await f.call('/close',{thread_id:foreign.data.thread_id},'alice','alice-elsewhere','elsewhere')).data.error,'NOT_TOPIC_OWNER');
  await f.call('/topics/leave',{name:f.topic},'bob');
  assert.equal((await f.call('/send',{to:'topic:'+f.topic,body:'no read/reply',thread_id:third},'bob')).data.error,'SESSION_ENDED');
});

test('same-name topics keep inbox, cursors, stream replay and claims isolated across workspaces',async t=>{
  const f=await fixture(t,{subscribers:['alice','bob']});
  await f.call('/workspaces',{name:'other'});f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
  await f.call('/topics',{name:f.topic},'alice','alice-other','other');
  f.store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','other',f.topic,'bob',Date.now());
  for(const user of ['alice','bob'])await f.call('/topics/subscribe',{name:f.topic},user,`${user}-other`,'other');
  const first=await f.post('default one'),foreign=(await f.call('/send',{to:'topic:'+f.topic,body:'other'},'alice','alice-other','other')).data,third=await f.post('default two');
  assert.deepEqual((await f.call('/inbox',undefined,'bob')).data.messages.map(m=>m.id),[first.id,third.id]);
  assert.deepEqual((await f.call('/inbox',undefined,'bob','bob-other','other')).data.messages.map(m=>m.id),[foreign.id]);
  await f.call('/ack',{id:third.id},'bob');await f.call('/ack',{id:first.id},'bob');assert.equal(f.cursor('bob'),third.id);
  const stream=await f.stream('bob','bob-other-stream','other');await stream.wait(es=>es.some(e=>e.data.id===foreign.id));
  const local=await f.stream('bob','bob-local-stream');
  await f.call('/ack',{id:foreign.id},'bob','bob-other','other');await stream.wait(es=>es.some(e=>e.event==='claimed'&&e.data.id===foreign.id));
  assert.equal(f.cursor('bob','other'),foreign.id);assert.equal(f.cursor('bob'),third.id);
  await local.wait(()=>local.heartbeats>=3);assert.equal(local.events.some(e=>e.event==='claimed'&&e.data.id===foreign.id),false);
  assert.equal((await f.call('/ack',{id:foreign.id},'bob')).data.error,'MESSAGE_NOT_FOUND');
});

test('visibleTopic directly rejects another workspace despite same-user same-topic subscriptions',async t=>{
  const f=await fixture(t,{subscribers:['alice','bob']});
  await f.call('/workspaces',{name:'other'});f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
  await f.call('/topics',{name:f.topic},'alice','alice-other','other');
  f.store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','other',f.topic,'bob',Date.now());
  for(const user of ['alice','bob'])assert.equal((await f.call('/topics/subscribe',{name:f.topic},user,`${user}-other`,'other')).status,200);
  const local=await f.post('local visibility');
  const foreign=await f.call('/send',{to:'topic:'+f.topic,body:'foreign visibility'},'alice','alice-other','other');
  assert.equal(foreign.status,201);
  for(const [id,own,other] of [[local.id,'bob-default','bob-other'],[foreign.data.id,'bob-other','bob-default']]) {
    const message=f.store.get('SELECT * FROM messages WHERE id=?',id);
    assert.equal(f.broker.visibleTopic(message,'bob',own),true);
    assert.equal(f.broker.visibleTopic(message,'bob',other),false);
  }
});

test('participant directly refuses a departed subscriber who remains a workspace member',async t=>{
  const f=await fixture(t),sent=await f.post('participant contract');
  const scope={user:'bob',workspace:'default',topic:f.topic};
  assert.equal(f.broker.participant(scope,sent.thread_id).id,sent.thread_id);
  assert.equal((await f.call('/topics/leave',{name:f.topic},'bob')).status,200);
  assert.ok(f.broker.member('bob','default'));
  assert.throws(()=>f.broker.participant(scope,sent.thread_id),e=>e.code==='TOPIC_NOT_SUBSCRIBED');
});

test('topic inbox plans use the dedicated topic message index',async t=>{
  const f=await fixture(t);await f.post('indexed');let plan;
  const all=f.store.all.bind(f.store);f.store.all=(sql,...args)=>{
    if(sql.includes('SELECT m.*, NULL AS claim_generation FROM messages'))plan=all('EXPLAIN QUERY PLAN '+sql,...args);
    return all(sql,...args);
  };
  assert.equal(f.broker.inbox('bob','bob-default').length,1);assert.ok(plan.some(row=>/messages_topic/.test(row.detail)));
});

async function processes(t,{subscribers=['alice','bob'],heartbeat='1'}={}) {
  const dir=testTempDir('topic-process-'),file=join(dir,'broker.sqlite'),tokenPath=join(dir,'tokens.json');
  writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});new Store(file).close();const children=[];
  t.after(async()=>{for(const p of children){if(p.child.exitCode===null&&p.child.signalCode===null)p.child.kill('SIGTERM');await p.exited;}rmSync(dir,{recursive:true,force:true});});
  async function start() {
    const child=spawn(process.execPath,['broker/main.mjs'],{env:{...process.env,BROKER_HOST:'127.0.0.1',BROKER_PORT:'0',BROKER_DB:file,BROKER_TOKENS:tokenPath,BROKER_DEFAULT_OWNER:'bob',SSE_HEARTBEAT:heartbeat},stdio:['ignore','ignore','pipe']});
    const exited=new Promise(resolve=>child.once('exit',resolve));children.push({child,exited});let text='',port;
    child.stderr.on('data',chunk=>{text+=chunk;});
    await eventually(()=>{for(const line of text.split('\n')){try{const value=JSON.parse(line);if(value.event==='broker_listening')port=value.address.port;}catch{}}return Boolean(port);});
    return `http://127.0.0.1:${port}`;
  }
  const a=await start(),b=await start(),topic='shared/process';
  for(const url of [a,b]){bindings.set(url,{topic,owners:new Map([['default','bob']])});t.after(()=>bindings.delete(url));}
  assert.equal((await api(a,'/topics',{name:topic},'bob')).status,201);
  const setup=new Store(file);try{for(const user of subscribers)setup.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',topic,user,Date.now());}finally{setup.close();}
  for(const user of subscribers)assert.equal((await api(a,'/topics/subscribe',{name:topic},user)).status,200);
  return {a,b,topic,file};
}

test('regression guard: separate broker processes serialize competing topic claims for the same user',async t=>{
  // BEGIN IMMEDIATE plus the (message_id,user) primary key is the ownership guarantee.
  const f=await processes(t),sent=await api(f.a,'/send',{to:'topic:'+f.topic,body:'one owner per user'});assert.equal(sent.status,201);
  const claims=await Promise.all([api(f.a,'/ack',{id:sent.data.id},'bob','bob-one'),api(f.b,'/ack',{id:sent.data.id},'bob','bob-two')]);
  assert.deepEqual(claims.map(r=>r.status).sort(),[200,409]);assert.equal(claims.find(r=>r.status===409).data.error,'MESSAGE_CLAIMED');
  const winner=claims.findIndex(r=>r.status===200),session=winner===0?'bob-one':'bob-two';
  assert.equal((await api(f.b,'/ack',{id:sent.data.id},'bob',session)).data.changed,false);
  const store=new Store(f.file);try{assert.equal(store.get("SELECT count(*) n FROM topic_acks WHERE message_id=? AND user='bob'",sent.data.id).n,1);assert.equal(store.get("SELECT session FROM topic_acks WHERE message_id=? AND user='bob'",sent.data.id).session,session);}finally{store.close();}
});

test('another broker process replays a broadcast once and observes durable acknowledgments and unsubscribe',async t=>{
  const f=await processes(t),stream=await observe(t,f.b,'bob','bob-stream');
  const sent=await api(f.a,'/send',{to:'topic:'+f.topic,body:'cross-process delivery'});assert.equal(sent.status,201);
  await stream.wait(es=>es.some(e=>e.data.id===sent.data.id));const heartbeats=stream.heartbeats;
  await stream.wait(()=>stream.heartbeats>=heartbeats+2);assert.equal(messages(stream).filter(e=>e.data.id===sent.data.id).length,1);
  assert.equal((await api(f.a,'/ack',{id:sent.data.id},'bob')).status,200);
  assert.equal((await api(f.b,'/inbox',undefined,'bob')).data.messages.length,0);
  await api(f.a,'/topics/leave',{name:f.topic},'bob');
  const next=await api(f.a,'/send',{to:'topic:'+f.topic,body:'not subscribed'});assert.equal(next.status,201);
  const before=stream.heartbeats;await stream.wait(()=>stream.heartbeats>=before+2);
  assert.equal(messages(stream).some(e=>e.data.id===next.data.id),false);
});

for(const replay of [true,false])for(const leave of ['a','b'])for(const subscribe of ['a','b'])
test(`two-process ${replay?'replay':'non-replay'} leave ${leave} / subscribe ${subscribe} applies one marker`,async t=>{
  const f=await processes(t),query=replay?'?replay=1':'',stream=await observe(t,f.b,'bob','repeat-stream','default',query);
  const setup=new Store(f.file);try{setup.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',f.topic,'eve',Date.now());}finally{setup.close();}
  await api(f.a,'/topics/subscribe',{name:f.topic},'eve');const control=await observe(t,f.b,'eve','control-stream','default',query);
  const sent=await api(f.b,'/send',{to:'topic:'+f.topic,body:'offer again'},'alice');
  await stream.wait(es=>es.some(e=>e.data.id===sent.data.id));
  await control.wait(es=>es.some(e=>e.data.id===sent.data.id));
  assert.equal((await api(f[leave],'/topics/leave',{name:f.topic},'bob')).status,200);
  const left=stream.heartbeats;await stream.wait(()=>stream.heartbeats>=left+1);
  assert.equal((await api(f[subscribe],'/topics/subscribe',{name:f.topic},'bob')).status,200);
  const heartbeats=stream.heartbeats;await stream.wait(()=>stream.heartbeats>=heartbeats+3);
  assert.equal(messages(stream).filter(e=>e.data.id===sent.data.id).length,2);
  assert.equal(messages(control).filter(e=>e.data.id===sent.data.id).length,1);
});


test('remote first subscribe on a non-replay stream leaves historical messages in inbox only',async t=>{
  const f=await processes(t,{subscribers:['alice']});
  const sent=await api(f.a,'/send',{to:'topic:'+f.topic,body:'before first subscription'});
  assert.equal(sent.status,201);
  const stream=await observe(t,f.b,'bob','first-remote-stream','default','?since='+sent.data.id);
  assert.equal((await api(f.a,'/topics/subscribe',{name:f.topic},'bob')).status,200);
  const before=stream.heartbeats;await stream.wait(()=>stream.heartbeats>=before+3);
  assert.equal(messages(stream).filter(e=>e.data.id===sent.data.id).length,0);
  assert.ok((await api(f.b,'/inbox',undefined,'bob')).data.messages.some(m=>m.id===sent.data.id));
  const future=await api(f.b,'/send',{to:'topic:'+f.topic,body:'after first subscription'},'alice');
  assert.equal(future.status,201);
  await stream.wait(es=>es.some(e=>e.data.id===future.data.id));
  assert.equal(messages(stream).filter(e=>e.data.id===sent.data.id).length,0);
});


for(const replay of [true,false])test(`local subscribe applies a remote leave marker before a long ${replay?'replay':'non-replay'} heartbeat`,async t=>{
  const f=await processes(t,{heartbeat:'60'});
  const stream=await observe(t,f.b,'bob','prompt-marker-stream','default',replay?'?replay=1':'');
  const sent=await api(f.b,'/send',{to:'topic:'+f.topic,body:'prompt re-offer'},'alice');
  assert.equal(sent.status,201);
  await stream.wait(es=>es.some(e=>e.data.id===sent.data.id));
  assert.equal((await api(f.a,'/topics/leave',{name:f.topic},'bob')).status,200);
  assert.equal((await api(f.b,'/topics/subscribe',{name:f.topic},'bob')).status,200);
  await stream.wait(es=>es.filter(e=>e.event==='message'&&e.data.id===sent.data.id).length>=2,1000);
  assert.equal(stream.heartbeats,0);
  assert.equal(messages(stream).filter(e=>e.data.id===sent.data.id).length,2);
});
