import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { rmSync } from 'node:fs';
import { Store } from '../store.mjs';
import { Broker, envelope } from '../broker.mjs';
import { config } from '../config.mjs';
import { candidates } from '../retention.mjs';
import { httpFixture } from './http.mjs';
import { testTempDir } from '../../test-support/temp.mjs';

async function fixture(t,{file=':memory:'}={}) {
  const store=new Store(file),settings={...config({}),topicRetention:100,closedRetention:50,sseHeartbeat:60000};
  store.run("UPDATE broker_meta SET value='0' WHERE key='retention_epoch'");
  const broker=new Broker(store,settings,['alice','bob','eve'],{now:()=>1000,log:()=>{}}),wire=await httpFixture(t,broker,{topic:(user,session)=>store.get('SELECT topic FROM agents WHERE session_id=?',session)?.topic??(user==='alice'?null:'retain/API')});
  t.after(()=>store.close());
  const call=async(path,args,user='alice',workspace='default')=>{
    const requestId=args?.id??(path.startsWith('/queue/item?')?Number(new URL(path,'http://fixture').searchParams.get('id')):undefined);
    const target=args?.topic??(path.startsWith('/topics/')?args?.name:undefined)??(requestId?store.get('SELECT topic FROM messages WHERE id=?',requestId)?.topic:undefined)??topic;
    const binding=user==='alice'?null:target;
    const session=user+'-'+workspace+(binding&&binding!==topic?'-'+binding.replaceAll('/','_'):'');
    const payload=path==='/send'&&args?.to&&!args.to.startsWith('topic:')?{...args,topic}:args;
    const res=await wire.request(path,{user,session,headers:{'X-Workspace':workspace,...(binding?{'X-Topic':binding}:{})},...(payload===undefined?{}:{method:'POST',body:JSON.stringify(payload)})});
    const data=await res.json();assert.ok(res.ok,JSON.stringify(data));return data;
  };
  const topic='retain/API';
  const add=async(name=topic,mode='broadcast',workspace='default')=>{
    await call('/topics',{name,mode},'alice',workspace);
    await call('/topics/subscribe',{name},'alice',workspace);
    store.run('INSERT OR IGNORE INTO topic_subscriptions VALUES (?,?,?,?)',workspace,name,'bob',broker.now());
    await call('/topics/subscribe',{name},'bob',workspace);
  };
  const send=async(args={},user='alice',workspace='default')=>call('/send',{to:'topic:'+topic,body:'synthetic retained body <private>',...args},user,workspace);
  const ack=async(id,user='bob',workspace='default')=>call('/ack',{id},user,workspace);
  const scan=(now=2000,options={})=>candidates(store,settings,now,options);
  const ids=(now=2000,options={})=>scan(now,options).items.map(r=>r.id);
  await add();return {...wire,store,broker,settings,topic,call,add,send,ack,scan,ids};
}
const closed=async(f,yes)=>{if(yes)await f.call('/topics/close',{name:f.topic});};

for(const isClosed of [false,true]) {
  const label=isClosed?'closed':'open';
  test(`${label}: one current subscriber cursor blocks until ack, including sparse ack with an earlier gap`,async t=>{
    const f=await fixture(t),gap=await f.send(),m=await f.send();await closed(f,isClosed);
    assert.deepEqual(f.ids(),[]);assert.deepEqual(f.scan().topics[0].blockers.users,['bob']);
    await f.ack(m.id);assert.deepEqual(f.ids(),[m.id]);
    assert.equal(f.store.get("SELECT acked_through FROM topic_cursors WHERE user='bob'").acked_through,0);
    await f.ack(gap.id);assert.deepEqual(f.ids(),[gap.id,m.id]);
    // Contiguous progress remains sufficient without a sparse ack row.
    f.store.run('DELETE FROM topic_acks WHERE message_id=?',m.id);assert.ok(f.ids().includes(m.id));
  });
  test(`${label}: a pin blocks and unkeep restores the same candidate`,async t=>{
    const f=await fixture(t),m=await f.send();await f.ack(m.id);await closed(f,isClosed);
    assert.deepEqual(f.ids(),[m.id]);await f.call('/topics/keep',{id:m.id},'bob');
    assert.deepEqual(f.ids(),[]);assert.deepEqual(f.scan().topics[0].blockers.pinned_ids,[m.id]);assert.equal(f.scan().topics[0].blockers.pinned_count,1);
    await f.call('/topics/unkeep',{id:m.id},'bob');assert.deepEqual(f.ids(),[m.id]);
  });
  test(`${label}: an unsent webhook blocks and marking it sent restores the same candidate`,async t=>{
    const f=await fixture(t),m=await f.send();await f.ack(m.id);await closed(f,isClosed);
    f.store.run('INSERT INTO webhook_outbox(message_id,payload,next_attempt) VALUES (?,?,0)',m.id,'{}');
    assert.deepEqual(f.ids(),[]);assert.deepEqual(f.scan().topics[0].blockers.webhook_ids,[m.id]);
    f.store.run('UPDATE webhook_outbox SET sent_at=1 WHERE message_id=?',m.id);assert.deepEqual(f.ids(),[m.id]);
  });
  test(`${label}: an escalated thread blocks until resolved, without changing its message`,async t=>{
    const f=await fixture(t),m=await f.send();await f.ack(m.id);await closed(f,isClosed);
    f.store.run("UPDATE threads SET status='escalated' WHERE id=?",m.thread_id);
    assert.deepEqual(f.ids(),[]);assert.deepEqual(f.scan().topics[0].blockers.unfinished_ids,[m.id]);
    f.store.run("UPDATE threads SET status='resolved' WHERE id=?",m.thread_id);assert.deepEqual(f.ids(),[m.id]);
  });
}

for(const kind of ['question','blocker'])test(`a pending blocking ${kind} protects the whole thread until a later answer by another user`,async t=>{
  const f=await fixture(t),first=await f.send();await f.ack(first.id);
  // An earlier answer from another user must not satisfy the later ask.
  const earlier=await f.send({thread_id:first.thread_id,kind:'answer'},'bob');await f.ack(earlier.id,'alice');
  const ask=await f.send({thread_id:first.thread_id,kind,blocking:true});await f.ack(ask.id);
  const own=await f.send({thread_id:first.thread_id,kind:'answer'});await f.ack(own.id);
  const unrelated=await f.send();await f.ack(unrelated.id);
  await f.send({thread_id:unrelated.thread_id,kind:'answer'},'bob');
  assert.equal(f.ids().includes(first.id),false);assert.equal(f.ids().includes(ask.id),false);
  const answer=await f.send({thread_id:first.thread_id,kind:'answer'},'bob');
  assert.ok(f.ids().includes(first.id));assert.ok(f.ids().includes(ask.id));
  f.store.run("UPDATE messages SET kind='finding' WHERE id=?",answer.id);
  assert.equal(f.ids().includes(ask.id),false);
  f.store.run("UPDATE messages SET kind='answer' WHERE id=?",answer.id);assert.ok(f.ids().includes(ask.id));
  assert.equal(f.ids().includes(answer.id),false); // Alice still has to ack Bob's answer.
});

for(const state of ['ready','claimed','dead','missing'])test(`a ${state} queue item blocks and done makes the same message eligible without subscriber acks`,async t=>{
  const f=await fixture(t),topic='retain/Queue';await f.add(topic,'queue');
  const m=await f.send({to:'topic:'+topic});
  let claim;
  if(state!=='ready' && state!=='missing')claim=await f.call('/queue/claim',{id:m.id},'bob');
  if(state==='dead')f.store.run("UPDATE queue_items SET state='dead' WHERE message_id=?",m.id);
  if(state==='missing')f.store.run('DELETE FROM queue_items WHERE message_id=?',m.id);
  await f.call('/topics/close',{name:topic});
  assert.deepEqual(f.ids(),[]);
  const blockers=f.scan().topics[0].blockers;
  assert.deepEqual(state==='dead'?blockers.dead_ids:blockers.unfinished_ids,[m.id]);
  if(state==='missing')f.store.run("INSERT INTO queue_items(message_id,workspace,topic,state) VALUES (?,'default',?,'ready')",m.id,topic);
  if(state==='dead'){await f.call('/queue/requeue',{id:m.id});claim=null;}
  claim??=await f.call('/queue/claim',{id:m.id},'bob');
  await f.call('/queue/done',{id:m.id,generation:claim.generation},'bob');
  assert.deepEqual(f.ids(),[m.id]);assert.equal(f.store.get('SELECT count(*) n FROM topic_acks WHERE message_id=?',m.id).n,0);
});

test('broadcast with zero current subscribers is eligible on age alone',async t=>{
  const f=await fixture(t),m=await f.send();assert.deepEqual(f.ids(),[]);
  await f.call('/topics/leave',{name:f.topic},'bob');await f.call('/topics/leave',{name:f.topic});
  assert.deepEqual(f.ids(),[m.id]);
});

test('blocker reporting ignores young rows and unrelated topic progress, and requires a current member',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);
  f.store.run('DELETE FROM topic_acks WHERE message_id=?',m.id); // Only this topic's contiguous cursors grant eligibility.
  await f.add('retain/Other');const other=await f.send({to:'topic:retain/Other'});
  assert.deepEqual(f.ids(),[m.id]);
  f.store.run("INSERT INTO topic_pins VALUES (?,'bob','default',1)",other.id);
  f.store.run('INSERT INTO webhook_outbox(message_id,payload,next_attempt) VALUES (?,\'{}\',1)',other.id);
  assert.deepEqual(f.ids(),[m.id]);
  f.store.run('UPDATE messages SET created=1950 WHERE id=?',other.id);
  assert.equal(f.scan().topics.some(t=>t.topic==='retain/Other'),false);
  f.store.run("DELETE FROM workspace_members WHERE user='bob'");
  f.store.run('DELETE FROM topic_pins');f.store.run('DELETE FROM webhook_outbox');
  f.store.run('UPDATE messages SET created=1000 WHERE id=?',other.id);
  assert.deepEqual(f.ids(),[m.id,other.id]); // A stale subscription is not a current member.
});

test('decisions, notices, escalations and user/workstream messages are never candidates beside an eligible topic control',async t=>{
  const f=await fixture(t),control=await f.send();await f.ack(control.id);
  for(const kind of ['decision','notice','escalation']) {
    const m=await f.send();await f.ack(m.id);f.store.run('UPDATE messages SET kind=? WHERE id=?',kind,m.id);
  }
  const direct=await f.send({to:'user:bob'});await f.ack(direct.id);
  await f.call('/workstreams/register',{name:'target'},'bob');
  const ws=await f.send({to:'ws:target'});await f.ack(ws.id);
  // Even name collisions with topics must not admit non-topic addresses.
  f.store.run("UPDATE messages SET to_name=? WHERE id IN (?,?)",f.topic,direct.id,ws.id);
  f.store.run('UPDATE topic_cursors SET acked_through=?',ws.id);
  assert.deepEqual(f.ids(),[control.id]);
});

test('open age uses the later of creation and safety epoch, including the exact boundary',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);
  assert.deepEqual(f.ids(1099),[]);assert.deepEqual(f.ids(1100),[m.id]);
  f.store.run("UPDATE broker_meta SET value='1200' WHERE key='retention_epoch'");
  assert.deepEqual(f.ids(1299),[]);assert.deepEqual(f.ids(1300),[m.id]);
});

test('closed and deleted workspace clocks use their lifecycle time and the epoch, with protections intact',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);await f.call('/topics/close',{name:f.topic});
  f.store.run('UPDATE topics SET closed_at=1400');
  assert.deepEqual(f.ids(1449),[]);assert.deepEqual(f.ids(1450),[m.id]);
  f.store.run("UPDATE broker_meta SET value='1600' WHERE key='retention_epoch'");
  assert.deepEqual(f.ids(1649),[]);assert.deepEqual(f.ids(1650),[m.id]);
  f.store.run("UPDATE workspaces SET deleted_at=1800 WHERE name='default'");
  assert.deepEqual(f.ids(1849),[]);assert.deepEqual(f.ids(1850),[m.id]);
  f.store.run("INSERT INTO topic_pins VALUES (?,'bob','default',1)",m.id);assert.deepEqual(f.ids(1850),[]);
  f.store.run('DELETE FROM topic_pins');assert.deepEqual(f.ids(1850),[m.id]);
  // An archived workspace need not have explicitly closed its topic.
  f.store.run('UPDATE topics SET closed_at=NULL');assert.deepEqual(f.ids(1849),[]);assert.deepEqual(f.ids(1850),[m.id]);
});

test('same-name topic cursors/subscribers and workspace filter cannot leak eligibility across workspaces',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);
  f.store.run('DELETE FROM topic_acks WHERE message_id=?',m.id); // The other workspace has independent cursors.
  await f.call('/workspaces',{name:'other'});
  f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
  await f.add(f.topic,'broadcast','other');const other=await f.send({},'alice','other');
  assert.deepEqual(f.ids(),[m.id]);assert.deepEqual(f.ids(2000,{workspace:'other'}),[]);
  assert.deepEqual(f.scan().topics.find(t=>t.workspace==='other').blockers.users,['bob']);
  await f.ack(other.id,'bob','other');assert.deepEqual(f.ids(2000,{workspace:'other'}),[other.id]);
  assert.deepEqual(f.ids(2000,{workspace:'default'}),[m.id]);
  assert.deepEqual(f.scan(2000,{limit:1}).items.map(r=>r.id),[m.id]);assert.equal(f.scan(2000,{limit:1}).count,2);
  assert.equal(f.scan(2000,{limit:1}).topics.find(t=>t.workspace==='other').min_id,other.id);
});

test('missing/corrupt retention epoch fails closed and retention settings/limits are validated',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);assert.deepEqual(f.ids(),[m.id]);
  for(const value of ['garbage','-1','1.5','',' ','0x100']) {
    f.store.run("UPDATE broker_meta SET value=? WHERE key='retention_epoch'",value);
    assert.throws(()=>f.scan(),{code:'INVALID_RETENTION_EPOCH'});
  }
  f.store.run("DELETE FROM broker_meta WHERE key='retention_epoch'");assert.throws(()=>f.scan(),{code:'INVALID_RETENTION_EPOCH'});
  assert.throws(()=>f.scan(2000,{limit:0}),{code:'INVALID_RETENTION_LIMIT'});
  assert.equal(config({}).topicRetention,7*86400000);assert.equal(config({}).closedRetention,3*86400000);
  for(const key of ['TOPIC_RETENTION','CLOSED_RETENTION'])for(const value of ['0','-1','1.2','bad','9007199254740991'])assert.throws(()=>config({[key]:value}),{code:'INVALID_'+key});
});

test('an otherwise eligible tombstoned row is excluded from candidates, including purged_at zero',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);assert.deepEqual(f.ids(),[m.id]);
  f.store.run("UPDATE messages SET body='',purged_at=0,purge_run_id=0 WHERE id=?",m.id);
  assert.deepEqual(f.ids(),[]);
  const rendered=envelope(f.store.get('SELECT * FROM messages WHERE id=?',m.id));
  assert.equal(rendered.body,'[purged by retention run 0]');
});

test('unfinished ask and answer probes use the thread index instead of scanning the workspace per candidate',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);
  const all=f.store.all.bind(f.store);let plan;
  t.mock.method(f.store,'all',(sql,...args)=>{
    if(sql.includes('AS unfinished'))plan=all('EXPLAIN QUERY PLAN '+sql,...args);
    return all(sql,...args);
  });
  assert.deepEqual(f.ids(),[m.id]);assert.ok(plan);
  for(const alias of ['ask','answer']) {
    const probe=plan.filter(row=>new RegExp(`(?:SEARCH|SCAN) ${alias}\\b`).test(row.detail));
    assert.equal(probe.length,1);
    assert.match(probe[0].detail,/USING INDEX messages_thread_id \(thread_id=\?/);
  }
});

async function cycleFixture(t) {
  const dir=testTempDir('retention-cycle-');t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const file=join(dir,'db.sqlite'),f=await fixture(t,{file});
  const cli=(action,id)=>{
    const args=['broker/cli.mjs',action,id,...(action==='reopen'?['--reason','retention cycle regression']:[])];
    const result=spawnSync(process.execPath,args,{env:{PATH:process.env.PATH,TMPDIR:dir,BROKER_DB:file},encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout);
  };
  return {...f,cli};
}

test('decision 76: human resolve finishes a thread even with an unanswered blocking ask',async t=>{
  const f=await cycleFixture(t),ask=await f.send({kind:'question',blocking:true});await f.ack(ask.id);
  assert.deepEqual(f.ids(),[]);
  f.store.run("UPDATE threads SET status='answered' WHERE id=?",ask.thread_id);assert.deepEqual(f.ids(),[]);
  assert.equal(f.cli('resolve',ask.thread_id).status,'resolved');
  assert.deepEqual(f.ids(),[ask.id]);
});

test('decision 76: reopen leaves the old unanswered ask exactly at the cycle watermark eligible',async t=>{
  const f=await cycleFixture(t),ask=await f.send({kind:'question',blocking:true});await f.ack(ask.id);
  assert.deepEqual(f.ids(),[]);f.cli('resolve',ask.thread_id);
  const reopened=f.cli('reopen',ask.thread_id);
  assert.equal(reopened.status,'open');assert.equal(reopened.cycle_start_id,ask.id);
  assert.deepEqual(f.ids(),[ask.id]);
});

test('decision 76: a new unanswered ask after real resolve/reopen protects the entire thread until resolved again',async t=>{
  const f=await cycleFixture(t),old=await f.send({kind:'question',blocking:true});await f.ack(old.id);
  f.cli('resolve',old.thread_id);const reopened=f.cli('reopen',old.thread_id);
  assert.deepEqual(f.ids(),[old.id]);
  const ask=await f.send({thread_id:old.thread_id,kind:'blocker',blocking:true});await f.ack(ask.id);
  assert.ok(ask.id>reopened.cycle_start_id);assert.equal(f.ids().includes(old.id),false);assert.equal(f.ids().includes(ask.id),false);
  f.cli('resolve',old.thread_id);assert.deepEqual(f.ids(),[old.id,ask.id]);
});

test('tombstones render placeholders through envelope, pins and queue_item while HTTP inbox/stream never deliver them',async t=>{
  const f=await fixture(t),broadcast=await f.send(),queue='retain/Queue';await f.call('/topics/keep',{id:broadcast.id},'bob');
  await f.add(queue,'queue');const q=await f.send({to:'topic:'+queue});
  for(const id of [broadcast.id,q.id])f.store.run("UPDATE messages SET body='',purged_at=1500,purge_run_id=42 WHERE id=?",id);
  const row=f.store.get('SELECT * FROM messages WHERE id=?',broadcast.id);
  const rendered=envelope({...row,body:'stale body'});assert.equal(rendered.body,'[purged by retention run 42]');
  assert.equal(JSON.parse(rendered.content.split('\n')[1]).body,rendered.body);
  assert.equal(f.broker.eligible(row,'bob','bob-default'),false);
  assert.deepEqual((await f.call('/inbox',undefined,'bob')).messages,[]);
  assert.equal((await f.call('/topics/pins',undefined,'bob')).messages[0].body,rendered.body);
  assert.equal((await f.call('/queue/item?id='+q.id,undefined,'bob')).message.body,rendered.body);
  // Tombstone inspection still requires this workspace and current subscription.
  await f.add('eve/topic');f.store.run("INSERT INTO topic_subscriptions VALUES ('default','eve/topic','eve',1)");
  let denied=await f.request('/queue/item?id='+q.id,{user:'eve',session:'eve-default',headers:{'X-Topic':'eve/topic'}});assert.equal(denied.status,404);
  await f.call('/workspaces',{name:'other'});
  f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
  await f.add(queue,'queue','other');
  denied=await f.request('/queue/item?id='+q.id,{user:'bob',session:'bob-other-retain_Queue',headers:{'X-Workspace':'other','X-Topic':queue}});assert.equal(denied.status,404);
  assert.deepEqual(f.ids(),[]);
  const stream=await f.stream('bob','bob-default','?replay=1');const barrier=await f.send();
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===barrier.id));
  assert.equal(stream.events.some(e=>e.event==='message'&&[broadcast.id,q.id].includes(e.data.id)),false);
});

test('all four inbox branches and generic eligibility reject purged rows independently of ack state',async t=>{
  const f=await fixture(t),broadcast=await f.send();await f.add('retain/Queue','queue');
  const queue=await f.send({to:'topic:retain/Queue'}),direct=await f.send({to:'user:bob'}),notice=await f.send({to:'user:bob'});
  f.store.run("UPDATE messages SET kind='notice',notice_scope='topic',from_user='broker',from_session='broker' WHERE id=?",notice.id);
  f.store.run("INSERT INTO escalation_targets(message_id,user) VALUES (?,'bob')",notice.id);
  for(const m of [broadcast,queue,direct,notice]) {
    const session=m.id===queue.id?'bob-default-retain_Queue':'bob-default';
    const row=f.store.get('SELECT * FROM messages WHERE id=?',m.id);assert.equal(f.broker.eligible(row,'bob',session),true);
    assert.ok(f.broker.inbox('bob',session).some(r=>r.id===m.id));
    f.store.run("UPDATE messages SET body='',purged_at=1500,purge_run_id=0 WHERE id=?",m.id);
    const purged=f.store.get('SELECT * FROM messages WHERE id=?',m.id);
    assert.equal(f.broker.eligible(purged,'bob',session),false);
    assert.equal(f.broker.inbox('bob',session).some(r=>r.id===m.id),false);
  }
});

test('the stream rechecks a page snapshot tombstoned after inbox read and before emission',async t=>{
  const f=await fixture(t),m=await f.send(),inbox=f.broker.inbox.bind(f.broker);let injected=false;
  t.mock.method(f.broker,'inbox',(...args)=>{
    const page=inbox(...args);
    if(args[1]==='snapshot' && !injected){injected=true;f.store.run("UPDATE messages SET body='',purged_at=1500,purge_run_id=7 WHERE id=?",m.id);}
    return page;
  });
  const stream=await f.stream('bob','snapshot','?replay=1'),barrier=await f.send();
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===barrier.id));
  assert.equal(injected,true);assert.equal(stream.events.some(e=>e.event==='message'&&e.data.id===m.id),false);
});

test('human retention --dry-run prints counts, ranges and blockers without bodies or writes, and refuses mixed run modes',async t=>{
  const dir=testTempDir('retention-cli-');t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const file=join(dir,'db.sqlite'),f=await fixture(t,{file});
  const eligible=await f.send();await f.ack(eligible.id);const blocked=await f.send();
  await f.call('/workspaces',{name:'other'});
  f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
  await f.add(f.topic,'broadcast','other');
  await f.send({},'alice','other'); // Only its sender subscribes: another candidate.
  f.store.run("DELETE FROM topic_subscriptions WHERE workspace='other' AND user='bob'");
  const snapshot=()=>f.store.all("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").map(({name})=>({name,rows:f.store.all(`SELECT * FROM ${name} ORDER BY rowid`)}));
  const before=snapshot();
  const run=(...args)=>spawnSync(process.execPath,['broker/cli.mjs','retention',...args],{
    env:{PATH:process.env.PATH,TMPDIR:dir,BROKER_DB:file,TOPIC_RETENTION:'1',CLOSED_RETENTION:'1'},encoding:'utf8'});
  const result=run('--dry-run','--workspace','default');assert.equal(result.status,0,result.stderr);
  const data=JSON.parse(result.stdout);assert.equal(data.dry_run,true);assert.equal(data.count,1);
  assert.deepEqual([data.topics[0].min_id,data.topics[0].max_id],[eligible.id,eligible.id]);assert.deepEqual(data.topics[0].blockers.users,['bob']);
  assert.equal((result.stdout+result.stderr).includes('synthetic retained body'),false);
  assert.deepEqual(snapshot(),before);
  assert.equal(JSON.parse(run('--dry-run').stdout).count,2);
  for(const args of [[],['--run','--dry-run'],['--run','--workspace','default'],['--dry-run','positional'],['--dry-run','--reason','why']])assert.equal(JSON.parse(run(...args).stderr).error,'INVALID_CLI_ARGS');
  assert.equal(JSON.parse(run('--dry-run','--workspace','absent').stderr).error,'WORKSPACE_NOT_FOUND');
  assert.equal(blocked.id>eligible.id,true);
});
