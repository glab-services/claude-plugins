import { test } from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import http from 'node:http';
import { testTempDir } from '../../test-support/temp.mjs';
import { Store } from '../store.mjs';
import { Jobs } from '../jobs.mjs';
import { candidates,compact } from '../retention.mjs';
import { config } from '../config.mjs';
import { retentionFixture as fixture } from './fixtures/retention.mjs';

test('bounded batches tombstone only eligible bodies and audit each workspace/topic while preserving metadata and history',async t=>{
  const f=await fixture(t),ids=[];
  for(let i=0;i<7;i++){const m=await f.send();await f.ack(m.id);ids.push(m.id);}
  const pinned=await f.send();await f.ack(pinned.id);await f.call('/topics/keep',{id:pinned.id},'bob');
  const before=ids.map(f.row),threads=f.store.all('SELECT * FROM threads'),acks=f.store.all('SELECT * FROM topic_acks');
  f.broker.register('alice','alice-default','history');
  f.broker.recordDecision('alice','alice-default',{workstream:'history',body:'permanent decision'});
  const decisions=f.store.all('SELECT * FROM decisions'),audit=f.store.all('SELECT * FROM workspace_audit');
  assert.equal((await f.run()).purged,4);assert.equal((await f.run()).purged,3);assert.equal((await f.run()).purged,0);
  for(const original of before) {
    const after=f.row(original.id);assert.equal(after.body,'');assert.equal(after.purged_at,2000);assert.ok(after.purge_run_id>0);
    const {body,purged_at,purge_run_id,...saved}=after,{body:oldBody,purged_at:oldTime,purge_run_id:oldRun,...old}=original;
    assert.deepEqual(saved,old);
  }
  assert.equal(f.row(pinned.id).purged_at,null);
  const purges=f.store.all("SELECT * FROM workspace_audit WHERE action='retention:purge'");
  assert.deepEqual(purges.map(r=>JSON.parse(r.detail).count),[2,2,2,1]);
  assert.equal(purges.reduce((n,r)=>n+JSON.parse(r.detail).count,0),7);
  assert.ok(purges.every(r=>r.actor_user==='broker'&&r.actor_session===null&&JSON.parse(r.detail).topic===f.topic));
  assert.deepEqual(f.store.all('SELECT * FROM decisions'),decisions);
  assert.deepEqual(f.store.all('SELECT * FROM workspace_audit WHERE id<=?',audit.at(-1).id),audit);
  for(const old of threads)assert.deepEqual(f.store.get('SELECT * FROM threads WHERE id=?',old.id),old);
  assert.deepEqual(f.store.all('SELECT * FROM topic_acks'),acks);
  assert.deepEqual(f.store.all('SELECT purged FROM retention_runs ORDER BY id').map(r=>r.purged),[4,3,0]);
});

test('one batch audits distinct topics and workspaces with accurate ID ranges',async t=>{
  const f=await fixture(t,{retentionBatch:10}),ids=[];
  await f.add('compact/Other');
  for(const topic of [f.topic,'compact/Other']){const m=await f.send({to:'topic:'+topic});await f.ack(m.id);ids.push(m.id);}
  await f.call('/workspaces',{name:'other'});f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
  await f.add(f.topic,'broadcast','other');const m=await f.send({},'alice','other');await f.ack(m.id,'bob','other');ids.push(m.id);
  const run=await f.run();assert.equal(run.purged,3);
  const rows=f.store.all("SELECT workspace,detail FROM workspace_audit WHERE action='retention:purge'");assert.equal(rows.length,3);
  for(const id of ids){const audit=rows.find(r=>JSON.parse(r.detail).min_id===id);assert.ok(audit);assert.deepEqual(JSON.parse(audit.detail),{topic:f.row(id).to_name,run_id:run.id,count:1,min_id:id,max_id:id});assert.equal(audit.workspace,f.row(id).workspace);}
});

for(const fault of ['purge','audit','counter'])test(`a failed ${fault} rolls back the whole second batch, preserving the first and its audit`,async t=>{
  const f=await fixture(t),ids=[];
  for(let i=0;i<4;i++){const m=await f.send();await f.ack(m.id);ids.push(m.id);}
  const table=fault==='purge'?'messages':fault==='audit'?'workspace_audit':'retention_runs';
  const event=fault==='purge'?'UPDATE OF body':fault==='audit'?'INSERT':'UPDATE OF purged';
  const condition=fault==='purge'?`NEW.id=${ids[3]}`:fault==='audit'?`NEW.action='retention:purge' AND json_extract(NEW.detail,'$.min_id')=${ids[2]}`:'NEW.purged>2';
  f.store.db.exec(`CREATE TEMP TRIGGER fail_batch BEFORE ${event} ON ${table} WHEN ${condition} BEGIN SELECT RAISE(ABORT,'injected'); END`);
  await assert.rejects(()=>f.run(),/injected/);
  for(const id of ids.slice(0,2))assert.equal(f.row(id).body,'');
  for(const id of ids.slice(2)){assert.equal(f.row(id).purged_at,null);assert.equal(f.row(id).purge_run_id,null);assert.equal(f.row(id).body,'synthetic compaction body');}
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='retention:purge'").n,1);
  assert.equal(f.store.get('SELECT purged,finished FROM retention_runs').purged,2);assert.equal(f.store.get('SELECT finished FROM retention_runs').finished,null);
  f.store.db.exec('DROP TRIGGER fail_batch');assert.equal((await f.run()).purged,2);
});

test('every batch re-evaluates candidates after another connection pins a row between committed batches',async t=>{
  const dir=testTempDir('retention-between-');t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const file=join(dir,'db.sqlite'),f=await fixture(t,{file,retentionBatch:1,retentionMaxBatches:5}),first=await f.send(),second=await f.send();await f.ack(first.id);await f.ack(second.id);
  const other=new Store(file);t.after(()=>other.close());const transaction=f.store.transaction.bind(f.store);let changed=false;
  t.mock.method(f.store,'transaction',fn=>{
    const result=transaction(fn);
    if(!changed&&f.row(first.id).purged_at!==null){changed=true;other.run("INSERT INTO topic_pins VALUES (?,'bob','default',1)",second.id);}
    return result;
  });
  assert.equal((await f.run()).purged,1);assert.equal(changed,true);assert.equal(f.row(second.id).purged_at,null);
  await f.call('/topics/unkeep',{id:second.id},'bob');assert.equal((await f.run()).purged,1);
});

test('blocker notices/digests commit together, publish promptly, dedupe unchanged sets and clear without empty notices',async t=>{
  const f=await fixture(t),first=await f.send(),stream=await f.stream('alice','owner','?replay=1');
  const key='retention_blockers:default/'+f.topic;
  await f.run();const notice=()=>f.store.all("SELECT * FROM messages WHERE kind='notice' AND body LIKE 'Retention blocked%'");
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===notice()[0].id),1000);assert.equal(stream.heartbeats,0);
  assert.equal(notice().length,1);assert.ok(f.store.get('SELECT value FROM broker_meta WHERE key=?',key));
  await f.run();assert.equal(notice().length,1);
  const second=await f.send();await f.call('/topics/keep',{id:second.id},'bob');await f.run();assert.equal(notice().length,2);
  await f.ack(first.id);await f.ack(second.id);await f.call('/topics/unkeep',{id:second.id},'bob');await f.run();
  assert.equal(notice().length,2);assert.equal(f.store.get('SELECT value FROM broker_meta WHERE key=?',key),undefined);
  const third=await f.send();await f.run();assert.equal(notice().length,3);await f.ack(third.id);
});

test('blocker list caps preserve full counts and digests, including changes after the displayed prefix',async t=>{
  const dir=testTempDir('retention-cap-');t.after(()=>rmSync(dir,{recursive:true,force:true}));const file=join(dir,'db.sqlite');
  const f=await fixture(t,{file}),ids=[];
  for(let i=0;i<5;i++){const m=await f.send();await f.ack(m.id);await f.call('/topics/keep',{id:m.id},'bob');ids.push(m.id);}
  const scan=()=>candidates(f.store,f.settings,2000).topics[0].blockers;
  const before=scan();assert.deepEqual(before.pinned_ids,ids.slice(0,2));assert.equal(before.pinned_count,5);assert.equal(before.truncated,true);
  const dry=spawnSync(process.execPath,['broker/cli.mjs','retention','--dry-run'],{env:{PATH:process.env.PATH,TMPDIR:dir,BROKER_DB:file,TOPIC_RETENTION:'1',RETENTION_BLOCKER_LIMIT:'2'},encoding:'utf8'});
  assert.equal(dry.status,0,dry.stderr);const reported=JSON.parse(dry.stdout).topics[0].blockers;
  assert.deepEqual(reported.pinned_ids,ids.slice(0,2));assert.equal(reported.pinned_count,5);assert.equal(reported.truncated,true);
  await f.run();const notices=()=>f.store.get("SELECT count(*) n FROM messages WHERE kind='notice'").n;assert.equal(notices(),1);
  await f.call('/topics/unkeep',{id:ids[4]},'bob');const after=scan();assert.deepEqual(after.pinned_ids,before.pinned_ids);assert.equal(after.pinned_count,4);assert.notEqual(after.digest,before.digest);
  await f.run();assert.equal(notices(),2);
  const replacement=await f.send();await f.ack(replacement.id);await f.call('/topics/keep',{id:replacement.id},'bob');
  const replaced=scan();assert.equal(replaced.pinned_count,before.pinned_count);assert.deepEqual(replaced.pinned_ids,before.pinned_ids);assert.notEqual(replaced.digest,before.digest);
  await f.run();assert.equal(notices(),3);
});

test('failed blocker notice insertion rolls back its digest so the next run can retry',async t=>{
  const f=await fixture(t);await f.send();
  f.store.db.exec("CREATE TEMP TRIGGER no_notice BEFORE INSERT ON messages WHEN NEW.kind='notice' BEGIN SELECT RAISE(ABORT,'notice-fault'); END");
  await assert.rejects(()=>f.run(),/notice-fault/);assert.equal(f.store.get("SELECT count(*) n FROM broker_meta WHERE key GLOB 'retention_blockers:*'").n,0);
  f.store.db.exec('DROP TRIGGER no_notice');await f.run();assert.equal(f.store.get("SELECT count(*) n FROM messages WHERE kind='notice'").n,1);
});

test('a digest upsert failure rolls back its notice without publication, and successful notices publish only after commit',async t=>{
  const f=await fixture(t);await f.send();const stream=await f.stream('alice','digest-owner','?replay=1'),attempts=[];
  const run=()=>compact(f.store,f.settings,2000,{log:()=>{},publish:message=>{
    attempts.push({id:message.id,inTransaction:f.store.db.isTransaction});f.broker.publish(message);
  }});
  f.store.db.exec("CREATE TEMP TRIGGER fail_digest BEFORE INSERT ON broker_meta WHEN NEW.key GLOB 'retention_blockers:*' BEGIN SELECT RAISE(ABORT,'digest-fault'); END");
  await assert.rejects(run,/digest-fault/);
  const barrier=await f.send({to:'user:alice'},'bob');
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===barrier.id));
  assert.equal(f.store.get("SELECT count(*) n FROM messages WHERE kind='notice'").n,0);
  assert.equal(f.store.get("SELECT count(*) n FROM broker_meta WHERE key GLOB 'retention_blockers:*'").n,0);
  assert.equal(stream.events.some(e=>e.event==='message'&&e.data.kind==='notice'),false);assert.deepEqual(attempts,[]);
  f.store.db.exec('DROP TRIGGER fail_digest');await run();
  assert.equal(attempts.length,1);assert.equal(attempts[0].inTransaction,false);
  await stream.wait(es=>es.some(e=>e.event==='message'&&e.data.id===attempts[0].id));
});

test('growing aged unfinished backlogs do not repeat a notice until its blocker categories change',async t=>{
  const f=await fixture(t);await f.add('compact/Growing','queue');
  const send=()=>f.send({to:'topic:compact/Growing'}),first=await send();await f.run();
  const notices=()=>f.store.all("SELECT * FROM messages WHERE kind='notice'");assert.equal(notices().length,1);
  const second=await send();await f.run();assert.equal(notices().length,1);
  f.store.run("INSERT INTO webhook_outbox(message_id,payload,next_attempt) VALUES (?,'{}',0)",second.id);
  await f.run();assert.equal(notices().length,2);const body=JSON.parse(notices().at(-1).body.split(': ').slice(1).join(': '));
  assert.equal(body.unfinished,2);assert.equal(body.webhook,1);assert.deepEqual(body.unfinished_ids,[first.id,second.id]);assert.deepEqual(body.webhook_ids,[second.id]);
  await send();await f.run();assert.equal(notices().length,2);
  const blockers=candidates(f.store,f.settings,2000).topics[0].blockers;assert.equal(blockers.unfinished_count,3);assert.equal(blockers.unfinished_ids.length,2);assert.equal(blockers.truncated,true);
});

for(const category of ['dead','webhook'])test(`growing aged ${category} IDs do not repeat notices, while clearing and restoring the category does`,async t=>{
  const f=await fixture(t);if(category==='dead')await f.add('compact/Dead','queue');
  const add=async()=>{
    const m=await f.send(category==='dead'?{to:'topic:compact/Dead'}:{});
    if(category==='dead')f.store.run("UPDATE queue_items SET state='dead' WHERE message_id=?",m.id);
    else {await f.ack(m.id);f.store.run("INSERT INTO webhook_outbox(message_id,payload,next_attempt) VALUES (?,'{}',0)",m.id);}
  };
  const count=()=>f.store.get("SELECT count(*) n FROM messages WHERE kind='notice'").n;
  await add();await f.run();assert.equal(count(),1);await add();await f.run();assert.equal(count(),1);
  const blockers=candidates(f.store,f.settings,2000).topics[0].blockers;assert.equal(blockers[category+'_count'],2);
  if(category==='dead')f.store.run("UPDATE queue_items SET state='done'");else f.store.run('UPDATE webhook_outbox SET sent_at=1');
  assert.equal((await f.run()).purged,2);assert.equal(count(),1);
  await add();await f.run();assert.equal(count(),2);
});

test('human --run purges once across CLI restarts and records even empty runs without printing bodies',async t=>{
  const dir=testTempDir('retention-run-cli-');t.after(()=>rmSync(dir,{recursive:true,force:true}));const file=join(dir,'db.sqlite');
  const f=await fixture(t,{file}),m=await f.send();await f.ack(m.id);
  const run=()=>spawnSync(process.execPath,['broker/cli.mjs','retention','--run'],{env:{PATH:process.env.PATH,TMPDIR:dir,BROKER_DB:file,TOPIC_RETENTION:'1'},encoding:'utf8'});
  const first=run(),second=run();assert.equal(first.status,0,first.stderr);assert.equal(second.status,0,second.stderr);
  assert.equal(JSON.parse(first.stdout).purged,1);assert.equal(JSON.parse(second.stdout).purged,0);assert.doesNotMatch(first.stdout+second.stdout,/synthetic compaction body/);
  assert.equal(f.store.get('SELECT count(*) n FROM retention_runs WHERE finished IS NOT NULL').n,2);
});

test('automatic retention honors enabled/interval and logs guarded failures',async t=>{
  const f=await fixture(t,{retentionEnabled:false,retentionInterval:2000}),m=await f.send();await f.ack(m.id);f.tick();
  const jobs=new Jobs(f.broker);t.after(()=>jobs.stop());jobs.tick();await jobs.retaining;assert.equal(f.row(m.id).purged_at,null);
  f.settings.retentionEnabled=true;jobs.tick();await jobs.retaining;assert.notEqual(f.row(m.id).purged_at,null);
  const count=f.store.get('SELECT count(*) n FROM retention_runs').n;jobs.tick();await jobs.retaining;assert.equal(f.store.get('SELECT count(*) n FROM retention_runs').n,count);
  f.tick();jobs.tick();await jobs.retaining;assert.equal(f.store.get('SELECT count(*) n FROM retention_runs').n,count);f.tick();jobs.tick();await jobs.retaining;assert.equal(f.store.get('SELECT count(*) n FROM retention_runs').n,count+1);
  f.store.db.exec("CREATE TEMP TRIGGER fail_run BEFORE INSERT ON retention_runs BEGIN SELECT RAISE(ABORT,'retention-fault'); END");
  let threads=0,logs=[];t.mock.method(f.broker,'expireThreads',()=>{threads++;});t.mock.method(f.broker,'log',line=>logs.push(JSON.parse(line)));
  f.tick();f.tick();jobs.tick();await jobs.retaining;assert.equal(threads,1);assert.ok(logs.some(l=>l.event==='retention_maintenance_failed'));
  f.store.db.exec('DROP TRIGGER fail_run');
  for(const key of ['RETENTION_BATCH','RETENTION_MAX_BATCHES','RETENTION_BLOCKER_LIMIT','RETENTION_INTERVAL'])assert.throws(()=>config({[key]:'0'}),{code:'INVALID_'+key});
  assert.throws(()=>config({RETENTION_ENABLED:'2'}),{code:'INVALID_RETENTION_ENABLED'});
});

test('retention and queue failures cannot suppress each other or real webhook delivery',async t=>{
  let received=0;const hook=http.createServer((req,res)=>{req.resume();req.on('end',()=>{received++;res.end();});});
  await new Promise(resolve=>hook.listen(0,'127.0.0.1',resolve));t.after(()=>{hook.closeAllConnections();return new Promise(resolve=>hook.close(resolve));});
  const f=await fixture(t,{webhookUrl:'http://127.0.0.1:'+hook.address().port,retentionInterval:1,claimTtl:100,maxAttempts:1}),control=await f.send();await f.ack(control.id);
  await f.add('compact/Queue','queue');const q=await f.send({to:'topic:compact/Queue'});await f.call('/queue/claim',{id:q.id},'bob');
  const logs=[];t.mock.method(f.broker,'log',line=>logs.push(JSON.parse(line)));
  f.store.db.exec("CREATE TEMP TRIGGER fail_run BEFORE INSERT ON retention_runs BEGIN SELECT RAISE(ABORT,'retention-fault'); END");
  const jobs=new Jobs(f.broker);t.after(()=>jobs.stop());f.tick();jobs.tick();await jobs.retaining;await jobs.sending;
  assert.equal(f.store.get('SELECT state FROM queue_items WHERE message_id=?',q.id).state,'dead');assert.equal(received,1);assert.ok(logs.some(l=>l.event==='retention_maintenance_failed'));
  f.store.db.exec('DROP TRIGGER fail_run');await f.call('/queue/requeue',{id:q.id});await f.call('/queue/claim',{id:q.id},'bob');
  f.store.db.exec("CREATE TEMP TRIGGER fail_queue BEFORE UPDATE ON queue_items BEGIN SELECT RAISE(ABORT,'queue-fault'); END");
  f.tick();jobs.tick();await jobs.retaining;await jobs.sending;assert.equal(f.row(control.id).body,'');assert.ok(logs.some(l=>l.event==='queue_maintenance_failed'));
});

for(const guard of ['cursor','pin','webhook','escalated','blocking','open-age','closed-age','deleted-age',
  'decision','notice','escalation','user','ws','ready','claimed','dead'])test(`purge-time eligibility enforces ${guard} with a neighboring positive control`,async t=>{
  const f=await fixture(t,{retentionBatch:10,retentionMaxBatches:1});await f.add('compact/Control');
  const control=await f.send({to:'topic:compact/Control'});await f.ack(control.id);
  const queue=['ready','claimed','dead'].includes(guard);if(queue)await f.add('compact/Queue','queue');
  if(guard==='ws')await f.call('/workstreams/register',{name:'target'},'bob');
  const args={...(queue?{to:'topic:compact/Queue'}:{}),...(['user','ws'].includes(guard)?{to:guard==='user'?'user:bob':'ws:target'}:{}),...(guard==='blocking'?{kind:'question',blocking:true}:{})};
  const m=await f.send(args);if(!queue&&guard!=='cursor')await f.ack(m.id);
  if(guard==='pin')await f.call('/topics/keep',{id:m.id},'bob');
  if(guard==='webhook')f.store.run("INSERT INTO webhook_outbox(message_id,payload,next_attempt) VALUES (?,'{}',0)",m.id);
  if(guard==='escalated')f.store.run("UPDATE threads SET status='escalated' WHERE id=?",m.thread_id);
  if(['decision','notice','escalation'].includes(guard))f.store.run('UPDATE messages SET kind=? WHERE id=?',guard,m.id);
  if(['user','ws'].includes(guard)){f.store.run('UPDATE messages SET to_name=? WHERE id=?',f.topic,m.id);f.store.run('UPDATE topic_cursors SET acked_through=? WHERE topic=?',m.id,f.topic);}
  if(guard==='open-age')f.store.run('UPDATE messages SET created=1950 WHERE id=?',m.id);
  if(guard==='closed-age')f.store.run('UPDATE topics SET closed_at=1990 WHERE name=?',f.topic);
  if(guard==='deleted-age') {
    // Archive affects every topic in a workspace; put the control in another one.
    await f.call('/workspaces',{name:'other'});f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
    await f.add(f.topic,'broadcast','other');
    f.store.run("UPDATE messages SET workspace='other' WHERE id=?",m.id);f.store.run("UPDATE threads SET workspace='other' WHERE id=?",m.thread_id);
    f.store.run("UPDATE topic_cursors SET acked_through=? WHERE workspace='other'",m.id);
    f.store.run("UPDATE workspaces SET deleted_at=1990 WHERE name='other'");
  }
  if(['claimed','dead'].includes(guard))await f.call('/queue/claim',{id:m.id},'bob');
  if(guard==='dead')f.store.run("UPDATE queue_items SET state='dead' WHERE message_id=?",m.id);
  const original=f.row(m.id);assert.equal((await f.run()).purged,1);assert.equal(f.row(control.id).body,'');assert.deepEqual(f.row(m.id),original);
  if(['decision','notice','escalation','user','ws'].includes(guard))return;
  if(guard==='cursor')await f.ack(m.id);
  if(guard==='pin')await f.call('/topics/unkeep',{id:m.id},'bob');
  if(guard==='webhook')f.store.run('UPDATE webhook_outbox SET sent_at=1 WHERE message_id=?',m.id);
  if(['escalated','blocking'].includes(guard))f.store.run("UPDATE threads SET status='resolved' WHERE id=?",m.thread_id);
  if(guard==='open-age')f.store.run('UPDATE messages SET created=1000 WHERE id=?',m.id);
  if(guard==='closed-age')f.store.run('UPDATE topics SET closed_at=1000 WHERE name=?',f.topic);
  if(guard==='deleted-age')f.store.run("UPDATE workspaces SET deleted_at=1000 WHERE name='other'");
  if(queue)f.store.run("UPDATE queue_items SET state='done' WHERE message_id=?",m.id);
  assert.equal((await f.run()).purged,1);assert.equal(f.row(m.id).body,'');
});

test('purge-time epoch guard protects both open and closed topic ages until the safety period ends',async t=>{
  const f=await fixture(t,{retentionBatch:10}),m=await f.send();await f.ack(m.id);
  f.store.run("UPDATE broker_meta SET value='1980' WHERE key='retention_epoch'");
  assert.equal((await f.run()).purged,0);assert.equal(f.row(m.id).purged_at,null);
  await f.call('/topics/close',{name:f.topic});assert.equal((await f.run()).purged,0);
  f.store.run("UPDATE broker_meta SET value='0' WHERE key='retention_epoch'");assert.equal((await f.run()).purged,1);
});

test('purge-time unanswered ask requires a later answer kind from another user on the same thread',async t=>{
  const f=await fixture(t,{retentionBatch:100,retentionMaxBatches:1}),first=await f.send();await f.ack(first.id);
  const early=await f.send({thread_id:first.thread_id,kind:'answer'},'bob');await f.ack(early.id,'alice');
  const ask=await f.send({thread_id:first.thread_id,kind:'question',blocking:true});await f.ack(ask.id);
  const own=await f.send({thread_id:first.thread_id,kind:'answer'});await f.ack(own.id);
  const other=await f.send();await f.ack(other.id);const unrelated=await f.send({thread_id:other.thread_id,kind:'answer'},'bob');await f.ack(unrelated.id,'alice');
  assert.equal((await f.run()).purged,2);assert.equal(f.row(ask.id).purged_at,null);
  const finding=await f.send({thread_id:first.thread_id},'bob');await f.ack(finding.id,'alice');
  assert.equal((await f.run()).purged,0);
  const answer=await f.send({thread_id:first.thread_id,kind:'answer'},'bob');await f.ack(answer.id,'alice');
  assert.equal((await f.run()).purged,6);
});

test('purge-time resolution and real reopen respect the previous cycle watermark and protect new asks',async t=>{
  const dir=testTempDir('compact-cycle-');t.after(()=>rmSync(dir,{recursive:true,force:true}));const file=join(dir,'db.sqlite'),f=await fixture(t,{file});
  const cli=(action,id)=>{
    const result=spawnSync(process.execPath,['broker/cli.mjs',action,id,...(action==='reopen'?['--reason','cycle test']:[])],{env:{PATH:process.env.PATH,TMPDIR:dir,BROKER_DB:file},encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout);
  };
  const old=await f.send({kind:'question',blocking:true});await f.ack(old.id);assert.equal((await f.run()).purged,0);
  cli('resolve',old.thread_id);assert.equal(cli('reopen',old.thread_id).cycle_start_id,old.id);
  assert.equal((await f.run()).purged,1);const saved=f.row(old.id);
  const ask=await f.send({thread_id:old.thread_id,kind:'question',blocking:true});await f.ack(ask.id);
  assert.equal((await f.run()).purged,0);assert.equal(f.row(ask.id).purged_at,null);assert.deepEqual(f.row(old.id),saved);
  cli('resolve',old.thread_id);assert.equal((await f.run()).purged,1);assert.deepEqual(f.row(old.id),saved);
});

test('purge-time contiguous cursors and sparse acks are independent, scoped alternatives',async t=>{
  const f=await fixture(t,{retentionBatch:100});await f.add('compact/Other');
  const gap=await f.send(),acked=await f.send();await f.ack(acked.id);
  assert.equal((await f.run()).purged,1);assert.equal(f.row(acked.id).body,'');assert.equal(f.row(gap.id).purged_at,null);
  await f.ack(gap.id);f.store.run('DELETE FROM topic_acks WHERE message_id=?',gap.id);
  assert.equal((await f.run()).purged,1); // Other topic's zero cursor must not block this one.
  const pending=await f.send();assert.equal((await f.run()).purged,0);await f.ack(pending.id);assert.equal((await f.run()).purged,1);
});

test('purge-time same-name workspace cursors are independent and stale non-member subscriptions do not block',async t=>{
  const f=await fixture(t,{retentionBatch:100}),m=await f.send();await f.ack(m.id);f.store.run('DELETE FROM topic_acks WHERE message_id=?',m.id);
  await f.call('/workspaces',{name:'other'});f.store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'invite')");
  await f.add(f.topic,'broadcast','other');const pending=await f.send({},'alice','other');
  assert.equal((await f.run()).purged,1);assert.equal(f.row(m.id).body,'');assert.equal(f.row(pending.id).purged_at,null);
  f.store.run("DELETE FROM workspace_members WHERE workspace='other' AND user='bob'");
  assert.equal((await f.run()).purged,1);assert.equal(f.row(pending.id).body,'');
});

test('each purging transaction uses an ID-bounded eligibility statement',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);
  const transaction=f.store.transaction.bind(f.store),all=f.store.all.bind(f.store),traces=[];let active;
  t.mock.method(f.store,'transaction',fn=>{
    const trace={eligibility:[],purging:false};traces.push(trace);
    return transaction(()=>{active=trace;try{return fn();}finally{active=null;}});
  });
  t.mock.method(f.store,'all',(sql,...args)=>{
    if(active&&sql.includes('AS unfinished'))active.eligibility.push({sql,plan:all('EXPLAIN QUERY PLAN '+sql,...args)});
    if(active&&sql.includes("UPDATE messages SET body=''"))active.purging=true;
    return all(sql,...args);
  });
  assert.equal((await f.run()).purged,1);
  const batches=traces.filter(t=>t.purging);assert.ok(batches.length);
  for(const batch of batches) {
    assert.equal(batch.eligibility.length,1);assert.match(batch.eligibility[0].sql,/m\.id IN \(\?/);
    assert.ok(batch.eligibility[0].plan.some(r=>/SEARCH m USING INTEGER PRIMARY KEY \(rowid=\?\)/.test(r.detail)));
  }
  const blockerScans=traces.filter(t=>!t.purging&&t.eligibility.length);
  assert.equal(blockerScans.length,1);assert.equal(blockerScans[0].eligibility.length,1);
});

test('persisted page cursor reaches eligible rows behind more than one run of blocked pages',async t=>{
  const f=await fixture(t);for(let i=0;i<6;i++)await f.send();
  const eligible=await f.send();await f.ack(eligible.id);
  assert.equal((await f.run()).purged,0);
  assert.equal(f.store.get("SELECT value FROM broker_meta WHERE key='retention_cursor'").value,'4');
  assert.equal((await f.run()).purged,1);assert.equal(f.row(eligible.id).body,'');
  assert.equal(f.store.get("SELECT value FROM broker_meta WHERE key='retention_cursor'").value,'0');
});

test('page cursor wraps within the remaining budget but does not rescan past its starting watermark',async t=>{
  const f=await fixture(t,{retentionBatch:2,retentionMaxBatches:10});const ids=[];
  for(let i=0;i<3;i++){const m=await f.send();await f.ack(m.id);ids.push(m.id);}
  f.store.run("INSERT INTO broker_meta(key,value) VALUES ('retention_cursor',?)",String(ids[1]));
  const all=f.store.all.bind(f.store),pages=[];
  t.mock.method(f.store,'all',(sql,...args)=>{const rows=all(sql,...args);if(sql.startsWith('SELECT m.id FROM messages'))pages.push(rows.map(r=>r.id));return rows;});
  let later;
  const run=await compact(f.store,f.settings,2000,{log:()=>{},yieldBatch:async()=>{
    if(!later){later=await f.send();await f.ack(later.id);}
  }});
  assert.equal(run.purged,3);assert.equal(f.row(later.id).purged_at,null);
  assert.deepEqual(pages,[[ids[2]],[ids[0],ids[1]]]);
  assert.equal(f.store.get("SELECT value FROM broker_meta WHERE key='retention_cursor'").value,'0');
  assert.equal((await f.run()).purged,1);
});

test('missing or malformed advisory cursors reset safely with metadata-only logs',async t=>{
  const f=await fixture(t),m=await f.send();await f.ack(m.id);
  for(const value of [undefined,'invalid-private-marker','-1','1.5','9007199254740992']) {
    f.store.run("DELETE FROM broker_meta WHERE key='retention_cursor'");
    if(value!==undefined)f.store.run("INSERT INTO broker_meta(key,value) VALUES ('retention_cursor',?)",value);
    const logs=[];await compact(f.store,f.settings,2000,{log:event=>logs.push(event)});
    assert.deepEqual(logs,[{event:'retention_cursor_reset',reason:value===undefined?'missing':'invalid'}]);
  }
  assert.equal(f.row(m.id).body,'');
});

test('automatic multi-batch compaction yields so a concurrent real HTTP writer can pin an unprocessed row',async t=>{
  const f=await fixture(t,{retentionBatch:1,retentionMaxBatches:100}),ids=[];
  for(let i=0;i<40;i++){const m=await f.send();await f.ack(m.id);ids.push(m.id);}
  f.tick();const jobs=new Jobs(f.broker);t.after(()=>jobs.stop());
  const pending=f.request('/topics/keep',{user:'bob',session:'bob-default',method:'POST',body:JSON.stringify({id:ids.at(-1)})});
  jobs.tick();await jobs.retaining;const response=await pending;assert.equal(response.status,200);
  await jobs.retaining;
  assert.equal(f.row(ids[0]).body,'');assert.equal(f.row(ids.at(-1)).body,'synthetic compaction body');
  assert.equal(f.store.get('SELECT count(*) n FROM messages WHERE purged_at IS NOT NULL').n,39);
});
