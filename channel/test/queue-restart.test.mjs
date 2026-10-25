import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Store } from '../../broker/store.mjs';
import { session, eventually } from './helpers.mjs';
import { setTimeout as sleep } from 'node:timers/promises';
import { queueProcess } from './fixtures/queue-process.mjs';

for(const operation of ['claim','done'])for(const phase of ['before_commit','after_commit']) {
  test(`SIGKILL mid-${operation} ${phase} preserves one durable queue transition`,{timeout:20000},async t=>{
    const f=await queueProcess(t),id=await f.send();
    if(operation==='done')assert.equal((await f.call('/queue/claim',{id},'bob')).status,200);
    const before=await f.read(id);await f.kill();const paused=await f.start(operation,phase);
    const pending=f.call('/queue/'+operation,{id,...(operation==='done'?{generation:before.generation,result:'durable result'}:{})},'bob').catch(()=>null);
    await paused.barrier();await f.kill();await pending;await f.start();
    const row=await f.read(id);
    if(phase==='before_commit')assert.deepEqual(row,before);
    if(operation==='claim') {
      if(phase==='before_commit')assert.equal((await f.call('/queue/claim',{id},'bob')).data.generation,1);
      else {assert.equal(row.state,'claimed');assert.equal(row.generation,1);assert.equal(row.attempts,1);assert.equal((await f.call('/queue/claim',{id},'bob')).data.error,'ITEM_CLAIMED');}
      assert.equal((await f.call('/queue/done',{id,generation:1,result:'durable result'},'bob')).data.changed,true);
    } else {
      const repeated=await f.call('/queue/done',{id,generation:1,result:'durable result'},'bob');assert.equal(repeated.status,200);
      assert.equal(repeated.data.changed,phase==='before_commit');
    }
    const complete=await f.read(id);assert.equal(complete.state,'done');assert.equal(complete.generation,1);assert.equal(complete.attempts,1);assert.equal(complete.done_generation,1);assert.equal(complete.result,'durable result');
    await f.kill();await f.start();assert.equal((await f.call('/queue/done',{id,generation:1,result:'durable result'},'bob')).data.changed,false);
  });
}


async function waitState(f,id,state) {
  const end=Date.now()+6000;
  while((await f.read(id)).state!==state){if(Date.now()>end)throw Error('Queue recovery state timed out');await sleep(10);}
}
for(const phase of ['before_commit','after_commit'])test(`SIGKILL mid-requeue ${phase} preserves its budget, fence and audit`,{timeout:20000},async t=>{
  const f=await queueProcess(t,{ttl:'1',attempts:'1'}),id=await f.send();await f.call('/queue/claim',{id},'bob');await waitState(f,id,'dead');
  const before=await f.read(id);await f.kill();const paused=await f.start('requeue',phase);
  const pending=f.call('/queue/requeue',{id}).catch(()=>null);await paused.barrier();await f.kill();await pending;await f.start();
  const after=await f.read(id);
  if(phase==='before_commit'){assert.deepEqual(after,before);assert.equal((await f.call('/queue/requeue',{id})).status,200);}
  else {assert.equal(after.state,'ready');assert.equal(after.attempts,0);assert.equal(after.generation,before.generation+1);assert.equal((await f.call('/queue/requeue',{id})).data.error,'ITEM_NOT_DEAD');}
  const store=new Store(f.file);try{assert.equal(store.get("SELECT count(*) n FROM workspace_audit WHERE action='queue:requeue'").n,1);}finally{store.close();}
  const claim=await f.call('/queue/claim',{id},'bob');assert.equal(claim.data.generation,before.generation+2);
  assert.equal((await f.call('/queue/done',{id,generation:claim.data.generation,result:'requeued completion'},'bob')).data.changed,true);
  assert.equal((await f.read(id)).attempts,1);
});

test('SIGKILL with a committed claim replays expiry to its former stdio holder and accepts one later completion',{timeout:20000},async t=>{
  const f=await queueProcess(t,{ttl:'1'}),receiver=await session(f,'bob','worker',{CHANNEL_RETRY_MAX_MS:'100',CHANNEL_POLL_MS:'20'}),id=await f.send();
  await eventually(()=>receiver.notifications.some(n=>n.meta.message_id===String(id)&&n.meta.claim_generation==='0'));
  const before=await f.read(id);assert.equal((await receiver.call('wait',{thread_id:before.message.thread_id,timeout_s:0})).structuredContent.messages.length,1);
  assert.equal((await receiver.call('claim',{id})).structuredContent.generation,1);await f.kill();await f.start();
  await eventually(()=>receiver.notifications.some(n=>n.meta.message_id===String(id)&&n.meta.claim_generation==='2'),6000);
  for(const name of ['done','renew'])assert.equal((await receiver.call(name,{id,generation:1})).structuredContent.error,'CLAIM_STALE');
  assert.deepEqual((await receiver.call('wait',{thread_id:before.message.thread_id,timeout_s:0})).structuredContent.messages.map(m=>m.claim_generation),[2]);
  assert.equal((await receiver.call('claim',{id})).structuredContent.generation,3);
  assert.equal((await receiver.call('done',{id,generation:3,result:'recovered'})).structuredContent.changed,true);
  await f.kill();await f.start();
  assert.equal((await receiver.call('done',{id,generation:3,result:'recovered'})).structuredContent.changed,false);
  const row=await f.read(id);assert.equal(row.attempts,2);assert.equal(row.generation,3);assert.equal(row.done_generation,3);assert.equal(row.state,'done');
});
