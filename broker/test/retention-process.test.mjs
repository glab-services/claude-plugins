import { test } from 'node:test';
import assert from 'node:assert/strict';
import { retentionProcess } from './fixtures/retention-process.mjs';

for(const phase of ['before_commit','after_commit'])for(const batch of [1,2])test(`SIGKILL retention ${phase} at batch ${batch} preserves atomic tombstone/audit/counter state and resumes once`,async t=>{
  const f=retentionProcess(t),child=f.start({phase,batch});await child.barrier();await child.kill();
  const expected=2*(batch-(phase==='before_commit'?1:0));
  const before=f.inspect(s=>({rows:s.all('SELECT * FROM messages'),run:s.get('SELECT * FROM retention_runs'),audit:s.all("SELECT * FROM workspace_audit WHERE action='retention:purge'")}));
  assert.equal(before.rows.filter(r=>r.purged_at!==null).length,expected);assert.equal(before.run.purged,expected);assert.equal(before.run.finished,null);
  assert.equal(before.audit.reduce((n,r)=>n+JSON.parse(r.detail).count,0),expected);
  for(const r of before.rows.filter(r=>f.ids.includes(r.id))){assert.equal(r.purged_at===null,r.purge_run_id===null);assert.equal(r.body,r.purged_at===null?'durable body':'');}
  for(const r of before.rows.filter(r=>r.kind==='notice'))assert.equal(r.purged_at,null);
  const resumed=await f.start().result();assert.equal(resumed.purged,6-expected);assert.equal((await f.start().result()).purged,0);
  f.inspect(s=>{
    assert.equal(s.get('SELECT sum(purged) n FROM retention_runs').n,6);
    assert.equal(s.get('SELECT count(*) n FROM messages WHERE purged_at IS NOT NULL').n,6);
    assert.equal(s.get('SELECT purged_at FROM messages WHERE id=?',f.pinned).purged_at,null);
    assert.equal(s.all("SELECT detail FROM workspace_audit WHERE action='retention:purge'").reduce((n,r)=>n+JSON.parse(r.detail).count,0),6);
    for(const r of before.rows.filter(r=>r.purged_at!==null))assert.deepEqual(s.get('SELECT * FROM messages WHERE id=?',r.id),r);
  });
});

test('regression guard: two actual broker processes compact concurrently without duplicate purges or touching a pinned row',async t=>{
  // BEGIN IMMEDIATE serializes each candidate evaluation plus guarded UPDATE;
  // the two test preloads release both brokers before their first batch lock.
  const f=retentionProcess(t,{count:41}),a=f.start({broker:true,phase:'before_batch',maxBatches:40}),b=f.start({broker:true,phase:'before_batch',maxBatches:40});
  await Promise.all([a.barrier(),b.barrier()]);f.release();await Promise.all([a.listening(),b.listening()]);await f.waitFinished(2);await a.kill();await b.kill();
  f.inspect(s=>{
    const runs=s.all('SELECT * FROM retention_runs');assert.equal(runs.length,2);assert.ok(runs.every(r=>r.finished!==null));
    assert.equal(runs.reduce((n,r)=>n+r.purged,0),40);
    assert.equal(s.get('SELECT count(*) n FROM messages WHERE purged_at IS NOT NULL').n,40);
    assert.equal(s.get('SELECT body FROM messages WHERE id=?',f.pinned).body,'durable body');
    const audited=s.all("SELECT detail FROM workspace_audit WHERE action='retention:purge'").map(r=>JSON.parse(r.detail));
    assert.equal(audited.reduce((n,r)=>n+r.count,0),40);
    for(const id of f.ids.slice(0,-1))assert.equal(audited.filter(r=>r.min_id<=id&&id<=r.max_id).length,1);
  });
});

test('a pin committed over real HTTP after preflight but before the batch lock is re-evaluated before purge',async t=>{
  const f=retentionProcess(t),compactor=f.start({phase:'before_batch'});await compactor.barrier();
  const broker=f.start({broker:true,enabled:false}),url=await broker.listening();
  assert.equal((await f.call(url,'/topics/keep',{id:f.ids[0]})).status,200);f.release();
  const result=await compactor.result();await broker.kill();
  f.inspect(s=>{
    for(const id of [f.ids[0],f.pinned]){const r=s.get('SELECT * FROM messages WHERE id=?',id);assert.equal(r.body,'durable body');assert.equal(r.purged_at,null);}
    assert.equal(s.get('SELECT count(*) n FROM messages WHERE purged_at IS NOT NULL').n,5);
  });
  assert.equal(result.purged,5);
});
