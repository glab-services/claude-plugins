import {topicHeartbeat} from '../../test-support/topic-fixture.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { writeFileSync,rmSync } from 'node:fs';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { Jobs } from '../jobs.mjs';
import { testTempDir } from '../../test-support/temp.mjs';
import { eventually } from './http.mjs';

test('two broker processes claim one due webhook row before sending to a counting receiver',async t=>{
  const dir=testTempDir('webhook-two-brokers-'),file=join(dir,'db.sqlite'),tokenPath=join(dir,'tokens.json');
  const tokens={alice:'webhook-alice-synthetic-token-0000',bob:'webhook-bob-synthetic-token-00000'};
  writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});let posts=0,release;const gate=new Promise(resolve=>{release=resolve;});
  const receiver=http.createServer(async(req,res)=>{posts++;req.resume();await gate;res.end();});
  await new Promise(resolve=>receiver.listen(0,'127.0.0.1',resolve));const hook='http://127.0.0.1:'+receiver.address().port,children=[];
  t.after(async()=>{release();for(const p of children){if(p.child.exitCode===null&&p.child.signalCode===null)p.child.kill('SIGTERM');await p.exited;}receiver.closeAllConnections();await new Promise(resolve=>receiver.close(resolve));rmSync(dir,{recursive:true,force:true});});
  const store=new Store(file),broker=new Broker(store,{...config({}),webhookUrl:hook,threadCap:1},['alice','bob'],{log:()=>{}});
  topicHeartbeat(broker,'alice','producer');broker.send('alice','producer',{to:'user:bob',body:'synthetic work'});store.close();
  async function start() {
    const child=spawn(process.execPath,['broker/main.mjs'],{env:{TMPDIR:dir,BROKER_HOST:'127.0.0.1',BROKER_PORT:'0',BROKER_DB:file,BROKER_TOKENS:tokenPath,BROKER_WEBHOOK_URL:hook,RETENTION_ENABLED:'0',MAINTENANCE_INTERVAL_MS:'10'},stdio:['ignore','ignore','pipe']});
    const exited=new Promise(resolve=>child.once('exit',resolve));children.push({child,exited});let log='',port;
    child.stderr.on('data',b=>{log+=b;});await eventually(()=>{for(const line of log.split('\n')){try{const value=JSON.parse(line);if(value.event==='broker_listening')port=value.address.port;}catch{}}return Boolean(port);});
  }
  await Promise.all([start(),start()]);await eventually(()=>posts>0);
  await new Promise(resolve=>setTimeout(resolve,100));assert.equal(posts,1);release();
  const result=new Store(file);try {
    await eventually(()=>result.get('SELECT sent_at FROM webhook_outbox').sent_at!==null);
    assert.equal(result.get('SELECT attempts FROM webhook_outbox').attempts,1);assert.equal(posts,1);
  } finally {result.close();}
});

test('a stale due-row snapshot cannot claim a webhook already leased by another broker',async t=>{
  const dir=testTempDir('webhook-stale-snapshot-'),file=join(dir,'db.sqlite');let posts=0,release;
  const gate=new Promise(resolve=>{release=resolve;}),receiver=http.createServer(async(req,res)=>{posts++;req.resume();await gate;res.end();});
  await new Promise(resolve=>receiver.listen(0,'127.0.0.1',resolve));
  const a=new Store(file),b=new Store(file),settings={...config({}),threadCap:1,webhookUrl:'http://127.0.0.1:'+receiver.address().port};
  const brokerA=new Broker(a,settings,['alice','bob'],{now:()=>1000,log:()=>{}}),brokerB=new Broker(b,settings,['alice','bob'],{now:()=>1000,log:()=>{}});
  const jobsA=new Jobs(brokerA),jobsB=new Jobs(brokerB);
  t.after(async()=>{release();await jobsA.stop();await jobsB.stop();receiver.closeAllConnections();await new Promise(resolve=>receiver.close(resolve));b.close();a.close();rmSync(dir,{recursive:true,force:true});});
  topicHeartbeat(brokerA,'alice','stale-producer');brokerA.send('alice','stale-producer',{to:'user:bob',body:'stale snapshot regression'});
  const all=b.all.bind(b);let pendingA,stale=false;
  t.mock.method(b,'all',(sql,...args)=>{
    const rows=all(sql,...args);
    if(sql.startsWith('SELECT * FROM webhook_outbox')) {
      assert.equal(rows.length,1);assert.ok(rows[0].next_attempt<=1000);stale=true;
      // A leases synchronously before its first network await; B still holds
      // the due snapshot obtained above, using an independent SQLite connection.
      pendingA=jobsA.deliver();assert.ok(a.get('SELECT next_attempt FROM webhook_outbox').next_attempt>1000);
    }
    return rows;
  });
  const pendingB=jobsB.deliver();await eventually(()=>posts>0);release();await Promise.all([pendingA,pendingB]);
  assert.equal(stale,true);assert.equal(posts,1);assert.equal(a.get('SELECT attempts FROM webhook_outbox').attempts,1);
});
