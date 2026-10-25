import {prepareTopicFixture,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import {Broker} from '../broker.mjs';
import {config} from '../config.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { testTempDir } from '../../test-support/temp.mjs';
import { Store } from '../store.mjs';
import { eventually } from './http.mjs';

test('two broker processes sharing SQLite serialize message writes and competing claims', async t => {
  const dir=testTempDir('two-brokers-');
  const db=join(dir,'broker.sqlite'), tokenPath=join(dir,'tokens.json');
  const tokens={alice:'alice-concurrency-fixture-000000000000',bob:'bob-concurrency-fixture-00000000000000'};
  writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});
  const initial=new Store(db);prepareTopicFixture(new Broker(initial,config({}),['alice','bob'],{log:()=>{}}));initial.close();
  const processes=[];
  t.after(async()=>{
    for(const {child,exited} of processes){ if(child.exitCode===null&&child.signalCode===null)child.kill('SIGTERM'); await exited; }
    rmSync(dir,{recursive:true,force:true});
  });
  async function start(){
    const child=spawn(process.execPath,['broker/main.mjs'],{env:{BROKER_PORT:'0',BROKER_HOST:'127.0.0.1',BROKER_DB:db,BROKER_TOKENS:tokenPath},stdio:['ignore','ignore','pipe']});
    let text='',port;
    child.stderr.on('data',chunk=>{text+=chunk;});
    const exited=new Promise(resolve=>child.once('exit',resolve));
    processes.push({child,exited});
    await eventually(()=>{for(const line of text.split('\n')){try{const value=JSON.parse(line);if(value.event==='broker_listening')port=value.address.port;}catch{}}return Boolean(port);});
    return `http://127.0.0.1:${port}`;
  }
  const urls=await Promise.all([start(),start()]);
  const post=(url,path,user,session,body)=>fetch(url+path,{method:'POST',headers:{Authorization:`Bearer ${tokens[user]}`,'X-Session-Id':session,'X-Topic':FIXTURE_TOPIC,'Content-Type':'application/json'},body:JSON.stringify(body)});
  const responses=await Promise.all(Array.from({length:40},(_,i)=>post(urls[i%2],'/send','alice',`a${i%2}`,{to:'user:bob',body:`write ${i}`})));
  const ids=[];
  for(const response of responses){assert.equal(response.status,201);ids.push((await response.json()).id);}
  assert.equal(new Set(ids).size,40);
  const claims=await Promise.all(urls.map((url,i)=>post(url,'/ack','bob',`b${i}`,{id:ids[0]})));
  assert.deepEqual(claims.map(r=>r.status).sort(),[200,409]);
  const winner=claims.findIndex(r=>r.status===200);
  assert.equal((await claims[1-winner].json()).error,'MESSAGE_CLAIMED');
  const resume=await fetch(urls[1-winner]+'/resume',{headers:{Authorization:`Bearer ${tokens.bob}`,'X-Session-Id':`b${winner}`,'X-Topic':FIXTURE_TOPIC}});
  assert.equal((await resume.json()).acked_id,ids[0]);
  const store=new Store(db);
  try{
    assert.equal(store.get('SELECT count(*) n FROM messages').n,40);
    assert.equal(store.get('SELECT acked_by_session FROM messages WHERE id=?',ids[0]).acked_by_session,`b${winner}`);
    assert.deepEqual(store.all('PRAGMA foreign_key_check'),[]);
    assert.equal(store.get('PRAGMA integrity_check').integrity_check,'ok');
  }finally{store.close();}
});
