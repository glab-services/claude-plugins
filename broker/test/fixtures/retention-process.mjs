import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { writeFileSync,rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Store } from '../../store.mjs';
import { DatabaseSync } from 'node:sqlite';
import { Broker } from '../../broker.mjs';
import { config } from '../../config.mjs';
import { testTempDir } from '../../../test-support/temp.mjs';
async function eventually(predicate) {
  const deadline=Date.now()+6000;
  while(!predicate()) {
    if(Date.now()>=deadline)throw Error('Timed out waiting for retention child');
    await new Promise(resolve=>setTimeout(resolve,10));
  }
}

export function retentionProcess(t,{count=7}={}) {
  const dir=testTempDir('retention-process-'),file=join(dir,'db.sqlite'),tokenPath=join(dir,'tokens.json'),release=join(dir,'release');
  const tokens={alice:'retention-alice-synthetic-token-000',bob:'retention-bob-synthetic-token-00000'};
  writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});
  const store=new Store(file);new Broker(store,config({}),['alice','bob'],{log:()=>{}});
  store.run("UPDATE broker_meta SET value='0' WHERE key='retention_epoch'");
  store.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('default','process','broadcast','alice',0)");
  for(const user of ['alice','bob']) {
    store.run("INSERT INTO topic_subscriptions VALUES ('default','process',?,0)",user);
    store.run("INSERT INTO topic_cursors VALUES ('default','process',?,999999)",user);
  }
  const ids=[];
  store.transaction(()=>{for(let i=0;i<count;i++) {
    const thread='process-'+i;
    store.run("INSERT INTO threads(id,status,created,last_activity,workspace,topic) VALUES (?,'open',0,0,'default','process')",thread);
    ids.push(Number(store.run("INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace,topic) VALUES (?,'alice','fixture','topic','process','finding',0,'durable body',0,'default','process')",thread).lastInsertRowid));
  }});
  const pinned=ids.at(-1);store.run("INSERT INTO topic_pins VALUES (?,'bob','default',0)",pinned);store.close();
  const children=[];
  t.after(async()=>{for(const p of children){if(p.child.exitCode===null&&p.child.signalCode===null)p.child.kill('SIGKILL');await p.exited;}rmSync(dir,{recursive:true,force:true});});
  const start=({broker=false,phase,batch=1,enabled=true,maxBatches=20}={})=>{
    const child=fork(fileURLToPath(new URL(broker?'../../main.mjs':'../../cli.mjs',import.meta.url)),broker?[]:['retention','--run'],{
      execArgv:phase?['--import',fileURLToPath(new URL('./retention-crash-loader.mjs',import.meta.url))]:[],
      env:{TMPDIR:dir,BROKER_DB:file,BROKER_TOKENS:tokenPath,BROKER_HOST:'127.0.0.1',BROKER_PORT:'0',BROKER_WEBHOOK_URL:'',
        RETENTION_ENABLED:enabled?'1':'0',TOPIC_RETENTION:'1',CLOSED_RETENTION:'1',RETENTION_BATCH:'2',RETENTION_MAX_BATCHES:String(maxBatches),
        RETENTION_INTERVAL:'999999',MAINTENANCE_INTERVAL_MS:'60000',IDLE_CLOSE:'9999999999',BLOCKING_TIMEOUT:'9999999999',
        ...(phase?{RETENTION_TEST_PHASE:phase,RETENTION_TEST_BATCH:String(batch),RETENTION_TEST_RELEASE:release}:{})},
      stdio:['ignore','pipe','pipe','ipc'],
    });
    let reached=false,stdout='',stderr='';
    child.on('message',m=>{if(m.event==='retention_test_barrier')reached=true;});
    child.stdout.on('data',b=>{stdout+=b;});child.stderr.on('data',b=>{stderr+=b;});
    const exited=new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));
    const p={child,exited,barrier:()=>eventually(()=>reached),kill:async()=>{child.kill('SIGKILL');await exited;},
      result:async()=>{const status=await exited;assert.equal(status.code,0,stderr);return JSON.parse(stdout);},
      listening:async()=>{let address;await eventually(()=>{for(const line of stderr.split('\n')){try{const value=JSON.parse(line);if(value.event==='broker_listening')address=value.address;}catch{}}return Boolean(address);});return 'http://127.0.0.1:'+address.port;}};
    children.push(p);return p;
  };
  const inspect=fn=>{const s=new Store(file);try{return fn(s);}finally{s.close();}};
  const call=async(url,path,args)=>{
    const res=await fetch(url+path,{method:'POST',headers:{Authorization:'Bearer '+tokens.bob,'X-Session-Id':'process-bob','X-Topic':'process','Content-Type':'application/json'},body:JSON.stringify(args)});
    return {status:res.status,data:await res.json()};
  };
  const waitFinished=async count=>eventually(()=>{
    const db=new DatabaseSync(file,{readOnly:true});
    try{return db.prepare('SELECT count(*) n FROM retention_runs WHERE finished IS NOT NULL').get().n===count;}finally{db.close();}
  });
  return {file,ids,pinned,start,inspect,call,waitFinished,release:()=>writeFileSync(release,'go',{mode:0o600})};
}
