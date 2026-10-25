import {Store} from '../../../broker/store.mjs';
import { fork } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { testTempDir } from '../../../test-support/temp.mjs';
import { tokens, eventually } from '../helpers.mjs';

export async function queueProcess(t,{ttl='60',attempts='3'}={}) {
  const dir=testTempDir('queue-recovery-crash-'),file=join(dir,'broker.sqlite'),tokenPath=join(dir,'tokens.json');
  writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});
  const children=[],f={clients:[],file,topic:'crash/queue',bindingTopic:'crash/queue'};let port=0,active;
  t.after(async()=>{
    for(const client of f.clients)await client.close();
    for(const p of children){if(p.child.exitCode===null&&p.child.signalCode===null)p.child.kill('SIGKILL');await p.exited;}
    rmSync(dir,{recursive:true,force:true});
  });
  f.start=async(operation,phase)=>{
    // Without TMPDIR the child falls back to this test's own scratch dir, so its
    // temp files stay inside the test directory on any machine.
    const child=fork(fileURLToPath(new URL('../../../broker/main.mjs',import.meta.url)),[],{
      execArgv:operation?['--import',fileURLToPath(new URL('./queue-crash-loader.mjs',import.meta.url))]:[],
      env:{TMPDIR:process.env.TMPDIR??dir,BROKER_WEBHOOK_URL:'',BROKER_HOST:'127.0.0.1',BROKER_PORT:String(port),BROKER_DB:file,BROKER_TOKENS:tokenPath,
        CLAIM_TTL:ttl,MAX_ATTEMPTS:attempts,MAINTENANCE_INTERVAL_MS:'20',SSE_HEARTBEAT:'1',
        ...(operation?{QUEUE_TEST_OPERATION:operation,QUEUE_TEST_PHASE:phase}:{})},
      stdio:['ignore','ignore','pipe','ipc'],
    });
    const exited=new Promise(resolve=>child.once('exit',resolve));let reached=false;
    child.on('message',message=>{if(message.event==='queue_test_barrier')reached=true;});
    active={child,exited};children.push(active);let log='',listening;
    child.stderr.on('data',chunk=>{log+=chunk;});
    await eventually(()=>{for(const line of log.split('\n')){try{const row=JSON.parse(line);if(row.event==='broker_listening')listening=row.address.port;}catch{}}return Boolean(listening);},6000);
    port=listening;f.url=`http://127.0.0.1:${port}`;
    return {barrier:()=>eventually(()=>reached,6000)};
  };
  f.kill=async()=>{active.child.kill('SIGKILL');await active.exited;};
  f.call=async(path,args,user='alice',session=user==='alice'?'producer':'worker')=>{
    const response=await fetch(f.url+path,{method:args===undefined?'GET':'POST',headers:{Authorization:`Bearer ${tokens[user]}`,'X-Session-Id':session,...(user==='alice'?{}:{'X-Topic':f.topic}),'Content-Type':'application/json'},...(args===undefined?{}:{body:JSON.stringify(args)})});
    return {status:response.status,data:await response.json()};
  };
  await f.start();
  await f.call('/topics',{name:f.topic,mode:'queue'});const setup=new Store(file);setup.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',f.topic,'bob',Date.now());setup.close();for(const user of ['alice','bob'])await f.call('/topics/subscribe',{name:f.topic},user);
  f.send=async()=>{const sent=await f.call('/send',{to:'topic:'+f.topic,body:'durable work'});if(sent.status!==201)throw Error('Queue fixture send failed');return sent.data.id;};
  f.read=async id=>(await f.call('/queue/item?id='+id,undefined,'bob')).data;
  return f;
}
