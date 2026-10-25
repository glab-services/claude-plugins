import {spawn} from 'node:child_process';
import {writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import http from 'node:http';
import {Store} from '../broker/store.mjs';
import {Broker} from '../broker/broker.mjs';
import {config} from '../broker/config.mjs';
import {testTempDir} from './temp.mjs';
export const tokens=Object.fromEntries(['alice','bob','eve'].map(u=>[u,u+'-topic-binding-process-fixture-00000000']));
export async function eventually(predicate,timeout=4000){const end=Date.now()+timeout;while(!predicate()){if(Date.now()>end)throw Error('Timed out waiting for topic-binding state');await new Promise(r=>setTimeout(r,5));}}
export async function topicProcesses(t){
 const dir=testTempDir('topic-binding-process-'),file=join(dir,'state.sqlite'),tokenPath=join(dir,'tokens.json'),children=[];
 writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});
 const store=new Store(file);new Broker(store,config({BROKER_DEFAULT_OWNER:'alice'}),Object.keys(tokens),{log:()=>{}});
 store.run("INSERT INTO workspaces VALUES ('other',?,'alice',NULL,NULL)",Date.now());
 for(const user of ['alice','bob','eve'])store.run('INSERT INTO workspace_members VALUES (?,?,?,?,?)','other',user,user==='alice'?'owner':'member',Date.now(),'static');
 for(const workspace of ['default','other'])for(const topic of ['x','y']){
  store.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES (?,?,'broadcast','alice',?)",workspace,topic,Date.now());
  for(const user of ['alice','bob','eve'])store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)',workspace,topic,user,Date.now());
 }
 store.close();
 t.after(async()=>{for(const p of children){if(p.child.exitCode===null&&p.child.signalCode===null)p.child.kill('SIGTERM');await p.exited;}rmSync(dir,{recursive:true,force:true});});
 const start=async()=>{
  const child=spawn(process.execPath,['broker/main.mjs'],{env:{...process.env,BROKER_HOST:'127.0.0.1',BROKER_PORT:'0',BROKER_DB:file,BROKER_TOKENS:tokenPath,BROKER_DEFAULT_OWNER:'alice',RETENTION_ENABLED:'0',BROKER_WEBHOOK_URL:'',SSE_HEARTBEAT:'1'},stdio:['ignore','ignore','pipe']});
  const exited=new Promise(r=>child.once('exit',r));children.push({child,exited});let buffer='',port;
  child.stderr.on('data',chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);try{const event=JSON.parse(line);if(event.event==='broker_listening')port=event.address.port;}catch{}}});
  await eventually(()=>Boolean(port));return 'http://127.0.0.1:'+port;
 };
 const headers=(user,session,topic,workspace='default')=>({Authorization:'Bearer '+tokens[user],'X-Session-Id':session,'X-Workspace':workspace,...(topic===null?{}:{'X-Topic':topic})});
 const call=async(url,user,session,topic,path,args,workspace='default')=>{
  const response=await fetch(url+path,{method:args===undefined?'GET':'POST',headers:{...headers(user,session,topic,workspace),'Content-Type':'application/json'},...(args===undefined?{}:{body:JSON.stringify(args)})});
  return {status:response.status,data:await response.json()};
 };
 const stream=async(url,user,session,topic,workspace='default')=>{
  const req=http.get(url+'/stream?replay=1',{headers:headers(user,session,topic,workspace)});req.on('error',()=>{});t.after(()=>req.destroy());
  const response=await new Promise((resolve,reject)=>{req.once('response',resolve);req.once('error',reject);});if(response.statusCode!==200)throw Error('Topic-binding stream refused');
  const messages=[],events=[];let buffer='',heartbeats=0;response.setEncoding('utf8');response.on('error',()=>{});
  response.on('data',chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf('\n\n'))>=0){const frame=buffer.slice(0,end);buffer=buffer.slice(end+2);if(frame.startsWith(': heartbeat'))heartbeats++;const data=frame.split('\n').find(l=>l.startsWith('data: '));if(data){const event=frame.split('\n').find(l=>l.startsWith('event: '))?.slice(7),value=JSON.parse(data.slice(6));events.push({event,data:value});if(event==='message')messages.push(value);}}});
  return {messages,events,get heartbeats(){return heartbeats;}};
 };
 return {a:await start(),b:await start(),call,stream,file};
}
