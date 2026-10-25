import {test} from 'node:test';
import assert from 'node:assert/strict';
import {brokerFixture,tokens,eventually} from './helpers.mjs';
import {scratch,cli,savedEnv,channel} from '../../test-support/client-config.mjs';
import {credentialPath,writeCredential,readCredential} from '../credentials.mjs';
import {statSync} from 'node:fs';
import {Store} from '../../broker/store.mjs';
async function issue(f,topic){
 if(!f.store.get('SELECT 1 FROM topics WHERE name=?',topic))f.store.run("INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES ('default',?,'broadcast','alice',?)",topic,Date.now());
 const response=await fetch(f.url+'/invites',{method:'POST',headers:{Authorization:'Bearer '+tokens.alice,'X-Session-Id':'issuer','Content-Type':'application/json'},body:JSON.stringify({topics:[topic]})});assert.equal(response.status,201);return (await response.json()).code;
}
const added=(s,client)=>s.readCalls().filter(c=>c.client===client&&c.args[1]==='add').at(-1);
const nameOf=call=>call.args[call.args.indexOf('--')-1];

for(const client of ['claude'])test(`${client} failed replacement add returns an executable secret-free recovery command`,async t=>{
 const {existsSync,readFileSync}=await import('node:fs'),{join}=await import('node:path'),{spawn}=await import('node:child_process');
 const f=await brokerFixture(t),s=scratch(t),topic='replacement/recovery';
 s.env.XDG_CONFIG_HOME=join(s.env.HOME,"config 'quoted'");
 const run=async env=>cli(env,['join',f.url,'-','--name','recovery-user','--client',client],await issue(f,topic));
 const first=await run(s.env);assert.equal(first.code,0);
 const old=added(s,client),oldEnv=savedEnv(old.args),credential=readCredential(oldEnv.BROKER_TOKEN_FILE),before=s.readCalls().length;
 const original=f.server.listeners('request')[0];f.server.removeAllListeners('request');
 f.server.on('request',(req,res)=>{if(req.url!=='/session/status')return original(req,res);res.writeHead(409,{'Content-Type':'application/json'});res.end(JSON.stringify({error:'SESSION_ENDED',reason:'topic_left'}));});
 const failed=await run({...s.env,MCP_FAKE_FAIL:'1'});assert.equal(failed.code,0);
 const adapter=JSON.parse(failed.stdout).adapters[0];assert.equal(adapter.status,'failed');
 assert.match(adapter.hint,/removed.*replacement.*failed/);assert.match(adapter.hint,/Re-add manually: /);
 assert.equal(adapter.hook_env,undefined);
 const command=adapter.hint.split('Re-add manually: ')[1];
 const calls=s.readCalls().slice(before);assert.equal(calls.filter(c=>c.args[1]==='remove').length,1);assert.equal(calls.filter(c=>c.args[1]==='add').length,1);
 const state=join(s.dir,client+'-'+nameOf(old)+'.json');assert.equal(existsSync(state),false);
 assert.ok(!(failed.stdout+failed.stderr+JSON.stringify(calls)).includes(credential),'failure output and command contain no credential');
 assert.equal(command.includes('BROKER_TOKEN='),false);assert.ok(command.includes('BROKER_TOKEN_FILE='));assert.ok(command.includes('CHANNEL_TOPIC='));
 const recovered=spawn('/bin/sh',['-c',command],{env:s.env,cwd:s.env.HOME,stdio:'ignore'});
 assert.equal(await new Promise((resolve,reject)=>{recovered.once('error',reject);recovered.once('exit',resolve);}),0);
 assert.equal(existsSync(state),true);const restored=savedEnv(JSON.parse(readFileSync(state,'utf8')));
 assert.equal(restored.CHANNEL_TOPIC,topic);assert.equal(restored.BROKER_TOKEN_FILE,oldEnv.BROKER_TOKEN_FILE);assert.notEqual(restored.CHANNEL_SESSION_ID,oldEnv.CHANNEL_SESSION_ID);
});

for(const client of ['claude'])for(const outcome of ['STATUS_UNAVAILABLE','SESSION_OWNED','SESSION_TOPIC_MISMATCH','invalid status'])test(`${client} registration preserves config after non-ended probe outcome ${outcome}`,async t=>{
 const {readFileSync}=await import('node:fs'),{join}=await import('node:path');
 const f=await brokerFixture(t),s=scratch(t),topic='probe/refusal';
 const run=async()=>cli(s.env,['join',f.url,'-','--name','probe-user','--client',client],await issue(f,topic));
 assert.equal((await run()).code,0);
 const old=added(s,client),config=join(s.env.CLAUDE_CONFIG_DIR,'.claude.json');
 const before=readFileSync(config),calls=s.readCalls().length;let probes=0;
 const original=f.server.listeners('request')[0];f.server.removeAllListeners('request');
 f.server.on('request',(req,res)=>{
  if(req.url!=='/session/status')return original(req,res);
  probes++;
  if(outcome==='STATUS_UNAVAILABLE'){res.destroy();return;}
  res.writeHead(outcome==='invalid status'?200:409,{'Content-Type':'application/json'});
  res.end(JSON.stringify(outcome==='invalid status'?{state:'unknown',user:'probe-user',session_id:savedEnv(old.args).CHANNEL_SESSION_ID,workspace:'default',topic}:{error:outcome}));
 });
 const result=await run();assert.equal(result.code,1);assert.equal(JSON.parse(result.stdout).adapters[0].status,'conflict');assert.ok(probes>0);
 assert.equal(s.readCalls().slice(calls).filter(c=>['add','remove'].includes(c.args[1])).length,0);
 assert.ok(readFileSync(config).equals(before),'non-ended probe leaves exact config bytes unchanged');
});

test('join creates disjoint sanitized registrations and pinned IDs for two topics using one credential',async t=>{
 const f=await brokerFixture(t),s=scratch(t),topics=['feature/x','feature_x'],results=[];
 for(const topic of topics){const code=await issue(f,topic),result=await cli(s.env,['join',f.url,'-','--name','topic-user'],code);assert.equal(result.code,0);const data=JSON.parse(result.stdout);assert.equal(data.topic,topic);results.push(data);}
 assert.equal(f.store.get("SELECT count(*) n FROM credentials WHERE user='topic-user'").n,1);assert.equal(results[0].credential_path,results[1].credential_path);
 for(const client of ['claude']){
  const calls=s.readCalls().filter(c=>c.client===client&&c.args[1]==='add');assert.equal(calls.length,2);assert.notEqual(nameOf(calls[0]),nameOf(calls[1]));
  for(let i=0;i<2;i++){assert.match(nameOf(calls[i]),/^[A-Za-z0-9_-]+$/);assert.ok(nameOf(calls[i]).length<=64);assert.equal(savedEnv(calls[i].args).CHANNEL_TOPIC,topics[i]);}
  assert.notEqual(savedEnv(calls[0].args).CHANNEL_SESSION_ID,savedEnv(calls[1].args).CHANNEL_SESSION_ID);
 }
 const claude=results[0].adapters.find(a=>a.client==='claude');assert.equal(claude.hook_env.CHANNEL_TOPIC,topics[0]);
 const token=readCredential(results[0].credential_path);assert.ok(!JSON.stringify(s.readCalls()).includes(token),'argv contains no credential');
});

test('re-invite after remote leave repairs only the ended registration without touching an active pin or lease',async t=>{
 const {topicProcesses,tokens:processTokens}=await import('../../test-support/topic-processes.mjs'),f=await topicProcesses(t),s=scratch(t),topic='x';f.clients=[];t.after(async()=>{for(const c of f.clients)await c.close();});
 const path=credentialPath(f.a,'bob',{env:s.env});writeCredential(path,processTokens.bob,{home:s.env.HOME});const original=statSync(path);
 const invite=async()=>{const r=await f.call(f.a,'alice','issuer',null,'/invites',{topics:[topic]});assert.equal(r.status,201);return r.data.code;};
 const join=async(client)=>{const code=await invite(),r=await cli(s.env,['join',f.a,'-','--name','bob',...(client?['--client',client]:[])],code);assert.equal(r.code,0);return JSON.parse(r.stdout);};
 await join();const oldClaude=savedEnv(added(s,'claude').args);
 const context={url:f.a,clients:f.clients},first=await channel(context,{...s.env,...oldClaude});
 assert.equal((await first.call('members')).isError,undefined);
 const sent=await f.call(f.a,'alice','publisher',null,'/send',{to:'topic:x',body:'retained across rejoin'});assert.equal(sent.status,201);
 await first.client.close();
 assert.equal((await f.call(f.b,'bob','remote-leaver','x','/topics/leave',{name:'x'})).status,200);
 const restore=await f.call(f.a,'bob','restore','x','/join',{name:'bob',code:await invite()});assert.equal(restore.status,201);
 const repaired=await join('claude');assert.equal(repaired.status,'already_member');assert.equal(repaired.adapters.find(a=>a.client==='claude').status,'registered');const activeEnv=savedEnv(added(s,'claude').args);assert.notEqual(activeEnv.CHANNEL_SESSION_ID,oldClaude.CHANNEL_SESSION_ID);assert.equal(activeEnv.CHANNEL_TOPIC,'x');
 const active=await channel(context,{...s.env,...activeEnv});assert.equal((await active.call('register_workstream',{name:'preserved-lease'})).isError,undefined);await active.client.close();
 const db=new Store(f.file);db.run('UPDATE agents SET last_seen=1 WHERE session_id=?',activeEnv.CHANNEL_SESSION_ID);db.run("UPDATE workstreams SET lease_until=2 WHERE name='preserved-lease'");const count=db.get('SELECT count(*) n FROM agents').n;db.close();
 const beforeAdds=s.readCalls().filter(c=>c.client==='claude'&&c.args[1]==='add').length;
 const result=await join();assert.equal(result.status,'already_member');assert.equal(result.adapters.find(a=>a.client==='claude').status,'skipped');
 const verify=new Store(f.file);try{assert.equal(verify.get('SELECT last_seen FROM agents WHERE session_id=?',activeEnv.CHANNEL_SESSION_ID).last_seen,1);assert.equal(verify.get("SELECT lease_until FROM workstreams WHERE name='preserved-lease'").lease_until,2);assert.equal(verify.get('SELECT count(*) n FROM agents').n,count);}finally{verify.close();}
 assert.equal(s.readCalls().filter(c=>c.client==='claude'&&c.args[1]==='add').length,beforeAdds);assert.equal(statSync(path).ino,original.ino);
 const old=await f.call(f.b,'bob',oldClaude.CHANNEL_SESSION_ID,'x','/session/status');assert.equal(old.data.error,'SESSION_ENDED');
 const reader=await channel(context,{...s.env,...activeEnv});
 const inbox=await reader.call('inbox');assert.equal(inbox.isError,undefined);assert.equal(inbox.structuredContent.messages.filter(m=>m.id===sent.data.id).length,1);
 await eventually(()=>reader.notifications.some(n=>n.meta.message_id===String(sent.data.id)));const barrier=await f.stream(f.b,'alice','rejoin-owner-barrier',null),tick=barrier.heartbeats;await eventually(()=>barrier.heartbeats>=tick+3);
 assert.equal(reader.notifications.filter(n=>n.meta.message_id===String(sent.data.id)).length,1);await reader.client.close();
});

test('re-invite keeps never-started matching pins and creates no broker agents',async t=>{
 const f=await brokerFixture(t),s=scratch(t),topic='unstarted/topic';
 const run=async()=>{const r=await cli(s.env,['join',f.url,'-','--name','unstarted'],await issue(f,topic));assert.equal(r.code,0);return JSON.parse(r.stdout);};
 await run();const before=s.readCalls().filter(c=>c.args[1]==='add').map(c=>({client:c.client,env:savedEnv(c.args)}));
 assert.equal(f.store.get("SELECT count(*) n FROM agents WHERE user='unstarted'").n,0);
 const result=await run();assert.equal(result.status,'already_member');assert.ok(result.adapters.every(a=>a.status==='skipped'));
 assert.equal(s.readCalls().filter(c=>c.args[1]==='remove').length,0);assert.equal(s.readCalls().filter(c=>c.args[1]==='add').length,before.length);
 assert.equal(f.store.get("SELECT count(*) n FROM agents WHERE user='unstarted'").n,0);
 for(const c of before)assert.equal(savedEnv(added(s,c.client).args).CHANNEL_SESSION_ID,c.env.CHANNEL_SESSION_ID);
});

test('Claude lookup never invokes health checking get or exposes another server sentinel',async t=>{
 const {mkdirSync,writeFileSync,readFileSync,existsSync}=await import('node:fs'),{join}=await import('node:path'),f=await brokerFixture(t),s=scratch(t),topic='read-only/topic',cfg=join(s.env.CLAUDE_CONFIG_DIR,'.claude.json'),marker=join(s.dir,'transport-started'),sentinel='PRIVATE_OTHER_SERVER_SENTINEL_DO_NOT_ECHO';
 const run=async()=>cli({...s.env,MCP_FAKE_GET_MARKER:marker},['join',f.url,'-','--name','lookup-user','--client','claude'],await issue(f,topic));
 const first=await run();assert.equal(first.code,0);
 const config=JSON.parse(readFileSync(cfg,'utf8'));config.mcpServers.other={type:'stdio',command:'/bin/true',env:{SECRET:sentinel}};writeFileSync(cfg,JSON.stringify(config),{mode:0o600});
 const before=readFileSync(cfg,'utf8'),second=await run();assert.equal(second.code,0);assert.equal(JSON.parse(second.stdout).adapters[0].status,'skipped');
 assert.equal(existsSync(marker),false);assert.ok(readFileSync(cfg,'utf8')===before,'lookup leaves user config bytes unchanged');assert.ok(![first.stdout,first.stderr,second.stdout,second.stderr,JSON.stringify(s.readCalls())].join('').includes(sentinel),'unrelated secret never escapes');
});

test('Claude lookup refuses project/local shadows and unparseable config without exposing values',async t=>{
 const {writeFileSync,readFileSync,rmSync}=await import('node:fs'),{join}=await import('node:path'),f=await brokerFixture(t),s=scratch(t),topic='shadow/topic',cfg=join(s.env.CLAUDE_CONFIG_DIR,'.claude.json'),sentinel='PRIVATE_SHADOW_SENTINEL';
 const run=async()=>cli(s.env,['join',f.url,'-','--name','shadow-user','--client','claude'],await issue(f,topic));assert.equal((await run()).code,0);
 const original=readFileSync(cfg,'utf8'),name=nameOf(added(s,'claude')),before=s.readCalls().filter(c=>c.args[1]==='add'||c.args[1]==='remove').length;
 for(const kind of ['local','project','invalid']){
  const data=JSON.parse(original);if(kind==='local')data.projects={[s.env.HOME]:{mcpServers:{[name]:{env:{SECRET:sentinel}}}}};
  if(kind==='project')writeFileSync(join(s.env.HOME,'.mcp.json'),JSON.stringify({mcpServers:{[name]:{env:{SECRET:sentinel}}}}));
  const text=kind==='invalid'?'{"broken":"'+sentinel:JSON.stringify(data);writeFileSync(cfg,text,{mode:0o600});
  const result=await run();assert.equal(result.code,1);assert.equal(JSON.parse(result.stdout).adapters[0].status,'conflict');assert.ok(![result.stdout,result.stderr].join('').includes(sentinel));assert.ok(readFileSync(cfg,'utf8')===text);
  rmSync(join(s.env.HOME,'.mcp.json'),{force:true});
 }
 assert.equal(s.readCalls().filter(c=>c.args[1]==='add'||c.args[1]==='remove').length,before);
});

test('an ended custom entry and a registration removed during its probe are never overwritten',async t=>{
 const {writeFileSync,readFileSync,rmSync,existsSync}=await import('node:fs'),{join}=await import('node:path'),f=await brokerFixture(t),s=scratch(t),topic='guard/topic',cfg=join(s.env.CLAUDE_CONFIG_DIR,'.claude.json');
 const run=async()=>cli(s.env,['join',f.url,'-','--name','guard-user','--client','claude'],await issue(f,topic));assert.equal((await run()).code,0);
 const env=savedEnv(added(s,'claude').args),token=readCredential(env.BROKER_TOKEN_FILE),name=nameOf(added(s,'claude'));
 const leave=await fetch(f.url+'/topics/leave',{method:'POST',headers:{Authorization:'Bearer '+token,'X-Session-Id':env.CHANNEL_SESSION_ID,'X-Topic':topic,'Content-Type':'application/json'},body:JSON.stringify({name:topic})});assert.equal(leave.status,200);
 const restored=await fetch(f.url+'/join',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify({code:await issue(f,topic),name:'guard-user'})});assert.equal(restored.status,201);
 const original=readFileSync(cfg,'utf8'),data=JSON.parse(original);data.mcpServers[name].command='/bin/true';writeFileSync(cfg,JSON.stringify(data));
 const custom=readFileSync(cfg,'utf8'),denied=await run();assert.equal(denied.code,1);assert.equal(JSON.parse(denied.stdout).adapters[0].status,'conflict');assert.ok(readFileSync(cfg,'utf8')===custom);
 writeFileSync(cfg,original);const handler=f.server.listeners('request')[0];f.server.removeAllListeners('request');let removed=false;
 f.server.on('request',(req,res)=>{if(!removed&&req.url==='/session/status'&&req.headers['x-session-id']===env.CHANNEL_SESSION_ID){removed=true;rmSync(cfg);}handler(req,res);});
 const raced=await run();assert.equal(raced.code,1);assert.equal(JSON.parse(raced.stdout).adapters[0].status,'conflict');assert.equal(existsSync(cfg),false);
 assert.equal(s.readCalls().filter(c=>c.client==='claude'&&c.args[1]==='remove').length,0);
});
