import {test} from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {config} from '../config.mjs';
import {Channel} from '../channel.mjs';
import {eventually} from './helpers.mjs';
const base={BROKER_TOKEN:'channel-binding-fixture-token-00000000',CHANNEL_SESSION_ID:'topic-channel',CHANNEL_TOPIC:'feature/x'};
test('CHANNEL_TOPIC validates exact topic syntax at startup',()=>{
 for(const topic of ['', '..','feature/..',' feature/x','feature//x','K'])assert.throws(()=>config({...base,CHANNEL_TOPIC:topic}),e=>e.code==='INVALID_TOPIC');
 assert.equal(config(base).topic,'feature/x');assert.equal(config({...base,CHANNEL_TOPIC:undefined}).topic,null);
});
test('channel API and SSE send the same X-Topic binding header',async t=>{
 const seen=[];const server=http.createServer((req,res)=>{
  seen.push({path:req.url,topic:req.headers['x-topic']});
  if(req.url.startsWith('/stream')){res.writeHead(200,{'Content-Type':'text/event-stream'});res.write(': connected\n\n');return;}
  res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify(req.url==='/resume'?{acked_id:0}:req.url==='/inbox'?{messages:[]}:{}));
 });await new Promise(r=>server.listen(0,'127.0.0.1',r));
 const channel=new Channel(config({...base,BROKER_URL:'http://127.0.0.1:'+server.address().port}),async()=>{},()=>{});
 t.after(async()=>{await channel.stop();server.closeAllConnections();await new Promise(r=>server.close(r));});
 await channel.api('/inbox');channel.running=channel.run();
 await eventually(()=>seen.some(r=>r.path.startsWith('/stream')));
 assert.ok(seen.some(r=>r.path==='/heartbeat'));assert.ok(seen.some(r=>r.path==='/resume'));
 assert.ok(seen.every(r=>r.topic==='feature/x'));
});

for(const demoted of [false,true])test(demoted?'remote owner demotion ends a workspace channel once without retrying':'remote topic leave ends only a topic-bound channel after one terminal log without retrying',async t=>{
 const code=demoted?'TOPIC_REQUIRED':'SESSION_ENDED';
 const {topicProcesses,tokens}=await import('../../test-support/topic-processes.mjs');const f=await topicProcesses(t),requests=[];
 const proxy=http.createServer((req,res)=>{
  requests.push(req.url);const upstream=http.request(f.b+req.url,{method:req.method,headers:req.headers},response=>{res.writeHead(response.statusCode,response.headers);response.pipe(res);response.on('error',()=>res.destroy());});
  upstream.on('error',()=>res.destroy());res.on('close',()=>upstream.destroy());req.pipe(upstream);
 });await new Promise(r=>proxy.listen(0,'127.0.0.1',r));
 const logs=[],channel=new Channel(config({BROKER_URL:'http://127.0.0.1:'+proxy.address().port,BROKER_TOKEN:tokens[demoted?'alice':'bob'],CHANNEL_SESSION_ID:'remote-bound-channel',CHANNEL_TOPIC:demoted?undefined:'x',CHANNEL_POLL_MS:'25',CHANNEL_HEARTBEAT_MS:'30000'}),async()=>{},v=>logs.push(v));
 t.after(async()=>{await channel.stop();proxy.closeAllConnections();await new Promise(r=>proxy.close(r));});channel.start();
 await eventually(()=>requests.some(p=>p.startsWith('/stream')));
 let retained;
 if(!demoted){retained=await f.call(f.b,'alice','race-publisher',null,'/send',{to:'topic:x',body:'retained race offer'});assert.equal(retained.status,201);await eventually(()=>channel.buffer.has(retained.data.id));}
 const changed=demoted?await f.call(f.a,'alice','transfer-owner',null,'/members/transfer',{user:'bob'}):await f.call(f.a,'bob','leave-x','x','/topics/leave',{name:'x'});assert.equal(changed.status,200);
 if(!demoted){const invite=await f.call(f.a,'alice','race-owner',null,'/invites',{topics:['x']});assert.equal(invite.status,201);assert.equal((await f.call(f.a,'bob','race-rejoin','x','/join',{code:invite.data.code,name:'bob'})).status,201);}
 await eventually(()=>logs.some(l=>l.component==='binding'&&l.error===code));
 assert.equal(logs.filter(l=>l.component==='binding'&&l.error===code).length,1);
 const before=requests.length;await new Promise(r=>setTimeout(r,250));assert.equal(requests.length,before);
 await assert.rejects(channel.api('/inbox'),e=>e.code===code&&(demoted||e.reason==='topic_left'));
 if(!demoted){const fresh=await f.stream(f.b,'bob','fresh-race-session','x');await eventually(()=>fresh.messages.some(m=>m.id===retained.data.id));const tick=fresh.heartbeats;await eventually(()=>fresh.heartbeats>=tick+3);assert.equal(fresh.messages.filter(m=>m.id===retained.data.id).length,1);}
});

test('individual tool SESSION_ENDED and NOT_SUBSCRIBED responses do not terminate a topic-bound channel',async t=>{
 const {topicProcesses,tokens}=await import('../../test-support/topic-processes.mjs');const f=await topicProcesses(t);
 const channel=new Channel(config({BROKER_URL:f.b,BROKER_TOKEN:tokens.bob,CHANNEL_SESSION_ID:'tool-only',CHANNEL_TOPIC:'x'}),async()=>{},()=>{});t.after(()=>channel.stop());
 await channel.api('/heartbeat',{});
 assert.equal((await f.call(f.a,'bob','remove-tool-x','x','/topics/leave',{name:'x'})).status,200);
 await assert.rejects(channel.api('/inbox'),e=>e.code==='SESSION_ENDED'&&e.reason==='topic_left');assert.equal(channel.membershipEnded,false);
 const fresh=new Channel(config({BROKER_URL:f.b,BROKER_TOKEN:tokens.bob,CHANNEL_SESSION_ID:'unsubscribed-fresh',CHANNEL_TOPIC:'x'}),async()=>{},()=>{});t.after(()=>fresh.stop());
 await assert.rejects(fresh.api('/inbox'),e=>e.code==='NOT_SUBSCRIBED');assert.equal(fresh.membershipEnded,false);
});


test('an individual tool TOPIC_REQUIRED response does not terminate a workspace-level channel',async t=>{
 const {topicProcesses,tokens}=await import('../../test-support/topic-processes.mjs');const f=await topicProcesses(t);
 const channel=new Channel(config({BROKER_URL:f.b,BROKER_TOKEN:tokens.alice,CHANNEL_SESSION_ID:'owner-tool-only'}),async()=>{},()=>{});t.after(()=>channel.stop());
 await channel.api('/heartbeat',{});
 assert.equal((await f.call(f.a,'alice','demote-via-other',null,'/members/transfer',{user:'bob'})).status,200);
 await assert.rejects(channel.api('/inbox'),e=>e.code==='TOPIC_REQUIRED');assert.equal(channel.membershipEnded,false);
});

for(const mode of ['push','poll'])for(const bound of [true,false])test(`${mode}: initial NOT_SUBSCRIBED ${bound?'ends a topic binding once':'never ends the owner workspace binding'}`,async t=>{
 const {topicProcesses,tokens}=await import('../../test-support/topic-processes.mjs'),f=await topicProcesses(t),requests=[],logs=[];
 if(bound)assert.equal((await f.call(f.a,'bob','remove-before-start','x','/topics/leave',{name:'x'})).status,200);
 const proxy=http.createServer((req,res)=>{
  requests.push({path:req.url,topic:req.headers['x-topic']});
  // An owner heartbeat cannot naturally return this code; inject it here while
  // real fallback requests still prove the owner's workspace-level identity.
  if(!bound&&req.url==='/heartbeat'){req.resume();res.writeHead(403,{'Content-Type':'application/json'});res.end(JSON.stringify({error:'NOT_SUBSCRIBED',hint:'controlled heartbeat refusal'}));return;}
  const up=http.request(f.b+req.url,{method:req.method,headers:req.headers},r=>{res.writeHead(r.statusCode,r.headers);r.pipe(res);r.on('error',()=>res.destroy());});up.on('error',()=>res.destroy());res.on('close',()=>up.destroy());req.pipe(up);
 });await new Promise(r=>proxy.listen(0,'127.0.0.1',r));
 const channel=new Channel(config({BROKER_URL:'http://127.0.0.1:'+proxy.address().port,BROKER_TOKEN:tokens[bound?'bob':'alice'],CHANNEL_SESSION_ID:'not-sub-start-'+mode+'-'+bound,CHANNEL_TOPIC:bound?'x':undefined,CHANNEL_MODE:mode,CHANNEL_HEARTBEAT_MS:'10',CHANNEL_POLL_MS:'10',CHANNEL_RETRY_MAX_MS:'20'}),async()=>{},v=>logs.push(v));
 t.after(async()=>{await channel.stop();proxy.closeAllConnections();await new Promise(r=>proxy.close(r));});channel.start();
 if(bound){
  await eventually(()=>logs.some(l=>l.component==='binding'&&l.error==='NOT_SUBSCRIBED'));
  assert.equal(logs.filter(l=>l.component==='binding'&&l.error==='NOT_SUBSCRIBED').length,1);assert.equal(channel.membershipEnded,true);
  const before=requests.length;await new Promise(r=>setTimeout(r,100));assert.equal(requests.length,before);await assert.rejects(channel.api('/inbox'),e=>e.code==='NOT_SUBSCRIBED');
 }else{
  await eventually(()=>requests.filter(r=>r.path==='/heartbeat').length>=3&&requests.some(r=>r.path==='/inbox?since=0'));
  assert.equal(channel.membershipEnded,false);assert.equal(logs.some(l=>l.component==='binding'),false);
  const before=requests.filter(r=>r.path==='/heartbeat').length;await eventually(()=>requests.filter(r=>r.path==='/heartbeat').length>before);
 }
 assert.ok(requests.every(r=>r.topic===(bound?'x':undefined)));await channel.stop();
});
