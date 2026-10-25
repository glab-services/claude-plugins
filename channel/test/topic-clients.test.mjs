import {test} from 'node:test';
import assert from 'node:assert/strict';
import {brokerFixture,session,eventually,tokens} from './helpers.mjs';
import {FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';

test('invite schema requires exactly one topic before invoking the broker',async t=>{
 const f=await brokerFixture(t),owner=await session(f,'alice','invite-schema',{CHANNEL_TOPIC:undefined});
 const before=f.requests.filter(r=>r.path==='/invites').length;
 for(const args of [{},{topics:[]},{topics:[FIXTURE_TOPIC,'other']}]){
  const result=await owner.client.callTool({name:'invite',arguments:args});assert.equal(result.structuredContent.error,'INVALID_ARGUMENTS');
 }
 assert.equal(f.requests.filter(r=>r.path==='/invites').length,before);
 assert.equal((await owner.call('invite',{topics:[FIXTURE_TOPIC]})).isError,undefined);
});

test('only owner workspace clients expose the direct topic selector and route it over stdio',async t=>{
 const f=await brokerFixture(t),owner=await session(f,'alice','owner-client',{CHANNEL_TOPIC:undefined}),member=await session(f,'bob','member-client'),boundOwner=await session(f,'alice','bound-owner-client');
 const sent=await owner.call('send',{to:'user:bob',topic:FIXTURE_TOPIC,body:'owner-selected topic'});assert.equal(sent.isError,undefined);
 await eventually(()=>member.notifications.some(n=>n.meta.message_id===String(sent.structuredContent.id)));
 const asked=await owner.call('ask',{to:'user:bob',topic:FIXTURE_TOPIC,body:'owner-selected question'});assert.equal(asked.isError,undefined);
 for(const [client,allowed] of [[owner,true],[member,false],[boundOwner,false]]){
  const tools=(await client.client.listTools()).tools;assert.equal(tools.length,36);
  for(const name of ['send','ask'])assert.equal(Object.hasOwn(tools.find(t=>t.name===name).inputSchema.properties,'topic'),allowed);
 }
 const before=f.store.get('SELECT count(*) n FROM messages').n;
 for(const client of [member,boundOwner])for(const tool of ['send','ask'])assert.equal((await client.call(tool,{to:'user:alice',topic:'another',body:'cannot escape'})).structuredContent.error,'INVALID_ARGUMENTS');
 assert.equal(f.store.get('SELECT count(*) n FROM messages').n,before);
});

test('direct questions, replies and topic notices retain their topic in stdio context and notifications',async t=>{
 const f=await brokerFixture(t),owner=await session(f,'alice','metadata-owner',{CHANNEL_TOPIC:undefined}),member=await session(f,'bob','metadata-member');
 const asked=await member.call('ask',{to:'user:alice',body:'question with topic'});assert.equal(asked.isError,undefined);
 await eventually(()=>owner.notifications.some(n=>n.meta.thread_id===asked.structuredContent.thread_id));
 const question=(await owner.call('inbox')).structuredContent.messages.find(m=>m.thread_id===asked.structuredContent.thread_id);assert.equal(question.topic,FIXTURE_TOPIC);
 assert.equal(owner.notifications.find(n=>n.meta.thread_id===question.thread_id).meta.topic,FIXTURE_TOPIC);
 await owner.call('ack',{id:question.id});const answer=await owner.call('send',{to:'user:bob',kind:'answer',thread_id:question.thread_id,body:'scoped answer'});assert.equal(answer.isError,undefined);
 await eventually(()=>member.notifications.some(n=>n.meta.message_id===String(answer.structuredContent.id)));
 assert.equal(member.notifications.find(n=>n.meta.message_id===String(answer.structuredContent.id)).meta.topic,FIXTURE_TOPIC);
 assert.equal((await member.call('inbox')).structuredContent.messages.find(m=>m.id===answer.structuredContent.id).topic,FIXTURE_TOPIC);
 await owner.call('create_topic',{name:'notice/topic'});const invitation=await owner.call('invite',{topics:['notice/topic']});assert.equal(invitation.isError,undefined);
 const joined=await fetch(f.url+'/join',{method:'POST',headers:{Authorization:'Bearer '+tokens.bob,'Content-Type':'application/json'},body:JSON.stringify({code:invitation.structuredContent.code,name:'bob'})});assert.equal(joined.status,201);
 const notice=f.store.get("SELECT * FROM messages WHERE kind='notice' AND topic='notice/topic'");
 await eventually(()=>owner.notifications.some(n=>n.meta.message_id===String(notice.id)));
 assert.equal(owner.notifications.find(n=>n.meta.message_id===String(notice.id)).meta.topic,'notice/topic');
 assert.equal((await owner.call('inbox')).structuredContent.messages.find(m=>m.id===notice.id).topic,'notice/topic');
});

test('incoming topic labels validate syntax and topic-address consistency',async t=>{
 const {Channel}=await import('../channel.mjs'),{config}=await import('../config.mjs');const c=new Channel(config({BROKER_TOKEN:tokens.bob}),async()=>{},()=>{});t.after(()=>c.stop());
 const raw={id:1,thread_id:'topic-label',from_user:'alice',from_session:'a',kind:'finding',body:'data',to_kind:'user',to_name:'bob'};
 for(const topic of [1,[],{},'..','x//y','<tag>','x\n'])await assert.rejects(c.receive({...raw,topic}),e=>e.code==='INVALID_BROKER_MESSAGE');
 await assert.rejects(c.receive({...raw,to_kind:'topic',to_name:'x',topic:'y'}),e=>e.code==='INVALID_BROKER_MESSAGE');
});

for(const code of ['SESSION_OWNED','SESSION_WORKSPACE_MISMATCH','SESSION_TOPIC_MISMATCH','TOPIC_CLOSED','TOPIC_REQUIRED','NOT_SUBSCRIBED','SESSION_ENDED'])test(`startup binding refusal ${code} stops once without retries`,async t=>{
 const {topicProcesses,tokens:processTokens}=await import('../../test-support/topic-processes.mjs'),{Channel}=await import('../channel.mjs'),{config}=await import('../config.mjs'),http=await import('node:http');
 const f=await topicProcesses(t),id='startup-'+code,requests=[],logs=[];
 if(code==='SESSION_OWNED')await f.call(f.b,'alice',id,null,'/heartbeat',{});
 if(code==='SESSION_WORKSPACE_MISMATCH')await f.call(f.b,'bob',id,'x','/heartbeat',{},'other');
 if(code==='SESSION_TOPIC_MISMATCH')await f.call(f.b,'bob',id,'y','/heartbeat',{});
 if(code==='TOPIC_CLOSED')await f.call(f.a,'alice','owner',null,'/topics/close',{name:'x'});
 if(code==='SESSION_ENDED')await f.call(f.b,'bob',id,'x','/heartbeat',{});
 if(['NOT_SUBSCRIBED','SESSION_ENDED'].includes(code))await f.call(f.a,'bob','leave-before-'+code,'x','/topics/leave',{name:'x'});
 const proxy=http.createServer((req,res)=>{requests.push(req.url);const up=http.request(f.b+req.url,{method:req.method,headers:req.headers},r=>{res.writeHead(r.statusCode,r.headers);r.pipe(res);r.on('error',()=>res.destroy());});up.on('error',()=>res.destroy());res.on('close',()=>up.destroy());req.pipe(up);});await new Promise(r=>proxy.listen(0,'127.0.0.1',r));
 const channel=new Channel(config({BROKER_URL:'http://127.0.0.1:'+proxy.address().port,BROKER_TOKEN:processTokens.bob,CHANNEL_SESSION_ID:id,CHANNEL_TOPIC:code==='TOPIC_REQUIRED'?undefined:'x',CHANNEL_HEARTBEAT_MS:'10',CHANNEL_POLL_MS:'10',CHANNEL_RETRY_MAX_MS:'20'}),async()=>{},v=>logs.push(v));
 t.after(async()=>{await channel.stop();proxy.closeAllConnections();await new Promise(r=>proxy.close(r));});channel.start();
 await eventually(()=>logs.some(l=>l.component==='binding'&&l.error===code));assert.equal(logs.filter(l=>l.component==='binding'&&l.error===code).length,1);
 const before=requests.length;await new Promise(r=>setTimeout(r,100));assert.equal(requests.length,before);await assert.rejects(channel.api('/inbox'),e=>e.code===code);await channel.stop();
});
