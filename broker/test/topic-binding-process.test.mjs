import {test} from 'node:test';
import assert from 'node:assert/strict';
import {topicProcesses,eventually} from '../../test-support/topic-processes.mjs';
test('two brokers isolate replay, inbox, ack and owner reachability across topics and workspaces',async t=>{
 const f=await topicProcesses(t);
 const owner=await f.stream(f.b,'alice','owner',null),x=await f.stream(f.b,'alice','owner-x','x'),y=await f.stream(f.b,'alice','owner-y','y');
 const sent=[];
 for(const [topic,workspace,user,session] of [['x','default','bob','bob-x'],['y','default','eve','eve-y'],['x','other','bob','bob-other']]){
  const result=await f.call(f.a,user,session,topic,'/send',{to:'user:alice',kind:'question',body:'scoped question'},workspace);assert.equal(result.status,201);sent.push(result.data);
 }
 await eventually(()=>owner.messages.some(m=>m.id===sent[1].id));
 const before=owner.heartbeats;await eventually(()=>owner.heartbeats>=before+2);
 assert.deepEqual(owner.messages.map(m=>m.id),sent.slice(0,2).map(m=>m.id));assert.deepEqual(owner.messages.map(m=>m.topic),['x','y']);
 assert.deepEqual(x.messages.map(m=>m.id),[sent[0].id]);assert.deepEqual(y.messages.map(m=>m.id),[sent[1].id]);
 assert.equal((await f.call(f.a,'alice','owner-x','x','/workstreams/register',{name:'coordination'})).status,200);
 const ws=await f.call(f.a,'bob','bob-x','x','/send',{to:'ws:coordination',kind:'question',body:'workstream owner reachability'});assert.equal(ws.status,201);
 await eventually(()=>owner.messages.some(m=>m.id===ws.data.id)&&x.messages.some(m=>m.id===ws.data.id));
 assert.equal(y.messages.some(m=>m.id===ws.data.id),false);
 assert.equal((await f.call(f.a,'alice','owner-x','x','/ack',{id:sent[1].id})).data.error,'MESSAGE_NOT_FOUND');
 assert.equal((await f.call(f.a,'alice','owner',null,'/ack',{id:sent[2].id})).data.error,'MESSAGE_NOT_FOUND');
 assert.equal((await f.call(f.a,'alice','owner',null,'/ack',{id:sent[0].id})).status,200);
 const reply=await f.call(f.a,'alice','owner',null,'/send',{to:'user:bob',kind:'answer',thread_id:sent[0].thread_id,body:'same-topic reply'});assert.equal(reply.status,201);
 const bx=await f.stream(f.b,'bob','reply-x','x'),by=await f.stream(f.b,'bob','reply-y','y');
 await eventually(()=>bx.messages.some(m=>m.id===reply.data.id));const tick=by.heartbeats;await eventually(()=>by.heartbeats>=tick+2);
 assert.equal(by.messages.some(m=>m.id===reply.data.id),false);
});

test('a leave and immediate resubscribe on A cannot revive the old bound session on B',async t=>{
 const f=await topicProcesses(t);
 assert.equal((await f.call(f.b,'bob','ended-old','x','/heartbeat',{})).status,200);
 const sent=await f.call(f.a,'alice','publisher',null,'/send',{to:'topic:x',body:'retained replay'});assert.equal(sent.status,201);
 assert.equal((await f.call(f.a,'bob','leave-through-a','x','/topics/leave',{name:'x'})).status,200);
 const invitation=await f.call(f.a,'alice','owner',null,'/invites',{topics:['x']});assert.equal(invitation.status,201);
 assert.equal((await f.call(f.a,'bob','unused','x','/join',{name:'bob',code:invitation.data.code})).status,201);
 const old=await f.call(f.b,'bob','ended-old','x','/heartbeat',{});assert.equal(old.status,409);assert.equal(old.data.error,'SESSION_ENDED');assert.equal(old.data.reason,'topic_left');
 const fresh=await f.stream(f.b,'bob','fresh-after-rejoin','x');await eventually(()=>fresh.messages.some(m=>m.id===sent.data.id));
 const before=fresh.heartbeats;await eventually(()=>fresh.heartbeats>=before+3);
 assert.equal(fresh.messages.filter(m=>m.id===sent.data.id).length,1);
});

test('every broker endpoint keeps foreign workspace ids, names and state isolated',async t=>{
 const {Store}=await import('../store.mjs');const {createHash}=await import('node:crypto');const f=await topicProcesses(t);
 const foreign=(path,args,topic=null,user='alice',session='foreign-owner')=>f.call(f.a,user,session,topic,path,args,'other');
 assert.equal((await foreign('/topics',{name:'foreign-only',mode:'queue'})).status,201);
 const invite=await foreign('/invites',{topics:['foreign-only']});assert.equal(invite.status,201);
 const queue=await foreign('/send',{to:'topic:foreign-only',body:'FOREIGN_WORKSPACE_BODY'});assert.equal(queue.status,201);
 const direct=await foreign('/send',{to:'user:alice',kind:'question',body:'FOREIGN_WORKSPACE_BODY'},'x','bob','foreign-bob');assert.equal(direct.status,201);
 assert.equal((await foreign('/workstreams/register',{name:'foreign-stream'},'x','alice','foreign-bound-owner')).status,200);
 assert.equal((await foreign('/decisions/record',{workstream:'foreign-stream',body:'FOREIGN_WORKSPACE_BODY'},'x','alice','foreign-bound-owner')).status,201);
 const setup=new Store(f.file);setup.run("INSERT INTO workspace_members VALUES ('other','foreign-user','member',1,'invite')");setup.run("UPDATE queue_items SET state='dead',attempts=3,generation=7 WHERE message_id=?",queue.data.id);setup.close();
 const snapshot=()=>{const store=new Store(f.file);try{return createHash('sha256').update(JSON.stringify(['workspaces','workspace_members','workspace_audit','agents','topics','topic_subscriptions','topic_cursors','topic_pins','workstreams','decisions','threads','messages','invites','queue_items'].map(table=>[table,store.all(`SELECT * FROM ${table} WHERE ${table==='workspaces'?'name':'workspace'}='other' ORDER BY rowid`)]))).digest('hex');}finally{store.close();}};
 const before=snapshot(),owner=(path,args)=>f.call(f.b,'alice','endpoint-owner',null,path,args),checked=new Set();
 const denied=[
  ['/queue/requeue',{id:queue.data.id}],['/queue/claim',{id:queue.data.id}],['/queue/renew',{id:queue.data.id,generation:7}],['/queue/done',{id:queue.data.id,generation:7}],
  ['/queue/item?id='+queue.data.id,undefined],['/queue/dead-letters?topic=foreign-only',undefined],
  ['/topics/skip',{topic:'foreign-only'}],['/topics/keep',{id:queue.data.id}],['/topics/unkeep',{id:queue.data.id}],['/topics/pins?topic=foreign-only',undefined],
  ['/topics/subscribe',{name:'foreign-only'}],['/topics/leave',{name:'foreign-only'}],['/topics/close',{name:'foreign-only'}],
  ['/invites',{topics:['foreign-only']}],['/invites/revoke',{invite_id:invite.data.invite_id}],
  ['/members/remove',{user:'foreign-user'}],['/members/transfer',{user:'foreign-user'}],
  ['/members/leave',{workspace:'other'}],['/members/finish',{summary:'not authorized',workspace:'other'}],
  ['/workstreams/check-overlap',{name:'foreign-stream'}],['/workstreams/release',{name:'foreign-stream'}],['/decisions/record',{workstream:'foreign-stream',body:'cannot write there'}],
  ['/send',{to:'user:foreign-user',topic:'x',body:'no cross workspace'}],['/send',{to:'ws:foreign-stream',topic:'x',body:'no cross workspace'}],
  ['/send',{to:'user:bob',thread_id:direct.data.thread_id,kind:'answer',body:'cannot reply there'}],
  ['/ack',{id:direct.data.id}],['/close',{thread_id:direct.data.thread_id}],
 ];
 for(const [path,args] of denied){const result=await owner(path,args);assert.ok(result.status>=400,path+' refuses foreign identifier');assert.ok(!JSON.stringify(result.data).includes('FOREIGN_WORKSPACE_BODY'),path+' returns no foreign body');checked.add((args===undefined?'GET ':'POST ')+path.split('?')[0]);assert.ok(snapshot()===before,path+' leaves foreign state unchanged');}
 for(const path of ['/topics','/invites','/members','/decisions','/inbox','/resume','/session/status']){
  const result=await owner(path);assert.equal(result.status,200);assert.ok(!JSON.stringify(result.data).includes('FOREIGN_WORKSPACE_BODY'));if(path==='/topics')assert.equal(result.data.topics.some(r=>r.name==='foreign-only'),false);if(path==='/invites')assert.equal(result.data.invites.some(r=>r.invite_id===invite.data.invite_id),false);if(path==='/members')assert.equal(result.data.members.some(r=>r.user==='foreign-user'),false);checked.add('GET '+path);
 }
 const member=await f.call(f.b,'bob','endpoint-member','x','/heartbeat',{});assert.equal(member.status,200);assert.equal(member.data.workspace,'default');checked.add('POST /heartbeat');
 const changed=await f.call(f.b,'bob','endpoint-member','x','/heartbeat',{},'other');assert.equal(changed.data.error,'SESSION_WORKSPACE_MISMATCH');
 // The same names can be created locally; this must never update the foreign rows.
 assert.equal((await owner('/topics',{name:'foreign-only',mode:'queue'})).status,201);checked.add('POST /topics');
 assert.equal((await owner('/workstreams/register',{name:'foreign-stream'})).status,200);checked.add('POST /workstreams/register');
 // Identity routes are intentionally independent of a session binding and may
 // expose membership metadata or redeem an explicit workspace invite capability.
 assert.equal((await owner('/workspaces')).status,200);checked.add('GET /workspaces');
 assert.equal((await f.call(f.b,'bob','endpoint-member','x','/workspaces',{name:'endpoint-created'})).status,201);checked.add('POST /workspaces');
 assert.equal((await owner('/credentials/revoke',{})).data.error,'NOT_MINTED_CREDENTIAL');checked.add('POST /credentials/revoke');
 assert.equal((await owner('/join',{code:'invalid-fixture-code',name:'alice'})).data.error,'INVITE_INVALID');checked.add('POST /join');
 const stream=await f.stream(f.b,'alice','endpoint-stream',null);checked.add('GET /stream');
 const control=await f.call(f.b,'bob','endpoint-member','x','/send',{to:'user:alice',body:'local positive control'});assert.equal(control.status,201);
 await eventually(()=>stream.messages.some(m=>m.id===control.data.id));const tick=stream.heartbeats;await eventually(()=>stream.heartbeats>=tick+1);
 assert.ok(stream.messages.every(m=>m.workspace==='default'&&!m.content.includes('FOREIGN_WORKSPACE_BODY')));assert.ok(snapshot()===before,'foreign business state survives every endpoint probe');
 const inventory=['GET /session/status','POST /join','GET /workspaces','POST /workspaces','POST /credentials/revoke','POST /queue/requeue','POST /queue/claim','POST /queue/renew','POST /queue/done','GET /queue/item','GET /queue/dead-letters','POST /topics/skip','POST /topics/keep','POST /topics/unkeep','GET /topics/pins','POST /topics','GET /topics','POST /topics/subscribe','POST /topics/leave','POST /topics/close','POST /invites','GET /invites','POST /invites/revoke','GET /members','POST /members/remove','POST /members/leave','POST /members/finish','POST /members/transfer','POST /heartbeat','POST /workstreams/register','POST /workstreams/check-overlap','POST /workstreams/release','POST /decisions/record','GET /decisions','POST /send','GET /inbox','GET /resume','POST /ack','POST /close','GET /stream'];
 assert.deepEqual([...checked].sort(),inventory.sort());
});

test('queue claimed controls require the workspace owner subscription across broker connections',async t=>{
 const {Store}=await import('../store.mjs'),f=await topicProcesses(t),topic='claimed-control';
 assert.equal((await f.call(f.a,'alice','owner',null,'/topics',{name:topic,mode:'queue'})).status,201);
 const setup=new Store(f.file);setup.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)','default',topic,'bob',Date.now());setup.close();
 const owner=await f.stream(f.b,'alice','control-owner',null),sibling=await f.stream(f.b,'bob','control-sibling',topic);
 const first=await f.call(f.b,'alice','publisher',null,'/send',{to:'topic:'+topic,body:'first offer'});assert.equal(first.status,201);
 assert.equal((await f.call(f.b,'bob','worker',topic,'/queue/claim',{id:first.data.id})).status,200);
 const barrier=await f.call(f.b,'bob','worker',topic,'/send',{to:'user:alice',body:'ordered delivery barrier'});assert.equal(barrier.status,201);
 await eventually(()=>owner.messages.some(m=>m.id===barrier.data.id));
 assert.equal(owner.events.some(e=>e.event==='claimed'&&e.data.id===first.data.id),false);
 await eventually(()=>sibling.events.some(e=>e.event==='claimed'&&e.data.id===first.data.id));
 assert.equal((await f.call(f.a,'alice','owner',null,'/topics/subscribe',{name:topic})).status,200);
 const second=await f.call(f.b,'alice','publisher',null,'/send',{to:'topic:'+topic,body:'subscribed offer'});assert.equal(second.status,201);
 assert.equal((await f.call(f.b,'bob','worker',topic,'/queue/claim',{id:second.data.id})).status,200);
 await eventually(()=>owner.events.some(e=>e.event==='claimed'&&e.data.id===second.data.id));
 assert.equal(owner.events.filter(e=>e.event==='claimed'&&e.data.id===second.data.id).length,1);
});
