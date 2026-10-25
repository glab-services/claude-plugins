import {test} from 'node:test';
import assert from 'node:assert/strict';
import {Store} from '../store.mjs';
import {Broker} from '../broker.mjs';
import {config} from '../config.mjs';
import {createServer} from '../server.mjs';
import {leaveTopic} from '../topics.mjs';
async function fixture(t){
 const store=new Store(':memory:');let now=1000;const broker=new Broker(store,config({}),['alice','bob'],{now:()=>now,log:()=>{}}),tokens={alice:'alice-session-status-fixture-000000',bob:'bob-session-status-fixture-0000000'};
 for(const topic of ['x','y','closed'])store.run("INSERT INTO topics VALUES ('default',?,'broadcast','alice',1,?,NULL)",topic,topic==='closed'?900:null);
 for(const user of ['alice','bob'])for(const topic of ['x','closed'])store.run("INSERT INTO topic_subscriptions VALUES ('default',?,?,1)",topic,user);
 broker.createWorkspace('alice',{name:'other'});store.run("INSERT INTO workspace_members VALUES ('other','bob','member',1,'static')");
 broker.heartbeat('alice','active-owner','manager','default',null);broker.register('alice','active-owner','root-lease');broker.heartbeat('bob','active-member','worker','default','x');broker.register('bob','active-member','member-lease');
 const server=createServer(broker,new Map(Object.entries(tokens).map(([u,v])=>[v,u])));await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(async()=>{server.closeAllConnections();await new Promise(r=>server.close(r));store.close();});
 const call=async(path,args,user='alice',session='active-owner',topic=null,workspace='default',token=tokens[user])=>{const response=await fetch('http://127.0.0.1:'+server.address().port+path,{method:args===undefined?'GET':'POST',headers:{Authorization:'Bearer '+token,'X-Session-Id':session,'X-Workspace':workspace,'Content-Type':'application/json',...(topic===null?{}:{'X-Topic':topic})},...(args===undefined?{}:{body:JSON.stringify(args)})});return {status:response.status,data:await response.json()};};
 const snapshot=()=>JSON.stringify(['agents','workstreams','workspace_members','topic_subscriptions','workspace_audit','messages','invites'].map(table=>[table,store.all('SELECT * FROM '+table+' ORDER BY rowid')]));
 const probe=async(user,session,topic,workspace='default')=>{const before=snapshot(),changes=store.get('SELECT total_changes() n').n,result=await call('/session/status',undefined,user,session,topic,workspace);assert.ok(snapshot()===before,'session probe preserves all rows');assert.equal(store.get('SELECT total_changes() n').n,changes);return result;};
 return {store,broker,call,probe,snapshot,advance:()=>{now+=500;}};
}
test('session status reads active/unregistered bindings without creating rows or renewing timestamps/leases',async t=>{
 const f=await fixture(t);f.advance();
 for(const [user,id,topic,state] of [['alice','active-owner',null,'active'],['bob','active-member','x','active'],['alice','never-started-owner',null,'unregistered'],['bob','never-started-member','x','unregistered']]){
  const result=await f.probe(user,id,topic);assert.equal(result.status,200);assert.equal(result.data.state,state);
 }
 assert.equal(f.store.get("SELECT count(*) n FROM agents WHERE session_id LIKE 'never-started-%'").n,0);
 assert.equal(f.store.get("SELECT last_seen FROM agents WHERE session_id='active-owner'").last_seen,1000);assert.equal(f.store.get("SELECT role FROM agents WHERE session_id='active-owner'").role,'manager');
});
test('session status validates every denied branch and leaves total_changes/rows unchanged',async t=>{
 const f=await fixture(t);f.advance();
 for(const [user,id,topic,workspace,error] of [['bob','active-owner',null,'default','SESSION_OWNED'],['bob','active-member','y','default','SESSION_TOPIC_MISMATCH'],['bob','active-member','x','other','SESSION_WORKSPACE_MISMATCH'],['bob','new-no-topic',null,'default','TOPIC_REQUIRED'],['bob','new-y','y','default','NOT_SUBSCRIBED'],['bob','new-closed','closed','default','TOPIC_CLOSED'],['bob','new-missing','x','missing','NOT_A_MEMBER']])assert.equal((await f.probe(user,id,topic,workspace)).data.error,error);
 leaveTopic(f.broker,'bob','active-member',{name:'x'});
 const before=f.snapshot(),changes=f.store.get('SELECT total_changes() n').n;const invalid=await f.call('/session/status?unexpected=1');assert.equal(invalid.status,400);assert.ok(f.snapshot()===before);assert.equal(f.store.get('SELECT total_changes() n').n,changes);
 const ended=await f.probe('bob','active-member','x');assert.equal(ended.status,409);assert.equal(ended.data.error,'SESSION_ENDED');assert.equal(ended.data.reason,'topic_left');
 assert.equal((await f.probe('bob','active-member','y')).data.error,'SESSION_TOPIC_MISMATCH');
});
test('ALREADY_MEMBER discloses only verified binding metadata with no invite/audit/membership writes',async t=>{
 const f=await fixture(t),invite=await f.call('/invites',{topics:['x']});assert.equal(invite.status,201);
 const before=f.snapshot(),changes=f.store.get('SELECT total_changes() n').n;
 const same=await f.call('/join',{code:invite.data.code,name:'bob'},'bob','not-a-session','x');assert.equal(same.status,409);assert.equal(same.data.error,'ALREADY_MEMBER');assert.deepEqual({user:same.data.user,workspace:same.data.workspace,topic:same.data.topic},{user:'bob',workspace:'default',topic:'x'});
 assert.ok(f.snapshot()===before,'already-member preserves rows');assert.equal(f.store.get('SELECT total_changes() n').n,changes);assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',invite.data.invite_id).redeemed_at,null);
 const wrong=invite.data.code.slice(0,-1)+(invite.data.code.endsWith('A')?'B':'A');
 for(const args of [{code:wrong,name:'bob'},{code:invite.data.code,name:'alice'}]){
  const denied=await f.call('/join',args,'bob');assert.deepEqual(Object.keys(denied.data).sort(),['error','hint']);
 }
 f.store.run('UPDATE invites SET expires=0 WHERE id=?',invite.data.invite_id);
 const denied=await f.call('/join',{code:invite.data.code,name:'bob'},'bob','unused','x','default','invalid-session-fixture-token');assert.equal(denied.status,401);assert.deepEqual(Object.keys(denied.data).sort(),['error','hint']);
 const expired=await f.call('/join',{code:invite.data.code,name:'bob'},'bob');assert.equal(expired.data.error,'INVITE_EXPIRED');assert.deepEqual(Object.keys(expired.data).sort(),['error','hint']);
});
