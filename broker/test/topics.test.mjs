import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { rmSync } from 'node:fs';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { topicName } from '../topics.mjs';
import { SCHEMA_VERSION } from '../schema.mjs';
import { httpFixture } from './http.mjs';
import { testTempDir } from '../../test-support/temp.mjs';

async function fixture(t,file=':memory:') {
  const store=new Store(file),broker=new Broker(store,config({}),['alice','bob','eve'],{log:()=>{}});
  const wire=await httpFixture(t,broker);t.after(()=>store.close());
  store.run("INSERT INTO workspaces(name,created,created_by_user) VALUES ('other',1,'alice')");
  store.run("INSERT INTO workspace_members VALUES ('other','alice','owner',1,'creator'),('other','bob','member',1,'static')");
  const url=`http://127.0.0.1:${wire.server.address().port}`;
  const primary=new Map();
  const call=async(path,args,user='alice',workspace='default',bindingTopic)=>{
    const owner=store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user;
    const binding=user===owner?null:(bindingTopic??primary.get(workspace));
    const session=`${user}-${workspace}`+(binding&&binding!==primary.get(workspace)?'-'+binding.replaceAll('/','_'):'');
    const res=await wire.request(path,{method:args===undefined?'GET':'POST',...(args===undefined?{}:{body:JSON.stringify(args)}),user,session,headers:{'X-Workspace':workspace,...(binding?{'X-Topic':binding}:{})}});
    const data=await res.json();if(path==='/topics'&&res.status===201&&!primary.has(workspace))primary.set(workspace,args.name);
    return {status:res.status,data};
  };
  const redeem=async(code,name)=>{
    const res=await fetch(url+'/join',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code,name})});
    return {status:res.status,data:await res.json()};
  };
  const subscribe=async(name,user='bob',workspace='default')=>{store.run('INSERT OR IGNORE INTO topic_subscriptions VALUES (?,?,?,?)',workspace,name,user,Date.now());return call('/topics/subscribe',{name},user,workspace,name);};
  // Preserve historical non-workspace topic owners as migration/lifecycle fixtures.
  const legacyTopic=async(name,owner='bob',workspace='default')=>{const result=await call('/topics',{name},'alice',workspace);if(result.status===201)store.run('UPDATE topics SET owner_user=? WHERE workspace=? AND name=?',owner,workspace,name);return result;};
  return {store,broker,wire,call,redeem,subscribe,legacyTopic};
}

test('slash topic names round-trip byte for byte and malformed names are refused',async t=>{
  const f=await fixture(t);
  for(const value of ['Flow/Build_v1.2','a'.repeat(64)+'/'+'b'.repeat(63),'.a/..b']) {
    assert.equal(topicName(value),value);assert.equal((await f.call('/topics',{name:value})).status,201);
  }
  assert.deepEqual((await f.call('/topics')).data.topics.map(t=>t.name),['.a/..b','Flow/Build_v1.2','a'.repeat(64)+'/'+'b'.repeat(63)]);
  for(const name of ['a//b','a/./b','a/../b','a/','a'.repeat(129),'a'.repeat(64)+'/'+'b'.repeat(64),'API\u212A','a'.repeat(65),'/a','a\n','a b','é/a','.', '..']) {
    const r=await f.call('/topics',{name});assert.equal(r.status,400);assert.equal(r.data.error,'INVALID_TOPIC_NAME');
  }
  assert.equal(f.store.get('PRAGMA user_version').user_version,SCHEMA_VERSION);
  assert.ok(f.store.all('PRAGMA table_info(threads)').some(c=>c.name==='topic'));
  assert.ok(f.store.all('PRAGMA table_info(invites)').some(c=>c.name==='topics'));
});

test('topic creation, subscription and close follow ownership rules and notices reach only subscribers',async t=>{
  const f=await fixture(t);
  assert.equal((await f.legacyTopic('api/auth')).status,201);
  await f.subscribe('api/auth');
  assert.equal((await f.call('/topics',{name:'member-created'},'bob')).data.error,'NOT_WORKSPACE_OWNER');
  assert.equal((await f.call('/topics',{name:'api/auth'},'alice')).data.error,'TOPIC_EXISTS');
  assert.equal((await f.call('/topics',{name:'queue',mode:'queue'})).status,201);
  await f.call('/topics',{name:'zz/eve'});await f.subscribe('zz/eve','eve');
  assert.equal((await f.call('/topics/close',{name:'api/auth'},'eve', 'default','zz/eve')).data.error,'TOPIC_SCOPE_MISMATCH');
  await f.subscribe('api/auth');await f.call('/topics/subscribe',{name:'api/auth'},'alice');
  assert.equal((await f.call('/topics',undefined,'bob')).data.topics[0].subscribed,true);
  assert.deepEqual((await f.call('/topics',undefined,'eve','default','zz/eve')).data.topics.map(r=>r.name),['zz/eve']);
  assert.equal((await f.call('/topics/close',{name:'api/auth'},'bob')).status,200);
  assert.ok((await f.call('/inbox',undefined,'alice')).data.messages.some(m=>m.kind==='notice'&&m.body.includes('api/auth')));
  assert.ok((await f.call('/inbox',undefined,'bob')).data.messages.some(m=>m.kind==='notice'));
  assert.equal((await f.call('/inbox',undefined,'eve','default','zz/eve')).data.messages.length,0);
  assert.equal((await f.call('/topics')).data.topics[0].closed,true);
  assert.equal((await f.call('/topics/leave',{name:'api/auth'},'bob')).data.subscribed,false);
  const before=f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='topic:close'").n;
  await f.call('/topics/close',{name:'api/auth'});assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='topic:close'").n,before);
  await f.legacyTopic('workspace-owner-can-close');
  assert.equal((await f.call('/topics/close',{name:'workspace-owner-can-close'},'alice')).status,200);
});

test('closed topics refuse subscriptions and sends; unsubscribed topic sends store no message',async t=>{
  const f=await fixture(t);await f.legacyTopic('Flow/Build');
  const before=f.store.get('SELECT count(*) n FROM messages').n;
  for(const kind of ['finding','question']) {
    const r=await f.call('/send',{to:'topic:Flow/Build',body:'future delivery',kind},'bob');
    assert.equal(r.status,403);assert.equal(r.data.error,'NOT_SUBSCRIBED');
  }
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n,before);
  assert.equal((await f.call('/send',{to:'topic:missing',body:'not here'})).data.error,'TOPIC_NOT_FOUND');
  await f.call('/topics/close',{name:'Flow/Build'},'alice');
  assert.equal((await f.call('/topics/subscribe',{name:'Flow/Build'})).data.error,'TOPIC_CLOSED');
  assert.equal((await f.call('/send',{to:'topic:Flow/Build',body:'closed'})).data.error,'TOPIC_CLOSED');
});

test('topic scope follows the bound session and removal deletes only its subscriptions and transfers only its topics',async t=>{
  const f=await fixture(t);
  for(const workspace of ['default','other']) {
    await f.legacyTopic('same/name','bob',workspace);
    await f.subscribe('same/name','bob',workspace);
  }
  await f.call('/topics/subscribe',{name:'same/name'},'alice');
  await f.call('/topics',{name:'foreign/only'},'alice','other');
  assert.equal((await f.call('/topics/subscribe',{name:'foreign/only'})).data.error,'TOPIC_NOT_FOUND');
  assert.equal((await f.call('/send',{to:'topic:foreign/only',body:'no leak'})).data.error,'TOPIC_NOT_FOUND');
  assert.equal((await f.call('/topics',undefined,'eve','other')).data.error,'NOT_A_MEMBER');
  await f.call('/members/remove',{user:'bob'});
  assert.deepEqual(f.store.all("SELECT workspace FROM topic_subscriptions WHERE user='bob'").map(r=>r.workspace),['other']);
  assert.equal(f.store.get("SELECT owner_user FROM topics WHERE workspace='default' AND name='same/name'").owner_user,'alice');
  assert.equal(f.store.get("SELECT owner_user FROM topics WHERE workspace='other' AND name='same/name'").owner_user,'bob');
  assert.ok(f.store.get("SELECT 1 FROM topic_subscriptions WHERE workspace='default' AND user='alice'"));
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='topic:owner_transfer' AND workspace='default'").n,1);
  assert.equal((await f.call('/topics',undefined,'bob')).data.error,'NOT_A_MEMBER');
});

test('topic-scoped invites subscribe the new member atomically and revalidate closed topics before redemption',async t=>{
  const f=await fixture(t);await f.call('/topics',{name:'api/one'});await f.call('/topics',{name:'api/two'});
  assert.equal((await f.call('/invites',{topics:['absent']})).data.error,'TOPIC_NOT_FOUND');
  assert.equal((await f.call('/invites',{topics:'api/one'})).data.error,'INVALID_INVITE_TOPICS');
  assert.equal((await f.call('/invites',{topics:['api/one','api/two']})).data.error,'INVALID_INVITE_TOPICS');
  const i=(await f.call('/invites',{topics:['api/one']})).data;
  f.store.db.exec("CREATE TEMP TRIGGER fail_subscription BEFORE INSERT ON topic_subscriptions WHEN NEW.topic='api/one' BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.equal((await f.redeem(i.code,'joiner')).status,500);
  assert.equal(f.store.get("SELECT 1 FROM broker_users WHERE name='joiner'"),undefined);
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',i.invite_id).redeemed_at,null);
  assert.equal(f.store.get("SELECT count(*) n FROM topic_subscriptions WHERE user='joiner'").n,0);
  f.store.db.exec('DROP TRIGGER fail_subscription');
  assert.equal((await f.redeem(i.code,'joiner')).status,201);
  assert.deepEqual(f.store.all("SELECT topic FROM topic_subscriptions WHERE user='joiner' ORDER BY topic").map(r=>r.topic),['api/one']);
  const stale=(await f.call('/invites',{topics:['api/one']})).data;
  await f.call('/topics/close',{name:'api/one'});
  assert.equal((await f.redeem(stale.code,'later')).data.error,'TOPIC_CLOSED');
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',stale.invite_id).redeemed_at,null);
  assert.equal(f.store.get("SELECT 1 FROM broker_users WHERE name='later'"),undefined);
});

test('workspace archival clears subscriptions without topic owner handover and preserves topic history',async t=>{
  const dir=testTempDir('topics-archive-'),f=await fixture(t,join(dir,'broker.sqlite'));t.after(()=>rmSync(dir,{recursive:true,force:true}));
  await f.legacyTopic('retained/topic','bob','other');await f.subscribe('retained/topic','bob','other');
  const result=spawnSync(process.execPath,['broker/cli.mjs','delete-workspace','other','--reason','complete'],{env:{...process.env,BROKER_DB:join(dir,'broker.sqlite')},encoding:'utf8'});
  assert.equal(result.status,0);assert.equal(f.store.get("SELECT owner_user FROM topics WHERE workspace='other'").owner_user,'bob');
  assert.equal(f.store.get("SELECT count(*) n FROM topic_subscriptions WHERE workspace='other'").n,0);
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='topic:owner_transfer'").n,0);
  const unsub=f.store.get("SELECT actor_user,subject_user,detail FROM workspace_audit WHERE workspace='other' AND action='topic:unsubscribe'");
  assert.equal(unsub.actor_user,null);assert.equal(unsub.subject_user,'bob');
  assert.deepEqual(JSON.parse(unsub.detail),{topic:'retained/topic',reason:'workspace_deleted'});
});

test('F1: owning another workspace does not authorize closing this workspace topic',async t=>{
  const f=await fixture(t);
  f.store.run("INSERT INTO workspace_members VALUES ('other','eve','member',1,'invite')");
  assert.equal((await f.call('/members/transfer',{user:'eve'},'alice','other')).status,200);
  await f.legacyTopic('protected/topic');await f.subscribe('protected/topic','eve');
  const denied=await f.call('/topics/close',{name:'protected/topic'},'eve');
  assert.equal(denied.status,403);assert.equal(denied.data.error,'NOT_TOPIC_OWNER');
  assert.equal(f.store.get("SELECT closed_at FROM topics WHERE workspace='default' AND name='protected/topic'").closed_at,null);
  assert.equal((await f.call('/topics/close',{name:'protected/topic'},'alice')).status,200);
});

test('F2: same-name topics scope list, subscription, leave, close and notice recipients independently',async t=>{
  const f=await fixture(t),name='same/name';
  f.store.run("INSERT INTO workspace_members VALUES ('other','eve','member',1,'invite')");
  for(const workspace of ['default','other'])await f.legacyTopic(name,'bob',workspace);
  await f.call('/topics',{name:'local/only'},'alice');await f.call('/topics',{name:'foreign/only'},'alice','other');
  await f.call('/topics/subscribe',{name},'alice','other');
  const local=(await f.call('/topics')).data.topics,foreign=(await f.call('/topics',undefined,'alice','other')).data.topics;
  assert.deepEqual(local.map(t=>t.name),['local/only',name]);assert.deepEqual(foreign.map(t=>t.name),['foreign/only',name]);
  assert.equal(local.find(t=>t.name===name).subscribed,false);assert.equal(foreign.find(t=>t.name===name).subscribed,true);
  for(const workspace of ['default','other'])await f.subscribe(name,'bob',workspace);
  await f.call('/topics/leave',{name},'bob');
  assert.equal((await f.call('/topics',undefined,'bob')).data.error,'SESSION_ENDED');
  assert.equal((await f.call('/topics',undefined,'bob','other')).data.topics.find(t=>t.name===name).subscribed,true);
  await f.call('/topics/leave',{name},'alice','other');await f.call('/topics/subscribe',{name},'alice');
  await f.subscribe(name,'eve','other');
  assert.equal((await f.call('/topics/close',{name},'alice')).status,200);
  assert.ok(f.store.get("SELECT closed_at FROM topics WHERE workspace='default' AND name=?",name).closed_at);
  assert.equal(f.store.get("SELECT closed_at FROM topics WHERE workspace='other' AND name=?",name).closed_at,null);
  const closing=rows=>rows.filter(m=>m.kind==='notice'&&m.body.includes('closed topic same/name.'));
  assert.equal(closing((await f.call('/inbox')).data.messages).length,1);
  const notice=f.store.get("SELECT id FROM messages WHERE kind='notice' ORDER BY id DESC").id;
  for(const user of ['bob','eve'])assert.equal(f.store.get('SELECT 1 FROM escalation_targets WHERE message_id=? AND user=?',notice,user),undefined);
});

test('F6: every subscription ended by membership lifecycle is audited with its scoped reason',async t=>{
  for(const reason of ['remove','leave','finish','redeem_failed'])await t.test(reason,async t=>{
    const f=await fixture(t);for(const name of ['one','two'])await f.call('/topics',{name});let user='bob',token;
    if(reason==='redeem_failed') {
      const invitation=(await f.call('/invites',{topics:['one']})).data;
      const joined=await f.redeem(invitation.code,'joiner');assert.equal(joined.status,201);
      user='joiner';token=joined.data.token;
      f.store.run("INSERT INTO workspace_members VALUES ('other','joiner','member',1,'invite')");
    }
    const userCall=async(path,args,workspace='default')=>{
      if(path==='/topics/subscribe')f.store.run('INSERT OR IGNORE INTO topic_subscriptions VALUES (?,?,?,?)',workspace,args.name,user,Date.now());
      if(!token)return f.call(path,args,user,workspace,args?.name);
      const response=await f.wire.request(path,{method:'POST',body:JSON.stringify(args),user,session:`${user}-${workspace}`+(args.name?'-'+args.name:''),
        headers:{Authorization:`Bearer ${token}`,'X-Workspace':workspace,'X-Topic':args.name??'one'}});
      return {status:response.status,data:await response.json()};
    };
    for(const name of ['one','two']){await userCall('/topics/subscribe',{name});}
    await f.call('/topics',{name:'one'},'alice','other');await userCall('/topics/subscribe',{name:'one'},'other');
    const response=reason==='remove'?await f.call('/members/remove',{user}):reason==='redeem_failed'?await userCall('/credentials/revoke',{}):
      await userCall('/members/'+reason,reason==='finish'?{summary:'complete'}:{});
    assert.equal(response.status,200);
    const rows=f.store.all("SELECT * FROM workspace_audit WHERE action='topic:unsubscribe' ORDER BY id");
    assert.equal(rows.length,2);
    assert.deepEqual(rows.map(r=>JSON.parse(r.detail)).sort((a,b)=>a.topic.localeCompare(b.topic)),[{topic:'one',reason},{topic:'two',reason}]);
    for(const row of rows){assert.equal(row.workspace,'default');assert.equal(row.subject_user,user);assert.equal(row.actor_user,reason==='remove'?'alice':user);}
    assert.ok(f.store.get("SELECT 1 FROM topic_subscriptions WHERE workspace='other' AND user=?",user));
  });
});

test('F6: subscription removal and its audit roll back with a failed membership end',async t=>{
  const f=await fixture(t);await f.call('/topics',{name:'rollback'});await f.subscribe('rollback');
  f.store.db.exec("CREATE TEMP TRIGGER fail_end BEFORE DELETE ON workspace_members WHEN OLD.user='bob' BEGIN SELECT RAISE(ABORT,'injected'); END");
  assert.equal((await f.call('/members/remove',{user:'bob'})).status,500);
  assert.ok(f.store.get("SELECT 1 FROM topic_subscriptions WHERE workspace='default' AND topic='rollback' AND user='bob'"));
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='topic:unsubscribe'").n,0);
  f.store.db.exec('DROP TRIGGER fail_end');
  assert.equal((await f.call('/members/remove',{user:'bob'})).status,200);
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='topic:unsubscribe'").n,1);
});
