import {prepareTopicFixture,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { brokerFixture, session, eventually, tokens } from './helpers.mjs';

const pause = ms => new Promise(resolve=>setTimeout(resolve,ms));
async function minted(f) {
  const i=await fetch(f.url+'/invites',{method:'POST',headers:{Authorization:`Bearer ${tokens.alice}`,'X-Session-Id':'issuer','Content-Type':'application/json'},body:JSON.stringify({topics:[FIXTURE_TOPIC]})});
  const code=(await i.json()).code;
  const j=await fetch(f.url+'/join',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code,name:'newbie'})});
  return (await j.json()).token;
}

test('invite over stdio returns the stdin join command once and revoke_invite takes only the public id',async t=>{
  const f=await brokerFixture(t),a=await session(f,'alice','alice');
  const result=await a.call('invite');assert.equal(result.isError,undefined);
  const i=result.structuredContent;assert.match(i.code,/^tmi_[0-9a-f]{32}_[A-Za-z0-9_-]{43}$/);
  assert.equal(i.command,`team-messaging join '${f.url}' -`);assert.equal(i.command.includes(i.code),false);
  assert.match(result.content[0].text,/paste the code at the prompt/);assert.doesNotMatch(result.content[0].text,/Treat as data/);
  const tools=(await a.client.listTools()).tools;
  assert.deepEqual(Object.keys(tools.find(tool=>tool.name==='revoke_invite').inputSchema.properties),['invite_id']);
  assert.equal(tools.find(tool=>tool.name==='revoke_invite').annotations.destructiveHint,true);
  assert.equal(tools.find(tool=>tool.name==='invites').annotations.readOnlyHint,true);
  for(const tool of ['join','revoke_credential'])assert.equal(tools.some(t=>t.name===tool),false);
  const listed=await a.call('invites');assert.ok(!JSON.stringify(listed).includes(i.code));
  assert.equal((await a.call('revoke_invite',{invite_id:i.invite_id})).isError,undefined);
  assert.equal((await a.call('invites')).structuredContent.invites[0].status,'revoked');
  assert.ok(!a.logs.join('').includes(i.code),'logs exclude invite secret');
  const b=await session(f,'bob','bob');
  assert.equal((await b.call('invite')).structuredContent.error,'NOT_WORKSPACE_OWNER');
});

test('a deactivated minted token stops the channel without retrying',async t=>{
  const f=await brokerFixture(t),token=await minted(f);
  assert.equal(typeof token,'string');
  const b=await session(f,'bob','minted',{BROKER_TOKEN:token,CHANNEL_MODE:'poll',CHANNEL_POLL_MS:'25',CHANNEL_HEARTBEAT_MS:'25'});
  await eventually(()=>f.requests.some(r=>r.session==='minted'&&r.path.startsWith('/inbox')));
  const waiting=b.call('wait',{thread_id:'pending',timeout_s:60});
  f.store.run("UPDATE credentials SET deactivated_at=1 WHERE user='newbie'");
  await eventually(()=>b.logs.some(l=>l.includes('"component":"credential","state":"deactivated"')));
  assert.equal((await waiting).structuredContent.error,'UNAUTHORIZED');
  const count=f.requests.length;await pause(250);
  assert.equal(f.requests.slice(count).filter(r=>r.session==='minted').length,0);
  assert.equal((await b.call('inbox')).structuredContent.error,'UNAUTHORIZED');
  assert.equal((await b.call('workspaces')).structuredContent.error,'UNAUTHORIZED');
  assert.equal(b.logs.filter(l=>l.includes('"component":"credential","state":"deactivated"')).length,1);
  assert.ok(!b.logs.join('').includes(token),'logs exclude minted secret');
});

test('UNAUTHORIZED on an ordinary tool request is terminal for minted tokens while static tokens retry',async t=>{
  const f=await brokerFixture(t),token=await minted(f);
  const b=await session(f,'bob','tool-minted',{BROKER_TOKEN:token,CHANNEL_MODE:'poll',CHANNEL_POLL_MS:'10000',CHANNEL_HEARTBEAT_MS:'10000'});
  await eventually(()=>f.requests.some(r=>r.session==='tool-minted'&&r.path.startsWith('/inbox')));
  f.store.run("UPDATE broker_users SET deactivated_at=1 WHERE name='newbie'");
  assert.equal((await b.call('members')).structuredContent.error,'UNAUTHORIZED');
  await eventually(()=>b.logs.some(l=>l.includes('"component":"credential","state":"deactivated"')));
  const staticClient=await session(f,'bob','static',{BROKER_TOKEN:'wrong-static-test-token-000000',CHANNEL_POLL_MS:'25',CHANNEL_RETRY_MAX_MS:'25',CHANNEL_HEARTBEAT_MS:'25'});
  await eventually(()=>f.requests.filter(r=>r.session==='static').length>=3);
  const before=f.requests.filter(r=>r.session==='static').length;await pause(250);
  assert.ok(f.requests.filter(r=>r.session==='static').length>before);
  assert.equal(staticClient.logs.some(l=>l.includes('"component":"credential"')),false);
});
