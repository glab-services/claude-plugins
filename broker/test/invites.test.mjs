import {prepareTopicFixture,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import { join as pathJoin } from 'node:path';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { createServer } from '../server.mjs';
import { mint, parse, verify } from '../credentials.mjs';
import { testTempDir } from '../../test-support/temp.mjs';
import { eventually } from './http.mjs';

const stores=new Map();
const tokens = { alice: 'alice-invite-fixture-00000000000000', bob: 'bob-invite-fixture-0000000000000000' };
async function call(url, path, args, token, session, workspace = 'default') {
  if(path==='/invites'&&args!==undefined&&args.topics===undefined)args={...args,topics:[FIXTURE_TOPIC]};
  let admin=false;
  if(['/members/remove','/members/transfer'].includes(path)&&stores.has(url)){const source=stores.get(url),store=typeof source==='string'?new Store(source):source;try{const user=Object.keys(tokens).find(u=>tokens[u]===token)??store.get('SELECT user FROM credentials WHERE id=?',parse('tmc',token)?.id??'')?.user;admin=store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user===user;}finally{if(typeof source==='string')store.close();}}
  const response = await fetch(url + path, { method: args === undefined ? 'GET' : 'POST',
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(session ? { 'X-Session-Id': admin?session+'-workspace-admin':session, 'X-Workspace': workspace,...(admin?{}:{'X-Topic':FIXTURE_TOPIC}) } : {}) },
    ...(args === undefined ? {} : { body: JSON.stringify(args) }) });
  const data=await response.json();
  if(path==='/workspaces'&&response.status===201&&stores.has(url)){const source=stores.get(url),store=typeof source==='string'?new Store(source):source;try{prepareTopicFixture({store,now:Date.now},args.name);}finally{if(typeof source==='string')store.close();}}
  return { status: response.status, data, cache: response.headers.get('cache-control') };
}
async function fixture(t, overrides = {}) {
  const store = new Store(':memory:');
  let now = Date.now(); const logs = [];
  const broker = new Broker(store, { ...config({}), joinRatePerMin: 1000, sseHeartbeat: 20, ...overrides }, Object.keys(tokens), { now: () => now, log: value => logs.push(value) });
  prepareTopicFixture(broker);
  const server = createServer(broker, new Map(Object.entries(tokens).map(([user, token]) => [token, user])));
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); store.close(); });
  const url = `http://127.0.0.1:${server.address().port}`;
  stores.set(url,store);t.after(()=>stores.delete(url));
  const owner = (path, args) => call(url, path, args, tokens.alice, 'owner');
  const issue = async () => { const r = await owner('/invites', {}); assert.equal(r.status, 201); return r.data; };
  return { store, broker, url, logs, owner, issue, advance: ms => { now += ms; } };
}
function safeEqual(actual, expected, label) { assert.ok(actual === expected, label); } // Never emit credential values on assertion failure.
async function stream(t, url, token, session, workspace = 'default') {
  const request = http.get(url + '/stream?replay=1', { headers: { Authorization: `Bearer ${token}`, 'X-Session-Id': session, 'X-Workspace': workspace, 'X-Topic':FIXTURE_TOPIC } });
  const response = await new Promise((resolve, reject) => { request.once('response', resolve); request.once('error', reject); });
  assert.equal(response.statusCode, 200); let closed = false;
  const frames=[];response.on('data', chunk=>frames.push(chunk.toString()));
  response.resume(); response.on('error', () => {}); response.once('close', () => { closed = true; });
  t.after(() => request.destroy());
  const ended = () => closed; ended.frames = frames; return ended;
}

test('credential formats are strict and digest comparison never accepts a forged secret', () => {
  for (const prefix of ['tmi','tmc']) {
    const value = mint(prefix), parsed = parse(prefix, value.value);
    safeEqual(parsed.id, value.id, 'public id roundtrip'); safeEqual(parsed.secret, value.secret, 'secret roundtrip');
    assert.ok(verify({ secret_sha256: value.sha256 }, parsed.secret));
    assert.equal(verify({ secret_sha256: value.sha256 }, mint(prefix).secret), false);
    for (const invalid of [null, '', value.value+'x', ' '+value.value, value.value+'\n', value.value.replace(prefix,'xxx')]) assert.equal(parse(prefix, invalid), null);
  }
});

test('an invite redeems once into a new user whose token works for a workspace round trip', async t => {
  const f = await fixture(t), invitation = await f.issue();
  const joined = await call(f.url, '/join', { code: invitation.code, name: 'newbie' });
  assert.equal(joined.status, 201); assert.equal(joined.cache, 'no-store');
  assert.ok(parse('tmc', joined.data.token));
  assert.equal(f.store.get('SELECT secret_sha256 FROM credentials').secret_sha256.length, 64);
  assert.equal((await call(f.url, '/workspaces', undefined, joined.data.token)).data.workspaces[0].name, 'default');
  const sent = await call(f.url, '/send', { to: 'user:alice', body: 'hello' }, joined.data.token, 'newbie');
  assert.equal(sent.status, 201);
  assert.ok((await f.owner('/inbox')).data.messages.some(m => m.body === 'hello'));
  const reused = await call(f.url, '/join', { code: invitation.code, name: 'other' });
  assert.equal(reused.data.error, 'INVITE_USED'); assert.ok(reused.data.hint);
  assert.equal(f.store.get('SELECT count(*) n FROM broker_users').n, 1);
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE action='member:add' AND subject_user='newbie'").n, 1);
  const listing = await f.owner('/invites');
  assert.equal(listing.data.invites[0].status, 'redeemed');
  assert.equal(JSON.stringify(listing).includes(invitation.code), false);
  assert.deepEqual(Object.keys(listing.data.invites[0]).sort(), ['expires','invite_id','status']);
});

test('expired, used, revoked and forged codes return stable codes and hints without disclosing state to a wrong secret', async t => {
  const f = await fixture(t), expired = await f.issue(); f.advance(3600001);
  assert.equal((await call(f.url,'/join',{code:expired.code,name:'expired'})).data.error,'INVITE_EXPIRED');
  const revoked = await f.issue(); await f.owner('/invites/revoke',{invite_id:revoked.invite_id});
  assert.equal((await call(f.url,'/join',{code:revoked.code,name:'revoked'})).data.error,'INVITE_REVOKED');
  const forged = `tmi_${revoked.invite_id}_${mint('tmi').secret}`;
  const bad = await call(f.url,'/join',{code:forged,name:'forged'});
  assert.equal(bad.data.error,'INVITE_INVALID'); assert.ok(bad.data.hint);
  const deleted = await f.issue();
  f.store.run("UPDATE workspaces SET deleted_at=1 WHERE name='default'");
  assert.equal((await call(f.url,'/join',{code:deleted.code,name:'deleted'})).data.error,'INVITE_INVALID');
});

test('non-owners cannot invite, list or revoke and the invite limit excludes expired and revoked rows', async t => {
  const f=await fixture(t,{inviteLimit:1}), i=await f.issue();
  for (const [path,args] of [['/invites',{}],['/invites',undefined],['/invites/revoke',{invite_id:i.invite_id}]]) {
    const denied=await call(f.url,path,args,tokens.bob,'bob'); assert.equal(denied.status,403); assert.equal(denied.data.error,'NOT_WORKSPACE_OWNER');
  }
  assert.equal((await f.owner('/invites',{})).data.error,'INVITE_LIMIT');
  await f.owner('/invites/revoke',{invite_id:i.invite_id}); await f.issue();
  f.advance(3600001); await f.issue();
  assert.ok(f.store.get("SELECT 1 FROM workspace_audit WHERE action='invite:list'")===undefined);
  await f.owner('/invites');
  for(const action of ['invite:create','invite:revoke','invite:list'])assert.ok(f.store.get('SELECT 1 FROM workspace_audit WHERE action=?',action));
});

test('a history-only name is refused, including a removed default member', async t => {
  const f=await fixture(t), i=await f.issue();
  await f.owner('/send',{to:'user:bob',body:'history'});
  f.store.run("UPDATE messages SET from_user='history-only' WHERE kind='finding'");
  // The removed member remains reserved through the durable membership audit.
  f.store.run("INSERT INTO workspace_members VALUES ('default','departed','member',1,'static')");
  await f.owner('/members/remove',{user:'departed'});
  for (const name of ['history-only','departed']) {
    const r=await call(f.url,'/join',{code:i.code,name}); assert.equal(r.data.error,'USER_EXISTS'); assert.match(r.data.hint,/choose another name/);
  }
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',i.invite_id).redeemed_at,null);
});

test('a static or minted name is reserved; an existing bearer binds that same user without minting', async t => {
  const f=await fixture(t), i=await f.issue();
  assert.equal((await call(f.url,'/join',{code:i.code,name:'alice'})).data.error,'USER_EXISTS');
  assert.equal((await call(f.url,'/join',{code:i.code,name:'alice'},tokens.alice)).data.error,'ALREADY_MEMBER');
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',i.invite_id).redeemed_at,null);
  assert.equal((await call(f.url,'/join',{code:i.code,name:'x'},'invalid')).data.error,'UNAUTHORIZED');
  const joined=await call(f.url,'/join',{code:i.code,name:'newbie'});
  const i2=await f.issue();assert.equal((await call(f.url,'/join',{code:i2.code,name:'newbie'})).data.error,'USER_EXISTS');
  await f.owner('/workspaces',{name:'second'});
  const issued=await call(f.url,'/invites',{},tokens.alice,'owner-second','second');
  const bound=await call(f.url,'/join',{code:issued.data.code,name:'newbie'},joined.data.token);
  assert.equal(bound.status,201);assert.equal(bound.data.user,'newbie');assert.equal('token' in bound.data,false);
  assert.equal(f.store.get('SELECT count(*) n FROM credentials').n,1);
  const staticI=await call(f.url,'/invites',{},tokens.alice,'owner-second','second');
  assert.equal((await call(f.url,'/join',{code:staticI.data.code},tokens.bob)).status,201);
});

test('last membership removal deactivates minted users but static tokens remain authenticating', async t => {
  const f=await fixture(t), i=await f.issue(), joined=await call(f.url,'/join',{code:i.code,name:'newbie'}), token=joined.data.token;
  const closed=await stream(t,f.url,token,'newbie-stream');
  assert.equal((await call(f.url,'/members/finish',{summary:'complete'},token,'newbie')).status,200);
  await eventually(closed);
  assert.ok(closed.frames.join('').includes('event: removed'), 'local removal control precedes stream end');
  const denied=await call(f.url,'/workspaces',undefined,token);assert.equal(denied.status,401);assert.equal(denied.data.error,'UNAUTHORIZED');
  assert.ok(f.store.get("SELECT deactivated_at FROM broker_users WHERE name='newbie'").deactivated_at);
  assert.ok(f.store.get("SELECT 1 FROM workspace_audit WHERE action='user:deactivate' AND subject_user='newbie'"));
  await f.owner('/members/remove',{user:'bob'});
  assert.equal((await call(f.url,'/workspaces',undefined,tokens.bob)).status,200);
});

test('credentials/revoke needs no session, ends the redemption membership, and deactivates the user', async t => {
  const f=await fixture(t), i=await f.issue(), j=await call(f.url,'/join',{code:i.code,name:'newbie'});
  const r=await call(f.url,'/credentials/revoke',{},j.data.token);assert.equal(r.status,200);
  assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE user='newbie'"),undefined);
  assert.equal((await call(f.url,'/workspaces',undefined,j.data.token)).status,401);
  assert.equal(f.store.get("SELECT count(*) n FROM agents WHERE user='newbie'").n,0);
  assert.ok(f.store.get("SELECT 1 FROM workspace_audit WHERE action='member:redeem_failed'"));
  assert.ok(f.store.get("SELECT 1 FROM workspace_audit WHERE action='credential:revoke'"));
  assert.equal((await call(f.url,'/credentials/revoke',{},tokens.alice)).data.error,'NOT_MINTED_CREDENTIAL');
});

test('decision 67: recovery revoke cannot orphan an owner and works after ownership transfer', async t => {
  const f=await fixture(t), i=await f.issue(), j=await call(f.url,'/join',{code:i.code,name:'newbie'}), token=j.data.token;
  await f.owner('/members/transfer',{user:'newbie'});
  const before=f.store.get('SELECT count(*) n FROM workspace_audit').n;
  const r=await call(f.url,'/credentials/revoke',{},token);assert.equal(r.status,409);assert.equal(r.data.error,'OWNER_MUST_TRANSFER');
  assert.equal(f.store.get('SELECT count(*) n FROM workspace_audit').n,before);
  assert.equal(f.store.get("SELECT role FROM workspace_members WHERE user='newbie'").role,'owner');
  assert.equal((await call(f.url,'/workspaces',undefined,token)).status,200);
  await call(f.url,'/members/transfer',{user:'alice'},token,'newbie');
  assert.equal((await call(f.url,'/credentials/revoke',{},token)).status,200);
  assert.equal((await call(f.url,'/workspaces',undefined,token)).status,401);
});

test('unauthenticated join is globally rate limited before DB work and has a bounded body', async t => {
  const f=await fixture(t,{joinRatePerMin:1});
  const first=await call(f.url,'/join',{code:'invalid',name:'x'});assert.equal(first.data.error,'INVITE_INVALID');
  const second=await call(f.url,'/join',{code:'invalid',name:'y'});assert.equal(second.status,429);assert.equal(second.data.error,'RATE_LIMITED');
  f.advance(60000);
  const huge=await call(f.url,'/join',{code:'x'.repeat(2049)});assert.equal(huge.status,413);assert.equal(huge.data.error,'BODY_TOO_LARGE');
});

for(const identity of ['static','minted'])test(`anonymous join floods cannot exhaust ${identity} bearer admission`,async t=>{
  const f=await fixture(t,{joinRatePerMin:1});let token=tokens.bob,user='bob';
  if(identity==='minted') {
    const invite=await f.issue(),joined=await call(f.url,'/join',{code:invite.code,name:'isolated'});assert.equal(joined.status,201);
    token=joined.data.token;user='isolated';f.advance(60000);
  }
  await f.owner('/workspaces',{name:'admission'});
  const issued=await call(f.url,'/invites',{},tokens.alice,'admission-owner','admission');assert.equal(issued.status,201);
  for(let n=0;n<20;n++) {
    const result=await call(f.url,'/join',{code:'invalid',name:'anonymous'});
    assert.equal(result.status,n===0?403:429);
  }
  assert.equal((await call(f.url,'/join',{code:'invalid'},'invalid-bearer')).status,429);
  const joined=await call(f.url,'/join',{code:issued.data.code,name:user},token);
  assert.equal(joined.status,201);assert.ok(f.broker.member(user,'admission'));
  assert.equal((await call(f.url,'/join',{code:'invalid'},token)).status,429);
});

async function processes(t) {
  const dir=testTempDir('invites-process-'), db=pathJoin(dir,'broker.sqlite'), tokenPath=pathJoin(dir,'tokens.json');
  writeFileSync(tokenPath,JSON.stringify(tokens),{mode:0o600});const setup=new Store(db);prepareTopicFixture(new Broker(setup,config({}),Object.keys(tokens),{log:()=>{}}));setup.close();
  const children=[], logs=[];
  t.after(async()=>{for(const p of children){if(p.child.exitCode===null && p.child.signalCode===null)p.child.kill('SIGTERM');await p.exited;}rmSync(dir,{recursive:true,force:true});});
  async function start() {
    const child=spawn(process.execPath,['broker/main.mjs'],{env:{...process.env,BROKER_PORT:'0',BROKER_HOST:'127.0.0.1',BROKER_DB:db,BROKER_TOKENS:tokenPath,SSE_HEARTBEAT:'1',JOIN_RATE_PER_MIN:'1000'},stdio:['ignore','pipe','pipe']});
    let port;const lines=[];child.stdout.on('data',c=>logs.push(c.toString()));child.stderr.on('data',c=>{const value=c.toString();logs.push(value);lines.push(value);});
    const exited=new Promise(resolve=>child.once('exit',resolve));children.push({child,exited});
    await eventually(()=>{for(const line of lines.join('').split('\n')){try{const v=JSON.parse(line);if(v.event==='broker_listening')port=v.address.port;}catch{}}return Boolean(port);});
    return {url:`http://127.0.0.1:${port}`,child,exited};
  }
  const a=await start(), b=await start();stores.set(a.url,db);stores.set(b.url,db);t.after(()=>{stores.delete(a.url);stores.delete(b.url);});
  return {a,b,start,db,logs,owner:(path,args)=>call(a.url,path,args,tokens.alice,'owner')};
}

test('regression guard: five redemptions from separate broker processes yield exactly one user', async t => {
  // BEGIN IMMEDIATE and the guarded invite update are the actual cross-process guarantee.
  const f=await processes(t), i=(await f.owner('/invites',{})).data;
  const rs=await Promise.all(Array.from({length:5},(_,k)=>call(k%2?f.a.url:f.b.url,'/join',{code:i.code,name:`new${k}`})));
  assert.equal(rs.filter(r=>r.status===201).length,1);
  assert.equal(rs.filter(r=>r.data.error==='INVITE_USED').length,4);
  const store=new Store(f.db);try{assert.equal(store.get('SELECT count(*) n FROM broker_users').n,1);assert.equal(store.get('SELECT count(*) n FROM credentials').n,1);}finally{store.close();}
});

test('regression guard: revoke racing redeem ends in exactly one of revoked or redeemed', async t => {
  const f=await processes(t),i=(await f.owner('/invites',{})).data;
  await Promise.all([call(f.a.url,'/invites/revoke',{invite_id:i.invite_id},tokens.alice,'owner'),call(f.b.url,'/join',{code:i.code,name:'newbie'})]);
  const store=new Store(f.db);try{const row=store.get('SELECT * FROM invites WHERE id=?',i.invite_id);assert.ok((row.revoked_at!==null)!==(row.redeemed_at!==null));assert.equal(store.get('SELECT count(*) n FROM broker_users').n,row.redeemed_at!==null?1:0);}finally{store.close();}
});

test('a minted token survives restart and separate-process revocation ends a stream and next request is 401', async t => {
  const f=await processes(t),i=(await f.owner('/invites',{})).data,j=await call(f.a.url,'/join',{code:i.code,name:'newbie'}),token=j.data.token;
  f.b.child.kill('SIGTERM');await f.b.exited;
  const restarted=await f.start();assert.equal((await call(restarted.url,'/workspaces',undefined,token)).status,200);
  const closed=await stream(t,restarted.url,token,'other-process');
  await f.owner('/workspaces',{name:'second'});
  const i2=await call(f.a.url,'/invites',{},tokens.alice,'owner-second','second');
  assert.equal((await call(f.a.url,'/join',{code:i2.data.code},token)).status,201);
  const closedSecond=await stream(t,restarted.url,token,'second-stream','second');
  // Real HTTP revocation in A leaves the second membership; B must check the credential.
  assert.equal((await call(f.a.url,'/credentials/revoke',{},token)).status,200);
  const store=new Store(f.db);try{assert.ok(store.get("SELECT 1 FROM workspace_members WHERE user='newbie' AND workspace='second'"));}finally{store.close();}
  await eventually(closedSecond);
  await eventually(closed);
  const denied=await call(restarted.url,'/workspaces',undefined,token);assert.equal(denied.status,401);assert.equal(denied.data.error,'UNAUTHORIZED');
});

test('no code or token value appears in broker logs or error bodies', async t => {
  const f=await processes(t),i=(await f.owner('/invites',{})).data,j=await call(f.a.url,'/join',{code:i.code,name:'newbie'});
  const errors=[];
  errors.push(await call(f.b.url,'/join',{code:i.code,name:'repeat'}));
  errors.push(await call(f.a.url,'/join',{code:'malformed-secret-invite'}));
  errors.push(await call(f.a.url,'/workspaces',undefined,'malformed-secret-token'));
  await call(f.a.url,'/credentials/revoke',{},j.data.token);
  errors.push(await call(f.b.url,'/workspaces',undefined,j.data.token));
  const captured=f.logs.join('')+JSON.stringify(errors);
  for(const secret of [i.code,j.data.token,...Object.values(tokens),'malformed-secret-invite','malformed-secret-token']) assert.ok(!captured.includes(secret),'secret absent from logs and rejection bodies');
});


test('a restarted minted user finishes their last membership and every broker rejects its token', async t => {
  const f=await processes(t),i=(await f.owner('/invites',{})).data,j=await call(f.a.url,'/join',{code:i.code,name:'newbie'}),token=j.data.token;
  f.a.child.kill('SIGTERM');await f.a.exited;
  const a=await f.start();assert.equal((await call(a.url,'/workspaces',undefined,token)).status,200);
  const closed=await stream(t,f.b.url,token,'finish-stream');
  assert.equal((await call(a.url,'/members/finish',{summary:'finished after restart'},token,'finish')).status,200);
  await eventually(closed);
  for(const url of [a.url,f.b.url]){const denied=await call(url,'/workspaces',undefined,token);assert.equal(denied.status,401);assert.equal(denied.data.error,'UNAUTHORIZED');}
});

test('redemption rolls back the user, credential, membership and invite consumption after a real insert failure', async t => {
  const f=await fixture(t),i=await f.issue();
  const before=f.store.get('SELECT count(*) n FROM workspace_audit').n;
  f.store.db.exec("CREATE TEMP TRIGGER fail_redemption BEFORE INSERT ON workspace_audit WHEN NEW.action='invite:redeem' BEGIN SELECT RAISE(ABORT, 'injected'); END");
  const failed=await call(f.url,'/join',{code:i.code,name:'newbie'});assert.equal(failed.status,500);assert.equal(failed.data.error,'INTERNAL_ERROR');
  assert.equal(f.store.get('SELECT count(*) n FROM broker_users').n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM credentials').n,0);
  assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE user='newbie'"),undefined);
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',i.invite_id).redeemed_at,null);
  assert.equal(f.store.get('SELECT count(*) n FROM workspace_audit').n,before);
  f.store.db.exec('DROP TRIGGER fail_redemption');
  assert.equal((await call(f.url,'/join',{code:i.code,name:'newbie'})).status,201);
});

test('startup refuses static users colliding with minted identities before bootstrapping membership', async t => {
  const f=await fixture(t),i=await f.issue();await call(f.url,'/join',{code:i.code,name:'newbie'});
  const before=f.store.get('SELECT count(*) n FROM workspace_audit').n;
  assert.throws(()=>new Broker(f.store,config({}),['newbie']),{code:'USER_CONFLICT'});
  assert.equal(f.store.get('SELECT count(*) n FROM workspace_audit').n,before);
});

test('a minted user remains active after one membership ends and deactivates only after the last', async t => {
  const f=await fixture(t),i=await f.issue(),j=await call(f.url,'/join',{code:i.code,name:'newbie'}),token=j.data.token;
  await f.owner('/workspaces',{name:'second'});
  const second=await call(f.url,'/invites',{},tokens.alice,'owner-second','second');
  await call(f.url,'/join',{code:second.data.code},token);
  assert.equal((await call(f.url,'/members/leave',{},token,'newbie')).status,200);
  assert.equal((await call(f.url,'/workspaces',undefined,token)).status,200);
  assert.equal((await call(f.url,'/members/leave',{},token,'newbie-second','second')).status,200);
  assert.equal((await call(f.url,'/workspaces',undefined,token)).status,401);
});

test('F1: credential recovery refuses while the caller owns any active workspace', async t => {
  const f=await fixture(t),i=await f.issue(),j=await call(f.url,'/join',{code:i.code,name:'newbie'}),token=j.data.token;
  assert.equal((await call(f.url,'/workspaces',{name:'self-created'},token)).status,201);
  const before=f.store.get('SELECT count(*) n FROM workspace_audit').n;
  const refused=await call(f.url,'/credentials/revoke',{},token);
  assert.equal(refused.status,409);assert.equal(refused.data.error,'OWNER_MUST_TRANSFER');
  assert.equal(f.store.get('SELECT count(*) n FROM workspace_audit').n,before);
  assert.equal((await call(f.url,'/workspaces',undefined,token)).status,200);
  assert.ok(f.store.get("SELECT 1 FROM workspace_members WHERE workspace='default' AND user='newbie'"));
  // Seed the second member; the tested operation is recovery's ownership guard.
  f.store.run("INSERT INTO workspace_members VALUES ('self-created','alice','member',1,'invite')");
  assert.equal((await call(f.url,'/members/transfer',{user:'alice'},token,'self-owner','self-created')).status,200);
  assert.equal((await call(f.url,'/credentials/revoke',{},token)).status,200);
  assert.equal((await call(f.url,'/workspaces',undefined,token)).status,401);
  assert.equal(f.store.get("SELECT role FROM workspace_members WHERE workspace='self-created' AND user='alice'").role,'owner');
});

test('F2 regression guard: a guarded redemption update that changes zero rows rolls back everything', async t => {
  const f=await fixture(t),i=await f.issue();
  // BEGIN IMMEDIATE serializes real processes. This trigger deterministically changes
  // the invite after its read but before the conditional update in the same transaction.
  f.store.db.exec(`CREATE TEMP TRIGGER compete_redemption AFTER INSERT ON workspace_members
    WHEN NEW.user='newbie' BEGIN UPDATE invites SET redeemed_at=1 WHERE id='${i.invite_id}'; END`);
  const before=f.store.get('SELECT count(*) n FROM workspace_audit').n;
  const rejected=await call(f.url,'/join',{code:i.code,name:'newbie'});
  assert.equal(rejected.status,409);assert.equal(rejected.data.error,'INVITE_USED');
  for(const table of ['broker_users','credentials'])assert.equal(f.store.get(`SELECT count(*) n FROM ${table}`).n,0);
  assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE user='newbie'"),undefined);
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',i.invite_id).redeemed_at,null);
  assert.equal(f.store.get('SELECT count(*) n FROM workspace_audit').n,before);
  f.store.db.exec('DROP TRIGGER compete_redemption');
  assert.equal((await call(f.url,'/join',{code:i.code,name:'newbie'})).status,201);
});

test('F3: a credential revoked while a request body is delayed gets 401 before the operation', async t => {
  const f=await fixture(t),i=await f.issue(),j=await call(f.url,'/join',{code:i.code,name:'newbie'}),token=j.data.token;
  await f.owner('/workspaces',{name:'second'});
  const second=await call(f.url,'/invites',{},tokens.alice,'owner-second','second');
  await call(f.url,'/join',{code:second.data.code},token);
  const req=http.request(f.url+'/workstreams/register',{method:'POST',headers:{Authorization:`Bearer ${token}`,
    'X-Session-Id':'delayed','X-Topic':FIXTURE_TOPIC,'X-Workspace':'second','Content-Type':'application/json'}});
  t.after(()=>req.destroy());
  const received=new Promise((resolve,reject)=>{
    req.once('error',reject);req.once('response',res=>{
      let body='';res.setEncoding('utf8');res.on('data',chunk=>{body+=chunk;});
      res.on('end',()=>resolve({status:res.statusCode,data:JSON.parse(body)}));
    });
  });
  req.write('{"name":');
  // Heartbeat happens after authentication but before the request-body await.
  await eventually(()=>f.store.get("SELECT 1 FROM agents WHERE session_id='delayed'"));
  assert.equal((await call(f.url,'/credentials/revoke',{},token)).status,200);
  assert.ok(f.store.get("SELECT 1 FROM workspace_members WHERE workspace='second' AND user='newbie'"));
  req.end('"too-late"}');
  const response=await received;assert.equal(response.status,401);assert.equal(response.data.error,'UNAUTHORIZED');
  assert.equal(f.store.get("SELECT 1 FROM workstreams WHERE name='too-late'"),undefined);
});

test('F4: same-process credential revoke closes a stream before a long heartbeat while membership remains', async t => {
  const f=await fixture(t,{sseHeartbeat:60000}),i=await f.issue(),j=await call(f.url,'/join',{code:i.code,name:'newbie'}),token=j.data.token;
  await f.owner('/workspaces',{name:'second'});
  const second=await call(f.url,'/invites',{},tokens.alice,'owner-second','second');
  await call(f.url,'/join',{code:second.data.code},token);
  const closed=await stream(t,f.url,token,'same-process','second');
  assert.equal((await call(f.url,'/credentials/revoke',{},token)).status,200);
  assert.ok(f.store.get("SELECT 1 FROM workspace_members WHERE workspace='second' AND user='newbie'"));
  // Two seconds is well below the 60-second heartbeat, so a timer cannot mask
  // a missing same-process credential close after the revocation commits.
  await eventually(closed);
});

test('F5: every independent historical identity source reserves its name', async t => {
  const sources=[
    ['static users', (f,user)=>f.broker.users.add(user)],
    ['broker_users.name', (f,user)=>f.store.run("INSERT INTO broker_users(name,origin,created) VALUES (?,'self',1)",user)],
    ['workspace_members.user', (f,user)=>f.store.run("INSERT INTO workspace_members VALUES ('default',?,'member',1,'migrated')",user)],
    ['agents.user', (f,user)=>f.store.run("INSERT INTO agents(session_id,user,role,last_seen,workspace) VALUES ('history-agent',?,'worker',1,'default')",user)],
    ['delivery_targets.user', (f,user,m)=>f.store.run('UPDATE delivery_targets SET user=? WHERE message_id=?',user,m.id)],
    ['escalation_targets.user', (f,user,m)=>f.store.run('INSERT INTO escalation_targets(message_id,user) VALUES (?,?)',m.id,user)],
    ['participants.user', (f,user,m)=>f.store.run('INSERT INTO participants(thread_id,user) VALUES (?,?)',m.thread_id,user)],
    ['messages.from_user', (f,user,m)=>f.store.run('UPDATE messages SET from_user=? WHERE id=?',user,m.id)],
    ['decisions.from_user', (f,user)=>f.store.run("INSERT INTO decisions(workstream,from_user,from_session,body,created,workspace) VALUES ('api',?,'owner','history',1,'default')",user)],
    ['workspace_audit.subject_user', (f,user)=>f.store.run("INSERT INTO workspace_audit(workspace,action,subject_user,created) VALUES ('default','history',?,1)",user)],
    ['workspace_audit.actor_user', (f,user)=>f.store.run("INSERT INTO workspace_audit(workspace,action,actor_user,created) VALUES ('default','history',?,1)",user)],
  ];
  for (const [source,seed] of sources) await t.test(source,async t=>{
    const f=await fixture(t),i=await f.issue(),user='reserved-only-here';
    const message=(await f.owner('/send',{to:'user:bob',body:'seed identity references'})).data;
    seed(f,user,message);
    const response=await call(f.url,'/join',{code:i.code,name:user});
    assert.equal(response.status,409);assert.equal(response.data.error,'USER_EXISTS');
    assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',i.invite_id).redeemed_at,null);
    assert.equal(f.store.get('SELECT count(*) n FROM credentials').n,0);
  });
});

test('decision 69: an existing bearer with a mismatched name refuses redemption without side effects',async t=>{
  const f=await fixture(t);await f.owner('/workspaces',{name:'second'});
  const invitation=(await call(f.url,'/invites',{},tokens.alice,'owner-second','second')).data;
  const members=f.store.all('SELECT * FROM workspace_members ORDER BY workspace,user');
  const audit=f.store.all('SELECT * FROM workspace_audit ORDER BY id');
  const result=await call(f.url,'/join',{code:invitation.code,name:'alice'},tokens.bob);
  assert.equal(result.status,409);assert.equal(result.data.error,'JOIN_NAME_MISMATCH');
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',invitation.invite_id).redeemed_at,null);
  assert.deepEqual(f.store.all('SELECT * FROM workspace_members ORDER BY workspace,user'),members);
  assert.deepEqual(f.store.all('SELECT * FROM workspace_audit ORDER BY id'),audit);
  assert.equal(f.store.get('SELECT count(*) n FROM credentials').n,0);
  assert.equal(f.store.get('SELECT count(*) n FROM broker_users').n,0);
  assert.equal((await call(f.url,'/join',{code:invitation.code,name:'bob'},tokens.bob)).status,201);
});
