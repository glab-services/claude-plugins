import {prepareTopicFixture,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import http from 'node:http';
import { Jobs } from '../jobs.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { httpFixture, eventually } from './http.mjs';

async function fixture(t, options = {}) {
  const store = new Store(':memory:'); t.after(() => store.close());
  let now = 10000;
  const broker = new Broker(store, { ...config({}), ...options }, ['alice','bob','eve'], { now: () => now, log: () => {} });
  // Membership tools belong to M6; seed two projects with identical members here.
  store.run("INSERT INTO workspaces(name,created,created_by_user) VALUES ('proj-b',1,'alice')");
  store.run("INSERT INTO workspace_members VALUES ('proj-b','alice','owner',1,'creator'),('proj-b','bob','member',1,'static')");
  for(const workspace of ['default','proj-b'])prepareTopicFixture(broker,workspace);
  const http = await httpFixture(t, broker,{workspaceAdminSessions:true});
  async function request(path, { user = 'alice', session = user + '-a', workspace = 'default', method = 'GET', body, ...extra } = {}) {
    return http.request(path, { user, session, method, headers: { 'X-Workspace': workspace }, ...(body === undefined ? {} : { body: JSON.stringify(body) }), ...extra });
  }
  async function call(path, body, user = 'alice', workspace = 'default', session = user + (workspace === 'default' ? '-a' : '-b')) {
    const response = await request(path, { method: 'POST', body, user, workspace, session });
    return { status: response.status, data: await response.json() };
  }
  async function get(path, user = 'bob', workspace = 'default', session = user + (workspace === 'default' ? '-a' : '-b')) {
    const response = await request(path, { user, workspace, session });
    return { status: response.status, data: await response.json() };
  }
  async function stream(user, workspace, session) {
    const controller = new AbortController();
    const response = await request('/stream?replay=1', { user, workspace, session, signal: controller.signal });
    assert.equal(response.status, 200);
    const events = []; let closed = false;
    const reading = (async () => {
      let pending = ''; const decoder = new TextDecoder();
      try {
        for await (const bytes of response.body) {
          pending += decoder.decode(bytes, { stream: true });
          let end;
          while ((end = pending.indexOf('\n\n')) >= 0) {
            const frame = pending.slice(0,end); pending = pending.slice(end+2);
            const data = frame.split('\n').find(line => line.startsWith('data: '));
            if (data) events.push({ event: frame.split('\n').find(line => line.startsWith('event: '))?.slice(7), data: JSON.parse(data.slice(6)) });
          }
        }
      } catch (error) { if (!controller.signal.aborted) throw error; }
      finally { closed = true; }
    })();
    t.after(async () => { controller.abort(); await reading; });
    return { events, get closed() { return closed; } };
  }
  return { store, broker, http, request, call, get, stream, advance: ms => { now += ms; } };
}

test('the same users, workstream names and overlapping paths remain isolated on HTTP and SSE', async t => {
  const f = await fixture(t, { decisionMaxPerWorkstream: 1 });
  const streams = {};
  for (const workspace of ['default','proj-b']) {
    for (const user of ['alice','bob']) streams[user + workspace] = await f.stream(user, workspace, user + (workspace === 'default' ? '-a' : '-b'));
    assert.equal((await f.call('/workstreams/register', { name: 'api', paths: ['src'], contracts: [workspace] }, 'bob', workspace)).status, 200);
    await f.call('/decisions/record', { workstream: 'api', body: workspace }, 'bob', workspace);
  }
  for (const workspace of ['default','proj-b']) assert.equal(f.store.get('SELECT contracts FROM workstreams WHERE workspace=? AND name=?', workspace, 'api').contracts, JSON.stringify([workspace]));
  const a = await f.call('/send', { to: 'user:bob', body: 'A', workspace: 'proj-b' });
  assert.equal(a.status, 201);
  await eventually(() => streams.bobdefault.events.some(e => e.data.id === a.data.id));
  assert.deepEqual((await f.get('/inbox')).data.messages.map(m => m.id), [a.data.id]);
  assert.deepEqual((await f.get('/inbox','bob','proj-b')).data.messages, []);
  assert.equal((await f.call('/ack', { id: a.data.id }, 'bob', 'proj-b')).data.error, 'MESSAGE_NOT_FOUND');
  for (const endpoint of ['/send','/close']) {
    const args = endpoint === '/send' ? { to: 'user:alice', kind: 'answer', body: 'cross', thread_id: a.data.thread_id } : { thread_id: a.data.thread_id };
    assert.equal((await f.call(endpoint, args, 'bob', 'proj-b')).data.error, 'THREAD_NOT_FOUND');
  }
  assert.equal((await f.call('/ack', { id: a.data.id }, 'bob')).status, 200);
  assert.equal((await f.get('/resume')).data.acked_id, a.data.id);
  assert.equal((await f.get('/resume','bob','proj-b')).data.acked_id, 0);
  // A sibling in A gets a claim notice; a session of the same user in B must not.
  const sibling = await f.stream('bob','default','bob-sibling');
  const wsA = await f.call('/send', { to: 'ws:api', body: 'workstream A' });
  const wsB = await f.call('/send', { to: 'ws:api', body: 'workstream B' }, 'alice', 'proj-b');
  await eventually(() => streams.bobdefault.events.some(e => e.data.id === wsA.data.id) && streams['bobproj-b'].events.some(e => e.data.id === wsB.data.id));
  await f.call('/ack', { id: wsA.data.id }, 'bob');
  // Workstream messages are exclusive to their owner; use a user message for sibling fanout.
  const userA = await f.call('/send', { to: 'user:bob', body: 'claim notice' });
  await eventually(() => sibling.events.some(e => e.data.id === userA.data.id));
  await f.call('/ack', { id: userA.data.id }, 'bob');
  await eventually(() => sibling.events.some(e => e.event === 'claimed' && e.data.id === userA.data.id));
  for (const workspace of ['default','proj-b']) {
    const decisions = (await f.get('/decisions','alice',workspace)).data.decisions;
    assert.deepEqual(decisions.map(d => d.body), [workspace]);
    assert.deepEqual((await f.get('/decisions?workstream=api','alice',workspace)).data.decisions.map(d => d.body), [workspace]);
    assert.equal((await f.call('/workstreams/check-overlap', { name: 'api' }, 'alice', workspace)).data.overlaps.length, 0);
  }
  await f.call('/workstreams/register', { name: 'only-b', paths: ['src'] }, 'alice', 'proj-b');
  assert.equal((await f.call('/workstreams/check-overlap', { name: 'api', include_expired: true }, 'bob')).data.overlaps.length, 0);
  assert.deepEqual((await f.call('/workstreams/check-overlap', { name: 'api' }, 'bob', 'proj-b')).data.overlaps.map(w => w.name), ['only-b']);
  assert.equal((await f.call('/workstreams/release', { name: 'api' }, 'bob')).status, 200);
  const onlyB = await f.call('/send', { to: 'ws:api', body: 'B still registered' }, 'alice', 'proj-b');
  assert.equal(onlyB.status, 201);
  await eventually(() => streams['bobproj-b'].events.some(e => e.data.id === onlyB.data.id));
  // A release must not route B's messages into A's same-user fallback inbox.
  assert.ok((await f.get('/inbox')).data.messages.every(m => m.workspace === 'default'));
  // The B marker above is a FIFO barrier for any earlier misplaced claimed notice.
  assert.deepEqual(streams['bobproj-b'].events.filter(e => e.event === 'claimed'), []);
  assert.ok(streams['bobproj-b'].events.every(e => e.data.workspace === 'proj-b'));
  assert.ok(streams.bobdefault.events.filter(e => e.event === 'message').every(e => e.data.workspace === 'default'));
});

test('non-members and unknown workspaces have byte-identical errors on every route', async t => {
  const f = await fixture(t);
  for (const path of ['/inbox','/decisions','/resume','/stream','/workstreams/check-overlap']) {
    const options = { user: 'eve', session: 'eve-b', method: path.endsWith('check-overlap') ? 'POST' : 'GET', body: path.endsWith('check-overlap') ? { name: 'api' } : undefined };
    const known = await f.request(path, { ...options, workspace: 'proj-b' });
    const unknown = await f.request(path, { ...options, workspace: 'missing' });
    assert.equal(known.status, 403); assert.equal(unknown.status, 403);
    const text = await known.text(); assert.equal(text, await unknown.text()); assert.equal(JSON.parse(text).error, 'NOT_A_MEMBER');
  }
});

test('a session stays bound to its first workspace and absent headers use default', async t => {
  const f = await fixture(t);
  assert.equal((await f.http.request('/heartbeat', { method: 'POST', body: '{}' })).status, 200);
  assert.equal(f.store.get("SELECT workspace FROM agents WHERE session_id='a'").workspace, 'default');
  const moved = await f.call('/heartbeat', {}, 'alice', 'proj-b', 'a');
  assert.equal(moved.status, 409); assert.equal(moved.data.error, 'SESSION_WORKSPACE_MISMATCH');
  assert.equal(f.store.get("SELECT workspace FROM agents WHERE session_id='a'").workspace, 'default');
});

test('identity routes create and list workspaces without requiring or rebinding a session', async t => {
  const f = await fixture(t, { workspaceLimit: 3 });
  const created = await f.request('/workspaces', { method: 'POST', body: { name: 'project-c' }, headers: { 'X-Session-Id': '' } });
  assert.equal(created.status, 201);
  assert.equal(f.store.get("SELECT count(*) n FROM agents").n, 0);
  const list = await f.request('/workspaces', { headers: { 'X-Session-Id': '', 'X-Workspace': 'missing' } });
  assert.deepEqual((await list.json()).workspaces.map(w => w.name), ['default','proj-b','project-c']);
  assert.equal((await f.call('/workspaces', { name: 'project-c' })).data.error, 'WORKSPACE_EXISTS');
  assert.equal((await f.call('/workspaces', { name: 'project-d' })).data.error, 'WORKSPACE_LIMIT');
  assert.equal(f.store.get("SELECT count(*) n FROM workspace_audit WHERE workspace='project-c'").n, 2);
  assert.equal(f.store.get("SELECT origin FROM workspace_members WHERE workspace='project-c'").origin, 'creator');
  assert.equal(f.store.get("SELECT count(*) n FROM agents").n, 0);
});


test('user addresses resolve only current members of the bound workspace', async t => {
  const f = await fixture(t);
  const nonmember = await f.call('/send', { to: 'user:eve', body: 'not a member' }, 'alice', 'proj-b');
  const unknown = await f.call('/send', { to: 'user:nobody', body: 'unknown user' }, 'alice', 'proj-b');
  assert.equal(nonmember.status, 404); assert.deepEqual(nonmember, unknown);
  assert.equal(nonmember.data.error, 'USER_NOT_FOUND');
  f.store.run("INSERT INTO workspace_members VALUES ('proj-b','dynamic-user','member',1,'invite')");
  f.store.run("INSERT INTO topic_subscriptions VALUES ('proj-b','fixture','dynamic-user',1)");
  assert.equal((await f.call('/send', { to: 'user:dynamic-user', body: 'membership is authority' }, 'alice', 'proj-b')).status, 201);
  assert.equal((await f.call('/send', { to: 'user:eve', body: 'default member' })).status, 201);
});

test('membership removal stops core delivery before the stream timer and rejects all broker operations', async t => {
  const f = await fixture(t, { sseHeartbeat: 100 });
  await f.call('/heartbeat', {}, 'alice', 'proj-b');
  await f.call('/workstreams/register', { name: 'api' }, 'bob', 'proj-b');
  const stream = await f.stream('bob','proj-b','bob-b');
  const sent = f.broker.send('alice','alice-b', { to: 'user:bob', body: 'remove before drain' });
  const message = f.store.get('SELECT * FROM messages WHERE id=?', sent.id);
  assert.equal(f.broker.eligible(message,'bob','bob-b'), true);
  f.store.run("DELETE FROM workspace_members WHERE workspace='proj-b' AND user='bob'");
  assert.equal(f.broker.eligible(message,'bob','bob-b'), false);
  f.broker.publish(message);
  assert.equal((await f.call('/send', { to: 'user:bob', body: 'after removal' }, 'alice', 'proj-b')).data.error, 'USER_NOT_FOUND');
  const calls = [
    () => f.broker.inbox('bob','bob-b'), () => f.broker.resume('bob','bob-b'),
    () => f.broker.register('bob','bob-b','api'), () => f.broker.release('bob','bob-b','api'),
    () => f.broker.checkOverlap('bob','bob-b',{ name: 'api' }),
    () => f.broker.recordDecision('bob','bob-b',{ workstream: 'api', body: 'removed' }),
    () => f.broker.listDecisions('bob','bob-b',{}), () => f.broker.ack('bob','bob-b', sent.id),
    () => f.broker.close('bob','bob-b', sent.thread_id),
    () => f.broker.send('bob','bob-b',{ to: 'user:alice', body: 'removed' }),
  ];
  for (const call of calls) assert.throws(call, { code: 'NOT_A_MEMBER' });
  assert.equal((await f.get('/inbox','bob','proj-b')).data.error, 'NOT_A_MEMBER');
  await eventually(() => stream.closed);
  assert.deepEqual(stream.events, []);
  assert.equal((await f.get('/inbox','bob')).status, 200);
});

test('escalation delivery and per-user claims remain inside their workspace', async t => {
  const f = await fixture(t, { threadCap: 1 }), streams = [];
  for (const workspace of ['default','proj-b']) for (const user of ['alice','bob']) {
    streams.push({ workspace, user, stream: await f.stream(user,workspace,user + (workspace === 'default' ? '-a' : '-b')) });
  }
  const sent = await f.call('/send', { to: 'user:bob', body: 'B cap' }, 'alice', 'proj-b');
  const escalation = f.store.get("SELECT * FROM messages WHERE thread_id=? AND kind='escalation'", sent.data.thread_id);
  await eventually(() => streams.filter(s => s.workspace === 'proj-b').every(s => s.stream.events.some(e => e.data.id === escalation.id)));
  assert.equal(f.broker.eligible(escalation,'bob','bob-a'), false);
  assert.equal((await f.call('/ack', { id: escalation.id }, 'bob')).data.error, 'MESSAGE_NOT_FOUND');
  assert.equal((await f.call('/ack', { id: escalation.id }, 'bob','proj-b')).status, 200);
  assert.ok((await f.get('/inbox','alice','proj-b')).data.messages.some(m => m.id === escalation.id));
  assert.deepEqual((await f.get('/inbox')).data.messages, []);
  await f.call('/send', { to: 'user:bob', body: 'A barrier' });
  await eventually(() => streams.filter(s => s.workspace === 'default').every(s => s.stream.events.some(e => e.data.kind === 'escalation')));
  for (const s of streams) assert.ok(s.stream.events.filter(e => e.event === 'message').every(e => e.data.workspace === s.workspace));
});

test('maintenance timers and delivered webhook payloads keep the originating workspace', async t => {
  const payloads = [], receiver = http.createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk;
    payloads.push(JSON.parse(body)); res.end('ok');
  });
  await new Promise(resolve => receiver.listen(0,'127.0.0.1',resolve));
  t.after(() => new Promise(resolve => receiver.close(resolve)));
  const f = await fixture(t, { blockingTimeout: 50, webhookUrl: 'http://127.0.0.1:' + receiver.address().port });
  const a = await f.call('/send', { to: 'user:bob', kind: 'question', blocking: true, body: 'A blocking' });
  const b = await f.call('/send', { to: 'user:bob', kind: 'question', blocking: true, body: 'B answered' }, 'alice', 'proj-b');
  await f.call('/ack', { id: b.data.id }, 'bob', 'proj-b');
  await f.call('/send', { to: 'user:alice', kind: 'answer', thread_id: b.data.thread_id, body: 'B reply' }, 'bob', 'proj-b');
  f.advance(50);
  const jobs = new Jobs(f.broker); t.after(() => jobs.stop());
  jobs.tick(); if (jobs.sending) await jobs.sending;
  assert.deepEqual(payloads.map(p => [p.workspace,p.thread_id,p.reason]), [['default',a.data.thread_id,'blocking_timeout']]);
  assert.equal(f.store.get('SELECT status FROM threads WHERE id=?', b.data.thread_id).status, 'open');
  assert.deepEqual((await f.get('/inbox','bob','proj-b')).data.messages, []);
});


test('membership is rechecked when a delayed HTTP body finishes after removal', async t => {
  const f = await fixture(t);
  const body = JSON.stringify({ to: 'user:bob', body: 'must not commit after removal' });
  const request = http.request({ hostname: '127.0.0.1', port: f.http.server.address().port, path: '/send', method: 'POST', headers: {
    Authorization: 'Bearer alice-test-token', 'X-Session-Id': 'delayed', 'X-Topic':FIXTURE_TOPIC, 'X-Workspace': 'proj-b',
    'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body),
  } });
  t.after(() => request.destroy());
  const response = new Promise((resolve, reject) => { request.once('response', resolve); request.once('error', reject); });
  request.flushHeaders();
  await eventually(() => Boolean(f.store.get("SELECT 1 FROM agents WHERE session_id='delayed'")));
  f.store.run("DELETE FROM workspace_members WHERE workspace='proj-b' AND user='alice'");
  request.end(body);
  const result = await response; let text = ''; for await (const chunk of result) text += chunk;
  assert.equal(result.statusCode, 403); assert.equal(JSON.parse(text).error, 'NOT_A_MEMBER');
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n, 0);
});

test('workstream sends select the destination workspace owner when names collide', async t => {
  const f = await fixture(t);
  await f.call('/workstreams/register', { name: 'api' }, 'alice', 'default');
  await f.call('/workstreams/register', { name: 'api' }, 'bob', 'proj-b');
  const stream = await f.stream('bob','proj-b','bob-b');
  const sent = await f.call('/send', { to: 'ws:api', body: 'B owner only' }, 'alice', 'proj-b');
  assert.equal(sent.status, 201);
  assert.deepEqual((await f.get('/inbox','bob','proj-b')).data.messages.map(m => m.id), [sent.data.id]);
  assert.deepEqual((await f.get('/inbox','alice','proj-b')).data.messages, []);
  await eventually(() => stream.events.some(e => e.data.id === sent.data.id));
});

test('workstream sends cannot resolve a name present only in another workspace', async t => {
  const f = await fixture(t);
  await f.call('/workstreams/register', { name: 'api' }, 'alice', 'default');
  const sent = await f.call('/send', { to: 'ws:api', body: 'no B workstream' }, 'alice', 'proj-b');
  assert.equal(sent.status, 404); assert.equal(sent.data.error, 'WORKSTREAM_NOT_FOUND');
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n, 0);
});

test('inbox workstream joins ignore another workspace live owner with the same name', async t => {
  const f = await fixture(t);
  await f.call('/workstreams/register', { name: 'api' }, 'alice', 'default');
  await f.call('/workstreams/register', { name: 'api' }, 'bob', 'proj-b');
  assert.ok(f.store.get("SELECT last_seen FROM agents WHERE session_id='bob-b'").last_seen > f.broker.now() - f.broker.config.leaseTtl);
  const sent = await f.call('/send', { to: 'ws:api', body: 'A owner inbox once' }, 'bob', 'default');
  assert.equal(sent.status, 201);
  assert.deepEqual((await f.get('/inbox','alice','default')).data.messages.map(m => m.id), [sent.data.id]);
  assert.deepEqual((await f.get('/inbox','alice','proj-b')).data.messages, []);
});

test('workspace creation rejects unknown fields with a workspace-specific actionable error', async t => {
  const f = await fixture(t);
  const result = await f.call('/workspaces', { name: 'bad-fields', x: 1 });
  assert.equal(result.status, 400); assert.equal(result.data.error, 'INVALID_WORKSPACE_FIELDS');
  assert.match(result.data.hint, /name/);
  assert.equal(f.store.get("SELECT 1 FROM workspaces WHERE name='bad-fields'"), undefined);
});

test('broker is reserved as a workspace name and cannot bind a session', async t => {
  const f = await fixture(t);
  const created = await f.call('/workspaces', { name: 'broker' });
  assert.equal(created.status, 400); assert.equal(created.data.error, 'RESERVED_WORKSPACE');
  assert.match(created.data.hint, /broker/);
  const bound = await f.call('/heartbeat', {}, 'alice', 'broker');
  assert.equal(bound.status, 400); assert.equal(bound.data.error, 'RESERVED_WORKSPACE');
  assert.equal(f.store.get("SELECT 1 FROM workspaces WHERE name='broker'"), undefined);
});
