import {prepareTopicFixture,topicHeartbeat,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import { testTempDir } from '../../test-support/temp.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { Jobs } from '../jobs.mjs';
import { httpFixture, eventually } from './http.mjs';

function fixture(t, overrides = {}) {
  const store = new Store(':memory:');
  t.after(() => store.close());
  let clock = 100000;
  const logs = [];
  const broker = new Broker(store, { ...config({}), blockingTimeout: 100, idleClose: 1000, ...overrides }, ['alice', 'bob'], {
    now: () => clock, log: value => logs.push(JSON.parse(value)),
  });
  for (const [user, id] of [['alice', 'a'], ['bob', 'b'], ['bob', 'b2']]) topicHeartbeat(broker,user, id);
  return { broker, store, logs, advance: value => { clock += value; } };
}

test('an ack or agent close does not stop a blocking timer; escalation is emitted once', t => {
  const { broker, store, advance } = fixture(t);
  const sent = broker.send('alice', 'a', { to: 'user:bob', kind: 'blocker', blocking: true, body: 'blocked' });
  advance(50);
  broker.ack('bob', 'b', sent.id);
  broker.close('bob', 'b', sent.thread_id);
  advance(49);
  broker.expireThreads();
  assert.equal(store.get('SELECT status FROM threads WHERE id=?', sent.thread_id).status, 'answered');
  advance(1);
  broker.expireThreads();
  broker.expireThreads();
  assert.equal(store.get('SELECT status FROM threads WHERE id=?', sent.thread_id).status, 'escalated');
  assert.equal(store.get("SELECT count(*) n FROM messages WHERE kind='escalation'").n, 1);
  assert.equal(store.get('SELECT action FROM audit ORDER BY id DESC LIMIT 1').action, 'escalated:blocking_timeout');
});

test('only a later answer clears blocking asks, and later blocking asks start new timers', t => {
  const { broker, store, advance } = fixture(t);
  const sent = broker.send('alice', 'a', { to: 'user:bob', kind: 'question', blocking: true, body: 'question' });
  broker.ack('bob', 'b', sent.id);
  broker.send('bob', 'b', { to: 'user:alice', thread_id: sent.thread_id, kind: 'answer', body: 'answer' });
  advance(100);
  broker.expireThreads();
  assert.equal(store.get('SELECT status FROM threads WHERE id=?', sent.thread_id).status, 'open');
  const next = broker.send('bob', 'b', { to: 'user:alice', thread_id: sent.thread_id, kind: 'question', blocking: true, body: 'next question' });
  advance(100);
  broker.expireThreads();
  assert.equal(store.get('SELECT status FROM threads WHERE id=?', next.thread_id).status, 'escalated');
});

test('the asker cannot stop a blocking timer with their own answer', t => {
  const { broker, store, advance } = fixture(t);
  const sent = broker.send('alice','a',{to:'user:bob',kind:'question',blocking:true,body:'needs another participant'});
  broker.send('alice','a',{to:'user:bob',kind:'answer',thread_id:sent.thread_id,body:'self answer'});
  topicHeartbeat(broker,'alice','a2');
  broker.send('alice','a2',{to:'user:bob',kind:'answer',thread_id:sent.thread_id,body:'same user, different session'});
  broker.close('alice','a',sent.thread_id);
  advance(100);
  broker.expireThreads();
  assert.equal(store.get('SELECT status FROM threads WHERE id=?',sent.thread_id).status, 'escalated');
});

test('idle closure follows last activity, preserves the message budget, and permits late follow-ups', t => {
  const { broker, store, advance } = fixture(t, { idleClose: 100 });
  const sent = broker.send('alice', 'a', { to: 'user:bob', body: 'finding' });
  advance(90);
  broker.send('alice', 'a', { to: 'user:bob', thread_id: sent.thread_id, body: 'follow-up' });
  advance(99);
  broker.expireThreads();
  assert.equal(store.get('SELECT status FROM threads WHERE id=?', sent.thread_id).status, 'open');
  advance(1);
  broker.expireThreads();
  assert.equal(store.get('SELECT status FROM threads WHERE id=?', sent.thread_id).status, 'answered');
  broker.send('alice', 'a', { to: 'user:bob', thread_id: sent.thread_id, body: 'late correction' });
  assert.equal(store.get('SELECT msg_count FROM threads WHERE id=?', sent.thread_id).msg_count, 3);
  assert.equal(store.get("SELECT count(*) n FROM audit WHERE action='idle:close'").n, 1);
});

test('replay preserves an unacked gap below the ack cursor and discovers workstream takeover', async t => {
  const { broker, advance } = fixture(t, { leaseTtl: 1000, sseHeartbeat: 20, inboxLimit: 1 });
  const http = await httpFixture(t, broker);
  broker.register('bob', 'b', 'api');
  const deferred = broker.send('alice', 'a', { to: 'ws:api', body: 'deferred below cursor' });
  const later = broker.send('alice', 'a', { to: 'user:bob', body: 'later user message' });
  broker.ack('bob', 'b2', later.id);
  const stream = await http.stream('bob', 'b2', `?replay=1&since=${later.id}`);
  advance(1001);
  topicHeartbeat(broker,'bob', 'b2');
  broker.register('bob', 'b2', 'api');
  await stream.wait(events => events.some(e => e.data.id === deferred.id));
  const marker = broker.send('alice', 'a', { to: 'user:bob', body: 'barrier' });
  await stream.wait(events => events.some(e => e.data.id === marker.id));
  assert.deepEqual(stream.events.map(e => e.data.id), [deferred.id, marker.id]);
  const resume = await http.request('/resume', { user: 'bob', session: 'b2' });
  assert.equal((await resume.json()).acked_id, later.id);
});

test('a raw replay stream emits pending IDs once across several heartbeat rescans', async t => {
  const { broker, store } = fixture(t, { sseHeartbeat: 20 });
  const http = await httpFixture(t, broker);
  const ids = Array.from({ length: 3 }, () => broker.send('alice','a',{to:'user:bob',body:'still unacked'}).id);
  const stream = await http.stream('bob', 'b', '?replay=1');
  await stream.wait(() => stream.heartbeats >= 4);
  assert.deepEqual(stream.events.filter(e => e.event === 'message').map(e => e.data.id), ids);
  assert.equal(store.get('SELECT count(*) n FROM messages WHERE acked_at IS NULL').n, 3);
});

test('inbox queries use pending-only indexes instead of scanning acknowledged history', t => {
  const { broker, store } = fixture(t, { threadCap:1 });
  store.db.exec(`
    INSERT INTO threads(id,status,msg_count,created,last_activity,workspace) VALUES ('history','answered',0,1,1,'default');
    WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000)
      INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,acked_at,acked_by_session,workspace)
      SELECT 'history','alice','a','user','bob','finding',0,'history',1,1,'b','default' FROM n;
    INSERT INTO delivery_targets SELECT id,'bob' FROM messages;
    WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000)
      INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace)
      SELECT 'history','broker','broker','user','broker','escalation',0,'history',1,'default' FROM n;
    INSERT INTO escalation_targets(message_id,user,acked_at,acked_by_session)
      SELECT id,'bob',1,'b' FROM messages WHERE kind='escalation';
    INSERT INTO escalation_targets(message_id,user)
      SELECT id,'alice' FROM messages WHERE kind='escalation';
  `);
  const sent = broker.send('alice','a',{to:'user:bob',body:'new pending'});
  let query, bindings;
  const original = store.all.bind(store);
  t.mock.method(store,'all',(sql,...args)=>{ query=sql; bindings=args; return original(sql,...args); });
  const pending = broker.inbox('bob','b');
  assert.equal(pending.length,2); // One ordinary message and Bob's new escalation.
  assert.equal(pending[0].id,sent.id);
  const plan = original('EXPLAIN QUERY PLAN '+query,...bindings).map(row=>row.detail).join('\n');
  assert.match(plan,/messages_pending/);
  assert.match(plan,/escalation_targets_pending/);
});

test('human reopen begins a fresh blocking cycle without erasing history', t => {
  const dir = testTempDir('m3-reopen-');
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, 'broker.sqlite');
  const store = new Store(path);
  t.after(() => store.close());
  let now = Date.now();
  const broker = new Broker(store, { ...config({}), blockingTimeout: 100 }, ['alice', 'bob'], { now: () => now, log: () => {} });
  topicHeartbeat(broker,'alice', 'a');
  const sent = broker.send('alice', 'a', { to: 'user:bob', kind: 'question', blocking: true, body: 'original' });
  now += 100;
  broker.expireThreads();
  execFileSync(process.execPath, ['broker/cli.mjs', 'reopen', sent.thread_id, '--reason', 'human review'], { env: { ...process.env, BROKER_DB: path } });
  broker.expireThreads();
  assert.equal(store.get('SELECT status FROM threads WHERE id=?', sent.thread_id).status, 'open');
  assert.equal(store.get('SELECT body FROM messages WHERE id=?', sent.id).body, 'original');
});

test('webhook outbox survives reopen, retries with the same idempotency key, and marks success', async t => {
  const received = [];
  const receiver = http.createServer(async (req, res) => {
    let text = '';
    for await (const chunk of req) text += chunk;
    received.push({ key: req.headers['idempotency-key'], data: JSON.parse(text) });
    res.writeHead(received.length === 1 ? 503 : 204); res.end();
  });
  await new Promise(resolve => receiver.listen(0, '127.0.0.1', resolve));
  t.after(async () => { receiver.closeAllConnections(); await new Promise(resolve => receiver.close(resolve)); });
  const dir = testTempDir('m3-webhook-');
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, 'broker.sqlite');
  let store = new Store(path);
  t.after(() => store.close());
  let now = Date.now();
  const settings = { ...config({}), threadCap: 1, webhookRetryMs: 10, webhookUrl: `http://127.0.0.1:${receiver.address().port}` };
  let broker = new Broker(store, settings, ['alice','bob'], { now: () => now, log: () => {} });
  topicHeartbeat(broker,'alice', 'a');
  broker.send('alice', 'a', { to: 'user:bob', body: 'must not be in webhook payload' });
  await new Jobs(broker).deliver();
  assert.equal(store.get('SELECT attempts FROM webhook_outbox').attempts, 1);
  store.close();
  store = new Store(path);
  now += 10;
  broker = new Broker(store, settings, ['alice','bob'], { now: () => now, log: () => {} });
  await new Jobs(broker).deliver();
  await new Jobs(broker).deliver();
  assert.equal(received.length, 2);
  assert.deepEqual(received[0], received[1]);
  assert.equal(received[0].data.reason, 'thread_cap');
  assert.equal(JSON.stringify(received).includes('must not be in webhook payload'), false);
  assert.ok(store.get('SELECT sent_at FROM webhook_outbox').sent_at);
});

test('maintenance catches overdue asks on start and keeps running while a webhook is stalled', async t => {
  let requested;
  const arrived = new Promise(resolve => { requested = resolve; });
  const receiver = http.createServer(req => { req.resume(); requested(); }); // Deliberately never responds.
  await new Promise(resolve => receiver.listen(0, '127.0.0.1', resolve));
  t.after(async () => { receiver.closeAllConnections(); await new Promise(resolve => receiver.close(resolve)); });
  const { broker, store, advance } = fixture(t, { maintenanceMs: 10, webhookUrl: `http://127.0.0.1:${receiver.address().port}` });
  const first = broker.send('alice','a',{to:'user:bob',kind:'question',blocking:true,body:'overdue before startup'});
  advance(100);
  const jobs = new Jobs(broker);
  t.after(() => jobs.stop());
  jobs.start();
  await arrived;
  assert.equal(store.get('SELECT status FROM threads WHERE id=?', first.thread_id).status, 'escalated');
  const second = broker.send('alice','a',{to:'user:bob',kind:'question',blocking:true,body:'due while webhook stalls'});
  advance(100);
  await eventually(() => store.get('SELECT status FROM threads WHERE id=?', second.thread_id).status === 'escalated');
  await jobs.stop();
  assert.equal(store.get('SELECT count(*) n FROM webhook_outbox WHERE sent_at IS NULL').n, 2);
});

test('webhook maintenance permits only one in-flight POST while a receiver stalls', async t => {
  let requests=0, active=0, peak=0;
  const receiver = http.createServer((req,res)=>{
    req.resume(); requests++; active++; peak=Math.max(peak,active);
    res.on('close',()=>{active--;});
  });
  await new Promise(resolve=>receiver.listen(0,'127.0.0.1',resolve));
  t.after(async()=>{receiver.closeAllConnections();await new Promise(resolve=>receiver.close(resolve));});
  const {broker,store}=fixture(t,{threadCap:1,maintenanceMs:10,webhookUrl:`http://127.0.0.1:${receiver.address().port}`});
  broker.send('alice','a',{to:'user:bob',body:'one outbox event'});
  const expiry = t.mock.method(broker,'expireThreads');
  const jobs = new Jobs(broker);
  t.after(()=>jobs.stop());
  jobs.start();
  await eventually(()=>requests>=1 && expiry.mock.calls.length>=6);
  assert.equal(requests,1);
  assert.equal(active,1);
  assert.equal(peak,1);
  assert.equal(store.get('SELECT count(*) n FROM webhook_outbox WHERE sent_at IS NULL').n,1);
  await jobs.stop();
  await eventually(()=>active===0);
});
