import {prepareTopicFixture,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import {Store} from '../../broker/store.mjs';
import {Broker} from '../../broker/broker.mjs';
import {config} from '../../broker/config.mjs';
import { testTempDir } from '../../test-support/temp.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { session, tokens, eventually } from './helpers.mjs';

test('broker SIGKILL mid-stream preserves messages and acknowledgments without duplicate channel pushes', async t => {
  const dir = testTempDir('m3-crash-');
  const tokenPath = join(dir, 'tokens.json');
  writeFileSync(tokenPath, JSON.stringify(tokens), { mode: 0o600 });
  const initial=new Store(join(dir,'broker.sqlite'));prepareTopicFixture(new Broker(initial,config({}),Object.keys(tokens),{log:()=>{}}));initial.close();
  let active;
  let receiver;
  const fixture = { clients: [] };
  async function start(port = 0) {
    const child = spawn(process.execPath, ['broker/main.mjs'], { env: {
      BROKER_HOST: '127.0.0.1', BROKER_PORT: String(port), BROKER_DB: join(dir, 'broker.sqlite'),
      BROKER_TOKENS: tokenPath, MAINTENANCE_INTERVAL_MS: '20',
    }, stdio: ['ignore','ignore','pipe'] });
    let log = '';
    child.stderr.on('data', chunk => { log += chunk; });
    const exit = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
    active = { child, exit };
    let listening;
    await eventually(() => {
      for (const line of log.split('\n')) {
        try { const data = JSON.parse(line); if (data.event === 'broker_listening') listening = data.address.port; } catch {}
      }
      return Boolean(listening);
    });
    fixture.url = `http://127.0.0.1:${listening}`;
    return listening;
  }
  t.after(async () => {
    await receiver?.client.close();
    if (active?.child.exitCode === null && active.child.signalCode === null) { active.child.kill('SIGTERM'); await active.exit; }
    rmSync(dir, { recursive: true, force: true });
  });
  const port = await start();
  const planningHeaders = { Authorization: `Bearer ${tokens.alice}`, 'X-Session-Id': 'a', 'X-Topic':FIXTURE_TOPIC, 'Content-Type': 'application/json' };
  const registered = await fetch(fixture.url + '/workstreams/register', { method: 'POST', headers: planningHeaders,
    body: JSON.stringify({ name: 'durable-api', paths: ['src/api'], contracts: ['api:v1'] }) });
  assert.equal(registered.status, 200);
  const decision = await fetch(fixture.url + '/decisions/record', { method: 'POST', headers: planningHeaders,
    body: JSON.stringify({ workstream: 'durable-api', body: 'Keep v1 compatible' }) });
  assert.equal(decision.status, 201);
  const decisionId = (await decision.json()).id;
  async function send(body) {
    const response = await fetch(fixture.url + '/send', { method: 'POST', headers: {
      Authorization: `Bearer ${tokens.alice}`, 'X-Session-Id': 'a', 'X-Topic':FIXTURE_TOPIC, 'Content-Type': 'application/json',
    }, body: JSON.stringify({ to: 'user:bob', body }) });
    assert.equal(response.status, 201);
    return (await response.json()).id;
  }
  const ids = [];
  for (let i = 0; i < 200; i++) ids.push(await send('"'.repeat(16000)));
  let killed = false;
  fixture.onNotification = value => {
    if (!killed && value.meta.event === 'message') { killed = true; active.child.kill('SIGKILL'); }
  };
  receiver = await session(fixture, 'bob', 'b', { CHANNEL_RETRY_MAX_MS: '100' });
  await active.exit;
  assert.ok(receiver.notifications.length < ids.length, 'The broker must be killed before the stream finishes.');
  await start(port);
  const decisions = await fetch(fixture.url + '/decisions?workstream=durable-api', { headers: planningHeaders });
  assert.deepEqual((await decisions.json()).decisions.map(d => [d.id, d.body, d.from_user]), [[decisionId, 'Keep v1 compatible', 'alice']]);
  const restored = await fetch(fixture.url + '/workstreams/register', { method: 'POST', headers: planningHeaders,
    body: JSON.stringify({ name: 'durable-api' }) });
  const metadata = await restored.json();
  assert.deepEqual(metadata.paths, ['src/api']);
  assert.deepEqual(metadata.contracts, ['api:v1']);
  await eventually(() => new Set(receiver.notifications.map(n => n.meta.message_id)).size === ids.length, 10000);
  assert.deepEqual(receiver.notifications.map(n => Number(n.meta.message_id)), ids);
  await receiver.call('ack', { id: ids.at(-1) });
  await receiver.call('ack', { id: ids[1] }); // Out-of-order acknowledgments must not lose older pending gaps.
  active.child.kill('SIGKILL');
  await active.exit;
  await start(port);
  const resume = await fetch(fixture.url + '/resume', { headers: { Authorization: `Bearer ${tokens.bob}`, 'X-Session-Id': 'b', 'X-Topic':FIXTURE_TOPIC } });
  assert.equal((await resume.json()).acked_id, ids.at(-1));
  const marker = await send('after second restart');
  await eventually(() => receiver.notifications.some(n => Number(n.meta.message_id) === marker), 10000);
  assert.deepEqual(receiver.notifications.map(n => Number(n.meta.message_id)), [...ids, marker]);
});
