import {prepareTopicFixture,topicHeartbeat,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { Store } from '../store.mjs';
import { Broker } from '../broker.mjs';
import { config } from '../config.mjs';
import { httpFixture } from './http.mjs';
import { testTempDir } from '../../test-support/temp.mjs';

async function fixture(t, overrides = {}, file = ':memory:') {
  const store = new Store(file);
  t.after(() => store.close());
  let time = 1_000_000;
  const broker = new Broker(store, { ...config({}), ...overrides }, ['alice', 'bob', 'eve'], { now: () => time, log: () => {} });
  prepareTopicFixture(broker);
  const http = await httpFixture(t, broker);
  async function post(path, args, user = 'alice', session = user + '-session') {
    const response = await http.request(path, { method: 'POST', body: JSON.stringify(args), user, session });
    return { status: response.status, data: await response.json() };
  }
  async function get(path, user = 'bob') {
    const response = await http.request(path, { user, session: user + '-session' });
    return { status: response.status, data: await response.json() };
  }
  return { store, broker, post, get, advance: ms => { time += ms; } };
}

test('HTTP registration validates metadata atomically and preserves it across lease takeover', async t => {
  const f = await fixture(t, { planningMaxItems: 3, planningMaxItemChars: 30 });
  const register = args => f.post('/workstreams/register', { name: 'api', ...args });
  const first = await register({ paths: ['./src//api/', 'src/api'], contracts: ['api:v1', 'api:v1'] });
  assert.equal(first.status, 200);
  assert.deepEqual(first.data.paths, ['src/api']);
  assert.deepEqual(first.data.contracts, ['api:v1']);
  for (const paths of [['/etc'], ['../src'], ['src/../api'], ['C:/src'], ['src\\api'], ['src/*'], [''], ['a\nb'], ['a', 'b', 'c', 'd'], ['x'.repeat(31)], null]) {
    const result = await register({ paths, contracts: [] });
    assert.equal(result.data.error, 'INVALID_PATHS');
    assert.ok(result.data.hint);
    assert.deepEqual(JSON.parse(f.store.get('SELECT contracts FROM workstreams WHERE name=?', 'api').contracts), ['api:v1']);
  }
  for (const contracts of [[' '], ['api\n'], ['x'.repeat(31)], ['a', 'b', 'c', 'd'], null]) {
    assert.equal((await register({ contracts })).data.error, 'INVALID_CONTRACTS');
  }
  assert.equal((await register({ from_user: 'bob' })).data.error, 'INVALID_PLANNING_FIELDS');
  assert.deepEqual((await register({})).data.paths, ['src/api']);
  assert.equal((await f.post('/workstreams/register', { name: 'api', paths: [] }, 'bob')).data.error, 'WORKSTREAM_OWNED');
  assert.equal((await f.post('/workstreams/register', { name: 'api' }, 'alice', 'sibling')).status, 409);
  f.advance(f.broker.config.leaseTtl + 1);
  assert.equal((await f.post('/workstreams/register', { name: 'api' }, 'bob')).status, 409);
  const takeover = await f.post('/workstreams/register', { name: 'api' }, 'alice', 'sibling');
  assert.equal(takeover.status, 200);
  assert.deepEqual(takeover.data.paths, ['src/api']);
  assert.deepEqual(takeover.data.contracts, ['api:v1']);
  const cleared = await f.post('/workstreams/register', { name: 'api', paths: [], contracts: [] }, 'alice', 'sibling');
  assert.deepEqual(cleared.data.paths, []);
  assert.deepEqual(cleared.data.contracts, []);
});

test('HTTP overlap checks compare path segments, exact contracts, roots, and expired leases without blocking registration', async t => {
  const f = await fixture(t);
  await f.post('/workstreams/register', { name: 'api', paths: ['src/api'], contracts: ['api:v1'] });
  await f.post('/workstreams/register', { name: 'child', paths: ['src/api/file.ts'], contracts: ['API:v1'] }, 'bob');
  await f.post('/workstreams/register', { name: 'sibling', paths: ['src/apis'], contracts: ['api:v1'] }, 'bob');
  await f.post('/workstreams/register', { name: 'unrelated', paths: ['docs'], contracts: ['api:v10'] }, 'bob');
  const check = () => f.post('/workstreams/check-overlap', { name: 'api', include_expired: true }, 'eve');
  let result = (await check()).data;
  assert.deepEqual(result.overlaps.map(o => o.name), ['child', 'sibling']);
  assert.deepEqual(result.overlaps[0].paths, [{ path: 'src/api', other_path: 'src/api/file.ts' }]);
  assert.deepEqual(result.overlaps[0].contracts, []);
  assert.deepEqual(result.overlaps[1].paths, []);
  assert.deepEqual(result.overlaps[1].contracts, ['api:v1']);
  assert.ok(result.overlaps.every(o => o.active));
  f.advance(f.broker.config.leaseTtl + 1);
  assert.ok((await check()).data.overlaps.every(o => !o.active));
  await f.post('/workstreams/release', { name: 'child' }, 'bob');
  assert.deepEqual((await check()).data.overlaps.map(o => o.name), ['sibling']);
  await f.post('/workstreams/register', { name: 'root', paths: ['.'] }, 'bob');
  assert.ok((await check()).data.overlaps.some(o => o.name === 'root' && o.paths.length === 1));
  const reverse = await f.post('/workstreams/check-overlap', { name: 'root', include_expired: true });
  assert.equal(reverse.data.overlaps.length, 3);
  assert.equal((await f.post('/workstreams/check-overlap', { name: 'missing' })).status, 404);
});

test('HTTP decisions are owner-authored, team-readable, paged and durable beyond release and database reopen', async t => {
  const dir = testTempDir('planning-');
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const file = join(dir, 'broker.sqlite');
  const f = await fixture(t, { decisionLimit: 1, decisionMaxChars: 100 }, file);
  await f.post('/workstreams/register', { name: 'api', paths: ['src/api'], contracts: ['api:v1'] });
  const record = (args, user = 'alice', session) => f.post('/decisions/record', { workstream: 'api', body: 'Use v1', ...args }, user, session);
  assert.equal((await record({}, 'bob')).status, 403);
  assert.equal((await record({}, 'alice', 'sibling')).status, 403);
  assert.equal((await record({ from_user: 'bob' })).data.error, 'INVALID_PLANNING_FIELDS');
  for (const body of ['', ' ', 'x'.repeat(101), null]) assert.equal((await record({ body })).data.error, 'INVALID_DECISION_BODY');
  const first = await record({});
  assert.equal(first.status, 201);
  assert.equal(first.data.from_user, 'alice');
  assert.equal(first.data.from_session, 'alice-session');
  const second = await record({ body: 'Use v2' });
  await f.post('/workstreams/register', { name: 'docs' }, 'bob');
  const third = await f.post('/decisions/record', { workstream: 'docs', body: 'Describe v2' }, 'bob');
  assert.equal((await f.get('/decisions')).data.decisions[0].id, first.data.id);
  assert.equal((await f.get('/decisions?since=' + first.data.id)).data.decisions[0].id, second.data.id);
  assert.equal((await f.get('/decisions?workstream=docs')).data.decisions[0].id, third.data.id);
  assert.deepEqual((await f.get('/decisions?workstream=docs&since=' + third.data.id)).data.decisions, []);
  for (const since of ['-1', '1.2', '9007199254740992', 'abc']) assert.equal((await f.get('/decisions?since=' + since)).data.error, 'INVALID_DECISION_ID');
  assert.equal((await f.get('/decisions?from_user=bob')).data.error, 'INVALID_PLANNING_FIELDS');
  assert.equal(f.store.get('SELECT count(*) n FROM messages').n, 0);
  assert.equal(f.store.get('SELECT count(*) n FROM threads').n, 0);
  await f.post('/workstreams/release', { name: 'api' });
  assert.equal((await record({})).status, 404);
  assert.equal((await f.get('/decisions?workstream=api')).data.decisions[0].body, 'Use v1');
  await f.post('/workstreams/register', { name: 'api' }, 'bob');
  const next = await record({ body: 'New owner' }, 'bob');
  assert.equal(next.data.from_user, 'bob');
  assert.equal((await f.get('/decisions?workstream=api')).data.decisions[0].from_user, 'alice');
  // A second connection reopens the on-disk store; all planning and message state is committed.
  const reopened = new Store(file);
  try {
    assert.equal(reopened.get('SELECT count(*) n FROM decisions').n, 4);
    assert.equal(reopened.get('SELECT owner_user FROM workstreams WHERE name=?', 'api').owner_user, 'bob');
    assert.deepEqual(reopened.all('PRAGMA foreign_key_check'), []);
    assert.equal(reopened.get('PRAGMA integrity_check').integrity_check, 'ok');
  } finally { reopened.close(); }
});

test('HTTP overlaps bound candidates and evidence, page without loss, and skip stale registrations by default', async t => {
  const f = await fixture(t, { overlapLimit: 2, overlapPairLimit: 2, overlapScanLimit: 3 });
  await f.post('/workstreams/register', { name: 'api', paths: ['.', 'src'], contracts: ['a', 'b', 'c'] });
  for (const name of ['b', 'c', 'd', 'e', 'f']) await f.post('/workstreams/register', { name, paths: ['src', 'src/a', 'src/b'], contracts: ['a', 'b', 'c'] }, 'bob');
  const seen = [];
  let since;
  do {
    const result = (await f.post('/workstreams/check-overlap', { name: 'api', ...(since ? { since } : {}) }, 'eve')).data;
    assert.ok(result.overlaps.length <= 2);
    assert.ok(result.scanned <= 3);
    for (const overlap of result.overlaps) {
      assert.ok(overlap.paths.length + overlap.contracts.length <= 2);
      assert.equal(overlap.pairs_truncated, 7); // 6 path matches + 3 contracts, 2 retained.
      seen.push(overlap.name);
    }
    assert.equal(result.truncated, seen.length < 5);
    since = result.next_since;
  } while (since);
  assert.deepEqual(seen, ['b', 'c', 'd', 'e', 'f']);
  f.advance(f.broker.config.leaseTtl + 1);
  const stale = (await f.post('/workstreams/check-overlap', { name: 'api' }, 'eve')).data;
  assert.deepEqual(stale.overlaps, []);
  const explicit = (await f.post('/workstreams/check-overlap', { name: 'api', include_expired: true }, 'eve')).data;
  assert.equal(explicit.overlaps.length, 2);
  assert.ok(explicit.overlaps.every(o => !o.active));
  assert.equal((await f.post('/workstreams/check-overlap', { name: 'api', include_expired: 'yes' })).status, 400);
  // No matches still advances a bounded candidate page.
  await f.post('/workstreams/register', { name: 'isolated', paths: [] }, 'eve');
  const unmatched = (await f.post('/workstreams/check-overlap', { name: 'isolated', include_expired: true }, 'eve')).data;
  assert.deepEqual(unmatched.overlaps, []);
  assert.equal(unmatched.scanned, 3);
  assert.ok(unmatched.next_since);
});

test('HTTP decisions enforce separate body/count limits and byte-bounded resumable pages', async t => {
  // Include the new workspace field while keeping the oversized-row and paging boundaries.
  const pageBytes = 680;
  const f = await fixture(t, { decisionMaxChars: 100, decisionPageBytes: pageBytes, decisionMaxPerWorkstream: 3, decisionLimit: 100 });
  await f.post('/workstreams/register', { name: 'api' });
  const record = body => f.post('/decisions/record', { workstream: 'api', body });
  assert.equal((await record('x'.repeat(101))).data.error, 'INVALID_DECISION_BODY');
  assert.equal((await record('\u0001'.repeat(100))).data.error, 'DECISION_PAGE_TOO_SMALL');
  assert.equal(f.store.get('SELECT count(*) n FROM decisions').n, 0);
  const ids = [];
  for (const body of ['😀'.repeat(40), '\u0001'.repeat(80), 'last']) {
    const response = await record(body);
    assert.equal(response.status, 201);
    ids.push(response.data.id);
  }
  assert.equal((await record('too many')).data.error, 'DECISION_LIMIT_REACHED');
  const seen = [];
  let since = 0;
  do {
    const page = await f.get('/decisions?workstream=api&since=' + since);
    assert.equal(page.status, 200);
    assert.ok(Buffer.byteLength(JSON.stringify(page.data)) <= pageBytes);
    assert.ok(page.data.decisions.length);
    seen.push(...page.data.decisions.map(d => d.id));
    since = page.data.next_since;
  } while (since !== null);
  assert.deepEqual(seen, ids);
  await f.post('/workstreams/release', { name: 'api' });
  await f.post('/workstreams/register', { name: 'api' }, 'bob');
  assert.equal((await f.post('/decisions/record', { workstream: 'api', body: 'reset?' }, 'bob')).data.error, 'DECISION_LIMIT_REACHED');
  // A legacy row larger than the new page cap is never silently skipped or truncated.
  f.store.run('UPDATE decisions SET body=? WHERE id=?', 'old'.repeat(1000), ids[0]);
  assert.equal((await f.get('/decisions')).data.error, 'DECISION_PAGE_TOO_SMALL');
});

test('HTTP metadata rejects Unicode control and format characters atomically', async t => {
  const f = await fixture(t);
  await f.post('/workstreams/register', { name: 'api', paths: ['src'], contracts: ['safe'] });
  for (const char of ['\u0085', '\u009f', '\u200b', '\u202a', '\u202e', '\u2066', '\u2028', '\u2029', '\uFEFF']) {
    for (const field of ['paths', 'contracts']) {
      const result = await f.post('/workstreams/register', { name: 'api', [field]: ['a' + char + 'b'] });
      assert.equal(result.data.error, field === 'paths' ? 'INVALID_PATHS' : 'INVALID_CONTRACTS');
      assert.ok(result.data.hint);
    }
  }
  const unchanged = (await f.post('/workstreams/register', { name: 'api' })).data;
  assert.deepEqual(unchanged.paths, ['src']);
  assert.deepEqual(unchanged.contracts, ['safe']);
  assert.equal((await f.post('/workstreams/register', { name: 'unicode', paths: ['src/日本語'], contracts: ['café'] })).status, 200);
});

test('HTTP metadata rejects extglob patterns while preserving literal parentheses', async t => {
  const f = await fixture(t);
  for (const path of ['src/!(api)', 'src/@(api|web)', 'src/+(api)', 'src/?(api)', 'src/*(api)', 'src/@(nested@(api))']) {
    const result = await f.post('/workstreams/register', { name: 'api', paths: [path] });
    assert.equal(result.data.error, 'INVALID_PATHS');
    assert.ok(result.data.hint);
  }
  assert.equal((await f.post('/workstreams/register', { name: 'api', paths: ['src/(literal)', 'src/@literal', 'src/!literal'] })).status, 200);
});

test('decision authorization refuses expired leases before an HTTP request renews the owner', async t => {
  const f = await fixture(t);
  await f.post('/workstreams/register', { name: 'api' });
  f.advance(f.broker.config.leaseTtl); // Boundary equality is expired.
  // HTTP intentionally heartbeats before dispatch; exercise the core lease guard
  // without that automatic renewal, using the same authenticated owner identity.
  assert.throws(() => f.broker.recordDecision('alice', 'alice-session', { workstream: 'api', body: 'Expired' }),
    error => error.code === 'WORKSTREAM_OWNED' && error.status === 403 && Boolean(error.hint));
  assert.equal(f.store.get('SELECT count(*) n FROM decisions').n, 0);
  assert.equal((await f.post('/decisions/record', { workstream: 'api', body: 'Renewed by HTTP heartbeat' })).status, 201);
});

test('HTTP metadata rejects lone surrogates while preserving complete astral characters', async t => {
  const f = await fixture(t);
  await f.post('/workstreams/register', { name: 'api', paths: ['src'], contracts: ['safe'] });
  for (const char of ['\ud800', '\udbff', '\udc00', '\udfff', '\ud800x\udc00']) {
    for (const field of ['paths', 'contracts']) {
      const result = await f.post('/workstreams/register', { name: 'api', [field]: ['a' + char + 'b'] });
      assert.equal(result.data.error, field === 'paths' ? 'INVALID_PATHS' : 'INVALID_CONTRACTS');
    }
  }
  const unchanged = (await f.post('/workstreams/register', { name: 'api' })).data;
  assert.deepEqual(unchanged.paths, ['src']);
  assert.deepEqual(unchanged.contracts, ['safe']);
  const accepted = await f.post('/workstreams/register', { name: 'astral', paths: ['src/😀'], contracts: ['v1:𐐀'] });
  assert.equal(accepted.status, 200);
  assert.deepEqual(accepted.data.paths, ['src/😀']);
  assert.deepEqual(accepted.data.contracts, ['v1:𐐀']);
});

test('HTTP overlap pages seek live leases and use a bounded lookahead instead of tail counts', async t => {
  const f = await fixture(t, { overlapScanLimit: 2, overlapLimit: 1 });
  await f.post('/workstreams/register', { name: 'source', paths: ['.'] });
  for (const name of ['live-a', 'live-b', 'live-c']) await f.post('/workstreams/register', { name, paths: ['src'] }, 'bob');
  f.store.transaction(() => {
    for (let i = 0; i < 5000; i++) f.store.run("INSERT INTO workstreams(name,owner_session,owner_user,lease_until,paths,workspace,topic) VALUES (?,?,?,?,?,'default','fixture')",
      'expired-' + String(i).padStart(5, '0'), 'bob-session', 'bob', 0, '["src"]');
  });
  const statements = [];
  const all = f.store.all.bind(f.store), get = f.store.get.bind(f.store);
  f.store.all = (sql, ...args) => { statements.push({ sql, args }); return all(sql, ...args); };
  f.store.get = (sql, ...args) => { statements.push({ sql, args }); return get(sql, ...args); };
  const first = await f.post('/workstreams/check-overlap', { name: 'source' }, 'eve');
  f.store.all = all;
  f.store.get = get;
  const query = statements.find(s => /SELECT \* FROM workstreams WHERE|SELECT \* FROM workstreams INDEXED/.test(s.sql) && s.sql.includes('ORDER BY'));
  assert.ok(query);
  const plan = f.store.all('EXPLAIN QUERY PLAN ' + query.sql, ...query.args).map(r => r.detail).join('\n');
  assert.match(plan, /SEARCH workstreams USING INDEX workstreams_topic_lease \(workspace=\? AND topic=\? AND lease_until>\?\)/);
  assert.ok(!statements.some(s => /count\(\*\)/i.test(s.sql)));
  assert.equal(query.args.at(-1), 3); // At most the scan budget plus one lookahead row.
  assert.equal(first.data.truncated, true);
  assert.equal(first.data.next_since, 'live-a');
  const seen = [first.data.overlaps[0].name];
  let since = first.data.next_since;
  while (since) {
    const page = (await f.post('/workstreams/check-overlap', { name: 'source', since }, 'eve')).data;
    seen.push(...page.overlaps.map(o => o.name));
    since = page.next_since;
    assert.equal(page.truncated, Boolean(since));
  }
  assert.deepEqual(seen, ['live-a', 'live-b', 'live-c']);
  const expired = (await f.post('/workstreams/check-overlap', { name: 'source', include_expired: true }, 'eve')).data;
  assert.equal(expired.overlaps[0].name, 'expired-00000');
  assert.equal(expired.truncated, true);
});
