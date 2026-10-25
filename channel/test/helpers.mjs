import {prepareTopicFixture,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import { fileURLToPath } from 'node:url';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { chmodSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join as pathJoin } from 'node:path';
import { testTempDir } from '../../test-support/temp.mjs';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import * as z from 'zod/v4';
import { Store } from '../../broker/store.mjs';
import { Broker } from '../../broker/broker.mjs';
import { config } from '../../broker/config.mjs';
import { createServer } from '../../broker/server.mjs';

export const tokens = { alice: 'alice-test-token-0000000000000000', bob: 'bob-test-token-000000000000000000' };
export async function brokerFixture(t, { streamStatus, ...overrides } = {}) {
  const store = new Store(':memory:');
  const broker = new Broker(store, { ...config({}), ...overrides }, Object.keys(tokens), { log: () => {} });
  prepareTopicFixture(broker);
  const server = createServer(broker, new Map(Object.entries(tokens).map(([user, token]) => [token, user])));
  const original = server.listeners('request')[0];
  server.removeAllListeners('request');
  const requests = [];
  server.on('request', (req, res) => {
    requests.push({ path: req.url, method: req.method, session: req.headers['x-session-id'] });
    if (streamStatus && req.url.startsWith('/stream')) { res.writeHead(streamStatus); res.end(); }
    else void original(req, res);
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const clients = [];
  t.after(async () => {
    for (const client of clients) await client.close();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    store.close();
  });
  return { broker, store, server, clients, requests, url: `http://127.0.0.1:${server.address().port}` };
}

export async function session(fixture, user, id, extraEnv = {}) {
  const workspace=extraEnv.CHANNEL_WORKSPACE??'default',workspaceLevel=Object.hasOwn(extraEnv,'CHANNEL_TOPIC')&&extraEnv.CHANNEL_TOPIC===undefined;
  const binding=workspaceLevel?undefined:(extraEnv.CHANNEL_TOPIC??fixture.bindingTopic??FIXTURE_TOPIC);
  const actualToken=extraEnv.BROKER_TOKEN??tokens[user];
  const actualUser=Object.keys(tokens).find(u=>tokens[u]===actualToken)??fixture.store?.get('SELECT user FROM credentials WHERE id=?',actualToken?.split('_')[1]??'')?.user??user;
  if(fixture.broker){
    if(binding===FIXTURE_TOPIC||binding===undefined)prepareTopicFixture(fixture.broker,workspace);
    else if(fixture.store.get('SELECT 1 FROM topics WHERE workspace=? AND name=?',workspace,binding)&&fixture.broker.member(actualUser,workspace))fixture.store.run('INSERT OR IGNORE INTO topic_subscriptions VALUES (?,?,?,?)',workspace,binding,actualUser,Date.now());
  }
  const client = new Client({ name: `test-${id}`, version: '1.0.0' });
  const notifications = [];
  client.setNotificationHandler(z.object({
    method: z.literal('notifications/claude/channel'),
    params: z.object({ content: z.string(), meta: z.record(z.string(), z.string()) }),
  }), event => { notifications.push(event.params); fixture.onNotification?.(event.params); });
  const logs = [];
  const transport = new StdioClientTransport({
    command: process.execPath, args: [fileURLToPath(new URL('../main.mjs', import.meta.url))],
    env: Object.fromEntries(Object.entries({ BROKER_URL: fixture.url, BROKER_TOKEN: tokens[user], CHANNEL_SESSION_ID: id, CHANNEL_POLL_MS: '25', CHANNEL_TOPIC:binding, ...extraEnv }).filter(([,value])=>value!==undefined)),
    stderr: 'pipe',
  });
  transport.stderr.on('data', chunk => logs.push(chunk.toString()));
  fixture.clients.push(client);
  await client.connect(transport);
  let admin;
  const call=async(name,args={})=>{
    if(name==='invite'&&args.topics===undefined)args={...args,topics:[binding??FIXTURE_TOPIC]};
    if(!workspaceLevel&&['remove_member','transfer_owner'].includes(name)&&fixture.store?.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user===actualUser){
      admin??=session(fixture,user,id+'-workspace-admin',{...extraEnv,CHANNEL_TOPIC:undefined});return (await admin).call(name,args);
    }
    return client.callTool({name,arguments:args},undefined,{timeout:5000});
  };
  return {client,notifications,logs,call};
}

export async function eventually(predicate, timeout = 4000) {
  const end = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() >= end) throw new Error('Timed out waiting for channel state');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

// Drop only the first stream's downstream bytes; ordinary HTTP requests keep working.
export async function freezingProxy(t, fixture) {
  const state = { freeze: false, streams: 0, comments: 0, heartbeats: 0 };
  const server = http.createServer((req, res) => {
    const stream = req.url.startsWith('/stream') ? ++state.streams : 0;
    if (req.url === '/heartbeat') state.heartbeats++;
    const upstream = http.request(fixture.url + req.url, { method: req.method, headers: req.headers }, incoming => {
      res.writeHead(incoming.statusCode, incoming.headers);
      incoming.on('data', chunk => {
        if (stream === 1 && state.freeze) return;
        if (stream && chunk.includes(Buffer.from(': heartbeat'))) state.comments++;
        res.write(chunk);
      });
      incoming.on('end', () => res.end());
      incoming.on('error', () => res.destroy());
    });
    upstream.on('error', () => res.destroy());
    res.on('close', () => upstream.destroy());
    req.pipe(upstream);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  return { state, url: `http://127.0.0.1:${server.address().port}` };
}

// Plugin hook and watcher tests: a private HOME holding a mode-0600 token
// file and a plugin data directory, as Claude Code lays them out.
export function pluginScratch(t, { token = tokens.bob } = {}) {
  const dir = testTempDir('plugin-'); t.after(() => rmSync(dir, { recursive: true, force: true }));
  const home = pathJoin(dir, 'home'), data = pathJoin(home, '.claude', 'plugins', 'data', 'team-messaging'), tokenFile = pathJoin(home, '.config', 'team-messaging', 'credentials', 'plugin.token');
  mkdirSync(dirname(tokenFile), { recursive: true, mode: 0o700 }); writeFileSync(tokenFile, token, { mode: 0o600 });
  return { dir, home, data, tokenFile, env: (fixture, extra = {}) => ({ HOME: home, TMPDIR: process.env.TMPDIR, CLAUDE_PLUGIN_DATA: data,
    CLAUDE_PLUGIN_OPTION_BROKER_URL: fixture.url, CLAUDE_PLUGIN_OPTION_TOKEN_FILE: tokenFile, CLAUDE_PLUGIN_OPTION_WORKSPACE: 'default', CLAUDE_PLUGIN_OPTION_SESSION_ID: 'shared-b', ...extra }) };
}
// Recreates what Claude Code leaves under umask 002: ~/.claude/plugins,
// plugins/data and the plugin data dir at `mode`, below `claude` (the
// ~/.claude mode) and the scratch HOME at `home`. Needs no extra group, so it
// runs in CI.
export function umaskPluginData(s, { mode = 0o775, claude = 0o700, home = 0o700 } = {}) {
  mkdirSync(s.data, { recursive: true, mode: 0o700 });
  const plugins = pathJoin(s.home, '.claude', 'plugins');
  for (const dir of [plugins, pathJoin(plugins, 'data'), s.data]) chmodSync(dir, mode);
  chmodSync(pathJoin(s.home, '.claude'), claude); chmodSync(s.home, home);
}
// Runs one channel entrypoint as a real child process.
export function runChannel(script, args = [], env = {}, stdin = '') {
  const child = spawn(process.execPath, [fileURLToPath(new URL(`../${script}`, import.meta.url)), ...args], { env, stdio: ['pipe', 'pipe', 'pipe'] });
  let stdout = '', stderr = ''; const started = Date.now();
  child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
  child.stdin.on('error', () => {}); child.stdin.end(stdin);
  const result = new Promise((resolve, reject) => { child.once('error', reject); child.once('close', (code, signal) => resolve({ code, signal, stdout, stderr, ms: Date.now() - started })); });
  return { child, result };
}
