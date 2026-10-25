import http from 'node:http';
import { EventEmitter } from 'node:events';
import { createServer } from '../server.mjs';

export async function httpFixture(t, broker, {topic=broker.fixtureTopic,workspaceAdminSessions=false}={}) {
  const tokens = new Map(['alice', 'bob', 'eve'].map(user => [`${user}-test-token`, user]));
  const server = createServer(broker, tokens);
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  t.after(() => {
    server.closeAllConnections();
    return new Promise(resolve => server.close(resolve));
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const headers = (user, session,workspace='default') => {const binding=typeof topic==='function'?topic(user,session,workspace):topic;return { Authorization: `Bearer ${user}-test-token`, 'X-Session-Id': session, ...(binding?{'X-Topic':binding}:{}) };};
  const request = (path, { user = 'alice', session = 'a', ...options } = {}) => {
    const workspace=options.headers?.['X-Workspace']??'default';
    // Legacy lifecycle tests use an actual, separate workspace-level owner
    // session for administration; production still enforces decision98.
    const admin=workspaceAdminSessions&&['/members/remove','/members/transfer'].includes(path)&&broker.store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user===user;
    const outgoing={...headers(user,admin?session+'-workspace-admin':session,workspace),'Content-Type':'application/json',...options.headers};
    if(admin)delete outgoing['X-Topic'];
    return fetch(base+path,{...options,headers:outgoing});
  };
  async function stream(user, session, query = '', { paused = false } = {}) {
    const events = [];
    const updates = new EventEmitter();
    let pending = '';
    let closed = false;
    let heartbeats = 0;
    const req = http.get(base + '/stream' + query, { headers: headers(user, session) });
    const res = await new Promise((resolve, reject) => { req.once('response', resolve); req.once('error', reject); });
    if (res.statusCode !== 200) { res.resume(); throw new Error(`Stream HTTP ${res.statusCode}`); }
    if (paused) res.pause();
    res.setEncoding('utf8');
    res.on('data', chunk => {
      pending += chunk;
      let end;
      while ((end = pending.indexOf('\n\n')) >= 0) {
        const frame = pending.slice(0, end);
        pending = pending.slice(end + 2);
        const lines = frame.split('\n');
        if (lines.some(line => line.startsWith(': heartbeat'))) heartbeats++;
        const data = lines.find(line => line.startsWith('data: '));
        if (data) events.push({ event: lines.find(line => line.startsWith('event: '))?.slice(7), data: JSON.parse(data.slice(6)) });
      }
      updates.emit('update');
    });
    res.on('error', () => {});
    res.on('close', () => { closed = true; updates.emit('update'); });
    t.after(() => req.destroy());
    return {
      events,
      get heartbeats() { return heartbeats; },
      close: () => req.destroy(),
      resume: () => res.resume(),
      wait(predicate, timeout = 3000) {
        return new Promise((resolve, reject) => {
          const timer = setTimeout(() => finish(new Error('Timed out waiting for SSE event')), timeout);
          const finish = error => {
            clearTimeout(timer);
            updates.off('update', onUpdate);
            error ? reject(error) : resolve(events);
          };
          const onUpdate = () => {
            if (predicate(events)) finish();
            else if (closed) finish(new Error('SSE closed before expected delivery'));
          };
          updates.on('update', onUpdate);
          onUpdate();
        });
      },
    };
  }
  return { server, request, stream };
}

export async function eventually(predicate) {
  const deadline = Date.now() + 2000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('Condition did not become true');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}
