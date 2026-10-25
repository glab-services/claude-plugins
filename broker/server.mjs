import { claim, renew, done, queueItem, deadLetters, requeue } from './queue.mjs';
import http from 'node:http';
import { authenticate, activeCredential } from './credentials.mjs';
import { invite, invites, revokeInvite, join, revokeCredential } from './invites.mjs';
import { BrokerError, check, rejection } from './config.mjs';
import { perUserAck } from './broker.mjs';
import { DEFAULT_WORKSPACE } from './schema.mjs';
import { sseWriter } from './sse.mjs';
import { topicReplayVersion, topicReplayVersions } from './topic-replay.mjs';
import { createTopic, topics, subscribe, leaveTopic, closeTopic, skip, keep, unkeep, pins } from './topics.mjs';

async function body(req, limit) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    check(size <= limit, 'BODY_TOO_LARGE', 413);
    chunks.push(chunk);
  }
  let value;
  try { value = JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { throw new BrokerError('INVALID_JSON'); }
  check(value && typeof value === 'object' && !Array.isArray(value), 'INVALID_JSON');
  return value;
}

function json(res, status, value) {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(value));
}

function messageId(value) {
  check(typeof value === 'string' && /^\d+$/.test(value), 'INVALID_SINCE');
  const id = Number(value);
  check(Number.isSafeInteger(id), 'INVALID_SINCE');
  return id;
}

export function createServer(broker, tokens) {
  // Anonymous requests never spend trusted join capacity. Both buckets remain
  // globally bounded per broker; unknown/invalid bearer strings stay anonymous.
  const joinBuckets=Array.from({length:2},()=>({tokens:broker.config.joinRatePerMin,refillAt:broker.now()}));
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://broker');
      const route = `${req.method} ${url.pathname}`;
      if (route === 'POST /join') {
        const auth=req.headers.authorization===undefined?null:authenticate(broker.store,tokens,req.headers.authorization);
        const now = broker.now(),bucket=joinBuckets[auth?1:0];
        bucket.tokens = Math.min(broker.config.joinRatePerMin, bucket.tokens + Math.max(0, now - bucket.refillAt) * broker.config.joinRatePerMin / 60000); bucket.refillAt = now;
        check(bucket.tokens >= 1, 'RATE_LIMITED', 429); bucket.tokens--;
        check(req.headers['content-type']?.split(';')[0] === 'application/json', 'JSON_REQUIRED', 415);
        const args = await body(req, broker.config.joinMaxBodyBytes);
        check(req.headers.authorization === undefined || auth, 'UNAUTHORIZED', 401);
        return json(res, 201, join(broker, auth, args));
      }
      const auth = authenticate(broker.store, tokens, req.headers.authorization);
      check(auth, 'UNAUTHORIZED', 401);
      const { user, credential_id } = auth;
      const session = req.headers['x-session-id'];
      const role = req.headers['x-agent-role'] ?? 'worker';
      // Decision 20: absent headers keep M4 channels bound to default.
      const workspace = req.headers['x-workspace'] ?? DEFAULT_WORKSPACE;
      const identityRoute = ['GET /workspaces', 'POST /workspaces', 'POST /credentials/revoke'].includes(route);
      const statusRoute=route==='GET /session/status';
      if (!identityRoute&&!statusRoute) broker.heartbeat(user, session, role, workspace, req.headers['x-topic']??null);
      let args;
      if (req.method === 'POST') {
        check(req.headers['content-type']?.split(';')[0] === 'application/json', 'JSON_REQUIRED', 415);
        args = await body(req, broker.config.maxBodyBytes);
        // A slow body must not keep a concurrently revoked identity authorized.
        check(!credential_id || activeCredential(broker.store, credential_id, user), 'UNAUTHORIZED', 401);
      }
      if (route === 'GET /workspaces') return json(res, 200, broker.workspaces(user));
      if (route === 'POST /workspaces') return json(res, 201, broker.createWorkspace(user, args));
      if (route === 'POST /credentials/revoke') return json(res, 200, revokeCredential(broker, auth, args));
      switch (route) {
        case 'GET /session/status':
          check([...url.searchParams.keys()].length===0,'INVALID_SESSION_FIELDS');
          return json(res,200,broker.sessionStatus(user,session,workspace,req.headers['x-topic']??null));
        case 'POST /queue/requeue': return json(res,200,requeue(broker,user,session,args));
        case 'GET /queue/dead-letters': {
          check([...url.searchParams.keys()].every(key=>key==='topic'),'INVALID_QUEUE_FIELDS');
          return json(res,200,deadLetters(broker,user,session,{topic:url.searchParams.get('topic')}));
        }
        case 'POST /queue/claim': return json(res,200,claim(broker,user,session,args));
        case 'POST /queue/renew': return json(res,200,renew(broker,user,session,args));
        case 'POST /queue/done': return json(res,200,done(broker,user,session,args));
        case 'GET /queue/item': {
          check([...url.searchParams.keys()].every(key=>key==='id'),'INVALID_QUEUE_FIELDS');
          return json(res,200,queueItem(broker,user,session,{id:messageId(url.searchParams.get('id')??'0')}));
        }
        case 'POST /topics/skip': return json(res, 200, skip(broker,user,session,args));
        case 'POST /topics/keep': return json(res, 200, keep(broker,user,session,args));
        case 'POST /topics/unkeep': return json(res, 200, unkeep(broker,user,session,args));
        case 'GET /topics/pins': {
          check([...url.searchParams.keys()].every(key=>key==='topic'),'INVALID_TOPIC_FIELDS');
          return json(res, 200, pins(broker,user,session,url.searchParams.has('topic')?{topic:url.searchParams.get('topic')}:{}));
        }
        case 'POST /topics': return json(res, 201, createTopic(broker,user,session,args));
        case 'GET /topics': return json(res, 200, topics(broker,user,session));
        case 'POST /topics/subscribe': return json(res, 200, subscribe(broker,user,session,args));
        case 'POST /topics/leave': return json(res, 200, leaveTopic(broker,user,session,args));
        case 'POST /topics/close': return json(res, 200, closeTopic(broker,user,session,args));
        case 'POST /invites': return json(res, 201, invite(broker, user, session, args));
        case 'GET /invites': return json(res, 200, invites(broker, user, session));
        case 'POST /invites/revoke': return json(res, 200, revokeInvite(broker, user, session, args));
        case 'GET /members': return json(res, 200, broker.members(user, session));
        case 'POST /members/remove': return json(res, 200, broker.removeMember(user, session, args));
        case 'POST /members/leave': return json(res, 200, broker.leave(user, session, args));
        case 'POST /members/finish': return json(res, 200, broker.finish(user, session, args));
        case 'POST /members/transfer': return json(res, 200, broker.transferOwner(user, session, args));
        case 'POST /heartbeat':
          return json(res, 200, { user, session_id: session, workspace, topic:req.headers['x-topic']??null });
        case 'POST /workstreams/register':
          return json(res, 200, broker.register(user, session, args.name, args));
        case 'POST /workstreams/check-overlap':
          return json(res, 200, broker.checkOverlap(user, session, args));
        case 'POST /decisions/record':
          return json(res, 201, broker.recordDecision(user, session, args));
        case 'GET /decisions': {
          check([...url.searchParams.keys()].every(key => ['workstream', 'since'].includes(key)), 'INVALID_PLANNING_FIELDS');
          const since = url.searchParams.get('since') ?? '0';
          check(/^\d+$/.test(since) && Number.isSafeInteger(Number(since)), 'INVALID_DECISION_ID');
          return json(res, 200, broker.listDecisions(user, session, { since: Number(since),
            ...(url.searchParams.has('workstream') ? { workstream: url.searchParams.get('workstream') } : {}) }));
        }
        case 'POST /workstreams/release':
          return json(res, 200, broker.release(user, session, args.name));
        case 'POST /send':
          return json(res, 201, broker.send(user, session, args));
        case 'GET /inbox':
          return json(res, 200, { messages: broker.inbox(user, session, messageId(url.searchParams.get('since') ?? '0')) });
        case 'GET /resume':
          return json(res, 200, broker.resume(user, session));
        case 'POST /ack':
          return json(res, 200, broker.ack(user, session, args.id));
        case 'POST /close':
          check(typeof args.thread_id === 'string', 'INVALID_THREAD');
          return json(res, 200, broker.close(user, session, args.thread_id));
        case 'GET /stream': {
          check(!broker.streams.has(session), 'SESSION_STREAM_EXISTS', 409);
          const since = messageId(url.searchParams.get('since') ?? req.headers['last-event-id'] ?? '0');
          const replay = url.searchParams.get('replay') === '1';
          const maxId = broker.store.get('SELECT coalesce(max(id),0) AS id FROM messages WHERE workspace=?', workspace).id;
          // A future cursor must not suppress messages created after this connection.
          let cursor = replay ? 0 : Math.min(since, maxId);
          const queued = broker.inbox(user, session, cursor);
          const topicVersions=topicReplayVersions(broker.store,workspace,user);
          res.writeHead(200, {
            'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform',
            Connection: 'keep-alive', 'X-Accel-Buffering': 'no',
          });
          const write = sseWriter(res, broker.config);
          write(': connected\n\n');
          const emitFrame = (event, data) => {
            // The wrapped content already contains the body. Avoid transmitting it twice.
            const { body: _body, ...payload } = data;
            write(`${event === 'message' ? `id: ${data.id}\n` : ''}event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);
          };
          let initial = queued;
          let pumping = false;
          let pending = false;
          let reset = false;
          let forgotten = null;
          const delivered = new Map();
          async function pump() {
            if (pumping) { pending = true; return; }
            pumping = true;
            try {
              let page;
              do {
                pending = false;
                if (reset) { cursor = 0; initial = null; reset = false; }
                // A running pump can advance past a concurrent forget. Apply
                // the earliest rewind here, after its previous page finishes.
                if(forgotten!==null) {cursor=Math.min(cursor,forgotten-1);initial=null;forgotten=null;}
                page = initial ?? broker.inbox(user, session, cursor);
                initial = null;
                for (const message of page) {
                  // A durable newer queue generation is a new offer, even when
                  // another broker process performed the release.
                  const seen=delivered.get(message.id);
                  if(seen && (message.claim_generation===undefined || seen.generation===message.claim_generation)) {cursor=message.id;continue;}
                  if (!await write.ready()) return;
                  // Retention may have tombstoned this page snapshot while the socket waited.
                  const current=broker.store.get('SELECT purged_at FROM messages WHERE id=?',message.id);
                  if(!current || current.purged_at!==null) {cursor=message.id;continue;}
                  // A sibling may have claimed this page snapshot while the socket waited.
                  const queue=message.to_kind==='topic'?broker.store.get('SELECT * FROM queue_items WHERE message_id=?',message.id):null;
                  const claim = message.to_kind==='topic'
                    ? broker.store.get('SELECT acked_at FROM topic_acks WHERE message_id=? AND user=?',message.id,user)
                    : perUserAck(message.kind)
                    ? broker.store.get('SELECT acked_at FROM escalation_targets WHERE message_id=? AND user=?', message.id, user)
                    : broker.store.get('SELECT acked_at FROM messages WHERE id=?', message.id);
                  if ((queue || (message.to_kind==='topic' ? !claim : claim && claim.acked_at === null)) &&
                    (message.to_kind==='topic'?broker.pendingTopic(message,user,session):broker.eligible(message, user, session))) {
                    if(queue)message.claim_generation=queue.generation;
                    emitFrame('message', message);
                    delivered.set(message.id,{topic:message.to_kind==='topic'?message.to_name:null,generation:message.claim_generation});
                  }
                  cursor = message.id;
                }
              } while (pending || forgotten!==null || page.length === broker.config.inboxLimit);
            } catch (error) {
              broker.log(JSON.stringify({ event: 'stream_error', error: error.message }));
              res.destroy();
            } finally { pumping = false; }
          }
          const emit = (event, data) => {
            // Message bodies stay in SQLite while the producer waits for the socket.
            if (event === 'message') void pump();
            else emitFrame(event, data);
          };
          const rescan = () => { if (replay) { reset = true; void pump(); } };
          const applyTopicVersion = (topic,version) => {
            // New subscriptions have no applied marker. Only a changed known
            // marker requests replay; remember it across subscription absence.
            const changed=topicVersions.has(topic)&&topicVersions.get(topic)!==version;
            if(changed) {
              stream.forgetTopic(topic);
              const first=broker.store.get("SELECT min(id) AS id FROM messages WHERE workspace=? AND to_kind='topic' AND to_name=? AND purged_at IS NULL",workspace,topic).id;
              if(first!==null)forgotten=Math.min(forgotten??first,first);
            }
            topicVersions.set(topic,version);
            return changed;
          };
          const stream = { user, credential_id, workspace, topic:req.headers['x-topic']??null, emit, rescan, forget: id => {delivered.delete(id);forgotten=Math.min(forgotten??id,id);},
            applyTopicVersion: topic=>{
              if(applyTopicVersion(topic,topicReplayVersion(broker.store,workspace,topic,user))&&!replay)void pump();
            },
            forgetTopic: topic => {for(const [id,entry] of delivered)if(entry.topic===topic)delivered.delete(id);}, end: () => res.end() };
          broker.streams.set(session, stream);
          const timer = setInterval(() => {
            if (credential_id && !activeCredential(broker.store, credential_id, user)) return res.end();
            try { broker.scope(user, session); } catch { return res.end(); }
            const agent = broker.store.get('SELECT last_seen FROM agents WHERE session_id=?', session);
            if (!agent || agent.last_seen <= broker.now() - broker.config.leaseTtl) return res.end();
            const next=topicReplayVersions(broker.store,workspace,user);
            let topicChanged=false;
            for(const [topic,version] of next) {
              if(applyTopicVersion(topic,version))topicChanged=true;
            }
            write(': heartbeat\n\n');
            rescan();
            if(topicChanged&&!replay)void pump();
          }, broker.config.sseHeartbeat);
          res.on('close', () => {
            clearInterval(timer);
            if (broker.streams.get(session) === stream) broker.streams.delete(session);
          });
          void pump();
          return;
        }
        default: throw new BrokerError('NOT_FOUND', 404);
      }
    } catch (error) {
      if (res.headersSent || res.destroyed) return res.destroy();
      if (!(error instanceof BrokerError)) broker.log(JSON.stringify({ event: 'request_error', error: error.message }));
      json(res, error instanceof BrokerError ? error.status : 500, rejection(error));
    }
  });
  return server;
}
