import { releaseClaims, reoffer } from './queue.mjs';
import { check } from './config.mjs';
import { topicPattern } from '../shared/address.mjs';
import { envelope } from './broker.mjs';
import { invalidateTopic } from './topic-replay.mjs';
import { subscribed } from './topic-delivery.mjs';
import { notice,endSessions } from './workspaces.mjs';

export function topicName(value) {
  check(typeof value === 'string' && topicPattern.test(value), 'INVALID_TOPIC_NAME');
  return value;
}
export function getTopic(store, workspace, name, { open = true } = {}) {
  topicName(name);
  const row = store.get('SELECT * FROM topics WHERE workspace=? AND name=?', workspace, name);
  check(row, 'TOPIC_NOT_FOUND', 404); check(!open || row.closed_at === null, 'TOPIC_CLOSED', 409);
  return row;
}
export function inviteTopics(store, workspace, topics) {
  check(Array.isArray(topics)&&topics.length===1, 'INVALID_INVITE_TOPICS');
  const names = [...new Set(topics.map(topicName))];
  for (const name of names) getTopic(store, workspace, name);
  return names;
}
function fields(args, allowed) { check(Object.keys(args).every(key => allowed.includes(key)), 'INVALID_TOPIC_FIELDS'); }
function audit(broker, scope, action, topic, detail = {}) {
  broker.store.run('INSERT INTO workspace_audit(workspace,action,actor_user,actor_session,detail,created) VALUES (?,?,?,?,?,?)',
    scope.workspace, action, scope.user, scope.session, JSON.stringify({ topic, ...detail }), broker.now());
}
export function createTopic(broker, user, session, args) {
  fields(args, ['name','mode']);topicName(args.name);
  const mode = args.mode ?? 'broadcast';
  check(['broadcast','queue'].includes(mode), 'INVALID_TOPIC_MODE');
  return broker.store.transaction(() => {
    const scope = broker.scope(user, session);broker.requireOwner(user,scope.workspace);
    check(!broker.store.get('SELECT 1 FROM topics WHERE workspace=? AND name=?', scope.workspace, args.name), 'TOPIC_EXISTS', 409);
    broker.store.run('INSERT INTO topics(workspace,name,mode,owner_user,created) VALUES (?,?,?,?,?)',scope.workspace,args.name,mode,user,broker.now());
    audit(broker,scope,'topic:create',args.name);
    return { name: args.name, mode, owner: user, closed: false, subscribed: false };
  });
}
export function topics(broker, user, session) {
  return broker.store.transaction(() => {
    const { workspace,topic } = broker.scope(user,session);
    return { topics: broker.store.all(`SELECT t.name,t.mode,t.owner_user AS owner,t.closed_at,
      EXISTS(SELECT 1 FROM topic_subscriptions s WHERE s.workspace=t.workspace AND s.topic=t.name AND s.user=?) AS subscribed
      FROM topics t WHERE t.workspace=? AND (? IS NULL OR t.name=?) ORDER BY t.name`,user,workspace,topic,topic)
      .map(row=>({...row,closed:row.closed_at!==null,subscribed:Boolean(row.subscribed)})) };
  });
}
export function subscribe(broker,user,session,args) {
  fields(args,['name']);let workspace;
  const result = broker.store.transaction(() => {
    const scope=broker.scope(user,session);workspace=scope.workspace;broker.assertTopicScope(scope,args.name);getTopic(broker.store,scope.workspace,args.name);
    const changed=broker.store.run('INSERT OR IGNORE INTO topic_subscriptions VALUES (?,?,?,?)',scope.workspace,args.name,user,broker.now()).changes;
    broker.store.run('INSERT INTO topic_cursors(workspace,topic,user,acked_through) VALUES (?,?,?,0) ON CONFLICT(workspace,topic,user) DO NOTHING',scope.workspace,args.name,user);
    if(changed)audit(broker,scope,'topic:subscribe',args.name);
    return { name:args.name,subscribed:true };
  });
  for (const stream of broker.streams.values()) if(stream.user===user && stream.workspace===workspace) {
    stream.applyTopicVersion?.(args.name);stream.rescan?.();
  }
  return result;
}
export function leaveTopic(broker,user,session,args) {
  fields(args,['name']);let workspace,released;
  const result = broker.store.transaction(() => {
    const scope=broker.scope(user,session);workspace=scope.workspace;broker.assertTopicScope(scope,args.name);getTopic(broker.store,scope.workspace,args.name,{open:false});
    released=releaseClaims(broker.store,workspace,user,args.name);
    if(broker.store.run('DELETE FROM topic_subscriptions WHERE workspace=? AND topic=? AND user=?',scope.workspace,args.name,user).changes) {
      endSessions(broker.store,workspace,user,'topic_left',broker.now(),args.name);
      broker.store.run('DELETE FROM workstreams WHERE workspace=? AND topic=? AND owner_user=?',workspace,args.name,user);
      invalidateTopic(broker.store,workspace,args.name,user);
      audit(broker,scope,'topic:leave',args.name);
    }
    broker.store.run("DELETE FROM topic_pins WHERE workspace=? AND user=? AND message_id IN (SELECT id FROM messages WHERE workspace=? AND to_kind='topic' AND to_name=?)",workspace,user,workspace,args.name);
    broker.store.run('DELETE FROM topic_cursors WHERE workspace=? AND topic=? AND user=?',scope.workspace,args.name,user);
    return { name:args.name,subscribed:false };
  });
  reoffer(broker,released);
  for (const stream of broker.streams.values()) if(stream.user===user && stream.workspace===workspace) {
    stream.forgetTopic?.(args.name);
    if(stream.topic===args.name)stream.end();
  }
  return result;
}
export function closeTopic(broker,user,session,args) {
  fields(args,['name']);let announcement;
  const result=broker.store.transaction(() => {
    const scope=broker.scope(user,session);broker.assertTopicScope(scope,args.name);const row=getTopic(broker.store,scope.workspace,args.name,{open:false});
    check(row.owner_user===user || broker.store.get("SELECT 1 FROM workspace_members WHERE workspace=? AND user=? AND role='owner'",scope.workspace,user), 'NOT_TOPIC_OWNER',403);
    if(row.closed_at!==null)return { name:args.name,closed:true,closed_at:row.closed_at };
    const now=broker.now();broker.store.run('UPDATE topics SET closed_at=?,closed_by_user=? WHERE workspace=? AND name=?',now,user,scope.workspace,args.name);
    audit(broker,scope,'topic:close',args.name);
    const recipients=broker.store.all('SELECT user FROM topic_subscriptions WHERE workspace=? AND topic=?',scope.workspace,args.name).map(r=>r.user);
    announcement=notice(broker.store,scope.workspace,recipients,`${user} closed topic ${args.name}.`,now,args.name);
    return { name:args.name,closed:true,closed_at:now };
  });
  if(announcement)broker.publish(announcement);return result;
}


function subscriber(broker, scope, topic) {
  broker.assertTopicScope(scope,topic);
  getTopic(broker.store,scope.workspace,topic,{open:false});
  check(subscribed(broker.store,scope.workspace,topic,scope.user),'TOPIC_NOT_SUBSCRIBED',403);
}
export function skip(broker,user,session,args) {
  fields(args,['topic','through']);
  check(args.through===undefined || (Number.isSafeInteger(args.through)&&args.through>=0),'INVALID_SKIP_THROUGH');
  let workspace;
  const result=broker.store.transaction(()=>{
    const scope=broker.scope(user,session);({workspace}=scope);subscriber(broker,scope,args.topic);
    // Snapshot the cap inside the same write transaction: future sends must remain pending.
    check(getTopic(broker.store,workspace,args.topic,{open:false}).mode==='broadcast','USE_CLAIM',409);
    const max=broker.store.get("SELECT coalesce(max(id),0) id FROM messages WHERE workspace=? AND to_kind='topic' AND to_name=?",workspace,args.topic).id;
    const from=broker.store.get('SELECT acked_through FROM topic_cursors WHERE workspace=? AND topic=? AND user=?',workspace,args.topic,user)?.acked_through??0;
    const to=Math.max(from,Math.min(args.through??max,max));
    const skipped=broker.store.get(`SELECT count(*) n FROM messages m WHERE workspace=? AND to_kind='topic' AND to_name=? AND id>? AND id<=?
      AND NOT EXISTS(SELECT 1 FROM topic_acks a WHERE a.message_id=m.id AND a.user=?)`,workspace,args.topic,from,to,user).n;
    broker.store.run('INSERT INTO topic_cursors VALUES (?,?,?,?) ON CONFLICT(workspace,topic,user) DO UPDATE SET acked_through=excluded.acked_through',workspace,args.topic,user,to);
    if(to>from)audit(broker,scope,'topic:skip',args.topic,{from,to,skipped});
    return {topic:args.topic,acked_through:to,skipped};
  });
  // Invalidate sibling presentation only after the cursor transaction commits.
  for(const stream of broker.streams.values())if(stream.user===user && stream.workspace===workspace)stream.emit('skipped',{topic:result.topic,acked_through:result.acked_through});
  return result;
}
function pinnable(broker,user,session,id) {
  check(Number.isSafeInteger(id)&&id>0,'INVALID_MESSAGE_ID');
  const scope=broker.scope(user,session),message=broker.store.get('SELECT * FROM messages WHERE id=?',id);
  check(message && broker.visibleTopic(message,user,session)&&subscribed(broker.store,scope.workspace,message.to_name,user),'MESSAGE_NOT_FOUND',404);
  return {scope,message};
}
export function keep(broker,user,session,args) {
  fields(args,['id']);
  return broker.store.transaction(()=>{
    const {scope}=pinnable(broker,user,session,args.id);
    const old=broker.store.get('SELECT 1 FROM topic_pins WHERE message_id=? AND user=?',args.id,user);
    check(old || broker.store.get('SELECT count(*) n FROM topic_pins WHERE workspace=? AND user=?',scope.workspace,user).n<broker.config.maxPins,'PIN_LIMIT',409);
    const changed=broker.store.run('INSERT OR IGNORE INTO topic_pins VALUES (?,?,?,?)',args.id,user,scope.workspace,broker.now()).changes;
    return {id:args.id,kept:true,changed:Boolean(changed)};
  });
}
export function unkeep(broker,user,session,args) {
  fields(args,['id']);
  return broker.store.transaction(()=>{
    const {scope}=pinnable(broker,user,session,args.id);
    const changed=broker.store.run('DELETE FROM topic_pins WHERE message_id=? AND user=? AND workspace=?',args.id,user,scope.workspace).changes;
    return {id:args.id,kept:false,changed:Boolean(changed)};
  });
}
export function pins(broker,user,session,args) {
  fields(args,['topic']);
  return broker.store.transaction(()=>{
    const scope=broker.scope(user,session);
    if(args.topic!==undefined)subscriber(broker,scope,args.topic);
    const rows=broker.store.all(`SELECT m.* FROM topic_pins p JOIN messages m ON m.id=p.message_id
      JOIN topic_subscriptions s ON s.workspace=m.workspace AND s.topic=m.to_name AND s.user=p.user
      WHERE p.workspace=? AND p.user=? AND m.workspace=? AND (? IS NULL OR m.to_name=?) AND m.to_kind='topic' ${args.topic===undefined?'':'AND m.to_name=?'} ORDER BY m.id`,scope.workspace,user,scope.workspace,scope.topic,scope.topic,...(args.topic===undefined?[]:[args.topic]));
    return {messages:rows.map(envelope)};
  });
}
