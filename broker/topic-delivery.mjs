import { check } from './config.mjs';

export function subscribed(store, workspace, topic, user) {
  return Boolean(store.get('SELECT 1 FROM topic_subscriptions WHERE workspace=? AND topic=? AND user=?',workspace,topic,user));
}
export function visibleTopic(broker, message, user, session) {
  let scope;try {scope=broker.scope(user,session);}catch(error){if(['SESSION_OWNED','NOT_A_MEMBER','NOT_SUBSCRIBED','TOPIC_REQUIRED','SESSION_ENDED'].includes(error.code))return false;throw error;}
  return message.to_kind==='topic' && message.workspace===scope.workspace && (scope.topic===null || scope.topic===message.to_name) && message.purged_at==null && (scope.topic===null || subscribed(broker.store,scope.workspace,message.to_name,user));
}
export function pendingTopic(broker, message, user, session) {
  if(!visibleTopic(broker,message,user,session)||!subscribed(broker.store,message.workspace,message.to_name,user))return false;
  const item=broker.store.get('SELECT state,holder_user,holder_session FROM queue_items WHERE message_id=? AND workspace=?',message.id,message.workspace);
  if(item)return item.state==='ready' || (item.state==='claimed' && item.holder_user===user && item.holder_session===session);
  const cursor=broker.store.get('SELECT acked_through FROM topic_cursors WHERE workspace=? AND topic=? AND user=?',message.workspace,message.to_name,user)?.acked_through??0;
  return message.id>cursor && !broker.store.get('SELECT 1 FROM topic_acks WHERE message_id=? AND user=?',message.id,user);
}
// Called inside the sender/ack transaction. Sparse acks never jump over a gap.
export function claimTopic(store, message, user, session, now, limit) {
  const old=store.get('SELECT session FROM topic_acks WHERE message_id=? AND user=?',message.id,user);
  check(!old || old.session===session,'MESSAGE_CLAIMED',409);
  if(!old)store.run('INSERT INTO topic_acks(message_id,user,session,acked_at) VALUES (?,?,?,?)',message.id,user,session,now);
  store.run('INSERT INTO topic_cursors(workspace,topic,user,acked_through) VALUES (?,?,?,0) ON CONFLICT(workspace,topic,user) DO NOTHING',message.workspace,message.to_name,user);
  let cursor=store.get('SELECT acked_through FROM topic_cursors WHERE workspace=? AND topic=? AND user=?',message.workspace,message.to_name,user).acked_through;
  for(let step=0;step<limit;step++) {
    const next=store.get("SELECT id FROM messages INDEXED BY messages_topic WHERE workspace=? AND to_name=? AND to_kind='topic' AND id>? ORDER BY id LIMIT 1",message.workspace,message.to_name,cursor);
    if(!next || !store.get('SELECT 1 FROM topic_acks WHERE message_id=? AND user=?',next.id,user))break;
    cursor=next.id;
  }
  store.run('UPDATE topic_cursors SET acked_through=? WHERE workspace=? AND topic=? AND user=?',cursor,message.workspace,message.to_name,user);
  return {changed:!old};
}
