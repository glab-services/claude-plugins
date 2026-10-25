import { randomUUID } from 'node:crypto';
import { getTopic } from './topics.mjs';
import { subscribed } from './topic-delivery.mjs';
import { notice, workspaceOwner } from './workspaces.mjs';
import { check } from './config.mjs';
import { envelope } from './broker.mjs';

function fields(args,allowed) {check(Object.keys(args).every(key=>allowed.includes(key)),'INVALID_QUEUE_FIELDS');}
function item(broker,user,session,id,{includePurged=false}={}) {
  check(Number.isSafeInteger(id)&&id>0,'INVALID_MESSAGE_ID');
  const scope=broker.scope(user,session),message=broker.store.get('SELECT * FROM messages WHERE id=?',id);
  // Read-only queue inspection may render a tombstone; mutations still require visibility.
  const visible=message && (includePurged
    ? message.to_kind==='topic' && message.workspace===scope.workspace && (scope.topic===null || (message.to_name===scope.topic && subscribed(broker.store,scope.workspace,message.to_name,user)))
    : broker.visibleTopic(message,user,session)&&subscribed(broker.store,scope.workspace,message.to_name,user));
  check(visible,'MESSAGE_NOT_FOUND',404);
  // Defense in depth: visibleTopic already requires the message's bound workspace;
  // retain the queue-row scope check rather than relying on it alone.
  const row=broker.store.get('SELECT * FROM queue_items WHERE message_id=? AND workspace=?',id,scope.workspace);
  check(row,'MESSAGE_NOT_FOUND',404);return {scope,message,row};
}
function generation(value) {check(Number.isSafeInteger(value)&&value>0,'INVALID_GENERATION');}
function holder(row,user,session,value) {return row.holder_user===user && row.holder_session===session && row.generation===value;}
function live(row,user,session,value,now) {
  check(row.state==='claimed' && holder(row,user,session,value) && row.lease_until>now,'CLAIM_STALE',409);
}
export function claim(broker,user,session,args) {
  fields(args,['id']);let message,expired;
  const result=broker.store.transaction(()=>{
    const found=item(broker,user,session,args.id);message=found.message;let {row}=found;const now=broker.now();
    if(row.state==='claimed' && row.lease_until<=now) {
      expired=expireOne(broker,row,now);
      if(expired.dead)return null;
      row=broker.store.get('SELECT * FROM queue_items WHERE message_id=?',args.id);
    }
    check(!['done','dead'].includes(row.state),'ITEM_DONE',409);
    check(row.state==='ready' || (row.state==='claimed' && row.lease_until<=now),'ITEM_CLAIMED',409);
    // BEGIN IMMEDIATE serializes this read/transition across broker processes.
    const next=row.generation+1,lease=now+broker.config.claimTtl;
    broker.store.run("UPDATE queue_items SET state='claimed',generation=?,attempts=attempts+1,holder_user=?,holder_session=?,lease_until=? WHERE message_id=?",next,user,session,lease,args.id);
    return {id:args.id,generation:next,lease_until:lease};
  });
  // The exhausted transition must commit before ITEM_DONE is raised.
  if(expired?.dead)publishDead(broker,expired);
  check(result,'ITEM_DONE',409);
  for(const [other,stream] of broker.streams)if(other!==session && stream.workspace===message.workspace && broker.visibleTopic(message,stream.user,other) && subscribed(broker.store,message.workspace,message.to_name,stream.user))
    stream.emit('claimed',{id:args.id,thread_id:message.thread_id,claimed_by_session:session,claim_generation:result.generation});
  return result;
}
export function renew(broker,user,session,args) {
  fields(args,['id','generation']);generation(args.generation);
  return broker.store.transaction(()=>{
    const {row}=item(broker,user,session,args.id),now=broker.now();live(row,user,session,args.generation,now);
    const lease=now+broker.config.claimTtl;broker.store.run('UPDATE queue_items SET lease_until=? WHERE message_id=?',lease,args.id);
    return {id:args.id,generation:args.generation,lease_until:lease};
  });
}
export function done(broker,user,session,args) {
  fields(args,['id','generation','result']);generation(args.generation);
  return broker.store.transaction(()=>{
    const {row}=item(broker,user,session,args.id),now=broker.now();
    if(row.state==='done' && holder(row,user,session,args.generation) && row.done_generation===args.generation) {
      check(row.result===null?args.result===undefined:args.result===row.result,'DONE_RESULT_MISMATCH',409);
      return {id:args.id,generation:args.generation,changed:false,result:row.result};
    }
    live(row,user,session,args.generation,now);
    check(args.result===undefined || (typeof args.result==='string' && args.result.length<=broker.config.maxMessageChars),'INVALID_RESULT');
    broker.store.run("UPDATE queue_items SET state='done',done_at=?,done_generation=?,result=? WHERE message_id=?",now,args.generation,args.result??null,args.id);
    return {id:args.id,generation:args.generation,changed:true,result:args.result??null};
  });
}
export function queueItem(broker,user,session,args) {
  fields(args,['id']);const {row,message}=item(broker,user,session,args.id,{includePurged:true});
  return {...row,id:args.id,message:envelope({...message,claim_generation:row.generation})};
}
// These writes run inside the caller's leave/membership transaction.
export function releaseClaims(store,workspace,user,topic) {
  const rows=store.all(`SELECT message_id FROM queue_items WHERE workspace=? AND holder_user=? AND state='claimed' ${topic===undefined?'':'AND topic=?'}`,workspace,user,...(topic===undefined?[]:[topic]));
  for(const {message_id:id} of rows)store.run("UPDATE queue_items SET state='ready',generation=generation+1,holder_user=NULL,holder_session=NULL,lease_until=NULL WHERE message_id=?",id);
  return rows.map(r=>r.message_id);
}
export function reoffer(broker,ids=[]) {
  for(const id of ids) {
    for(const stream of broker.streams.values())stream.forget?.(id);
    broker.publish(broker.store.get('SELECT * FROM messages WHERE id=?',id));
  }
}


function audit(broker,scope,action,detail,now) {
  broker.store.run('INSERT INTO workspace_audit(workspace,action,actor_user,actor_session,detail,created) VALUES (?,?,?,?,?,?)',scope.workspace,action,scope.user,scope.session??null,JSON.stringify(detail),now);
}
// Call only for a claimed, expired row inside the caller's transaction.
function expireOne(broker,row,now) {
  const {store}=broker,id=row.message_id;
  if(row.attempts<broker.config.maxAttempts) {
    store.run("UPDATE queue_items SET state='ready',generation=generation+1,holder_user=NULL,holder_session=NULL,lease_until=NULL WHERE message_id=?",id);
    return {id,dead:false};
  }
  store.run("UPDATE queue_items SET state='dead' WHERE message_id=?",id);
  const topic=getTopic(store,row.workspace,row.topic,{open:false});
  const announcement=notice(store,row.workspace,[topic.owner_user,workspaceOwner(store,row.workspace)],`Queue item ${id} in ${row.topic} exhausted ${row.attempts} attempts.`,now,row.topic);
  audit(broker,{workspace:row.workspace,user:'broker'},'queue:dead_letter',{topic:row.topic,message_id:id,attempts:row.attempts},now);
  if(broker.config.webhookUrl && announcement) {
    const payload={event:'queue_dead_letter',event_id:randomUUID(),workspace:row.workspace,topic:row.topic,queue_message_id:id,attempts:row.attempts,message_id:announcement.id};
    store.run('INSERT INTO webhook_outbox(message_id,payload,next_attempt) VALUES (?,?,?)',announcement.id,JSON.stringify(payload),now);
  }
  return {id,dead:true,attempts:row.attempts,announcement};
}
function publishDead(broker,outcome) {
  if(outcome.announcement)broker.publish(outcome.announcement);
  broker.log(JSON.stringify({event:'queue_dead_letter',message_id:outcome.id,attempts:outcome.attempts}));
}
export function expireClaims(broker,now=broker.now()) {
  const outcomes=broker.store.transaction(()=>broker.store.all("SELECT * FROM queue_items WHERE state='claimed' AND lease_until<=? ORDER BY lease_until,message_id LIMIT ?",now,broker.config.queueExpiryBatch).map(row=>expireOne(broker,row,now)));
  reoffer(broker,outcomes.filter(o=>!o.dead).map(o=>o.id));
  for(const outcome of outcomes)if(outcome.dead)publishDead(broker,outcome);
  return {ready:outcomes.filter(o=>!o.dead).length,dead:outcomes.filter(o=>o.dead).length};
}
function ownedTopic(broker,user,session,topic) {
  const scope=broker.scope(user,session);broker.assertTopicScope(scope,topic);const row=getTopic(broker.store,scope.workspace,topic,{open:false});
  check(row.mode==='queue','NOT_QUEUE_TOPIC',409);
  check(row.owner_user===user || workspaceOwner(broker.store,scope.workspace)===user,'NOT_TOPIC_OWNER',403);
  return scope;
}
export function deadLetters(broker,user,session,args) {
  fields(args,['topic']);
  return broker.store.transaction(()=>{
    const scope=ownedTopic(broker,user,session,args.topic);
    const rows=broker.store.all("SELECT * FROM queue_items WHERE workspace=? AND topic=? AND state='dead' ORDER BY message_id",scope.workspace,args.topic);
    return {items:rows.map(row=>({...row,id:row.message_id,message:envelope({...broker.store.get('SELECT * FROM messages WHERE id=?',row.message_id),claim_generation:row.generation})}))};
  });
}
export function requeue(broker,user,session,args) {
  fields(args,['id']);check(Number.isSafeInteger(args.id)&&args.id>0,'INVALID_MESSAGE_ID');let expired;
  const result=broker.store.transaction(()=>{
    const scope=broker.scope(user,session);let row=broker.store.get('SELECT * FROM queue_items WHERE message_id=? AND workspace=? AND (? IS NULL OR topic=? OR ?)',args.id,scope.workspace,scope.topic,scope.topic,Number(workspaceOwner(broker.store,scope.workspace)===user));
    if(row)broker.assertTopicScope(scope,row.topic);
    check(row,'MESSAGE_NOT_FOUND',404);ownedTopic(broker,user,session,row.topic);
    // An owner need not wait for the bounded sweep after the final lease ends.
    // Both the exhausted attempt and explicit reset commit in this transaction.
    if(row.state==='claimed'&&row.lease_until<=broker.now()&&row.attempts>=broker.config.maxAttempts) {
      expired=expireOne(broker,row,broker.now());row=broker.store.get('SELECT * FROM queue_items WHERE message_id=?',args.id);
    }
    check(row.state==='dead','ITEM_NOT_DEAD',409);
    const next=row.generation+1;
    broker.store.run("UPDATE queue_items SET state='ready',attempts=0,generation=?,holder_user=NULL,holder_session=NULL,lease_until=NULL,done_at=NULL,done_generation=NULL,result=NULL WHERE message_id=?",next,args.id);
    audit(broker,scope,'queue:requeue',{topic:row.topic,message_id:args.id,from_generation:row.generation,generation:next,previous_attempts:row.attempts},broker.now());
    return {id:args.id,state:'ready',attempts:0,generation:next};
  });
  reoffer(broker,[args.id]);if(expired)publishDead(broker,expired);return result;
}
