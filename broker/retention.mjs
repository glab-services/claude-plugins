import { check } from './config.mjs';
import { createHash } from 'node:crypto';
import { notice } from './workspaces.mjs';
import { setImmediate as yieldImmediate } from 'node:timers/promises';

// One SELECT gives a consistent read snapshot. Never read bodies or write state:
// M9b will re-evaluate this predicate inside each compaction transaction.
export function candidates(store, config, now, {limit=500, workspace, ids}={}) {
  check(Number.isSafeInteger(limit) && limit>0,'INVALID_RETENTION_LIMIT');
  if(ids!==undefined) {
    check(Array.isArray(ids) && ids.every(id=>Number.isSafeInteger(id)&&id>0),'INVALID_MESSAGE_ID');
    if(!ids.length)return {items:[],count:0,topics:[]};
  }
  const saved=store.get("SELECT value FROM broker_meta WHERE key='retention_epoch'");
  const epoch=Number(saved?.value);
  // Fail closed if the safety epoch is absent/corrupt; never silently use zero.
  check(saved && /^\d+$/.test(saved.value) && Number.isSafeInteger(epoch) && epoch>=0,'INVALID_RETENTION_EPOCH',500);
  const rows=store.all(`SELECT m.id,m.workspace,m.to_name AS topic,t.mode,
    q.state AS queue_state,
    (th.status<>'resolved' AND (th.status='escalated' OR EXISTS (
      SELECT 1 FROM messages ask INDEXED BY messages_thread_id WHERE ask.thread_id=th.id AND ask.workspace=m.workspace AND ask.blocking=1 AND ask.id>th.cycle_start_id
        AND NOT EXISTS(SELECT 1 FROM messages answer INDEXED BY messages_thread_id WHERE answer.thread_id=th.id AND answer.workspace=m.workspace
          AND answer.kind='answer' AND answer.id>ask.id AND answer.from_user<>ask.from_user)
    ))) AS unfinished,
    EXISTS(SELECT 1 FROM topic_pins pin WHERE pin.message_id=m.id) AS pinned,
    EXISTS(SELECT 1 FROM webhook_outbox hook WHERE hook.message_id=m.id AND hook.sent_at IS NULL) AS webhook,
    (SELECT json_group_array(s.user) FROM topic_subscriptions s
      JOIN workspace_members member ON member.workspace=s.workspace AND member.user=s.user
      LEFT JOIN topic_cursors c ON c.workspace=s.workspace AND c.topic=s.topic AND c.user=s.user
      WHERE s.workspace=m.workspace AND s.topic=m.to_name AND coalesce(c.acked_through,0)<m.id
        AND NOT EXISTS(SELECT 1 FROM topic_acks ack WHERE ack.message_id=m.id AND ack.user=s.user)
    ) AS users
    FROM messages m JOIN topics t ON t.workspace=m.workspace AND t.name=m.to_name
    JOIN workspaces w ON w.name=m.workspace JOIN threads th ON th.id=m.thread_id AND th.workspace=m.workspace
    LEFT JOIN queue_items q ON q.message_id=m.id AND q.workspace=m.workspace AND q.topic=m.to_name
    WHERE m.to_kind='topic' AND m.purged_at IS NULL AND m.kind NOT IN ('decision','notice','escalation')
      AND (? IS NULL OR m.workspace=?) ${ids===undefined?'':`AND m.id IN (${ids.map(()=>'?').join(',')})`}
      AND CASE WHEN w.deleted_at IS NOT NULL OR t.closed_at IS NOT NULL
        THEN max(coalesce(w.deleted_at,t.closed_at),?)<=?
        ELSE max(m.created,?)<=? END
    ORDER BY m.id`,workspace??null,workspace??null,...(ids??[]),epoch,now-config.closedRetention,epoch,now-config.topicRetention);
  const items=[],groups=new Map();
  for(const row of rows) {
    const key=JSON.stringify([row.workspace,row.topic]);
    if(!groups.has(key))groups.set(key,{workspace:row.workspace,topic:row.topic,count:0,min_id:null,max_id:null,
      blockers:{users:[],pinned_ids:[],unfinished_ids:[],dead_ids:[],webhook_ids:[]}});
    const group=groups.get(key),b=group.blockers,users=row.mode==='broadcast'?JSON.parse(row.users):[];
    b.users.push(...users);
    if(row.pinned)b.pinned_ids.push(row.id);
    if(row.webhook)b.webhook_ids.push(row.id);
    const dead=row.mode==='queue' && row.queue_state==='dead';
    const unfinished=Boolean(row.unfinished) || (row.mode==='queue' && row.queue_state!=='done' && !dead);
    if(dead)b.dead_ids.push(row.id);
    if(unfinished)b.unfinished_ids.push(row.id);
    if(users.length || row.pinned || row.webhook || dead || unfinished)continue;
    group.count++;group.min_id??=row.id;group.max_id=row.id;
    if(items.length<limit)items.push({id:row.id,workspace:row.workspace,topic:row.topic});
  }
  const topics=[...groups.values()];
  for(const {blockers:b} of topics) {
    b.users=[...new Set(b.users)].sort();
    for(const key of ['pinned','unfinished','dead','webhook'])b[key+'_count']=b[key+'_ids'].length;
    // Categories are coarse for growing unfinished/dead/webhook backlogs;
    // cursor users and ALL pinned IDs remain exact before presentation caps.
    const categories=[...(b.users.length?['cursor']:[]),...['pinned','unfinished','dead','webhook'].filter(key=>b[key+'_count'])];
    b.digest=createHash('sha256').update(JSON.stringify({categories,users:b.users,pinned_ids:b.pinned_ids})).digest('hex');
    b.truncated=false;
    for(const key of ['pinned','unfinished','dead','webhook']) {
      if(b[key+'_ids'].length>config.retentionBlockerLimit)b.truncated=true;
      b[key+'_ids']=b[key+'_ids'].slice(0,config.retentionBlockerLimit);
    }
  }
  return {items,count:topics.reduce((n,t)=>n+t.count,0),topics};
}

const blockerPrefix='retention_blockers:';
// Called inside a batch transaction. Message and digest commit together.
function blockerNotices(store,topics,now) {
  const keep=new Set(),messages=[];
  for(const {workspace,topic,blockers:b} of topics) {
    if(!b.users.length && !['pinned','unfinished','dead','webhook'].some(k=>b[k+'_count']))continue;
    const key=blockerPrefix+workspace+'/'+topic;keep.add(key);
    if(store.get('SELECT value FROM broker_meta WHERE key=?',key)?.value===b.digest)continue;
    const owner=store.get('SELECT owner_user FROM topics WHERE workspace=? AND name=?',workspace,topic);
    const detail={cursor_users:b.users.length,...Object.fromEntries(['pinned','unfinished','dead','webhook'].map(k=>[k,b[k+'_count']])),
      ...Object.fromEntries(['pinned','unfinished','dead','webhook'].map(k=>[k+'_ids',b[k+'_ids']])),truncated:b.truncated};
    const message=notice(store,workspace,[owner.owner_user],`Retention blocked in ${topic}: ${JSON.stringify(detail)}`,now,topic);
    store.run('INSERT INTO broker_meta(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',key,b.digest);
    if(message)messages.push(message);
  }
  for(const {key} of store.all("SELECT key FROM broker_meta WHERE key GLOB 'retention_blockers:*'"))
    if(!keep.has(key))store.run('DELETE FROM broker_meta WHERE key=?',key);
  return messages;
}

// Each selection and guarded update share BEGIN IMMEDIATE. Other brokers cannot
// pin, change a claim or ack state between eligibility evaluation and the write.
export async function compact(store,config,now=Date.now,{publish=()=>{},yieldBatch=yieldImmediate,log=event=>console.error(JSON.stringify(event))}={}) {
  const clock=typeof now==='function'?now:()=>now;
  const saved={topicRetention:config.topicRetention,closedRetention:config.closedRetention,
    batch:config.retentionBatch,maxBatches:config.retentionMaxBatches};
  const id=store.transaction(()=>Number(store.run('INSERT INTO retention_runs(started,config) VALUES (?,?)',clock(),JSON.stringify(saved)).lastInsertRowid));
  const cursorKey='retention_cursor',stored=store.get('SELECT value FROM broker_meta WHERE key=?',cursorKey);
  const valid=stored && /^\d+$/.test(stored.value) && Number.isSafeInteger(Number(stored.value));
  if(!valid)log({event:'retention_cursor_reset',reason:stored?'invalid':'missing'});
  const start=valid?Number(stored.value):0;let cursor=start,wrapped=false;
  for(let batch=0;batch<config.retentionMaxBatches;batch++) {
    // Advisory page only; pins, age and all other predicates are rechecked below.
    const page=store.all(`SELECT m.id FROM messages m WHERE m.to_kind='topic' AND m.purged_at IS NULL
      AND m.id>? ${wrapped?'AND m.id<=?':''} ORDER BY m.id LIMIT ?`,cursor,...(wrapped?[start]:[]),config.retentionBatch);
    const short=page.length<config.retentionBatch,last=page.at(-1)?.id??cursor;
    const finished=wrapped && (short || last>=start);
    const next=short||finished?0:last;
    store.transaction(()=>{
      const time=clock(),selection=candidates(store,config,time,{limit:config.retentionBatch,ids:page.map(r=>r.id)});
      const ids=selection.items.map(r=>r.id);
      const changed=ids.length?store.all(`UPDATE messages SET body='',purged_at=?,purge_run_id=?
        WHERE id IN (${ids.map(()=>'?').join(',')}) AND purged_at IS NULL RETURNING id,workspace,to_name`,time,id,...ids):[];
      const groups=new Map();
      for(const row of changed) {
        const key=JSON.stringify([row.workspace,row.to_name]);
        if(!groups.has(key))groups.set(key,{workspace:row.workspace,topic:row.to_name,ids:[]});
        groups.get(key).ids.push(row.id);
      }
      for(const {workspace,topic,ids:group} of groups.values())store.run(`INSERT INTO workspace_audit(workspace,action,actor_user,detail,created)
        VALUES (?,'retention:purge','broker',?,?)`,workspace,JSON.stringify({topic,run_id:id,count:group.length,min_id:Math.min(...group),max_id:Math.max(...group)}),time);
      store.run('UPDATE retention_runs SET purged=purged+? WHERE id=?',changed.length,id);
      store.run('INSERT INTO broker_meta(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',cursorKey,String(next));
    });
    await yieldBatch();
    if(finished || (short&&start===0))break;
    if(short)wrapped=true;
    cursor=next;
  }
  // One full blocker scan per run, separate from the id-bounded purge locks.
  const notices=store.transaction(()=>{const time=clock();return blockerNotices(store,candidates(store,config,time).topics,time);});
  for(const message of notices)publish(message);
  store.transaction(()=>store.run('UPDATE retention_runs SET finished=? WHERE id=?',clock(),id));
  const {config:_config,...result}=store.get('SELECT * FROM retention_runs WHERE id=?',id);
  return result;
}
