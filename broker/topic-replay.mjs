import { randomUUID } from 'node:crypto';

// One durable marker per scoped subscription identity, updated in the leave TX.
export function invalidateTopic(store,workspace,topic,user) {
  store.run('INSERT INTO broker_meta(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
    'topic_replay:'+JSON.stringify([workspace,topic,user]),randomUUID());
}
export function topicReplayVersion(store,workspace,topic,user) {
  return store.get('SELECT value FROM broker_meta WHERE key=?','topic_replay:'+JSON.stringify([workspace,topic,user]))?.value??'';
}
export function topicReplayVersions(store,workspace,user) {
  return new Map(store.all(`SELECT s.topic,coalesce(m.value,'') AS version FROM topic_subscriptions s
    LEFT JOIN broker_meta m ON m.key='topic_replay:'||json_array(s.workspace,s.topic,s.user)
    WHERE s.workspace=? AND s.user=?`,workspace,user).map(r=>[r.topic,r.version]));
}
