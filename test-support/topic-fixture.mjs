// Explicit setup for pre-M11 ordinary-message tests. This seeds an actual topic;
// requests still pass the production binding/authorization code without a bypass.
export const FIXTURE_TOPIC='fixture';
export function prepareTopicFixture(broker,workspace='default',topic=FIXTURE_TOPIC) {
 if(!broker.store.get('SELECT 1 FROM workspaces WHERE name=?',workspace))return;
 const owner=broker.store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'",workspace)?.user;
 if(!owner)return;
 broker.store.run("INSERT OR IGNORE INTO topics(workspace,name,mode,owner_user,created) VALUES (?,?,'broadcast',?,?)",workspace,topic,owner,broker.now());
 broker.store.run('INSERT OR IGNORE INTO topic_subscriptions(workspace,topic,user,subscribed_at) SELECT workspace,?,user,? FROM workspace_members WHERE workspace=?',topic,broker.now(),workspace);
 broker.fixtureTopic=topic;
}
export function topicHeartbeat(broker,user,session,role='worker',workspace='default') {
 prepareTopicFixture(broker,workspace);
 return broker.heartbeat(user,session,role,workspace,FIXTURE_TOPIC);
}
