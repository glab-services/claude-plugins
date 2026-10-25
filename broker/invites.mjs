import { check, name, BrokerError } from './config.mjs';
import { inviteTopics } from './topics.mjs';
import { mint, parse, verify, activeCredential } from './credentials.mjs';
import { endMembership, notice, workspaceOwner } from './workspaces.mjs';

const fields = (args, allowed) => check(Object.keys(args).every(key => allowed.includes(key)), 'INVALID_INVITE_FIELDS');
function audit(broker, workspace, action, actor, subject, detail = {}) {
  broker.store.run('INSERT INTO workspace_audit(workspace,action,actor_user,actor_session,subject_user,detail,created) VALUES (?,?,?,?,?,?,?)',
    workspace, action, actor.user, actor.session ?? null, subject ?? null, JSON.stringify(detail), broker.now());
}
function ownerScope(broker, user, session) {
  const scope = broker.scope(user, session); broker.requireOwner(user, scope.workspace); return scope;
}
export function invite(broker, user, session, args) {
  fields(args, ['topics']);
  return broker.store.transaction(() => {
    const scope = ownerScope(broker,user,session), {workspace}=scope, now = broker.now(), store = broker.store;
    check(store.get('SELECT count(*) n FROM invites WHERE workspace=? AND expires>? AND redeemed_at IS NULL AND revoked_at IS NULL', workspace, now).n < broker.config.inviteLimit, 'INVITE_LIMIT', 409);
    const topicNames = inviteTopics(store,workspace,args.topics);broker.assertTopicScope(scope,topicNames[0]);
    const code = mint('tmi'), expires = now + broker.config.inviteTtl;
    store.run('INSERT INTO invites(id,workspace,secret_sha256,created_by_user,created_by_session,created,expires,topics,topic) VALUES (?,?,?,?,?,?,?,?,?)', code.id, workspace, code.sha256, user, session, now, expires, JSON.stringify(topicNames),topicNames[0]);
    audit(broker, workspace, 'invite:create', { user, session }, null, { invite_id: code.id });
    return { invite_id: code.id, code: code.value, expires };
  });
}
export function invites(broker, user, session) {
  return broker.store.transaction(() => {
    const { workspace,topic } = ownerScope(broker, user, session);
    audit(broker, workspace, 'invite:list', { user, session });
    return { invites: broker.store.all(`SELECT id AS invite_id,expires,CASE WHEN revoked_at IS NOT NULL THEN 'revoked'
      WHEN redeemed_at IS NOT NULL THEN 'redeemed' WHEN expires<=? THEN 'expired' ELSE 'pending' END AS status
      FROM invites WHERE workspace=? AND (? IS NULL OR topic=?) ORDER BY created,id`, broker.now(), workspace,topic,topic) };
  });
}
export function revokeInvite(broker, user, session, args) {
  fields(args, ['invite_id']); check(typeof args.invite_id === 'string' && /^[0-9a-f]{32}$/.test(args.invite_id), 'INVITE_INVALID');
  return broker.store.transaction(() => {
    const scope=ownerScope(broker,user,session), {workspace}=scope, store = broker.store;
    const row = store.get('SELECT * FROM invites WHERE workspace=? AND id=?', workspace, args.invite_id);
    check(row, 'INVITE_INVALID', 404);broker.assertTopicScope(scope,row.topic); check(row.redeemed_at === null, 'INVITE_USED', 409);
    store.run('UPDATE invites SET revoked_at=coalesce(revoked_at,?),revoked_by_user=coalesce(revoked_by_user,?) WHERE id=?', broker.now(), user, row.id);
    audit(broker, workspace, 'invite:revoke', { user, session }, null, { invite_id: row.id });
    return { invite_id: row.id, revoked: true };
  });
}
function reserved(store, staticUsers, user) {
  if (staticUsers.has(user)) return true;
  // Keep every identity reserved even after removal/deactivation and history retention.
  return [['broker_users','name'], ['workspace_audit','subject_user'], ['workspace_audit','actor_user'], ['workspace_members','user'], ['agents','user'], ['delivery_targets','user'],
    ['escalation_targets','user'], ['participants','user'], ['messages','from_user'], ['decisions','from_user']]
    .some(([table, column]) => store.get(`SELECT 1 FROM ${table} WHERE ${column}=? LIMIT 1`, user));
}
export function join(broker, auth, args) {
  fields(args, ['code', 'name']);
  let announcement;
  const result = broker.store.transaction(() => {
    const store = broker.store, now = broker.now();
    if (auth) {
      check(!auth.credential_id || activeCredential(store, auth.credential_id, auth.user), 'UNAUTHORIZED', 401);
      check(args.name === undefined || args.name === auth.user, 'JOIN_NAME_MISMATCH', 409);
    }
    const parsed = parse('tmi', args.code);
    const row = parsed && store.get('SELECT * FROM invites WHERE id=?', parsed.id);
    check(parsed && verify(row, parsed.secret), 'INVITE_INVALID', 403);
    check(row.revoked_at === null, 'INVITE_REVOKED', 409);
    check(row.expires > now, 'INVITE_EXPIRED', 409); check(row.redeemed_at === null, 'INVITE_USED', 409);
    check(store.get('SELECT 1 FROM workspaces WHERE name=? AND deleted_at IS NULL', row.workspace), 'INVITE_INVALID', 403);
    const topicNames = inviteTopics(store,row.workspace,[row.topic]);
    let user = auth?.user, token;
    if (auth) {
      if(store.get('SELECT 1 FROM topic_subscriptions WHERE workspace=? AND topic=? AND user=?',row.workspace,row.topic,user)){const error=new BrokerError('ALREADY_MEMBER',409);error.binding={user,workspace:row.workspace,topic:row.topic};throw error;}
    } else {
      user = name(args.name); check(user !== 'broker', 'RESERVED_USER');
      check(!reserved(store, broker.users, user), 'USER_EXISTS', 409);
      token = mint('tmc');
      store.run("INSERT INTO broker_users(name,origin,created) VALUES (?,'self',?)", user, now);
      store.run('INSERT INTO credentials(id,user,secret_sha256,created,invite_id) VALUES (?,?,?,?,?)', token.id, user, token.sha256, now, row.id);
    }
    const added=store.run("INSERT OR IGNORE INTO workspace_members VALUES (?,?,'member',?,'invite')", row.workspace, user, now).changes;
    for (const topic of topicNames) {
      store.run('INSERT INTO topic_subscriptions VALUES (?,?,?,?)',row.workspace,topic,user,now);
      store.run('INSERT INTO topic_cursors(workspace,topic,user,acked_through) VALUES (?,?,?,0)',row.workspace,topic,user);
    }
    // BEGIN IMMEDIATE plus the guarded update enforces one winner across broker processes.
    check(store.run('UPDATE invites SET redeemed_at=?,redeemed_by_user=? WHERE id=? AND redeemed_at IS NULL AND revoked_at IS NULL', now, user, row.id).changes === 1, 'INVITE_USED', 409);
    const actor = { user };
    audit(broker, row.workspace, 'invite:redeem', actor, user, { invite_id: row.id });
    if(added)audit(broker, row.workspace, 'member:add', actor, user, { origin: 'invite' });
    announcement = notice(store, row.workspace, [workspaceOwner(store, row.workspace)], added?`${user} joined workspace ${row.workspace}.`:`${user} joined topic ${row.topic} in workspace ${row.workspace}.`, now,added?null:row.topic);
    return { user, workspace: row.workspace, topic:row.topic, ...(token ? { token: token.value } : {}) };
  });
  if (announcement) broker.publish(announcement);
  return result;
}
// Decision 67: recovery cannot strand any active workspace owned by this user.
export function revokeCredential(broker, auth, args) {
  fields(args, []); check(auth.credential_id, 'NOT_MINTED_CREDENTIAL', 409);
  const changes = [];
  const result = broker.store.transaction(() => {
    const store = broker.store, now = broker.now();
    check(activeCredential(store, auth.credential_id, auth.user), 'UNAUTHORIZED', 401);
    check(!store.get(`SELECT 1 FROM workspace_members m JOIN workspaces w ON w.name=m.workspace
      WHERE m.user=? AND m.role='owner' AND w.deleted_at IS NULL LIMIT 1`, auth.user), 'OWNER_MUST_TRANSFER', 409);
    const credential = store.get('SELECT * FROM credentials WHERE id=?', auth.credential_id);
    const redemption = credential.invite_id && store.get('SELECT workspace FROM invites WHERE id=? AND redeemed_by_user=?', credential.invite_id, auth.user);
    if (redemption && broker.member(auth.user, redemption.workspace)) {
      changes.push(endMembership(store, { workspace: redemption.workspace, user: auth.user, reason: 'redeem_failed', actor_user: auth.user, now }));
    }
    store.run('UPDATE credentials SET deactivated_at=coalesce(deactivated_at,?) WHERE id=?', now, auth.credential_id);
    const affected = new Set([...store.all('SELECT workspace FROM workspace_members WHERE user=?', auth.user).map(r => r.workspace), ...(redemption ? [redemption.workspace] : [])]);
    for (const workspace of affected) audit(broker, workspace, 'credential:revoke', auth, auth.user, { credential_id: auth.credential_id });
    return { revoked: true };
  });
  for (const change of changes) broker.removed(change);
  for (const stream of broker.streams.values()) if (stream.credential_id === auth.credential_id) stream.end();
  return result;
}
