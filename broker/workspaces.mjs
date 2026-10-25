import { releaseClaims } from './queue.mjs';
import { randomUUID } from 'node:crypto';
import { check, name } from './config.mjs';
import { DEFAULT_WORKSPACE } from './schema.mjs';
import { invalidateTopic } from './topic-replay.mjs';

export function ensureDefaultWorkspace(store, staticUsers, { defaultOwner, now = Date.now } = {}) {
  return store.transaction(() => {
    const users = [...staticUsers];
    const created = now();
    const existing = store.get('SELECT 1 FROM workspaces WHERE name=?', DEFAULT_WORKSPACE);
    const owner = defaultOwner || users[0];
    if (!existing) {
      name(owner); check(owner !== 'broker', 'RESERVED_USER');
      store.run('INSERT INTO workspaces(name,created,created_by_user) VALUES (?,?,?)', DEFAULT_WORKSPACE, created, owner);
      store.run("INSERT INTO workspace_members VALUES (?,?,'owner',?,'static')", DEFAULT_WORKSPACE, owner, created);
      for (const [action, subject] of [['workspace:create', null], ['member:add', owner]]) {
        store.run('INSERT INTO workspace_audit(workspace,action,actor_user,subject_user,detail,created) VALUES (?,?,?,?,?,?)',
          DEFAULT_WORKSPACE, action, owner, subject, '{"origin":"static"}', created);
      }
    }
    for (const user of users) {
      if (store.get('SELECT 1 FROM workspace_members WHERE workspace=? AND user=?', DEFAULT_WORKSPACE, user) ||
        store.get("SELECT 1 FROM workspace_audit WHERE workspace=? AND subject_user=? AND action IN ('member:remove','member:leave','member:finish')", DEFAULT_WORKSPACE, user)) continue;
      store.run("INSERT INTO workspace_members VALUES (?,?,'member',?,'static')", DEFAULT_WORKSPACE, user, created);
      store.run("INSERT INTO workspace_audit(workspace,action,subject_user,detail,created) VALUES (?,'member:add',?,'{\"origin\":\"static\"}',?)", DEFAULT_WORKSPACE, user, created);
    }
  });
}

// Call inside the lifecycle transaction; publish the returned message only after commit.
export function notice(store, workspace, users, body, now, topic=null) {
  const recipients = [...new Set(users)].filter(user => store.get(
    'SELECT 1 FROM workspace_members WHERE workspace=? AND user=?', workspace, user));
  if (!recipients.length) return null;
  const thread = randomUUID();
  store.run("INSERT INTO threads(id,status,created,last_activity,workspace,topic) VALUES (?,'resolved',?,?,?,?)", thread, now, now, workspace,topic);
  const sent = store.run(`INSERT INTO messages(thread_id,from_user,from_session,to_kind,to_name,kind,blocking,body,created,workspace,topic,notice_scope)
    VALUES (?,'broker','broker','user','broker','notice',0,?,?,?,?,?)`, thread, body, now, workspace,topic,topic===null?'workspace':'topic');
  for (const user of recipients) {
    store.run('INSERT INTO escalation_targets(message_id,user) VALUES (?,?)', sent.lastInsertRowid, user);
    store.run('INSERT INTO participants(thread_id,user) VALUES (?,?)', thread, user);
  }
  return store.get('SELECT * FROM messages WHERE id=?', sent.lastInsertRowid);
}

export function workspaceOwner(store, workspace) {
  return store.get("SELECT user FROM workspace_members WHERE workspace=? AND role='owner'", workspace)?.user;
}

// Called in the lifecycle transaction. Ending never rewrites an earlier cause.
export function endSessions(store,workspace,user,reason,now,topic) {
  return store.run(`UPDATE agents SET ended_at=?,ended_reason=? WHERE workspace=? AND user=? AND ended_at IS NULL ${topic===undefined?'':'AND topic=?'}`,now,reason,workspace,user,...(topic===undefined?[]:[topic]));
}

export function endMembership(store, { workspace, user, reason, actor_user, actor_session, now }) {
  const pending = store.get(`SELECT count(*) n FROM (
    SELECT m.id FROM messages m JOIN delivery_targets d ON d.message_id=m.id
      WHERE m.workspace=? AND d.user=? AND m.acked_at IS NULL
    UNION ALL SELECT m.id FROM messages m JOIN escalation_targets e ON e.message_id=m.id
      WHERE m.workspace=? AND e.user=? AND e.acked_at IS NULL)`, workspace, user, workspace, user).n;
  endSessions(store,workspace,user,'membership_ended',now);
  const queueReleased=releaseClaims(store,workspace,user);
  const released = store.run('DELETE FROM workstreams WHERE workspace=? AND owner_user=?', workspace, user).changes;
  for (const { topic } of store.all('SELECT topic FROM topic_subscriptions WHERE workspace=? AND user=?',workspace,user)) {
    invalidateTopic(store,workspace,topic,user);
    store.run("INSERT INTO workspace_audit(workspace,action,actor_user,actor_session,subject_user,detail,created) VALUES (?,'topic:unsubscribe',?,?,?,?,?)",
      workspace,actor_user,actor_session??null,user,JSON.stringify({topic,reason}),now);
  }
  store.run('DELETE FROM topic_subscriptions WHERE workspace=? AND user=?', workspace, user);
  store.run('DELETE FROM topic_pins WHERE workspace=? AND user=?', workspace, user);
  store.run('DELETE FROM topic_cursors WHERE workspace=? AND user=?', workspace, user);
  if (reason !== 'workspace_deleted') {
    const owner = workspaceOwner(store, workspace);
    for (const topic of store.all('SELECT name FROM topics WHERE workspace=? AND owner_user=?',workspace,user)) {
      store.run('UPDATE topics SET owner_user=? WHERE workspace=? AND name=?',owner,workspace,topic.name);
      store.run("INSERT INTO workspace_audit(workspace,action,actor_user,actor_session,subject_user,detail,created) VALUES (?,'topic:owner_transfer',?,?,?,?,?)",
        workspace,actor_user,actor_session??null,user,JSON.stringify({topic:topic.name,owner}),now);
    }
  }
  store.run('DELETE FROM workspace_members WHERE workspace=? AND user=?', workspace, user);
  store.run('INSERT INTO workspace_audit(workspace,action,actor_user,actor_session,subject_user,detail,created) VALUES (?,?,?,?,?,?,?)',
    workspace, `member:${reason}`, actor_user, actor_session ?? null, user,
    JSON.stringify({ pending_messages: pending, released_workstreams: Number(released) }), now);
  const announcement = reason === 'workspace_deleted' ? null : notice(store, workspace, [workspaceOwner(store, workspace)],
    `${user} left workspace ${workspace} (reason: ${reason}).`, now);
  const sessions = store.all('SELECT session_id FROM agents WHERE workspace=? AND user=?', workspace, user).map(row => row.session_id);
  const deactivated = Boolean(store.get('SELECT 1 FROM broker_users WHERE name=?', user)) &&
    !store.get('SELECT 1 FROM workspace_members WHERE user=? LIMIT 1', user);
  if (deactivated) {
    store.run('UPDATE broker_users SET deactivated_at=coalesce(deactivated_at,?) WHERE name=?', now, user);
    store.run('UPDATE credentials SET deactivated_at=coalesce(deactivated_at,?) WHERE user=?', now, user);
    store.run("INSERT INTO workspace_audit(workspace,action,actor_user,actor_session,subject_user,created) VALUES (?,'user:deactivate',?,?,?,?)",
      workspace, actor_user, actor_session ?? null, user, now);
  }
  return { workspace, user, reason, sessions, announcement, deactivated, queueReleased };
}

// Human CLI only. Archive and membership cleanup share a transaction; history survives.
export function archiveWorkspace(store, workspace, reason, now = Date.now()) {
  name(workspace); check(workspace !== DEFAULT_WORKSPACE, 'WORKSPACE_PROTECTED', 409);
  check(typeof reason === 'string' && reason.trim(), 'REASON_REQUIRED');
  return store.transaction(() => {
    const row = store.get('SELECT * FROM workspaces WHERE name=?', workspace);
    check(row, 'WORKSPACE_NOT_FOUND', 404);
    if (row.deleted_at !== null) return { name: workspace, deleted_at: row.deleted_at, changed: false };
    store.run('UPDATE workspaces SET deleted_at=?,deleted_reason=? WHERE name=?', now, reason.trim(), workspace);
    for (const { user } of store.all('SELECT user FROM workspace_members WHERE workspace=?', workspace)) {
      endMembership(store, { workspace, user, reason: 'workspace_deleted', actor_user: null, now });
    }
    store.run("INSERT INTO workspace_audit(workspace,action,detail,created) VALUES (?,'workspace:delete',?,?)", workspace, JSON.stringify({ reason: reason.trim() }), now);
    return { name: workspace, deleted_at: now, changed: true };
  });
}
