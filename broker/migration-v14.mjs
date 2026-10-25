// Literal v14 DDL, shared with fresh creation; historical targets remain frozen.
export function v13ToV14(db,{now=Date.now}={}) {
 db.exec(`ALTER TABLE agents ADD COLUMN topic TEXT;
 ALTER TABLE agents ADD COLUMN ended_at INTEGER;
 ALTER TABLE agents ADD COLUMN ended_reason TEXT CHECK((ended_at IS NULL AND ended_reason IS NULL) OR (ended_at IS NOT NULL AND ended_reason IS NOT NULL AND ended_reason IN ('topic_left','membership_ended')));
 ALTER TABLE messages ADD COLUMN topic TEXT;
 ALTER TABLE messages ADD COLUMN notice_scope TEXT CHECK(notice_scope IS NULL OR notice_scope IN ('workspace','topic'));
 ALTER TABLE decisions ADD COLUMN topic TEXT;
 ALTER TABLE invites ADD COLUMN topic TEXT;
 CREATE TABLE workstreams_v14 (
   name TEXT NOT NULL, owner_session TEXT NOT NULL REFERENCES agents(session_id),
   owner_user TEXT NOT NULL, lease_until INTEGER NOT NULL, paths TEXT, contracts TEXT,
   workspace TEXT NOT NULL REFERENCES workspaces(name), topic TEXT,
   PRIMARY KEY(workspace,topic,name)
 );
 INSERT INTO workstreams_v14(name,owner_session,owner_user,lease_until,paths,contracts,workspace)
 SELECT name,owner_session,owner_user,lease_until,paths,contracts,workspace FROM workstreams;
 DROP TABLE workstreams;
 ALTER TABLE workstreams_v14 RENAME TO workstreams;
 CREATE UNIQUE INDEX workstreams_scope ON workstreams(workspace,coalesce(topic,''),name);
 CREATE INDEX workstreams_lease ON workstreams(lease_until,name);
 CREATE INDEX workstreams_ws_lease ON workstreams(workspace,lease_until,name);
 CREATE INDEX workstreams_topic_lease ON workstreams(workspace,topic,lease_until,name);
 CREATE INDEX messages_scope ON messages(workspace,topic,id);
 CREATE INDEX decisions_scope ON decisions(workspace,topic,id);
 UPDATE messages SET topic=(SELECT t.topic FROM threads t WHERE t.id=messages.thread_id);`);
 const at=now();let revoked_invites=0;
 for(const row of db.prepare('SELECT * FROM invites WHERE redeemed_at IS NULL AND revoked_at IS NULL AND expires>?').all(at)) {
  let names;try{names=JSON.parse(row.topics);}catch{}
  const topic=Array.isArray(names)&&names.length===1&&typeof names[0]==='string'
   ?db.prepare('SELECT name FROM topics WHERE workspace=? AND name=? AND closed_at IS NULL').get(row.workspace,names[0]):null;
  if(topic)db.prepare('UPDATE invites SET topic=? WHERE id=?').run(topic.name,row.id);
  else {
   db.prepare('UPDATE invites SET revoked_at=? WHERE id=?').run(at,row.id);
   db.prepare("INSERT INTO workspace_audit(workspace,action,detail,created) VALUES (?,'invite:migration_revoke',?,?)").run(row.workspace,JSON.stringify({invite_id:row.id,reason:'topic_binding_upgrade'}),at);
   revoked_invites++;
  }
 }
 return {revoked_invites};
}
