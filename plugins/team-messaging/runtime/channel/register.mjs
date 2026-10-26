import { readClaudeRegistration } from './client-config.mjs';
import { createHash, randomBytes } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { accessSync, constants } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const hash = value => createHash('sha256').update(value).digest('hex');
export function serverName(url, workspace,topic,client='') {
  const safe=value=>value.replace(/[^A-Za-z0-9_-]/g,'_').slice(0,8);
  // The marker separates topic registrations from legacy workspace-only names;
  // a structured tuple digest distinguishes truncated/sanitized labels.
  return `team-messaging-t-${hash(url).slice(0,8)}-${safe(workspace)}-${safe(topic??'workspace')}-${hash(JSON.stringify([client,url,workspace,topic??null])).slice(0,16)}`;
}
// A fresh pinned session ID per registration: a binding digest prefix plus random bytes.
export function pinnedSession(client,url,workspace,topic) {
  return `${client}-${hash(JSON.stringify([client,url,workspace,topic??null])).slice(0,12)}-${randomBytes(16).toString('hex')}`;
}
export function available(client, env = process.env) {
  return (env.PATH || '').split(':').some(dir => {
    try { accessSync(join(dir, client), constants.X_OK); return true; } catch { return false; }
  });
}
// Only 'claude' reaches register(); claude-plugin binds through bindProjectFile.
// client stays in serverName/pinnedSession because it is hashed into existing
// registration names and pinned session IDs.
export async function register(client, { url, workspace,topic, path }, { spawn = spawnSync, env = process.env,probe,cwd=process.cwd() } = {}) {
  const { BROKER_TOKEN: _token, TEAM_BROKER_TOKEN: _legacyToken, ...childEnv } = env;
  const run = args => {
    try { return spawn(client, args, { env: childEnv, encoding: 'utf8', timeout: 10000, stdio: 'pipe' }); }
    catch { return { status: null }; }
  };
  const name=serverName(url,workspace,topic,client);
  const command=[process.execPath,fileURLToPath(new URL('./main.mjs',import.meta.url))];
  if(spawn===spawnSync&&!available(client,env))return {client,server_name:name,status:'manual'};
  const lookup=()=>readClaudeRegistration(name,{env,cwd,command});
  const found=lookup();
  const result=(status,extra={})=>({client,server_name:name,status,...extra});
  const conflict=hint=>result('conflict',{hint:hint??'The existing registration has another credential, binding or owner, or could not be verified. It was not modified.'});
  let removed=false;
  if(found.conflict)return conflict();
  if(found.status===0){
    let configured=found.configured,owned=found.owned;
    if(!configured||configured.BROKER_TOKEN_FILE!==path||configured.BROKER_URL!==url||configured.CHANNEL_WORKSPACE!==workspace||(configured.CHANNEL_TOPIC??null)!==(topic??null)||typeof configured.CHANNEL_SESSION_ID!=='string'||!/^[A-Za-z0-9_.-]{1,128}$/.test(configured.CHANNEL_SESSION_ID)||configured.CHANNEL_SESSION_ID==='broker')return conflict();
    let status;try{status=await probe?.(configured.CHANNEL_SESSION_ID);}catch{}
    if(['active','unregistered'].includes(status?.state))return result('skipped',{hint:'already registered'});
    if(status?.error!=='SESSION_ENDED')return conflict();
    // Replacement is limited to the canonical tool-owned transport. Human
    // wrappers/custom entries require an explicit manual update instead.
    const known=new Set(['BROKER_URL','BROKER_TOKEN_FILE','CHANNEL_WORKSPACE','CHANNEL_TOPIC','CHANNEL_SESSION_ID']);
    if(!owned||Object.keys(configured).some(key=>!known.has(key)))return conflict('The ended registration has custom settings; update its pinned session manually or remove it before joining again.');
    const latest=lookup();
    if(latest.conflict||latest.status!==0||!latest.owned||JSON.stringify(latest.configured)!==JSON.stringify(configured))return conflict();
    if(run(['mcp','remove','--scope','user',name]).status!==0)return result('failed',{hint:'Could not remove the ended registration; it was not replaced.'});
    removed=true;
  }else{
    if(found.error?.code==='ENOENT'||found.status===null)return result('manual');
    if(found.status!==1)return result('failed');
  }
  const session=pinnedSession(client,url,workspace,topic);
  const values={BROKER_URL:url,BROKER_TOKEN_FILE:path,CHANNEL_WORKSPACE:workspace,...(topic==null?{}:{CHANNEL_TOPIC:topic}),CHANNEL_SESSION_ID:session};
  const args=['mcp','add','--scope','user',...Object.entries(values).flatMap(([k,v])=>['-e',`${k}=${v}`]),'--transport','stdio',name,'--',...command];
  const added=run(args);
  if(removed&&added.status!==0){
    // Only generated binding env fields go into this command; token bytes and
    // arbitrary client output are never included. Quote every shell argument.
    const quote=value=>"'"+value.replaceAll("'","'\"'\"'")+"'";
    return result(added.error?.code==='ENOENT'||added.status===null?'manual':'failed',{hint:'The ended registration was removed, but adding its replacement failed. Re-add manually: '+[client,...args].map(quote).join(' ')});
  }
  return result(added.status===0?'registered':added.error?.code==='ENOENT'||added.status===null?'manual':'failed',added.status===0?{hook_env:{BROKER_URL:url,CHANNEL_SESSION_ID:session,CHANNEL_WORKSPACE:workspace,...(topic==null?{}:{CHANNEL_TOPIC:topic}),BROKER_TOKEN_FILE:path}}:{});
}
