import {readFileSync,existsSync} from 'node:fs';
import {join,dirname,resolve} from 'node:path';
import {homedir} from 'node:os';
const fields=['BROKER_URL','BROKER_TOKEN_FILE','CHANNEL_WORKSPACE','CHANNEL_TOPIC','CHANNEL_SESSION_ID'];
const has=(value,name)=>value&&typeof value==='object'&&Object.hasOwn(value,name);
function projectShadow(name,cwd){
 for(let dir=resolve(cwd);;dir=dirname(dir)){
  try{const data=JSON.parse(readFileSync(join(dir,'.mcp.json'),'utf8'));if(has(data.mcpServers,name))return true;}
  catch(error){if(error.code!=='ENOENT')return true;}
  if(existsSync(join(dir,'.git'))||dirname(dir)===dir)return false;
 }
}
export function readClaudeRegistration(name,{env=process.env,cwd=process.cwd(),command}={}){
 let data,entry;
 try{data=JSON.parse(readFileSync(join(env.CLAUDE_CONFIG_DIR??env.HOME??homedir(),'.claude.json'),'utf8'));}
 catch(error){if(error.code!=='ENOENT')return {conflict:true};data={};}
 if(!data||typeof data!=='object'||Array.isArray(data))return {conflict:true};
 const local=Object.values(data.projects??{}).some(project=>has(project?.mcpServers,name));
 entry=has(data.mcpServers,name)?data.mcpServers[name]:undefined;
 const configured=Object.fromEntries(fields.map(key=>[key,entry?.env?.[key]]));
 const owned=entry?.type==='stdio'&&entry.command===command?.[0]&&Array.isArray(entry.args)&&entry.args.length===1&&entry.args[0]===command?.[1]&&Object.keys(entry).every(key=>['type','command','args','env'].includes(key))&&Object.keys(entry.env??{}).every(key=>fields.includes(key));
 const found=entry!==undefined;
 // Retain only the target's binding strings and a canonical-shape boolean;
 // unrelated servers, their environment values and the parsed tree stay here.
 data=null;entry=null;
 if(local||projectShadow(name,cwd))return {conflict:true};
 return found?{status:0,configured,owned}:{status:1};
}
