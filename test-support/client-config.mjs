import {fileURLToPath} from 'node:url';
import {serverName} from '../channel/register.mjs';
import {testTempDir} from './temp.mjs';
import {join as pathJoin} from 'node:path';
import {mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {Client} from '@modelcontextprotocol/sdk/client/index.js';
import * as z from 'zod/v4';
import {StdioClientTransport} from '@modelcontextprotocol/sdk/client/stdio.js';
export function scratch(t) {
  const dir=testTempDir('join-cli-');t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const home=pathJoin(dir,'home'),bin=pathJoin(dir,'bin'),record=pathJoin(dir,'calls.jsonl');
  mkdirSync(home,{mode:0o700});mkdirSync(pathJoin(home,'.git'),{mode:0o700});mkdirSync(bin,{mode:0o700});
  for(const client of ['claude','codex']) {
    const script=`#!${process.execPath}
const fs=require('node:fs'),path=require('node:path');
const args=process.argv.slice(2),name=args[1]==='get'?args[2]:args[1]==='remove'?args.at(-1):args.includes('--transport')?args[args.indexOf('--')-1]:args[2],state=path.join(${JSON.stringify(dir)},${JSON.stringify(client)}+'-'+name+'.json');
fs.appendFileSync(${JSON.stringify(record)},JSON.stringify({client:${JSON.stringify(client)},args,has_broker_token:Object.hasOwn(process.env,'BROKER_TOKEN'),has_legacy_token:Object.hasOwn(process.env,'TEAM_BROKER_TOKEN')})+'\\n');
if(args[1]==='get'){
 if(${JSON.stringify(client)}==='claude'&&process.env.MCP_FAKE_GET_MARKER)fs.writeFileSync(process.env.MCP_FAKE_GET_MARKER,'started');
 if(process.env.MCP_FAKE_SKIP!=='1'&&!fs.existsSync(state))process.exit(1);
 const saved=fs.existsSync(state)?JSON.parse(fs.readFileSync(state,'utf8')):null;
 const values=saved?Object.fromEntries(saved.flatMap((arg,i)=>arg==='-e'||arg==='--env'?[saved[i+1]]:[]).map(pair=>{const n=pair.indexOf('=');return [pair.slice(0,n),pair.slice(n+1)];})):{BROKER_TOKEN_FILE:process.env.MCP_FAKE_TOKEN_FILE,BROKER_URL:process.env.MCP_FAKE_BROKER_URL,CHANNEL_WORKSPACE:process.env.MCP_FAKE_WORKSPACE,CHANNEL_TOPIC:process.env.MCP_FAKE_TOPIC??'fixture',CHANNEL_SESSION_ID:process.env.MCP_FAKE_SESSION_ID??'${client}-fake-stored'};
 const marker=saved?.indexOf('--'),command=saved?.[marker+1]??process.execPath,commandArgs=saved?.slice(marker+2)??[];
 if(${JSON.stringify(client)}==='codex')console.log(JSON.stringify({transport:{type:'stdio',command,args:commandArgs,env:values}}));
 else console.log('Scope: User config\\nType: stdio\\nCommand: '+command+'\\nArgs: '+commandArgs.join(' ')+'\\nEnvironment:\\n'+Object.entries(values).map(([k,v])=>'    '+k+'='+v).join('\\n'));
 process.exit(0);
}
if(args[1]==='remove'){if(process.env.MCP_FAKE_REMOVE_FAIL==='1')process.exit(1);fs.rmSync(state,{force:true});if(${JSON.stringify(client)}==='claude'){const cfg=path.join(process.env.CLAUDE_CONFIG_DIR??process.env.HOME,'.claude.json');if(fs.existsSync(cfg)){const data=JSON.parse(fs.readFileSync(cfg,'utf8'));delete data.mcpServers?.[name];fs.writeFileSync(cfg,JSON.stringify(data),{mode:0o600});}}process.exit(0);}
if(args[1]==='add'){if(process.env.MCP_FAKE_FAIL==='1')process.exit(1);fs.writeFileSync(state,JSON.stringify(args));if(${JSON.stringify(client)}==='claude'){const cfg=path.join(process.env.CLAUDE_CONFIG_DIR??process.env.HOME,'.claude.json');fs.mkdirSync(path.dirname(cfg),{recursive:true,mode:0o700});const data=fs.existsSync(cfg)?JSON.parse(fs.readFileSync(cfg,'utf8')):{};const env=Object.fromEntries(args.flatMap((arg,i)=>arg==='-e'?[args[i+1]]:[]).map(v=>{const at=v.indexOf('=');return [v.slice(0,at),v.slice(at+1)];}));data.mcpServers??={};data.mcpServers[name]={type:'stdio',command:args[args.indexOf('--')+1],args:args.slice(args.indexOf('--')+2),env};fs.writeFileSync(cfg,JSON.stringify(data),{mode:0o600});}process.exit(0);}
process.exit(2);
`;
    writeFileSync(pathJoin(bin,client),script,{mode:0o700});
  }
  const env={...process.env,HOME:home,XDG_CONFIG_HOME:pathJoin(home,'.config'),CODEX_HOME:pathJoin(home,'.codex'),CLAUDE_CONFIG_DIR:pathJoin(home,'.claude'),PATH:bin};
  for(const key of ['BROKER_TOKEN','BROKER_TOKEN_FILE','TEAM_BROKER_TOKEN','NODE_TLS_REJECT_UNAUTHORIZED'])delete env[key];
  return {dir,env,readCalls:()=>{try{return readFileSync(record,'utf8').trim().split('\n').map(s=>JSON.parse(s));}catch{return [];}}};
}
export async function cli(env,args,stdin='') {
  if(env.MCP_FAKE_SKIP==='1'){const cfg=pathJoin(env.CLAUDE_CONFIG_DIR??env.HOME,'.claude.json');mkdirSync(pathJoin(cfg,'..'),{recursive:true,mode:0o700});const data={mcpServers:{[serverName(env.MCP_FAKE_BROKER_URL,env.MCP_FAKE_WORKSPACE,env.MCP_FAKE_TOPIC??'fixture','claude')]:{type:'stdio',command:process.execPath,args:[fileURLToPath(new URL('../channel/main.mjs',import.meta.url))],env:{BROKER_TOKEN_FILE:env.MCP_FAKE_TOKEN_FILE,BROKER_URL:env.MCP_FAKE_BROKER_URL,CHANNEL_WORKSPACE:env.MCP_FAKE_WORKSPACE,CHANNEL_TOPIC:env.MCP_FAKE_TOPIC??'fixture',CHANNEL_SESSION_ID:env.MCP_FAKE_SESSION_ID??'claude-fake-stored'}}}};writeFileSync(cfg,JSON.stringify(data),{mode:0o600});}
  const child=spawn(process.execPath,[fileURLToPath(new URL('../channel/cli.mjs',import.meta.url)),...args],{env,cwd:env.MCP_TEST_CWD??env.HOME,stdio:['pipe','pipe','pipe']});
  let stdout='',stderr='';child.stdout.on('data',c=>{stdout+=c;});child.stderr.on('data',c=>{stderr+=c;});child.stdin.end(stdin);
  const code=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',resolve);});
  return {code,stdout,stderr};
}
export function savedEnv(args) {
  return Object.fromEntries(args.flatMap((arg,i)=>arg==='-e'||arg==='--env'?[args[i+1]]:[]).map(pair=>{
    const split=pair.indexOf('=');return [pair.slice(0,split),pair.slice(split+1)];
  }));
}
export async function channel(f,env) {
  const client=new Client({name:'file-token-test',version:'1.0.0'});f.clients.push(client);const notifications=[];
  client.setNotificationHandler(z.object({method:z.literal('notifications/claude/channel'),params:z.object({content:z.string(),meta:z.record(z.string(),z.string())})}),event=>notifications.push(event.params));
  const transport=new StdioClientTransport({command:process.execPath,args:[fileURLToPath(new URL('../channel/main.mjs',import.meta.url))],cwd:env.MCP_TEST_CWD??env.HOME,env,stderr:'pipe'});
  const logs=[];transport.stderr.on('data',c=>logs.push(c.toString()));await client.connect(transport);
  return {client,logs,notifications,call:(name,args={})=>client.callTool({name,arguments:args})};
}
