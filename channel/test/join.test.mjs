import {scratch,cli,savedEnv,channel} from '../../test-support/client-config.mjs';
import {prepareTopicFixture,FIXTURE_TOPIC} from '../../test-support/topic-fixture.mjs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs, { constants, openSync, fstatSync, mkdirSync, writeFileSync, readFileSync, statSync, readdirSync, rmSync, symlinkSync, chmodSync, renameSync } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { join as pathJoin, dirname } from 'node:path';
import http from 'node:http';
import net from 'node:net';
import { brokerFixture, tokens, eventually } from './helpers.mjs';
import { join, normaliseUrl } from '../cli.mjs';
import { credentialPath, ensurePrivateDir, readCredential, writeCredential } from '../credentials.mjs';
import { config } from '../config.mjs';
import { mint } from '../../broker/credentials.mjs';

async function issue(f,workspace='default') {
  prepareTopicFixture(f.broker,workspace);
  const response=await fetch(f.url+'/invites',{method:'POST',headers:{Authorization:`Bearer ${tokens.alice}`,'X-Session-Id':`issuer-${workspace}`,'X-Workspace':workspace,'Content-Type':'application/json'},body:JSON.stringify({topics:[FIXTURE_TOPIC]})});
  assert.equal(response.status,201);return (await response.json()).code;
}
async function project(f,name) {
  const r=await fetch(f.url+'/workspaces',{method:'POST',headers:{Authorization:`Bearer ${tokens.alice}`,'Content-Type':'application/json'},body:JSON.stringify({name})});assert.equal(r.status,201);
}
const secretFree=(text,secrets)=>{for(const secret of secrets)assert.ok(!text.includes(secret),'secret must not appear in captured output or argv');};

for(const kind of ['symlinked','group-writable'])test(`credential preflight refuses a ${kind} ancestor before consuming an invite`,async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f),parent=s.env.XDG_CONFIG_HOME;
  if(kind==='symlinked') {
    const target=pathJoin(s.env.HOME,'real-config');mkdirSync(target,{mode:0o700});symlinkSync(target,parent);
  } else mkdirSync(parent,{mode:0o770});
  const result=await cli(s.env,['join',f.url,'-','--name','ancestor-check','--client','none'],code);
  assert.equal(result.code,1);assert.equal(JSON.parse(result.stderr).error,'INSECURE_CREDENTIAL_DIR');
  // The strict credential rule (decision 149) refuses 0770 even below the 0700
  // scratch HOME, and names the directory and its fix (decision 148).
  if(kind==='group-writable')assert.equal(JSON.parse(result.stderr).hint,`${parent} is group- or world-writable; run chmod go-w '${parent}'`);
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',code.slice(4,36)).redeemed_at,null);
  assert.equal(f.store.get('SELECT count(*) n FROM credentials').n,0);secretFree(result.stdout+result.stderr,[code]);
});

for(const home of ['', '.'])test(`credential ancestry does not trust cwd as the boundary for ${home?'relative':'empty'} HOME`,async t=>{
  const s=scratch(t),unsafe=pathJoin(s.dir,'unsafe'),cwd=pathJoin(unsafe,'working');mkdirSync(unsafe,{mode:0o770});mkdirSync(cwd,{mode:0o700});
  const source=`import {ensurePrivateDir} from ${JSON.stringify(new URL('../credentials.mjs',import.meta.url).href)};
    try {ensurePrivateDir(${JSON.stringify(pathJoin(cwd,'private','credentials'))},{home:process.env.HOME});console.log('accepted');}
    catch(error){console.log(error.code);}`;
  const run=async value=>{
    const child=spawn(process.execPath,['--input-type=module','--eval',source],{cwd,env:{PATH:process.env.PATH,TMPDIR:s.dir,HOME:value},stdio:['ignore','pipe','pipe']});let out='';child.stdout.on('data',b=>{out+=b;});child.stderr.resume();
    assert.equal(await new Promise(resolve=>child.once('exit',resolve)),0);return out.trim();
  };
  assert.equal(await run(home),'INSECURE_CREDENTIAL_DIR');
  // A real absolute home remains the explicitly trusted stopping boundary.
  assert.equal(await run(cwd),'accepted');
});

test('credential reads refuse a path replaced after open while ancestor validation runs',t=>{
  const s=scratch(t),dir=pathJoin(s.dir,'private'),path=pathJoin(dir,'token'),replacement=pathJoin(dir,'replacement');mkdirSync(dir,{mode:0o700});
  writeFileSync(path,'original-synthetic-value',{mode:0o600});writeFileSync(replacement,'replacement-synthetic-value',{mode:0o600});
  const lstat=fs.lstatSync;let swapped=false;
  t.mock.method(fs,'lstatSync',(file,...args)=>{
    if(file===dir&&!swapped){swapped=true;renameSync(path,pathJoin(dir,'original'));renameSync(replacement,path);}
    return lstat(file,...args);
  });syncBuiltinESMExports();
  try {assert.throws(()=>readCredential(path),{code:'INSECURE_TOKEN_FILE'});}
  finally {t.mock.restoreAll();syncBuiltinESMExports();}
  assert.equal(swapped,true);assert.ok(readCredential(path)==='replacement-synthetic-value','a stable replacement path remains readable');
});

for(const host of ['127.0.0.1','localhost','::1'])for(const client of ['cli','channel'])test(`loopback ${client} bypasses inherited proxies for ${host}`,async t=>{
  const f=await brokerFixture(t),s=scratch(t);let hits=0;
  if(host==='::1') {
    await new Promise(resolve=>f.server.close(resolve));await new Promise((resolve,reject)=>{f.server.once('error',reject);f.server.listen(0,'::1',resolve);});
    f.url='http://[::1]:'+f.server.address().port;
  } else if(host==='localhost')f.url=f.url.replace('127.0.0.1','localhost');
  const proxy=http.createServer((req,res)=>{hits++;req.resume();res.writeHead(502,{'Content-Type':'application/json',Connection:'close'});res.end('{"error":"unavailable"}');});
  const sockets=new Set();proxy.on('connection',socket=>{sockets.add(socket);socket.on('close',()=>sockets.delete(socket));});
  proxy.on('connect',(_req,socket,head)=>{
    hits++;const target=new URL(f.url),upstream=net.connect({host:target.hostname.replace(/^\[|\]$/g,''),port:Number(target.port)},()=>{
      socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');if(head.length)upstream.write(head);socket.pipe(upstream);upstream.pipe(socket);
    });
    upstream.on('error',()=>socket.destroy());upstream.on('close',()=>socket.destroy());socket.on('close',()=>upstream.destroy());
  });
  await new Promise(resolve=>proxy.listen(0,'127.0.0.1',resolve));t.after(()=>{for(const socket of sockets)socket.destroy();proxy.closeAllConnections();return new Promise(resolve=>proxy.close(resolve));});
  const proxyUrl='http://127.0.0.1:'+proxy.address().port;
  const env={...s.env,NODE_USE_ENV_PROXY:'1',HTTP_PROXY:proxyUrl,HTTPS_PROXY:proxyUrl,ALL_PROXY:proxyUrl,http_proxy:proxyUrl,https_proxy:proxyUrl,all_proxy:proxyUrl,NO_PROXY:'',no_proxy:''};
  if(client==='cli') {
    const code=await issue(f),result=await cli(env,['join',f.url,'-','--name','proxy-isolated','--client','none'],code);assert.equal(result.code,0);
    secretFree(result.stdout+result.stderr,[code]);
  } else {
    const b=await channel(f,{...env,BROKER_URL:f.url,BROKER_TOKEN:tokens.bob,CHANNEL_TOPIC:FIXTURE_TOPIC,CHANNEL_SESSION_ID:'proxy-bob',CHANNEL_POLL_MS:'25'});
    assert.equal((await b.call('members')).isError,undefined);await eventually(()=>f.broker.streams.has('proxy-bob'));
  }
  assert.equal(hits,0);
});

test('join stores a mode-600 token in a mode-700 directory and registers the Claude client without secrets in argv',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f);
  const result=await cli(s.env,['join',f.url+'/', '-', '--name','newbie'],code+'\n');
  assert.equal(result.code,0);assert.equal(result.stderr,'');const data=JSON.parse(result.stdout);
  assert.equal(data.user,'newbie');assert.equal(data.workspace,'default');assert.equal(data.adapters.length,1);
  assert.ok(data.adapters.every(a=>a.status==='registered'));
  const path=credentialPath(f.url,'newbie',{env:s.env}),token=readCredential(path);
  assert.equal(data.credential_path,path);assert.equal(statSync(path).mode&0o777,0o600);assert.equal(statSync(dirname(path)).mode&0o777,0o700);
  assert.equal(readdirSync(dirname(path)).length,1);
  const calls=s.readCalls(),adds=calls.filter(c=>c.args[1]==='add');assert.equal(adds.length,1);
  for(const {client,args} of adds) {
    const registration=data.adapters.find(a=>a.client===client);
    assert.ok(args.includes(registration.server_name));assert.match(registration.server_name,/^[A-Za-z0-9_-]{1,64}$/);const env=savedEnv(args);
    assert.equal(env.BROKER_TOKEN_FILE,path);assert.equal(env.BROKER_URL,f.url);assert.equal(env.CHANNEL_WORKSPACE,'default');
    assert.equal(env.CHANNEL_TOPIC,FIXTURE_TOPIC);
    assert.match(env.CHANNEL_SESSION_ID,new RegExp(`^${client}-[0-9a-f]{12}-[0-9a-f]{32}$`));
    assert.ok(env.CHANNEL_SESSION_ID.length<=128);assert.equal('BROKER_TOKEN' in env,false);
    assert.equal(args[args.indexOf('--')+1],process.execPath);assert.ok(args.at(-1).endsWith('/channel/main.mjs'));
    assert.equal(args[args.indexOf('--scope')+1],'user');assert.equal(args[args.indexOf('--transport')+1],'stdio');assert.ok(args.indexOf('--transport')>args.lastIndexOf('-e'));assert.equal(data.adapters.find(a=>a.client==='claude').hook_env.CHANNEL_SESSION_ID,env.CHANNEL_SESSION_ID);
  }
  secretFree(result.stdout+result.stderr+JSON.stringify(calls),[code,token]);
});

test('registered BROKER_TOKEN_FILE plus an explicit M11a topic completes stdio and reuses its pinned session',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f),result=await cli(s.env,['join',f.url,'-','--name','newbie'],code);
  assert.equal(result.code,0);
  const add=s.readCalls().find(c=>c.client==='claude'&&c.args[1]==='add'),env={...s.env,...savedEnv(add.args),CHANNEL_MODE:'poll',CHANNEL_TOPIC:FIXTURE_TOPIC,CHANNEL_POLL_MS:'25'};
  assert.equal('BROKER_TOKEN' in env,false);
  const a=await channel(f,env),b=await channel(f,env);
  const sent=await a.call('send',{to:'user:alice',body:'from file credential'});assert.equal(sent.isError,undefined);
  await eventually(()=>f.store.get('SELECT 1 FROM agents WHERE session_id=?',env.CHANNEL_SESSION_ID));
  assert.equal((await b.call('members')).isError,undefined);
  assert.equal(f.store.get("SELECT count(*) n FROM agents WHERE user='newbie'").n,1);
  const found=f.store.get("SELECT body FROM messages WHERE from_user='newbie'");assert.equal(found.body,'from file credential');
  secretFree(a.logs.join('')+b.logs.join(''),[readCredential(env.BROKER_TOKEN_FILE),code]);
});

test('a second join reuses the existing credential for a new workspace and ALREADY_MEMBER is a no-op',async t=>{
  const f=await brokerFixture(t),s=scratch(t),first=await issue(f);
  assert.equal((await cli(s.env,['join',f.url,'-','--name','newbie','--client','none'],first)).code,0);
  const path=credentialPath(f.url,'newbie',{env:s.env}),token=readCredential(path),before=statSync(path);
  await project(f,'second');const second=await issue(f,'second');
  const result=await cli(s.env,['join',f.url,'-','--name','newbie'],second);assert.equal(result.code,0);
  assert.equal(JSON.parse(result.stdout).workspace,'second');assert.equal(statSync(path).ino,before.ino);
  assert.ok(readCredential(path)===token,'credential remains identical');assert.equal(f.store.get('SELECT count(*) n FROM credentials').n,1);
  const list=await fetch(f.url+'/workspaces',{headers:{Authorization:`Bearer ${token}`}});
  assert.deepEqual((await list.json()).workspaces.map(w=>w.name),['default','second']);
  assert.equal(f.store.get("SELECT count(*) n FROM invites WHERE redeemed_by_user='newbie'").n,2);
  assert.ok(s.readCalls().filter(c=>c.args[1]==='add').every(c=>savedEnv(c.args).CHANNEL_WORKSPACE==='second'&&savedEnv(c.args).CHANNEL_TOPIC===FIXTURE_TOPIC));
  const again=await issue(f,'second'),noop=await cli(s.env,['join',f.url,'-','--name','newbie'],again);
  assert.equal(noop.code,0);assert.equal(JSON.parse(noop.stdout).status,'already_member');
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',again.slice(4,36)).redeemed_at,null);
  secretFree(result.stdout+noop.stdout+JSON.stringify(s.readCalls()),[token,first,second,again]);
});

test('existing client registrations are skipped without overwriting or advertising a new session',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f);
  const result=await cli({...s.env,MCP_FAKE_SKIP:'1',MCP_FAKE_TOKEN_FILE:credentialPath(f.url,'newbie',{env:s.env}),MCP_FAKE_BROKER_URL:f.url,MCP_FAKE_WORKSPACE:'default'},['join',f.url,'-','--name','newbie'],code);
  assert.equal(result.code,0);assert.ok(JSON.parse(result.stdout).adapters.every(a=>a.status==='skipped'&&a.hint==='already registered'&&!a.hook_env));
  assert.equal(s.readCalls().filter(c=>c.args[1]==='add').length,0);
});

test('insecure token files and directories fail before redemption, without following symlinks',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f),path=credentialPath(f.url,'newbie',{env:s.env});
  ensurePrivateDir(dirname(path));const target=pathJoin(s.dir,'target');writeFileSync(target,'synthetic-private-value',{mode:0o600});symlinkSync(target,path);
  for(const expected of ['symlink','group-readable']) {
    if(expected==='group-readable'){rmSync(path);writeFileSync(path,'synthetic-private-value',{mode:0o640});}
    const result=await cli(s.env,['join',f.url,'-','--name','newbie'],code);assert.equal(result.code,1);assert.equal(JSON.parse(result.stderr).error,'INSECURE_TOKEN_FILE');
  }
  rmSync(path);chmodSync(dirname(path),0o750);
  const result=await cli(s.env,['join',f.url,'-','--name','newbie'],code);assert.equal(JSON.parse(result.stderr).error,'INSECURE_CREDENTIAL_DIR');
  assert.equal(f.requests.filter(r=>r.path==='/join').length,0);
});

test('exclusive credential publication refuses overwrite and preserves exact bytes and permissions',t=>{
  const s=scratch(t),path=credentialPath('https://broker.example','newbie',{env:s.env}),token=mint('tmc').value;
  writeCredential(path,token);assert.ok(readCredential(path)===token);assert.throws(()=>writeCredential(path,'different'),{code:'CREDENTIAL_EXISTS'});
  assert.ok(readCredential(path)===token);assert.equal(readdirSync(dirname(path)).length,1);
  assert.ok(config({BROKER_TOKEN_FILE:path}).token===token,'config uses the file bytes unchanged');
  assert.throws(()=>config({BROKER_TOKEN:token,BROKER_TOKEN_FILE:path}),{code:'AMBIGUOUS_BROKER_TOKEN'});
  writeFileSync(path,token+'\n');assert.ok(readCredential(path)===token+'\n');assert.throws(()=>config({BROKER_TOKEN_FILE:path}),{code:'INVALID_BROKER_TOKEN'});
  const link=pathJoin(s.dir,'link');symlinkSync(dirname(path),link);assert.throws(()=>ensurePrivateDir(link),{code:'INSECURE_CREDENTIAL_DIR'});
});

test('remote HTTP, disabled TLS, invalid names and malformed codes fail before any network request',async t=>{
  const s=scratch(t);let requests=0;const fetch=()=>{requests++;throw Error('must not run');},code=mint('tmi').value;
  for(const [options,error,env] of [
    [{url:'http://broker.example',code,name:'newbie'},'INSECURE_BROKER_URL',s.env],
    [{url:'https://broker.example',code,name:'newbie'},'TLS_VERIFY_DISABLED',{...s.env,NODE_TLS_REJECT_UNAUTHORIZED:'0'}],
    [{url:'https://broker.example',code,name:'bad name'},'NAME_REQUIRED',s.env],
    [{url:'https://broker.example',code:'bad-secret-value',name:'newbie'},'INVALID_INVITE_CODE',s.env],
  ])await assert.rejects(join(options,{env,fetch}),{code:error});
  assert.equal(requests,0);assert.equal(normaliseUrl('http://[::1]:8091/'),'http://[::1]:8091');
});

test('redirects never send the code to their target and CLI failures never echo secrets',async t=>{
  const s=scratch(t),code=mint('tmi').value;let targetRequests=0;
  const target=http.createServer((req,res)=>{targetRequests++;res.end('{}');});await new Promise(resolve=>target.listen(0,'127.0.0.1',resolve));
  const redirect=http.createServer((req,res)=>{req.resume();res.writeHead(307,{Location:`http://127.0.0.1:${target.address().port}/join`});res.end();});await new Promise(resolve=>redirect.listen(0,'127.0.0.1',resolve));
  t.after(async()=>{for(const server of [redirect,target]){server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}});
  const result=await cli(s.env,['join',`http://127.0.0.1:${redirect.address().port}`,'-','--name','newbie'],code);
  assert.equal(result.code,1);assert.equal(JSON.parse(result.stderr).error,'JOIN_UNAVAILABLE');assert.equal(targetRequests,0);
  secretFree(result.stdout+result.stderr,[code]);
  const tls=await cli({...s.env,NODE_TLS_REJECT_UNAUTHORIZED:'0'},['join','https://broker.example','-','--name','newbie'],code);
  assert.equal(JSON.parse(tls.stderr).error,'TLS_VERIFY_DISABLED');secretFree(tls.stdout+tls.stderr,[code]);
});

test('a failed local write revokes the minted token, ends membership and leaves the name reserved',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f);let minted;
  await assert.rejects(join({url:f.url,code,name:'newbie',client:'none'},{env:s.env,write:(_path,token)=>{minted=token;throw Error(token);}}),{code:'CREDENTIAL_SAVE_FAILED'});
  assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE user='newbie'"),undefined);
  assert.ok(f.store.get("SELECT deactivated_at FROM broker_users WHERE name='newbie'").deactivated_at);
  const response=await fetch(f.url+'/workspaces',{headers:{Authorization:`Bearer ${minted}`}});assert.equal(response.status,401);assert.equal((await response.json()).error,'UNAUTHORIZED');
  const next=await issue(f),result=await cli(s.env,['join',f.url,'-','--name','newbie'],next);
  assert.equal(result.code,1);assert.equal(JSON.parse(result.stderr).error,'USER_EXISTS');assert.match(JSON.parse(result.stderr).hint,/choose another name/);
  secretFree(result.stdout+result.stderr,[minted,code,next]);
});

test('IPv6 loopback with a trailing slash joins through the real /join route',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f);
  const original=f.server.listeners('request')[0],v6=http.createServer(original);
  try { await new Promise((resolve,reject)=>{v6.once('error',reject);v6.listen(0,'::1',resolve);}); }
  catch(error){if(['EADDRNOTAVAIL','EAFNOSUPPORT'].includes(error.code)){t.skip('Host has no IPv6 loopback: '+error.code);return;}throw error;}
  t.after(async()=>{v6.closeAllConnections();await new Promise(resolve=>v6.close(resolve));});
  const result=await cli(s.env,['join',`http://[::1]:${v6.address().port}/`,'-','--name','newbie','--client','none'],code);
  assert.equal(result.code,0);assert.equal(JSON.parse(result.stdout).workspace,'default');
  assert.ok(f.requests.some(r=>r.path==='/join'));
});

test('selected missing or failed adapters report outcomes without losing the saved credential',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f);
  const missing=await cli({...s.env,PATH:''},['join',f.url,code,'--name','manual-user','--client','claude']);
  assert.equal(missing.code,0);assert.equal(JSON.parse(missing.stdout).adapters[0].status,'manual');
  const file=JSON.parse(missing.stdout).credential_path;assert.equal(statSync(file).mode&0o777,0o600);
  const second=await issue(f),failed=await cli({...s.env,MCP_FAKE_FAIL:'1'},['join',f.url,'-','--name','failed-user','--client','claude'],second);
  assert.equal(failed.code,0);assert.equal(JSON.parse(failed.stdout).adapters[0].status,'failed');
  const path=JSON.parse(failed.stdout).credential_path;assert.equal(statSync(path).mode&0o777,0o600);
  secretFree(missing.stdout+missing.stderr+failed.stdout+failed.stderr+JSON.stringify(s.readCalls()),[code,second,readCredential(file),readCredential(path)]);
});

test('join refuses --client codex as an unsupported client before using the invite',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f);
  const refused=await cli(s.env,['join',f.url,'-','--name','refused-client','--client','codex'],code);
  assert.equal(refused.code,1);assert.equal(refused.stdout,'');
  const error=JSON.parse(refused.stderr);assert.equal(error.error,'INVALID_CLIENT');assert.equal(error.hint,'Use --client claude, claude-plugin or none.');
  assert.equal(f.requests.filter(r=>r.path==='/join').length,0);assert.deepEqual(s.readCalls(),[]);
  assert.throws(()=>statSync(credentialPath(f.url,'refused-client',{env:s.env})),{code:'ENOENT'});
  const accepted=await cli(s.env,['join',f.url,'-','--name','refused-client','--client','none'],code);assert.equal(accepted.code,0);
  secretFree(refused.stdout+refused.stderr+accepted.stdout+accepted.stderr,[code]);
});

test('F1: dotted workspace names use collision-resistant client-safe registration names in the Claude adapter',async t=>{
  const f=await brokerFixture(t),s=scratch(t);
  for(const workspace of ['a.b','a_b']) {
    await project(f,workspace);const code=await issue(f,workspace);
    const result=await cli(s.env,['join',f.url,'-','--name','newbie'],code);assert.equal(result.code,0);
  }
  const adds=s.readCalls().filter(c=>c.args[1]==='add'),names=[];
  for(const {args} of adds) {
    const env=savedEnv(args);
    const name=args[args.indexOf('--')-1];names.push(name);
    assert.ok(['a.b','a_b'].includes(env.CHANNEL_WORKSPACE));assert.equal(env.CHANNEL_TOPIC,FIXTURE_TOPIC);
    assert.match(name,/^[A-Za-z0-9_-]{1,64}$/);
  }
  assert.equal(adds.length,2);assert.equal(new Set(names).size,2);
});


test('F2: credential file and directory are private at creation before any later permission change',t=>{
  const s=scratch(t),dir=pathJoin(s.dir,'creation-mode'),modes=[];
  ensurePrivateDir(dir,{mkdir:(path,options)=>{
    assert.equal(options.mode,0o700);mkdirSync(path,options);
    assert.equal(statSync(path).mode&0o777,0o700);modes.push('directory');
  }});
  const path=pathJoin(dir,'created.token');
  writeCredential(path,mint('tmc').value,{open:(path,flags,mode)=>{
    assert.equal(mode,0o600);const fd=openSync(path,flags,mode);
    assert.equal(fstatSync(fd).mode&0o777,0o600);modes.push('file');return fd;
  }});
  assert.deepEqual(modes,['directory','file']);
});

test('F4: an unwritable private directory is refused before consuming an invite or reserving a name',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f),path=credentialPath(f.url,'newbie',{env:s.env});
  ensurePrivateDir(dirname(path));chmodSync(dirname(path),0o500);t.after(()=>{try{chmodSync(dirname(path),0o700);}catch(error){if(error.code!=='ENOENT')throw error;}});
  const result=await cli(s.env,['join',f.url,'-','--name','newbie','--client','none'],code);
  assert.equal(result.code,1);assert.equal(JSON.parse(result.stderr).error,'CREDENTIAL_DIR_NOT_WRITABLE');
  assert.equal(f.requests.filter(r=>r.path==='/join').length,0);
  assert.equal(f.store.get("SELECT 1 FROM broker_users WHERE name='newbie'"),undefined);
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',code.slice(4,36)).redeemed_at,null);
});

test('F5: temporary credential creation retains O_EXCL and O_NOFOLLOW',t=>{
  const s=scratch(t),path=credentialPath('https://broker.example','newbie',{env:s.env});let observed=false;
  writeCredential(path,mint('tmc').value,{open:(path,flags,mode)=>{
    for(const flag of [constants.O_CREAT,constants.O_EXCL,constants.O_WRONLY,constants.O_NOFOLLOW])assert.equal(flags&flag,flag);
    observed=true;return openSync(path,flags,mode);
  }});
  assert.equal(observed,true);
});

test('F5: a real post-redemption filesystem write failure revokes the new credential and membership',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f),path=credentialPath(f.url,'newbie',{env:s.env});
  try {
    await assert.rejects(join({url:f.url,code,name:'newbie',client:'none'},{env:s.env,fetch:async(url,args)=>{
      const response=await fetch(url,args);if(url.endsWith('/join')&&response.ok)chmodSync(dirname(path),0o500);return response;
    }}),{code:'CREDENTIAL_SAVE_FAILED'});
  } finally {chmodSync(dirname(path),0o700);}
  assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE user='newbie'"),undefined);
  assert.ok(f.store.get("SELECT deactivated_at FROM broker_users WHERE name='newbie'").deactivated_at);
  assert.ok(f.store.get("SELECT deactivated_at FROM credentials WHERE user='newbie'").deactivated_at);
  assert.equal(readdirSync(dirname(path)).length,0);
  assert.ok(f.requests.some(r=>r.path==='/credentials/revoke'));
});

test('F5: inherited broker secrets are absent from spawned client environments',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f),secret=mint('tmc').value;
  const result=await cli({...s.env,BROKER_TOKEN:secret,TEAM_BROKER_TOKEN:secret},['join',f.url,'-','--name','newbie'],code);
  assert.equal(result.code,0);assert.ok(s.readCalls().length>0);
  for(const child of s.readCalls()){assert.equal(child.has_broker_token,false);assert.equal(child.has_legacy_token,false);}
  secretFree(result.stdout+result.stderr+JSON.stringify(s.readCalls()),[secret,code]);
});

test('F7: bare query and fragment delimiters are refused before network access',async t=>{
  const s=scratch(t),code=mint('tmi').value;let requests=0;
  for(const url of ['https://broker.example?','https://broker.example#','https://broker.example/path?','http://127.0.0.1:8091/#']) {
    await assert.rejects(join({url,code,name:'newbie',client:'none'},{env:s.env,fetch:()=>{requests++;throw Error('unexpected request');}}),{code:'INVALID_BROKER_URL'});
    assert.throws(()=>config({BROKER_URL:url,BROKER_TOKEN:mint('tmc').value}),{code:'INVALID_BROKER_URL'});
  }
  assert.equal(requests,0);
});

test('F9: another user token file in an existing registration reports conflict without overwrite',async t=>{
  const f=await brokerFixture(t),s=scratch(t);
  const first=await cli(s.env,['join',f.url,'-','--name','first-user'],await issue(f));assert.equal(first.code,0);
  const before=s.readCalls().filter(c=>c.args[1]==='add').length;
  const second=await cli(s.env,['join',f.url,'-','--name','second-user'],await issue(f));
  assert.equal(second.code,1);const result=JSON.parse(second.stdout);
  assert.ok(result.adapters.every(a=>a.status==='conflict'&&typeof a.hint==='string'));
  assert.equal(s.readCalls().filter(c=>c.args[1]==='add').length,before);
  const firstFile=JSON.parse(first.stdout).credential_path;
  for(const add of s.readCalls().filter(c=>c.args[1]==='add'))assert.equal(savedEnv(add.args).BROKER_TOKEN_FILE,firstFile);
});

test('F3: a newly minted token is revoked when the broker response fails identity validation',async t=>{
  const f=await brokerFixture(t),s=scratch(t),code=await issue(f);let token;
  await assert.rejects(join({url:f.url,code,name:'newbie',client:'none'},{env:s.env,fetch:async(url,args)=>{
    const response=await fetch(url,args);
    if(url.endsWith('/join')&&response.ok){const data=await response.json();token=data.token;return new Response(JSON.stringify({...data,user:'unexpected-user'}),{status:201,headers:{'Content-Type':'application/json'}});}
    return response;
  }}),{code:'INVALID_JOIN_RESPONSE'});
  const result=await fetch(f.url+'/workspaces',{headers:{Authorization:`Bearer ${token}`}});
  assert.equal(result.status,401);assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE user='newbie'"),undefined);
});

test('F3: a mismatched saved identity is refused by the broker without consuming the invite',async t=>{
  const f=await brokerFixture(t),s=scratch(t);
  assert.equal((await cli(s.env,['join',f.url,'-','--name','newbie','--client','none'],await issue(f))).code,0);
  const token=readCredential(credentialPath(f.url,'newbie',{env:s.env}));
  const wrongPath=credentialPath(f.url,'wrong-name',{env:s.env});writeCredential(wrongPath,token);
  await project(f,'second');const code=await issue(f,'second');
  const before=f.store.get('SELECT count(*) n FROM workspace_audit').n;
  const result=await cli(s.env,['join',f.url,'-','--name','wrong-name','--client','none'],code);
  assert.equal(result.code,1);assert.equal(JSON.parse(result.stderr).error,'JOIN_NAME_MISMATCH');
  assert.match(JSON.parse(result.stderr).hint,/saved credential belongs to another user/);
  assert.equal(f.store.get('SELECT count(*) n FROM workspace_audit').n,before);
  assert.equal(f.store.get('SELECT redeemed_at FROM invites WHERE id=?',code.slice(4,36)).redeemed_at,null);
  assert.equal(f.store.get("SELECT 1 FROM workspace_members WHERE workspace='second' AND user='newbie'"),undefined);
  assert.equal((await cli(s.env,['join',f.url,'-','--name','newbie','--client','none'],code)).code,0);
  secretFree(result.stdout+result.stderr,[code,token]);
});

test('F3: a saved credential from another broker is unauthorized without redeeming its invite',async t=>{
  const a=await brokerFixture(t),b=await brokerFixture(t),s=scratch(t);
  assert.equal((await cli(s.env,['join',a.url,'-','--name','newbie','--client','none'],await issue(a))).code,0);
  const token=readCredential(credentialPath(a.url,'newbie',{env:s.env}));writeCredential(credentialPath(b.url,'newbie',{env:s.env}),token);
  const code=await issue(b),result=await cli(s.env,['join',b.url,'-','--name','newbie','--client','none'],code);
  assert.equal(result.code,1);assert.equal(JSON.parse(result.stderr).error,'UNAUTHORIZED');
  assert.equal(b.store.get('SELECT redeemed_at FROM invites WHERE id=?',code.slice(4,36)).redeemed_at,null);
  assert.equal(b.store.get('SELECT count(*) n FROM broker_users').n,0);
  secretFree(result.stdout+result.stderr,[code,token]);
});
