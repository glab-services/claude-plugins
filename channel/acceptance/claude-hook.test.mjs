import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { testTempDir } from '../../test-support/temp.mjs';
import { brokerFixture, tokens } from '../test/helpers.mjs';

test('real Claude Code receives pending data from the SessionStart hook', {
  skip: process.env.RUN_CLAUDE_ACCEPTANCE !== '1', timeout: 60000,
}, async t => {
  const fixture = await brokerFixture(t);
  const dir = testTempDir('claude-hook-acceptance-');
  const marker = `M3_HOOK_PROBE_${randomUUID().replaceAll('-','')}`;
  const sharedSession = `hook-${randomUUID()}`;
  fixture.broker.heartbeat('alice','a');
  const sent = fixture.broker.send('alice','a',{to:'user:bob',body:marker});
  const settings = join(dir,'settings.json');
  writeFileSync(settings,JSON.stringify({hooks:{SessionStart:[{matcher:'startup',hooks:[{
    type:'command',command:`node ${JSON.stringify(fileURLToPath(new URL('../session-start.mjs',import.meta.url)))}`,timeout:15,
  }]}]}}),{mode:0o600});
  const child = spawn('claude',[
    '-p','Report only the M3_HOOK_PROBE code in the pending team message supplied by SessionStart context. If no such context arrived, report MISSING.',
    '--model',process.env.CLAUDE_ACCEPTANCE_MODEL ?? 'haiku','--max-budget-usd','0.25',
    '--tools','','--strict-mcp-config','--mcp-config','{"mcpServers":{}}',
    '--setting-sources','','--settings',settings,'--no-session-persistence','--output-format','json',
  ],{cwd:dir,env:{...process.env,BROKER_URL:fixture.url,TEAM_BROKER_TOKEN:tokens.bob,CHANNEL_SESSION_ID:sharedSession},stdio:['ignore','pipe','pipe']});
  let stdout='',stderr='';
  child.stdout.on('data',chunk=>{stdout+=chunk;});
  child.stderr.on('data',chunk=>{stderr+=chunk;});
  const exited=new Promise((resolve,reject)=>{child.once('close',code=>resolve(code));child.once('error',reject);});
  t.after(async()=>{
    let timer;
    if(child.exitCode===null&&child.signalCode===null){
      child.kill('SIGTERM');
      timer=setTimeout(()=>child.kill('SIGKILL'),1000);
      timer.unref();
    }
    await exited.catch(()=>{});
    clearTimeout(timer);
    rmSync(dir,{recursive:true,force:true});
  });
  const code=await exited;
  // Never include raw CLI output: it can contain personal configuration or credentials.
  if(code!==0) throw new Error(/login expired|not logged in|authentication/i.test(stdout+stderr)
    ? 'Claude acceptance requires an active CLI login.' : 'Claude acceptance CLI exited unsuccessfully; no raw output was logged.');
  let result;
  try{result=JSON.parse(stdout);}catch{throw new Error('Claude acceptance returned unexpected output; no raw output was logged.');}
  assert.ok(typeof result.result==='string' && result.result.includes(marker),'Claude must receive the marker through hook context, not the user prompt.');
  assert.ok(fixture.requests.some(req=>req.path.startsWith('/inbox')));
  assert.equal(fixture.store.get('SELECT acked_at FROM messages WHERE id=?',sent.id).acked_at,null);
  t.diagnostic('Real Claude Code consumed SessionStart context from a local broker; the pending message remained unacked.');
});
