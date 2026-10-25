// Isolated --import preload only: no production crash hook or remote command.
import { DatabaseSync } from 'node:sqlite';
import { existsSync } from 'node:fs';
const prepare=DatabaseSync.prototype.prepare,exec=DatabaseSync.prototype.exec;
const runs=new WeakSet(),armed=new WeakSet();let writes=0,held=false;
function hold() {
  if(held)return;held=true;process.send?.({event:'retention_test_barrier'});
  const wait=new Int32Array(new SharedArrayBuffer(4));
  while(!process.env.RETENTION_TEST_RELEASE || !existsSync(process.env.RETENTION_TEST_RELEASE))Atomics.wait(wait,0,0,10);
}
DatabaseSync.prototype.prepare=function(sql) {
  const stmt=prepare.call(this,sql),db=this;
  for(const method of ['run','all']) {
    const original=stmt[method];
    stmt[method]=function(...args) {
      const result=original.apply(this,args);
      if(/INSERT INTO retention_runs/.test(sql))runs.add(db);
      if(/UPDATE messages SET body=''/.test(sql) && ++writes===Number(process.env.RETENTION_TEST_BATCH??1)) {
        if(process.env.RETENTION_TEST_PHASE==='before_commit')hold();
        if(process.env.RETENTION_TEST_PHASE==='after_commit')armed.add(db);
      }
      return result;
    };
  }
  return stmt;
};
DatabaseSync.prototype.exec=function(sql) {
  if(runs.has(this) && /^BEGIN IMMEDIATE$/i.test(sql.trim()) && process.env.RETENTION_TEST_PHASE==='before_batch')hold();
  const result=exec.call(this,sql);
  if(armed.has(this) && /^COMMIT\s*;?$/i.test(sql.trim())){armed.delete(this);hold();}
  return result;
};
