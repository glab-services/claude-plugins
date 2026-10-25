// Test-only preload for the real broker/main.mjs. Pause after a queue write or
// its commit so the parent can SIGKILL an actual in-flight HTTP operation.
import { DatabaseSync } from 'node:sqlite';
const prepare=DatabaseSync.prototype.prepare,exec=DatabaseSync.prototype.exec;
const armed=new WeakSet();
let held=false;
function hold() {
  if(held)return;held=true;
  process.send?.({event:'queue_test_barrier'});
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0);
}
DatabaseSync.prototype.prepare=function(sql) {
  const statement=prepare.call(this,sql),run=statement.run,db=this;
  statement.run=function(...args) {
    const result=run.apply(this,args);
    const stage=process.env.QUEUE_TEST_OPERATION;
    const target=stage==='claim'?/UPDATE queue_items SET state='claimed'/:
      stage==='done'?/UPDATE queue_items SET state='done'/:
      stage==='requeue'?/UPDATE queue_items SET state='ready',attempts=0/:null;
    if(target?.test(sql)) {
      if(process.env.QUEUE_TEST_PHASE==='before_commit')hold();else armed.add(db);
    }
    return result;
  };
  return statement;
};
DatabaseSync.prototype.exec=function(sql) {
  const result=exec.call(this,sql);
  if(armed.has(this)&&/^COMMIT\s*;?$/i.test(sql.trim())){armed.delete(this);hold();}
  return result;
};
