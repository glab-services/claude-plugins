import { parseArgs } from 'node:util';
import { setTimeout as pause } from 'node:timers/promises';
import { statSync } from 'node:fs';
import { config, check, BrokerError, rejection, loadTokens, name } from './config.mjs';
import { migrate } from './migrate.mjs';
import { archiveWorkspace } from './workspaces.mjs';
import { Store } from './store.mjs';
import { candidates, compact } from './retention.mjs';

// Deliberately local-only: there is no HTTP/MCP resolve or reopen operation.
let store;
try {
  let parsed;
  try {
    parsed = parseArgs({ options: { reason: { type: 'string' }, backup: { type: 'string' }, owner: { type: 'string' }, to: { type: 'string' }, 'dry-run': { type: 'boolean' }, run: { type: 'boolean' }, workspace: { type: 'string' } }, allowPositionals: true });
  } catch { throw new BrokerError('INVALID_CLI_ARGS'); }
  const [action, threadId, extra] = parsed.positionals;
  const reason = parsed.values.reason?.trim();
  check(['list', 'resolve', 'reopen', 'migrate', 'delete-workspace', 'audit', 'retention'].includes(action) && !extra && (['list','migrate','retention'].includes(action) ? !threadId : Boolean(threadId)),
    'INVALID_CLI_ARGS');
  check(action==='retention'
    ? Boolean(parsed.values['dry-run'])!==Boolean(parsed.values.run) && (!parsed.values.run || parsed.values.workspace===undefined)
    : parsed.values['dry-run']===undefined && parsed.values.run===undefined && parsed.values.workspace===undefined,'INVALID_CLI_ARGS');
  check(['reopen','delete-workspace'].includes(action) || parsed.values.reason === undefined, 'INVALID_CLI_ARGS');
  check(!['reopen','delete-workspace'].includes(action) || reason, 'REASON_REQUIRED');
  check(action === 'migrate' || (parsed.values.backup === undefined && parsed.values.owner === undefined && parsed.values.to === undefined), 'INVALID_CLI_ARGS');
  const settings = config();
  // Only the broker initializes a database; administrative commands need its existing file.
  check(settings.dbPath !== ':memory:' && statSync(settings.dbPath, { throwIfNoEntry: false })?.isFile(), 'DATABASE_NOT_FOUND');
  if (action === 'migrate') {
    const staticUsers = statSync(settings.tokensPath, { throwIfNoEntry: false })?.isFile()
      ? [...loadTokens(settings.tokensPath).values()] : [];
    console.log(JSON.stringify(migrate(settings.dbPath, { backup: parsed.values.backup, owner: parsed.values.owner, staticUsers, ...(parsed.values.to === undefined ? {} : { to: Number(parsed.values.to) }) })));
  } else {
    store = new Store(settings.dbPath);
    if (action === 'retention') {
      if(parsed.values.run) {
        console.log(JSON.stringify(await compact(store,settings,Date.now,{yieldBatch:()=>pause(1)})));
      } else {
        const workspace=parsed.values.workspace;
        if(workspace!==undefined) {name(workspace);check(store.get('SELECT 1 FROM workspaces WHERE name=?',workspace),'WORKSPACE_NOT_FOUND');}
        const {count,topics}=candidates(store,settings,Date.now(),{workspace});
        console.log(JSON.stringify({dry_run:true,count,topics}));
      }
    } else if (action === 'delete-workspace') {
      console.log(JSON.stringify(archiveWorkspace(store, threadId, reason)));
    } else if (action === 'audit') {
      name(threadId); check(store.get('SELECT 1 FROM workspaces WHERE name=?', threadId), 'WORKSPACE_NOT_FOUND');
      console.log(JSON.stringify(store.all('SELECT * FROM workspace_audit WHERE workspace=? ORDER BY id', threadId)));
    } else if (action === 'list') {
      console.log(JSON.stringify(store.all('SELECT * FROM threads ORDER BY last_activity DESC'), null, 2));
    } else {
      const thread = store.transaction(() => {
        const previous = store.get('SELECT * FROM threads WHERE id=?', threadId);
        check(previous, 'THREAD_NOT_FOUND');
        check(action !== 'reopen' || ['escalated', 'resolved'].includes(previous.status), 'THREAD_NOT_REOPENABLE');
        check(action !== 'reopen' || previous.reopen_count < settings.maxReopens, 'REOPEN_LIMIT');
        const now = Date.now();
        // A human reopen grants a fresh cap budget, while preserving all message history.
        store.run(action === 'resolve'
          ? "UPDATE threads SET status='resolved',last_activity=? WHERE id=?"
          : "UPDATE threads SET status='open',msg_count=0,reopen_count=reopen_count+1,cycle_start_id=(SELECT coalesce(max(id),0) FROM messages WHERE thread_id=threads.id),last_activity=? WHERE id=?", now, threadId);
        store.run('INSERT INTO audit(thread_id,action,created,previous_msg_count,reason) VALUES (?,?,?,?,?)',
          threadId, `human:${action}`, now, previous.msg_count, reason ?? null);
        return store.get('SELECT * FROM threads WHERE id=?', threadId);
      });
      console.log(JSON.stringify(thread));
    }
  }
} catch (error) {
  console.error(JSON.stringify(rejection(error)));
  process.exitCode = 1;
} finally { store?.close(); }
