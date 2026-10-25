import { expireClaims } from './queue.mjs';
import { compact } from './retention.mjs';
import { setImmediate as yieldImmediate } from 'node:timers/promises';

export class Jobs {
  constructor(broker) {
    this.broker = broker;
    this.controller = new AbortController();
  }
  tick() {
    try { this.broker.expireThreads(); }
    catch { this.broker.log(JSON.stringify({ event: 'maintenance_failed' })); }
    try { expireClaims(this.broker,this.broker.now()); }
    catch { this.broker.log(JSON.stringify({ event: 'queue_maintenance_failed' })); }
    const time=this.broker.now();
    if(this.broker.config.retentionEnabled && !this.retaining && (this.nextRetention===undefined || time>=this.nextRetention)) {
      this.nextRetention=time+this.broker.config.retentionInterval;
      this.retaining=compact(this.broker.store,this.broker.config,this.broker.now,{publish:m=>this.broker.publish(m),yieldBatch:yieldImmediate,log:event=>this.broker.log(JSON.stringify(event))})
        .catch(()=>this.broker.log(JSON.stringify({event:'retention_maintenance_failed'})))
        .finally(()=>{this.retaining=null;});
    }
    if (!this.sending && this.broker.config.webhookUrl) {
      this.sending = this.deliver().catch(() => this.broker.log(JSON.stringify({ event: 'webhook_job_failed' }))).finally(() => { this.sending = null; });
    }
  }
  async deliver() {
    const { store, config, now, log } = this.broker;
    const rows = store.all('SELECT * FROM webhook_outbox WHERE sent_at IS NULL AND next_attempt<=? ORDER BY message_id LIMIT ?', now(), config.webhookBatch);
    for (const row of rows) {
      if (this.controller.signal.aborted) break;
      // next_attempt is also a durable delivery lease. A single guarded write
      // elects the sender across processes without changing the outbox schema.
      const at=now(),lease=at+config.webhookTimeoutMs+1000;
      const claimed=store.get(`UPDATE webhook_outbox SET next_attempt=? WHERE message_id=? AND sent_at IS NULL
        AND next_attempt<=? RETURNING attempts,payload`,lease,row.message_id,at);
      if(!claimed)continue;
      try {
        const response = await fetch(config.webhookUrl, {
          method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': JSON.parse(claimed.payload).event_id },
          body: claimed.payload, redirect: 'error',
          signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(config.webhookTimeoutMs)]),
        });
        await response.body?.cancel();
        if (!response.ok) throw new Error('WEBHOOK_REJECTED');
        store.run('UPDATE webhook_outbox SET sent_at=?,attempts=attempts+1 WHERE message_id=? AND sent_at IS NULL AND next_attempt=?', now(), row.message_id,lease);
      } catch {
        if (this.controller.signal.aborted) break;
        const delay = Math.min(config.webhookRetryMaxMs, config.webhookRetryMs * 2 ** Math.min(claimed.attempts, 30));
        store.run('UPDATE webhook_outbox SET attempts=attempts+1,next_attempt=? WHERE message_id=? AND sent_at IS NULL AND next_attempt=?', now() + delay, row.message_id,lease);
        // Do not log webhook URLs or exception text: URLs can contain credentials.
        log(JSON.stringify({ event: 'webhook_retry', message_id: row.message_id, attempt: claimed.attempts + 1 }));
      }
    }
  }
  start() {
    this.tick();
    this.timer = setInterval(() => this.tick(), this.broker.config.maintenanceMs);
    this.timer.unref();
  }
  async stop() {
    clearInterval(this.timer);
    this.controller.abort();
    await this.retaining;
    await this.sending;
  }
}
