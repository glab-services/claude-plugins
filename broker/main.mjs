import { config, loadTokens } from './config.mjs';
import { Store } from './store.mjs';
import { Broker } from './broker.mjs';
import { createServer } from './server.mjs';
import { Jobs } from './jobs.mjs';

const settings = config();
const tokens = loadTokens(settings.tokensPath);
const store = new Store(settings.dbPath);
const broker = new Broker(store, settings, tokens.values());
const server = createServer(broker, tokens);
const jobs = new Jobs(broker);
server.on('error', error => {
  console.error(JSON.stringify({ event: 'broker_error', error: error.message }));
  store.close();
  process.exitCode = 1;
});
server.listen(settings.port, settings.host, () => {
  jobs.start();
  console.error(JSON.stringify({ event: 'broker_listening', address: server.address() }));
});
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  await jobs.stop();
  for (const stream of broker.streams.values()) stream.end();
  server.close(() => store.close());
  server.closeAllConnections();
}
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
