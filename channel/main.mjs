#!/usr/bin/env node
import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { config } from './config.mjs';
import { createChannelServer } from './server.mjs';

// isActive (plugin-main.mjs, plan project-options AM-6) is passed through to
// the server; without it the server is always active, as before. So is scope
// (project-scoped sharing P3a), which only plugin-main.mjs passes.
export async function runStdio(env, { plugin = false, isActive, scope } = {}) {
  try {
    const { server, channel, recordHostRequest } = createChannelServer(config(env), undefined, { onIdle: stop, plugin, isActive, scope });
    let stopping;
    function stop() { return stopping ??= (async () => { await channel.stop(); await server.close(); })(); }
    process.once('SIGINT', () => { void stop(); });
    process.once('SIGTERM', () => { void stop(); });
    process.stdin.once('end', () => { void stop(); });
    const transport = new StdioServerTransport();
    // Protocol.connect preserves this public transport callback before dispatching requests.
    transport.onmessage = message => { if ('method' in message && 'id' in message) recordHostRequest(); };
    await server.connect(transport);
  } catch (error) {
    console.error(JSON.stringify({ error: error.code ?? 'CHANNEL_START_FAILED', hint: error.hint ?? 'Check the channel environment and installed dependencies.' }));
    process.exitCode = 1;
  }
}
// Importing this module (plugin-main.mjs) must not start a server.
if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) await runStdio(process.env);
