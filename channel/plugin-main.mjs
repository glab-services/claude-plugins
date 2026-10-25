#!/usr/bin/env node
import { pluginEnv } from './plugin-env.mjs';

// Checked before importing the server: an older Node cannot load the SDK.
if (Number(process.versions.node.split('.')[0]) < 24) {
  console.error(JSON.stringify({ error: 'NODE_VERSION_UNSUPPORTED', hint: 'Install Node.js 24 or later for the team-messaging plugin.' }));
  process.exit(1);
}
const plugin = pluginEnv(process.env);
if (!plugin.configured) {
  console.error(JSON.stringify({ error: 'PLUGIN_NOT_CONFIGURED', hint: 'Run /team-messaging:join or /team-messaging:bind, then restart Claude Code.' }));
  process.exit(1);
}
const { runStdio } = await import('./main.mjs');
await runStdio(plugin.env, { plugin: true });
