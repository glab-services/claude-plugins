#!/usr/bin/env node
import { agentCheck, resolveBinding, tmOptions } from './plugin-env.mjs';

// Checked before importing the server: an older Node cannot load the SDK.
if (Number(process.versions.node.split('.')[0]) < 24) {
  console.error(JSON.stringify({ error: 'NODE_VERSION_UNSUPPORTED', hint: 'Install Node.js 24 or later for the team-messaging plugin.' }));
  process.exit(1);
}
// Plan project-options P-8, P-10. The MCP server is active only in the
// project's agent session: CLAUDE_CODE_SESSION_ID must equal the project
// file's claude_session_id (findings 0.1: it equals the hooks' session_id).
// Every other process, including a `claude mcp list` spawn with its own fresh
// id, serves no tools, reads no token and never contacts the broker.
const plugin = resolveBinding({ options: tmOptions(process.env), claudeSession: process.env.CLAUDE_CODE_SESSION_ID, projectDir: process.env.CLAUDE_PROJECT_DIR, env: process.env });
if (plugin.state === 'active') {
  const { runStdio } = await import('./main.mjs');
  await runStdio(plugin.env, { plugin: true, isActive: agentCheck(plugin, { projectDir: process.env.CLAUDE_PROJECT_DIR, env: process.env }) });
} else await runInert(plugin);

// The inert server: tools [] and NOT_AGENT_SESSION for any call, with no
// Channel. Promotion never happens in place (P-10): after a bind, join or
// claim, the session reconnects the MCP server or restarts.
async function runInert(state) {
  const { Server } = await import('@modelcontextprotocol/sdk/server/index.js');
  const { StdioServerTransport } = await import('@modelcontextprotocol/sdk/server/stdio.js');
  const { CallToolRequestSchema, ListToolsRequestSchema } = await import('@modelcontextprotocol/sdk/types.js');
  const hint = state.state === 'idle' ? 'This project has no team-messaging binding; run /team-messaging:bind or /team-messaging:join in the session that should be the agent, then reconnect the MCP server.'
    : state.state === 'inert' ? 'This Claude session is not the agent session of this project; only the user can move the agent here with /team-messaging:claim, then reconnect the MCP server.'
      : `The project binding was refused (${state.code}): ${state.hint}`;
  // Only a fixed code and local text reach stderr, never a path or a token.
  console.error(JSON.stringify({ event: 'plugin_inert', state: state.state, ...(state.code ? { code: state.code } : {}) }));
  const { VERSION } = await import('./version.mjs');
  const server = new Server({ name: 'team-messaging', version: VERSION }, { capabilities: { tools: { listChanged: true } } });
  // An idle project has its own code; inert and refused bindings share NOT_AGENT_SESSION.
  const refused = { error: state.state === 'idle' ? 'PROJECT_NOT_BOUND' : 'NOT_AGENT_SESSION', hint };
  server.setRequestHandler(ListToolsRequestSchema, () => ({ tools: [] }));
  server.setRequestHandler(CallToolRequestSchema, () => ({ content: [{ type: 'text', text: JSON.stringify(refused) }], structuredContent: refused, isError: true }));
  process.stdin.once('end', () => { void server.close(); });
  await server.connect(new StdioServerTransport());
}
