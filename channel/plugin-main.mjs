#!/usr/bin/env node
import { realpathSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { agentCheck, resolveBinding, tmOptions } from './plugin-env.mjs';

// Review r1 R2: importing this module (a test driver, as watch.test.mjs does
// for watch.mjs) must not start a server; only the entrypoint runs below.
const entry = process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url);
// Checked before importing the server: an older Node cannot load the SDK.
if (entry && Number(process.versions.node.split('.')[0]) < 24) {
  console.error(JSON.stringify({ error: 'NODE_VERSION_UNSUPPORTED', hint: 'Install Node.js 24 or later for the team-messaging plugin.' }));
  process.exit(1);
}
// Plan project-options P-8, P-10. The MCP server is active only in the
// project's agent session: CLAUDE_CODE_SESSION_ID must equal the project
// file's claude_session_id (findings 0.1: it equals the hooks' session_id).
// Every other process, including a `claude mcp list` spawn with its own fresh
// id, serves no tools, reads no token and never contacts the broker, unless a
// later check finds it is now the agent (0.11.1 D6, runInert below).
const plugin = entry ? resolveBinding({ options: tmOptions(process.env), claudeSession: process.env.CLAUDE_CODE_SESSION_ID, projectDir: process.env.CLAUDE_PROJECT_DIR, env: process.env }) : null;
if (entry && plugin.state === 'active') {
  const { runStdio } = await import('./main.mjs');
  // Project-scoped sharing P3a: the team-message window marker lives in the
  // plugin data directory and is keyed by the project root. A missing or
  // relative CLAUDE_PLUGIN_DATA makes every teammate-bearing result
  // SCOPE_WINDOW_UNAVAILABLE (openWindow refuses it), which fails closed.
  const scope = { dataDir: process.env.CLAUDE_PLUGIN_DATA, session: process.env.CLAUDE_CODE_SESSION_ID, root: plugin.root, user: plugin.user };
  await runStdio(plugin.env, { plugin: true, isActive: agentCheck(plugin, { projectDir: process.env.CLAUDE_PROJECT_DIR, env: process.env }), scope });
} else if (entry) await runInert(plugin);

// The inert server: tools [] and NOT_AGENT_SESSION for any call, with no
// Channel. 0.11.1 D6 (F3) supersedes P-10's "promotion never happens in
// place": every PROMOTE_CHECK_MS it re-runs resolveBinding with the inputs it
// started with, and the first time this session is the agent it becomes the
// active server on the same connection and sends tools/list_changed, so a
// claim, bind or join here needs no reconnect. The conditions are those of a
// respawn: the same resolveBinding, the MAC verified with the token read only
// after the session id matches. A claim run by the model through Bash now
// takes effect within about 2 s; the Bash permission prompt is the gate, as
// the claim skill warns. The reconnect is still needed after /clear (this
// process keeps the old id) and after a first credential save (the TM_* env
// is fixed at spawn). Demotion stays final.
// `afterStart` is a test-only dependency (review r1 R2, the decision 153
// pattern): a test driver passes it to make the step after start() throw. The
// entrypoint never passes it, and no environment variable can set it.
export async function runInert(state, { afterStart } = {}) {
  // Declared here: runInert runs from the top-level await above, before a
  // module-level const below it would be initialized.
  const PROMOTE_CHECK_MS = 2000;
  const { Server } = await import('@modelcontextprotocol/sdk/server/index.js');
  const { StdioServerTransport } = await import('@modelcontextprotocol/sdk/server/stdio.js');
  const { CallToolRequestSchema, ListToolsRequestSchema } = await import('@modelcontextprotocol/sdk/types.js');
  const hint = state.state === 'idle' ? 'This project has no team-messaging binding; run /team-messaging:bind or /team-messaging:join in the session that should be the agent; the tools load within a few seconds, reconnect the MCP server if they do not.'
    : state.state === 'inert' ? 'This Claude session is not the agent session of this project; only the user can move the agent here with /team-messaging:claim; the tools load within a few seconds, reconnect the MCP server if they do not.'
      : `The project binding was refused (${state.code}): ${state.hint}`;
  // Only a fixed code and local text reach stderr, never a path or a token.
  console.error(JSON.stringify({ event: 'plugin_inert', state: state.state, ...(state.code ? { code: state.code } : {}) }));
  const { VERSION } = await import('./version.mjs');
  // 0.11.1 D3 (F1): the inert server's instructions, the only text it can give
  // the model before a call: its state and how to leave it, then the shared
  // rules. Instructions are fixed at initialize, so they also have to cover a
  // server that later serves tools in this session.
  const { MESSAGING_RULES } = await import('./server.mjs');
  const { SCOPE_INSTRUCTIONS } = await import('./outbound-scope.mjs');
  const stateLine = state.state === 'idle' ? 'team-messaging is idle in this project: this session has no team-messaging tools until the user runs /team-messaging:bind or /team-messaging:join here.'
    : state.state === 'inert' ? 'This Claude session is not the team-messaging agent of this project, so it has no team-messaging tools. Only /team-messaging:claim, bind or join run in this session makes it the agent (a model-run Bash command can do this when Bash is auto-approved; see docs/setup.md).'
      : `The team-messaging binding of this project was refused (${state.code}): ${state.hint}`;
  const instructions = stateLine + '\nIf team-messaging tools appear later in this session, these rules apply:\n' + MESSAGING_RULES + '\n' + SCOPE_INSTRUCTIONS;
  // D6: the active capability set, because capabilities cannot be added
  // after connect (SDK server/index.js registerCapabilities).
  const server = new Server({ name: 'team-messaging', version: VERSION }, { capabilities: { experimental: { 'claude/channel': {} }, tools: { listChanged: true } }, instructions });
  // An idle project has its own code; inert and refused bindings share NOT_AGENT_SESSION.
  const refused = { error: state.state === 'idle' ? 'PROJECT_NOT_BOUND' : 'NOT_AGENT_SESSION', hint };
  server.setRequestHandler(ListToolsRequestSchema, () => ({ tools: [] }));
  server.setRequestHandler(CallToolRequestSchema, () => ({ content: [{ type: 'text', text: JSON.stringify(refused) }], structuredContent: refused, isError: true }));
  // SESSION_ID_UNKNOWN can never match a project file, so it never re-checks.
  let timer;
  if (state.code !== 'SESSION_ID_UNKNOWN') {
    const failed = new Set();
    // Review r1 R2: set the moment a channel exists, before start(), so no
    // later throw (and no tick that overlaps a slow import) builds a second one.
    let promoted = false, inFlight = false;
    const created = () => { promoted = true; clearInterval(timer); };
    // Review r2 item 3: one attempt at a time, so two ticks cannot both reach
    // created() however long the imports in promote() take.
    timer = setInterval(async () => {
      if (promoted || inFlight) return;
      inFlight = true;
      try { await tick(); } finally { inFlight = false; }
    }, PROMOTE_CHECK_MS);
    const tick = async () => {
      const next = resolveBinding({ options: tmOptions(process.env), claudeSession: process.env.CLAUDE_CODE_SESSION_ID, projectDir: process.env.CLAUDE_PROJECT_DIR, env: process.env });
      if (next.state !== 'active') return;
      try {
        await promote(next, server, { created, afterStart });
      } catch (error) {
        // Before a channel exists (config() or createChannelServer threw) the
        // interval keeps polling (round 1, A4); after, `promoted` stops it.
        // Each distinct code is logged once, a fixed code only.
        const code = /^[A-Z_]{1,64}$/.test(error?.code ?? '') ? error.code : 'PROMOTION_FAILED';
        if (!failed.has(code)) { failed.add(code); console.error(JSON.stringify({ event: 'plugin_promotion_failed', code })); }
      }
    };
    timer.unref();
  }
  process.stdin.once('end', () => { clearInterval(timer); void server.close(); });
  await server.connect(new StdioServerTransport());
}
// D6: the active server of main.mjs runStdio, on the inert server's connection.
// A throw in config() leaves the inert server as it was. createChannelServer
// replaces the handlers as it goes, so a throw inside it can leave some
// replaced; the next tick then builds the server again over them. Once it has
// returned, `created` stops all further attempts (review r1 R2).
async function promote(next, server, { created: onCreated, afterStart } = {}) {
  const { createChannelServer } = await import('./server.mjs');
  const { config } = await import('./config.mjs');
  const scope = { dataDir: process.env.CLAUDE_PLUGIN_DATA, session: process.env.CLAUDE_CODE_SESSION_ID, root: next.root, user: next.user };
  let stopping, channel;
  const stop = () => stopping ??= (async () => { await channel.stop(); await server.close(); })();
  const created = createChannelServer(config(next.env), undefined, { plugin: true, isActive: agentCheck(next, { projectDir: process.env.CLAUDE_PROJECT_DIR, env: process.env }), scope, server, onIdle: stop });
  channel = created.channel;
  onCreated?.();
  // Review r1 S1: as runStdio does, every host request (pings included)
  // counts as activity for the idle timer. The transport is already
  // connected, so its handler is wrapped rather than preset.
  const transport = server.transport, handle = transport?.onmessage;
  if (transport) transport.onmessage = (message, extra) => { if ('method' in message && 'id' in message) created.recordHostRequest(); handle?.(message, extra); };
  // Review r2 item 1: a channel exists from here on, so the tools are
  // announced and the signal handlers registered even if a step throws; the
  // throw still reaches the tick's catch, which logs it.
  try {
    created.start();
    afterStart?.();
    console.error(JSON.stringify({ event: 'plugin_promoted' }));
  } finally {
    server.sendToolListChanged().catch(() => {});
    process.once('SIGINT', () => { void stop(); });
    process.once('SIGTERM', () => { void stop(); });
  }
}
