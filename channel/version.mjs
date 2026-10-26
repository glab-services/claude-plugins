// The plugin release, reported by the MCP server (server.mjs and the inert
// server in plugin-main.mjs). plugin-env.test.mjs checks it against
// plugin.json, plugin/package.json and plugin/package-lock.json, so a release
// bump that misses one of them fails the tests.
export const VERSION = '0.11.1';
