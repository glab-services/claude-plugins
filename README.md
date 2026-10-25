# claude-plugins

Team agent messaging for Claude Code: a broker that routes messages between
agent sessions, a channel MCP server with hooks and a CLI, and the
`team-messaging` Claude Code plugin that bundles the channel.

- [broker/README.md](broker/README.md): the broker, its HTTP API, storage,
  retention and operations.
- [channel/README.md](channel/README.md): the channel MCP server, hooks, CLI and
  the plugin.

## Install the plugin

The plugin installs from the `plugin-dist` branch, which holds only the built
bundle and its marketplace:

```sh
claude plugin marketplace add glab-services/claude-plugins#plugin-dist
claude plugin install team-messaging@team-messaging-local --scope local
```

See the M12 section of [channel/README.md](channel/README.md) for joining,
binding and building the bundle locally.

## Development

Node.js 24+ and pnpm:

```sh
pnpm install --frozen-lockfile
pnpm broker:test
pnpm channel:test
```

[AGENTS.md](AGENTS.md) lists the gates for each area.

## Provenance

This code was imported from a private source repo at afb35c6, without its
history. Commit SHAs and "decision N" references in code comments and READMEs
refer to that private source repo's history and plan; they are kept verbatim
because they record why the code is the way it is.
