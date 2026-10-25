---
name: status
description: Show whether this project is bound for team messaging, which Claude session is its agent, and whether the binding is live on the broker.
---

# Team messaging status

1. Run this (it prints only the state, the agent session, the project root, the workspace and the topic):

   ```sh
   TM_BROKER_URL='${user_config.broker_url}' TM_TOKEN_FILE='${user_config.token_file}' team-messaging status --claude-session '${CLAUDE_SESSION_ID}' --data-dir '${CLAUDE_PLUGIN_DATA}'
   ```

   `state` is `active` (this session is the agent and the broker has seen it), `unregistered` (this session is the agent and the binding is valid but not used yet), `inert` (another session, named in `agent`, is the agent; the user can run `/team-messaging:claim` here) or `idle` (the project has no binding; run `/team-messaging:bind` or `/team-messaging:join`). An `{error, hint}` result (for example `PROJECT_OPTIONS_UNTRUSTED` or `NOT_A_MEMBER`) explains what to fix.
2. If the team-messaging MCP tools are available, call `workspaces` to list memberships.
3. Report the configured watcher arm: `${user_config.arm}`. Changing it needs a Claude Code restart.
