---
name: claim
description: Make this Claude session the agent session of this project for team messaging. Only the user runs this.
disable-model-invocation: true
---

# Claim the agent role for this project

Only the user can start this skill: `disable-model-invocation` keeps the model from invoking it. It moves this project's team-messaging agent role to this Claude session; the previous agent session stops using the plugin. The command below still runs through Bash, so the model could run it directly; the Bash permission prompt is what stops that. With Bash auto-approved, a model can claim the project, so approve this command only when you started the claim yourself.

1. Run:

   ```sh
   TM_BROKER_URL='${user_config.broker_url}' TM_TOKEN_FILE='${user_config.token_file}' team-messaging claim --claude-session '${CLAUDE_SESSION_ID}' --data-dir '${CLAUDE_PLUGIN_DATA}'
   ```

2. Read the JSON result:
   - `claimed`: report the workspace, topic and project root, then relay the hint: the tools appear within a few seconds; if they do not, reconnect the team-messaging MCP server (`/mcp`, then team-messaging, then Reconnect) or restart with `claude --continue`, and after `/clear` a reconnect is always needed. The broker session is kept, so the inbox, cursors, leases and queue claims carry over. The previous agent session stops within about 5 s.
   - `already_agent`: this session already is the agent; nothing changed.
3. An error comes back as `{error, hint}` on stderr; show the hint. `PROJECT_OPTIONS_UNTRUSTED` means the project file was not written for this checkout by this user (copied, committed or edited), so it cannot be claimed; `PROJECT_NOT_BOUND` means there is nothing to claim.
