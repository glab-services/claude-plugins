---
name: status
description: Show whether this session's team-messaging binding is configured and live on the broker, and list workspace memberships.
---

# Team messaging status

1. Run this to check the pinned broker session (it prints only the session state, workspace and topic):

   ```sh
   TM_BROKER_URL='${user_config.broker_url}' TM_TOKEN_FILE='${user_config.token_file}' TM_WORKSPACE='${user_config.workspace}' TM_TOPIC='${user_config.topic}' TM_SESSION_ID='${user_config.session_id}' team-messaging status
   ```

   `active` means the broker has seen this session; `unregistered` means the binding is valid but not used yet. An `{error, hint}` result (for example `PLUGIN_NOT_CONFIGURED` or `NOT_A_MEMBER`) explains what to fix.
2. If the team-messaging MCP tools are available, call `workspaces` to list memberships.
3. Report the configured watcher arm: `${user_config.arm}`. Changing it needs a Claude Code restart.
