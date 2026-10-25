---
name: bind
description: Owner path for team messaging. Create a workspace, bind this plugin to it at workspace level with the existing credential, then create a topic and issue an invite.
argument-hint: "[broker-url] [name] [workspace]"
---

# Bind team messaging as a workspace owner

Arguments, if given: $ARGUMENTS

Use this when you already hold a credential for the broker (from an earlier join) and want to own a new workspace.

1. Ask for anything missing: the broker URL, your developer name and the new workspace name.
2. If the team-messaging MCP tools are available, call `create_workspace` with that name. Skip this if the workspace already exists and you own it. `create_workspace` does not move the binding: the tools keep acting in the current workspace until step 3 binds the new one and Claude Code restarts.
3. Bind the plugin to the workspace at workspace level (add `--topic '<topic>'` for a topic you are subscribed to):

   ```sh
   team-messaging bind '<broker-url>' --name '<name>' --workspace '<workspace>' --client claude-plugin
   ```

   It reuses the saved credential, fails with `CREDENTIAL_MISSING` if there is none, checks the binding with the broker, and writes the plugin configuration. Read `adapters[0].status` as in the join skill; on `registered`, ask the user to restart Claude Code.
4. After the restart, call `create_topic` for the work area, then `invite` with that topic. The invite returns a single-use code: save it with the Write tool to a private file (then `chmod 600` it) for the person joining, and do not repeat the code in the conversation. They run `/team-messaging:join` with that file.

Changing `arm` with `claude plugin configure` needs a Claude Code restart.
