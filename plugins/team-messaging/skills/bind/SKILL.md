---
name: bind
description: Bind this project for team messaging with this Claude session as its agent session. An owner binds a workspace (created if it does not exist) and optionally a topic (created if needed); a member binds a topic they are already subscribed to. Also adopts a 0.9 global binding.
argument-hint: "[broker-url] [name] [workspace] [topic]"
---

# Bind this project as a workspace owner

Arguments, if given: $ARGUMENTS

Run this in the Claude session that should be this project's agent session: only that session uses the plugin in this project. You need a credential for the broker from an earlier `/team-messaging:join`.

1. Ask for anything missing: the broker URL, your developer name, the workspace name and, for a topic binding, the topic name (and `broadcast` or `queue` for a new topic; broadcast is the default).
2. Run one command. It reuses the saved credential, creates the workspace if it does not exist (or reuses it when you own it), creates the topic if needed (or reuses an open one), subscribes you, and records this session as the project's agent in `.claude/settings.local.json`. Leave out `--topic` for a workspace-level binding, and add `--mode` only for a new topic:

   ```sh
   team-messaging bind '<broker-url>' --name '<name>' --workspace '<workspace>' --topic '<topic>' --claude-session '${CLAUDE_SESSION_ID}' --data-dir '${CLAUDE_PLUGIN_DATA}'
   ```

3. Read the JSON result. `adapters[0].status` is:
   - `registered`: the project is bound to this session. Relay the `hint`: reconnect the team-messaging MCP server (`/mcp`, then team-messaging, then Reconnect), or restart with `claude --continue` when the hint says the credential was just saved.
   - `skipped`: this project is already bound to the same workspace and topic for this session; nothing changed.
   - `conflict`, `failed` or `manual`: show the `hint` and stop.
4. An error comes back as `{error, hint}` on stderr; show the hint. Nothing is written for any of these:
   - `PLUGIN_ALREADY_BOUND`: this project is bound to another workspace, topic or agent session (the hint names the file), or the plugin's saved credential is for another broker or name.
   - `WORKSPACE_NOT_OWNED`: the workspace exists and is not yours; choose another name, or ask its owner for an invite and use `/team-messaging:join`.
   - `TOPIC_CLOSED` or `TOPIC_MODE_MISMATCH`: choose another topic name, or leave out `--mode` to reuse the topic.
   - `CREDENTIAL_MISSING`: run `/team-messaging:join` with an invite first.
   - `NOT_A_MEMBER` or `NOT_SUBSCRIBED`: you are not a member of that workspace, or not subscribed to that topic; ask its owner for an invite and use `/team-messaging:join`.
   A rerun after any failure skips what already exists.

A member (not the owner) of the workspace who is already subscribed to the topic, for example from an earlier `/team-messaging:join` in another project, runs the same command with `--topic` and without `--mode`: it binds this project to that topic with its own broker session, creates nothing and needs no new invite.
5. After reconnecting, invite people as a separate step: call `invite` with the topic. The invite returns a single-use code: save it with the Write tool to a private file (then `chmod 600` it) for the person joining, and do not repeat the code in the conversation. They run `/team-messaging:join` with that file in their own project.

## Upgrading a 0.9 global binding

If the session-start line says the plugin still holds a 0.9 binding, keep that binding (and its broker session, inbox and leases) by adopting it into this project:

```sh
team-messaging bind --adopt-global --claude-session '${CLAUDE_SESSION_ID}' --data-dir '${CLAUDE_PLUGIN_DATA}'
```

`adopted` means the project now holds the binding and the global options hold only the credential; restart with `claude --continue`. `NOTHING_TO_ADOPT` means there is no 0.9 binding left. A 0.9 binding is adopted into one project only.

Changing `arm` with `claude plugin configure` needs a Claude Code restart.
