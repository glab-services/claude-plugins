---
name: join
description: Join a team-messaging workspace topic with an invite code the user saved in a private file, and bind this project to it with this Claude session as its agent session.
argument-hint: "[broker-url] [name] [invite-file]"
---

# Join team messaging with an invite

Arguments, if given: $ARGUMENTS

Run this in the Claude session that should be this project's agent session: only that session uses the plugin in this project.

1. Ask the user for whatever is missing: the broker URL (HTTPS, or http://127.0.0.1 for a broker on this machine), their developer name, and the path of a private file they created that holds only the invite code. Never ask for the code itself in the conversation.
2. Run this exactly, with the invite file redirected to stdin:

   ```sh
   team-messaging join '<broker-url>' - --name '<name>' --client claude-plugin --claude-session '${CLAUDE_SESSION_ID}' --data-dir '${CLAUDE_PLUGIN_DATA}' < '<invite-file>'
   ```

   Never `cat`, `echo` or print the invite file, and never pass the code as an argument. The command saves a mode-0600 credential, writes only its path to the plugin configuration, and records this session as the project's agent in `.claude/settings.local.json`.
3. Read the JSON result. `adapters[0].status` is:
   - `registered`: tell the user to delete the invite file, then relay the `hint`: the tools appear within a few seconds; if they do not, reconnect the team-messaging MCP server (`/mcp`, then team-messaging, then Reconnect). Restart with `claude --continue` when the credential was just saved.
   - `conflict`, `failed` or `manual`: show the `hint` and stop. At this point the invite was already used; once the plugin is fixed, ask the owner for a new invite.
   If `legacy_registration` is present, relay its hint: an older MCP registration for the same binding would run a second channel.
4. An error comes back as `{error, hint}` on stderr; show the hint. Before using the invite, the command checks the project and the plugin; for these errors the invite was not used:
   - `PLUGIN_ALREADY_BOUND`: this project is already bound (the hint names the file and its agent session), or the plugin's saved credential is for another broker or name. Ask the user to delete the invite file if the project is already bound as intended.
   - `PLUGIN_NOT_READY`: the plugin is not installed, or its configuration could not be read. The user installs the plugin or fixes its configuration, then runs this skill again.
   - `PROJECT_OPTIONS_UNTRUSTED`, `PROJECT_OPTIONS_INVALID` or `PROJECT_UNKNOWN`: show the hint; the project file must be fixed, or Claude Code restarted in the project, first.
   Broker errors such as `INVITE_EXPIRED` or `INSECURE_BROKER_URL` come back the same way.

The plugin polls by default and wakes the agent session through its Stop hook (`arm=rewake`). Changing `arm` (rewake, bash or off) with `claude plugin configure` needs a Claude Code restart.
