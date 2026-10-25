---
name: join
description: Join a team-messaging workspace topic with an invite code the user saved in a private file, and bind this plugin to it.
argument-hint: "[broker-url] [name] [invite-file]"
---

# Join team messaging with an invite

Arguments, if given: $ARGUMENTS

1. Ask the user for whatever is missing: the broker URL (HTTPS, or http://127.0.0.1 for a broker on this machine), their developer name, and the path of a private file they created that holds only the invite code. Never ask for the code itself in the conversation.
2. Run this exactly, with the invite file redirected to stdin:

   ```sh
   team-messaging join '<broker-url>' - --name '<name>' --client claude-plugin < '<invite-file>'
   ```

   Never `cat`, `echo` or print the invite file, and never pass the code as an argument. The command saves a mode-0600 credential and writes only its path to the plugin configuration.
3. Read the JSON result. `adapters[0].status` is:
   - `registered`: tell the user to delete the invite file, then restart Claude Code to load the binding.
   - `skipped`: this plugin is already bound to that workspace and topic; ask the user to delete the invite file.
   - `conflict` or `manual`: show the `hint` and stop. Nothing was overwritten.
   If `legacy_registration` is present, relay its hint: an older MCP registration for the same binding would run a second channel.
4. A broker error (for example `INVITE_EXPIRED` or `INSECURE_BROKER_URL`) comes back as `{error, hint}` on stderr; show the hint.

The plugin polls by default and wakes the session through its Stop hook (`arm=rewake`). Changing `arm` (rewake, bash or off) with `claude plugin configure` needs a Claude Code restart.
