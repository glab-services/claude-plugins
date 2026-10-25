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
   - `skipped`: the same binding was written to the plugin while this join ran, for example by another session; ask the user to delete the invite file. Running join again on a plugin that is already bound, `ALREADY_MEMBER` included, does not give `skipped`: it stops with `PLUGIN_ALREADY_BOUND` (step 4).
   - `conflict` or `manual`: show the `hint` and stop. Nothing was overwritten. At this point the invite was already used, so once the plugin is fixed, use `/team-messaging:bind` instead of joining again.
   If `legacy_registration` is present, relay its hint: an older MCP registration for the same binding would run a second channel.
4. An error comes back as `{error, hint}` on stderr; show the hint. Before using the invite, the command checks the plugin, and two errors come from that check. In both cases the invite was not used:
   - `PLUGIN_ALREADY_BOUND`: the plugin already holds a binding, for the workspace and topic named in the hint, and the hint says whether that session is live or has ended. If it is live and the invite is for that same binding, nothing is needed; ask the user to delete the invite file. Otherwise, including every ended binding, the user resets the plugin (uninstall it, then install it again) and runs this skill again with the same invite file.
   - `PLUGIN_NOT_READY`: the plugin is not installed, or its configuration could not be read. The user installs the plugin or fixes its configuration, then runs this skill again.
   Broker errors such as `INVITE_EXPIRED` or `INSECURE_BROKER_URL` come back the same way.

The plugin polls by default and wakes the session through its Stop hook (`arm=rewake`). Changing `arm` (rewake, bash or off) with `claude plugin configure` needs a Claude Code restart.
