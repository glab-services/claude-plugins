# Setup guide: workspaces, topics, invites and joining

This guide walks an operator from a fresh broker to two people talking in one
topic: `alice` owns workspace `myproject` with topic `dev`, and `bob` joins
it. It also covers bootstrapping the first identity and moving a plugin that
is already bound somewhere else. Every behavior below cites the code that
implements it (`path:line`). Anything that was only observed by hand is marked
**observed**, and anything not checked is marked **UNVERIFIED**.

The example broker is `https://broker.example.com`. Replace it with your own,
and use exactly the same URL string everywhere (see
[the credential file name](#the-credential-file-name-hashes-the-broker-url)).

**The plugin works in the one workspace it is bound to.** Its tools act only
in that workspace (and topic), because every call carries it
(`channel/channel.mjs:78`). `create_workspace` is the one exception: it
creates another workspace but does not move the binding
(`channel/server.mjs:129-134`). `workspaces` lists your memberships
everywhere but changes nothing (`:137`). To work in a workspace the plugin is
not bound to, use the scripts in `scripts/ops/` or rebind the plugin
([section 6](#6-switch-a-plugin-that-is-already-bound)).

The scripts in `scripts/ops` read `TM_URL`, `TM_TOKENS`, `TM_SESSION`,
`TM_WORKSPACE` and `TM_TOPIC` from your shell. The plugin takes its binding
from the plugin options instead: `plugin/.mcp.json` sets the MCP server's
`TM_*` values from those options. The hooks, however, read
`CLAUDE_PLUGIN_OPTION_<KEY>` first and fall back to `TM_<KEY>` when that
variable is absent (`channel/plugin-env.mjs:10`). A hook test shows that a
workspace-level binding whose `CLAUDE_PLUGIN_OPTION_TOPIC` is absent sends
the exported `TM_TOPIC` (`channel/test/plugin-session-start.test.mjs`).
Whether Claude Code leaves an empty option unset or exports it as `""` is
**UNVERIFIED**. So an exported `TM_WORKSPACE` or `TM_TOPIC` can reach the
hooks: unset them in the shell you start Claude Code from.

## 1. Prerequisites

- A running broker. See [broker/README.md](../broker/README.md#run): it needs
  Node.js 24+, a token file (`broker/tokens.json` by default) and HTTPS in
  front of it for any remote client.
- Node.js 24+ and pnpm on every machine that runs the terminal commands below,
  plus a checkout of this repository:

  ```sh
  git clone https://github.com/glab-services/claude-plugins.git
  cd claude-plugins
  pnpm install --frozen-lockfile
  ```

- The operator helpers in `scripts/ops/` need **bash, curl and jq**.
- The Claude Code plugin, installed from the `plugin-dist` branch:

  ```sh
  claude plugin marketplace add glab-services/claude-plugins#plugin-dist
  claude plugin install team-messaging@team-messaging-local --scope local
  ```

Inside Claude Code the plugin's skills run its bundled `team-messaging`
command. From a terminal the same CLI is `node channel/cli.mjs` in the
checkout.

### The operator helpers

| Script | What it does |
|---|---|
| `scripts/ops/tm.sh <src> <method> <path> [json \| @file]` | One authenticated broker call. Prints the JSON body, then `HTTP <status>`. Exits 0 only on a 2xx status, 1 on any other status, and curl's own code if the request never completed (`HTTP 000`). |
| `scripts/ops/tm-invite.sh <src> <topic> <out-file>` | Creates an invite and writes only the code to `<out-file>`, mode 0600. Refuses an `<out-file>` that already exists or is a symlink. Prints `invite_id`, `expires` and the file's mode. |
| `scripts/ops/tm-join-static.sh <static-user> <code-file>` | Redeems an invite for a token-file user with its own bearer token. Deletes the code file only after HTTP 201, and keeps it after a network error, 4xx or 429 so you can retry. |

`<src>` is `static:<user>`, which reads that user's token from `$TM_TOKENS`,
or `file:<path>`, which reads a minted credential file. The token only goes
into a curl header through `-H @<(printf …)`, so it never appears in argv,
output or a file the script creates. The scripts read these variables:

```sh
export TM_URL=https://broker.example.com
export TM_TOKENS=broker/tokens.json   # only for static:<user>
export TM_WORKSPACE=myproject         # X-Workspace (default: default)
export TM_SESSION=alice-myproject     # X-Session-Id
export TM_TOPIC=dev                   # X-Topic; leave unset for a workspace-level session
```

The broker binds a session ID to its workspace and topic the first time it
sees it (`broker/broker.mjs:45-51`). Use one session ID for each
workspace/topic pair. Reusing one gives `SESSION_WORKSPACE_MISMATCH` or
`SESSION_TOPIC_MISMATCH`.

## 2. First identity on a fresh broker

### Static users and the owner of `default`

Static users are the keys of the broker token file. On first start the broker
creates workspace `default`. Its owner is `BROKER_DEFAULT_OWNER` if that is
set, or else the **first key** in the token file
(`broker/config.mjs:165`, `broker/workspaces.mjs:12`). Every other static user
becomes a plain member of `default` (`broker/workspaces.mjs:22-26`). The
ops-scripts test starts a broker with no `BROKER_DEFAULT_OWNER` and shows that
the first key owns `default`.

This guide uses one static operator identity, `admin`, plus an optional static
user `ci` that shows the static join path. Create the file without printing
the secrets:

```sh
umask 077
node -e 'const r=()=>require("node:crypto").randomBytes(32).toString("hex");
  console.log(JSON.stringify({admin:r(),ci:r()}))' > broker/tokens.json
```

A static name is reserved. Nobody can mint a credential under it later
(`broker/invites.mjs:46`, `:74`, error `USER_EXISTS`).

### A static non-owner cannot subscribe itself

A static user that does not own a workspace has no topic. A workspace-level
session is for the owner only (`TOPIC_REQUIRED`), and a topic session needs an
existing subscription (`NOT_SUBSCRIBED`) (`broker/broker.mjs:50-51`). Every
non-identity route goes through that check, `/topics/subscribe` included
(`broker/server.mjs:66`):

```sh
TM_TOPIC= TM_WORKSPACE=default TM_SESSION=ci-default \
  scripts/ops/tm.sh static:ci POST /heartbeat '{}'
# {"error":"TOPIC_REQUIRED",...}  HTTP 403  (exit 1)
TM_WORKSPACE=default TM_SESSION=ci-main TM_TOPIC=main \
  scripts/ops/tm.sh static:ci POST /topics/subscribe '{"name":"main"}'
# {"error":"NOT_SUBSCRIBED",...}  HTTP 403  (exit 1)
```

The only way in is an owner invite, redeemed with the user's own bearer
token (next sections).

### Create the bootstrap topic and invite

The owner of `default` creates topic `main` and an invite for it. Each person
needs a separate invite.

```sh
export TM_URL=https://broker.example.com TM_TOKENS=broker/tokens.json
export TM_WORKSPACE=default TM_SESSION=admin-default; unset TM_TOPIC   # workspace level
scripts/ops/tm.sh static:admin POST /topics '{"name":"main"}'               # HTTP 201
scripts/ops/tm-invite.sh static:admin main "$HOME/alice-main.code"
# {"invite_id":"…","expires":…}
# 600 …/alice-main.code
```

Hand the file to the person over a private channel. Do not paste the code into
chat, a ticket or a shell command.

### Mint a new identity (`alice`)

`alice` is not in the token file. She redeems the code from stdin, which mints
her credential and saves it with mode 0600:

```sh
node channel/cli.mjs join https://broker.example.com - --name alice --client none < "$HOME/alice-main.code"
# {"user":"alice","workspace":"default","topic":"main","credential_path":"…/alice.token","adapters":[]}
rm "$HOME/alice-main.code"
```

`--client none` only saves the credential and does not touch any client
configuration. The credential lives at
`${XDG_CONFIG_HOME:-~/.config}/team-messaging/credentials/<hash>-alice.token`
(`channel/credentials.mjs:8-11`). Note `credential_path` in the output, since
`tm.sh file:<path>` uses it.

### A static user joins (`ci`)

`join` from the CLI cannot do this for a token-file user, because it only
reuses a credential saved under `~/.config/team-messaging/credentials`
(`channel/cli.mjs:116-120`). Use the static helper. It sends the code from a
0600 body file and deletes the code file only after HTTP 201. After a network
error, a 4xx or a 429 it keeps the file, so you can retry with the same code:

```sh
scripts/ops/tm-invite.sh static:admin main "$HOME/ci-main.code"   # as admin, scope as above
scripts/ops/tm-join-static.sh ci "$HOME/ci-main.code"
# {"user":"ci","workspace":"default","topic":"main"}  HTTP 201
```

With a valid bearer token the broker adds the subscription and mints nothing
(`broker/invites.mjs:70-78`).

### The credential file name hashes the broker URL

The file name begins with the first 16 hex digits of SHA-256 of the normalized
broker URL (`channel/credentials.mjs:10`). A hostname and its IP address, or
`127.0.0.1` and `localhost`, therefore point to different files. With a
different spelling, `join` finds no credential, tries to mint the name again
and fails with `USER_EXISTS`, and
`bind` fails with `CREDENTIAL_MISSING`. The ops-scripts test proves the
`USER_EXISTS` case with `127.0.0.1` against `localhost`. Use one URL string in
every command and in the plugin.

## 3. Create a workspace and topic as its owner

Any identity can create a workspace and becomes its owner
(`broker/server.mjs:75`, `broker/broker.mjs:100-108`, limit `WORKSPACE_LIMIT`).
Only the workspace owner can create topics and invites
(`broker/topics.mjs:35`, `broker/invites.mjs:11-13`).

### From a terminal

```sh
cred=…/alice.token   # credential_path from alice's join
scripts/ops/tm.sh "file:$cred" POST /workspaces '{"name":"myproject"}'            # HTTP 201
export TM_WORKSPACE=myproject TM_SESSION=alice-myproject; unset TM_TOPIC   # workspace level
scripts/ops/tm.sh "file:$cred" POST /topics '{"name":"dev"}'                       # HTTP 201
scripts/ops/tm.sh "file:$cred" POST /topics/subscribe '{"name":"dev"}'             # HTTP 200
```

A static owner uses `static:admin` the same way.

### Through the plugin

`/team-messaging:bind` reuses a saved credential. A token-file user has none,
so the plugin owner must be a minted identity like `alice`
(`channel/cli.mjs:162`, `CREDENTIAL_MISSING`).

The plugin's tools act only in the workspace it is bound to.
`create_workspace` is the one exception, and it does not move the binding:
after step 1 the tools still act in the old workspace until step 2 binds the
new one and Claude Code restarts. To work in an unbound workspace, use the
terminal commands above or rebind.

1. If the plugin's MCP tools are not loaded yet, create the workspace from the
   terminal as above. Otherwise the skill calls `create_workspace`.
2. Run `/team-messaging:bind https://broker.example.com alice myproject`. It
   runs `team-messaging bind https://broker.example.com --name alice
   --workspace myproject --client claude-plugin` and writes a workspace-level
   binding. Restart Claude Code.
3. Call the `create_topic` tool with `{"name":"dev"}`, then `subscribe` with
   `{"name":"dev"}` to subscribe alice to it.
4. Call `invite` with `{"topics":["dev"]}`. The skill saves the code to a
   private file with mode 0600 and does not repeat it.

## 4. Invites

- **Single use.** Redeeming an invite marks it used. A second redemption fails
  with `INVITE_USED` (`broker/invites.mjs:66`, `:85`).
- **Exactly one open topic.** The topic must exist in the workspace
  (`broker/topics.mjs:20-22`, `INVALID_INVITE_TOPICS`).
- **Expiry.** An invite lasts `INVITE_TTL` seconds, 3600 by default
  (`broker/config.mjs:180`). The ops-scripts test checks
  `expires ≈ now + 3600 s`. After that, redemption fails with
  `INVITE_EXPIRED`.
- At most `INVITE_LIMIT` (default 20) pending invites per workspace.
- The code goes only to a 0600 file. `tm-invite.sh` never prints it or puts it
  in argv. `join` reads it from stdin (`-`), and `tm-join-static.sh` sends it
  from a 0600 body file. The ops-scripts test checks that the code never
  appears in the scripts' stdout or stderr.

```sh
export TM_WORKSPACE=myproject TM_SESSION=alice-myproject
scripts/ops/tm-invite.sh "file:$cred" dev "$HOME/bob-myproject-dev.code"
```

## 5. A member joins (`bob`)

**`join` checks the plugin binding first.** From plugin version 0.9.4,
`join --client claude-plugin` reads the plugin options before it redeems the
invite (`channel/cli.mjs:70-100`, called at `:118`). It stops with an error on
stderr, and the invite stays unused, in two cases:

- `PLUGIN_NOT_READY`: the plugin is not installed, or its options cannot be
  read. Install the plugin or fix its configuration, then join again.
- `PLUGIN_ALREADY_BOUND`: the plugin holds a saved binding, for the
  workspace and topic named in the hint. The hint also says whether that
  session is live (or could not be checked) or has ended. For a live binding
  to the same workspace and topic as the invite, nothing is needed.
  Otherwise, and for every ended binding, reset the plugin as in
  [section 6](#6-switch-a-plugin-that-is-already-bound) and run the same join
  again.

An ended binding is refused too, because the invite's workspace and topic are
known only after it is redeemed. Even re-joining exactly the same ended
binding now needs the reset first.

Older plugin versions redeem the invite before they check the binding. There,
a plugin bound elsewhere uses up the invite and the join reports `conflict`;
a retry gives `INVITE_USED`. With an older version, run
`/team-messaging:status` first and reset a plugin bound elsewhere **before**
you join. If an invite was used up this way, either ask the owner for a new
one, or, since you are already a member, reset the plugin and run `bind` as
described at the end of this section.

In Claude Code, with the code saved in a private file:

```
/team-messaging:join https://broker.example.com bob ~/bob-myproject-dev.code
```

From a terminal, the same thing:

```sh
node channel/cli.mjs join https://broker.example.com - --name bob --client claude-plugin < "$HOME/bob-myproject-dev.code"
```

Both save a 0600 credential and write the binding (broker URL, credential path,
workspace, topic, a fresh session ID, `mode`, `arm`) into the plugin options.
`adapters[0].status` should be `registered`. Then:

```sh
rm "$HOME/bob-myproject-dev.code"
```

and restart Claude Code.

### An identity that already has a credential joins a second workspace

This was tested with real processes: `bob` already holds a minted credential
(for `default/main`) and runs `join` again with a new invite for
`myproject/dev`, using the same URL and name. The CLI finds the saved
credential and sends it as the bearer token. The broker adds the membership
and the subscription and **mints no new token**, and the credential file is
left byte-for-byte unchanged. Afterwards the same credential opens sessions in
both `default/main` and `myproject/dev` (`channel/cli.mjs:116-120`,
`broker/invites.mjs:70-71`).

```sh
node channel/cli.mjs join https://broker.example.com - --name bob --client none < "$HOME/bob-myproject-dev.code"
# {"user":"bob","workspace":"myproject","topic":"dev","credential_path":"…/bob.token","adapters":[]}
```

With `--client claude-plugin` and a plugin still bound to `default/main`,
the join stops with `PLUGIN_ALREADY_BOUND` before it redeems the invite
(`channel/test/plugin-join.test.mjs`). Reset the plugin as in section 6 and
run the same join again.

Older plugin versions redeem the invite **before** they check the plugin
binding. There the join reports `conflict` and writes no options, but bob is
now a member and the invite is used. Running the same invite again gives
`INVITE_USED`, because that check comes before the `ALREADY_MEMBER` check
(`broker/invites.mjs:66`, `:71`). Switch the plugin as in section 6 and bind
with
`team-messaging bind https://broker.example.com --name bob --workspace myproject --topic dev --client claude-plugin`,
which needs no invite.

## 6. Switch a plugin that is already bound

### Bind and join never overwrite a binding

If the plugin's saved broker URL, credential path, workspace or topic differs
from the requested binding, or the saved session ID is malformed, `bind` and
`join` return `conflict` and write nothing. They also return `conflict` when
the saved configuration cannot be read in a known shape
(`channel/plugin-config.mjs:57-61`). For a matching binding, the saved session
is checked with the broker. `active` or `unregistered` gives `skipped`, and
`SESSION_ENDED` lets the binding be replaced. Any other result, including an
unreachable broker, gives `conflict` (`:62-64`). If `claude plugin configure`
itself fails, the status is `failed` (`:44`).

### Reset with uninstall and install

```sh
claude plugin uninstall team-messaging@team-messaging-local --scope local
claude plugin install team-messaging@team-messaging-local --scope local
```

Checked with claude 2.1.310 under a scratch HOME: uninstalling the **last**
install removes the plugin options and the plugin data directory
`~/.claude/plugins/data/team-messaging-team-messaging-local`. If the plugin is
still installed in another project, the options and the data directory stay.
The credential under `~/.config/team-messaging/credentials` is outside Claude
Code's directories and is kept, so the next `bind` or `join` can reuse it.

### Under umask 002, make the data directory private

Under umask 002, Claude Code creates the plugin directories with mode 0775.
The watcher state directory `${CLAUDE_PLUGIN_DATA}/state` accepts that only
when a directory above it, such as a 0700 `~/.claude`, belongs to you with no
group or world bits (`channel/watch-state.mjs:18-21`,
`channel/credentials.mjs:12-21`). Otherwise the hooks report
`INSECURE_CREDENTIAL_DIR` and name the directory
(`channel/credentials.mjs:24-27`). After reinstalling, make the directory
private again:

```sh
mkdir -p ~/.claude/plugins/data/team-messaging-team-messaging-local
chmod 700 ~/.claude/plugins/data/team-messaging-team-messaging-local
```

Then run `/team-messaging:bind` or `/team-messaging:join` for the new binding
and **restart Claude Code**. Options only take effect after a restart.

### Where the options are stored: one binding per user config

Checked with claude 2.1.310 under a scratch HOME: `claude plugin install
--scope local` records only `enabledPlugins` in the project's
`.claude/settings.local.json`. `claude plugin configure`, which `bind` and
`join` call (`channel/plugin-config.mjs:41-44`), writes the options under
`pluginConfigs` in the **user** settings `~/.claude/settings.json`. A second
project with its own `--scope local` install saw the same `workspace` and
`topic`. So the install scope does not separate bindings: there is **one
binding per user config directory (HOME)**, shared by every project where the
plugin is installed. To run two bindings at once, use separate HOMEs. Whether
`CLAUDE_CONFIG_DIR` alone separates them is **UNVERIFIED**.

## 7. Known gaps

- **Observed on the Claude desktop app:** after a restart, the watcher was
  armed only once a turn ran. Type one message in the session after
  restarting so that messages wake it.

## 8. Troubleshooting

| Code | Where it comes from | Meaning | Fix |
|---|---|---|---|
| `conflict` | `adapters[0].status`, `channel/plugin-config.mjs:57-64` | The plugin is bound to another broker, credential, workspace or topic, its configuration could not be read, or the saved session could not be checked (for example, the broker is unreachable). Nothing was written. A `join` that reports this has already redeemed the invite. From 0.9.4, `join` refuses any saved binding before the invite (`PLUGIN_ALREADY_BOUND`), so it reports `conflict` only when the binding changed during the join. | Reset as in section 6, then `bind` (the membership already exists). |
| `PLUGIN_ALREADY_BOUND` | `join` stderr, `channel/cli.mjs:70-100` | The plugin holds a saved binding for the workspace and topic named in the hint. The hint says whether that session is live (or could not be checked) or has ended. The invite was not used. | For a live binding to the same workspace and topic, nothing is needed. Otherwise, and for an ended binding, reset as in section 6 and run the same `join` again. |
| `PLUGIN_NOT_READY` | `join` stderr, `channel/cli.mjs:70-100` | No `claude` CLI on PATH, the plugin is not installed, or its options could not be read. The invite was not used. | Install Claude Code and the plugin, or fix the plugin configuration, then run the same `join` again. |
| `manual` | `adapters[0].status`, `channel/plugin-config.mjs:44`, `:54-56` | No `claude` CLI on PATH, or the plugin is not installed, so nothing could be written. | Install Claude Code and the plugin, then rerun. After a `join`, rerun as `bind`. |
| `failed` | `adapters[0].status`, `channel/plugin-config.mjs:44` | `claude plugin configure` exited non-zero, so the options were not saved. After a `join`, the invite is already redeemed. | Check `claude plugin list` and the Claude Code output, then rerun as `bind`. |
| `CREDENTIAL_MISSING` | `channel/cli.mjs:162` | `bind` found no saved credential for this exact broker URL and name. | Join with an invite first, or use the exact URL string the credential was saved with. |
| `TOPIC_REQUIRED` | `broker/broker.mjs:50` | A workspace-level session, which only the workspace owner may open. | Members bind with `--topic` / set `TM_TOPIC` to a subscribed topic. |
| `NOT_SUBSCRIBED` | `broker/broker.mjs:51` | A topic session for a topic this user is not subscribed to. | Redeem an invite for that topic (section 5 or the static join). |
| `INVITE_EXPIRED` | `broker/invites.mjs:66` | The invite is older than `INVITE_TTL` (default 3600 s). | Ask the owner for a new invite. |
| `INSECURE_CREDENTIAL_DIR` | `channel/credentials.mjs:24-27`, `:59-70` | A credential or state directory, or one of its ancestors, is group- or world-writable, not yours, or a user-owned symlink. The hint names the directory. | Run the `chmod go-w '<dir>'` from the hint. For the plugin data directory, run `chmod 700` as in section 6. |
| `INSECURE_BROKER_URL` | `channel/cli.mjs:29-30` | Plain `http://` to a host that is not loopback. | Use `https://`. HTTP is allowed only for `127.0.0.1`, `[::1]` and `localhost`. |
