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

**The plugin works in one project, through one Claude session.** From plugin
0.10, a project is bound by its own `.claude/settings.local.json`, and only
the Claude session recorded there (the project's *agent session*) uses the
plugin; every other session in that project gets no tools, makes no broker
request and sees one line at session start
([section 6](#one-agent-session-per-project)). Its tools act only in the
bound workspace (and topic), because every call carries it
(`channel/channel.mjs:78`). `create_workspace` is the one exception: it
creates another workspace but does not move the binding
(`channel/server.mjs:154-160`). `workspaces` lists your memberships
everywhere but changes nothing (`:162`). To work in another workspace, bind
another project, or use the scripts in `scripts/ops/`.

The scripts in `scripts/ops` read `TM_URL`, `TM_TOKENS`, `TM_SESSION`,
`TM_WORKSPACE` and `TM_TOPIC` from your shell. The plugin reads none of
those. Its global options hold only the credential (broker URL and token
file, plus `mode` and `arm`): the hooks read them as
`CLAUDE_PLUGIN_OPTION_BROKER_URL`, `_TOKEN_FILE`, `_MODE` and `_ARM`, and the
MCP server and the skills' commands as `TM_BROKER_URL`, `TM_TOKEN_FILE`,
`TM_MODE` and `TM_ARM` from `plugin/.mcp.json` and the skill text
(`channel/plugin-env.mjs:68-90`). No hook reads a `TM_*` value, and no
process reads a workspace, topic or session from the environment
(`channel/test/plugin-session-start.test.mjs`, "no env value reaches a
hook").

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
(`channel/cli.mjs:142-156`). Use the static helper. It sends the code from a
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
(`channel/cli.mjs:219`, `CREDENTIAL_MISSING`).

Run it in the Claude session that should be the project's agent session. One
command handles both cases: the workspace is created if it does not exist,
or reused when you own it, and the topic is created or reused
(`channel/cli.mjs:210-275`):

```
/team-messaging:bind https://broker.example.com alice myproject dev
```

It runs `team-messaging bind https://broker.example.com --name alice
--workspace myproject --topic dev --claude-session '${CLAUDE_SESSION_ID}'
--data-dir '${CLAUDE_PLUGIN_DATA}'`. The steps run in order, and each one is
idempotent:

1. The local pre-check: a project already bound to another workspace, topic
   or agent session is refused with `PLUGIN_ALREADY_BOUND` before anything is
   sent. The same binding for the same session gives `skipped`.
2. `GET /workspaces` (rows `{name, role, joined}`, no session needed). A
   workspace you own is reused; one you do not own, or a `WORKSPACE_EXISTS`
   for a name you are not an owner of, is `WORKSPACE_NOT_OWNED` and nothing
   is written. A missing workspace is created (`WORKSPACE_LIMIT` is reported).
3. With `--topic`: `POST /topics` (add `--mode queue` for a queue topic). An
   existing open topic is reused, a closed one gives `TOPIC_CLOSED`, and a
   `--mode` that differs from the existing one gives `TOPIC_MODE_MISMATCH`.
4. `POST /topics/subscribe` (idempotent).
5. A probe of the project's new broker session, then the project file.

Steps 3 and 4 use a one-off workspace-level session `claude-setup-<random>`,
because a broker session's scope is fixed at its first heartbeat
(`broker/broker.mjs:47-48`). The broker has no route to end a session, so the
setup session is left unused. The project's own session is only used at its
binding scope. Without `--topic` the binding is at workspace level and no
topic is created.

**Members bind too.** A member (not the owner) who is subscribed to the topic,
for example after a `join` in another project, binds a further project with
the same command and `--topic`, without `--mode`. It skips steps 3 and 4,
creates nothing, needs no new invite and gives the project its own broker
session. It refuses with `NOT_SUBSCRIBED` for a topic the member is not
subscribed to, and `NOT_A_MEMBER` for a workspace they are not a member of.
A member without `--topic` gets `WORKSPACE_NOT_OWNED`, because a
workspace-level binding is the owner's (`channel/cli.mjs`, AM-9). A rerun after any failure skips what exists; a rerun after
success is `skipped`.

Then reconnect the team-messaging MCP server (`/mcp`, then team-messaging,
then Reconnect), or restart with `claude --continue` if the credential was
saved for the first time. Inviting is a separate step: call `invite` with
`{"topics":["dev"]}`. The skill saves the code to a private file with mode
0600 and does not repeat it.

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

**`join` checks the project and the plugin first.** `join --client
claude-plugin` runs in the session that should be the project's agent
session. Before it redeems the invite (`channel/cli.mjs:146-151`) it stops
with an error on stderr, and the invite stays unused, when:

- `PLUGIN_ALREADY_BOUND`: this project is already bound (the hint names the
  file, its workspace and topic, and its agent session), or the plugin's
  saved credential is for another broker or name. A 0.9 global workspace
  binding never blocks (P-13).
- `PLUGIN_NOT_READY`: the plugin is not installed, or its options cannot be
  read. Install the plugin or fix its configuration, then join again.
- `PROJECT_OPTIONS_UNTRUSTED`, `PROJECT_OPTIONS_INVALID` or
  `PROJECT_UNKNOWN`: see [section 8](#8-troubleshooting).

Plugin versions before 0.9.4 redeem the invite before they check the
binding. There, a plugin bound elsewhere uses up the invite and the join
reports `conflict`; a retry gives `INVITE_USED`.

In Claude Code, with the code saved in a private file:

```
/team-messaging:join https://broker.example.com bob ~/bob-myproject-dev.code
```

From a terminal, the same thing:

```sh
node channel/cli.mjs join https://broker.example.com - --name bob --client claude-plugin --claude-session <agent-session-id> --project ~/src/myproject < "$HOME/bob-myproject-dev.code"
```

Both save a 0600 credential, save its broker URL and path in the plugin
options when none is saved yet, and write the project file with a fresh
broker session for the invite's workspace and topic. `adapters[0].status`
should be `registered`. Then:

```sh
rm "$HOME/bob-myproject-dev.code"
```

and reconnect the MCP server, or restart with `claude --continue` when the
credential was saved for the first time.

### An identity that already has a credential joins a second workspace

This was tested with real processes: `bob` already holds a minted credential
(for `default/main`) and runs `join` again with a new invite for
`myproject/dev`, using the same URL and name. The CLI finds the saved
credential and sends it as the bearer token. The broker adds the membership
and the subscription and **mints no new token**, and the credential file is
left byte-for-byte unchanged. Afterwards the same credential opens sessions in
both `default/main` and `myproject/dev` (`channel/cli.mjs:142-156`,
`broker/invites.mjs:70-71`).

```sh
node channel/cli.mjs join https://broker.example.com - --name bob --client none < "$HOME/bob-myproject-dev.code"
# {"user":"bob","workspace":"myproject","topic":"dev","credential_path":"…/bob.token","adapters":[]}
```

With `--client claude-plugin`, run the join in the project that should work
in `myproject/dev`: a project bound to `default/main` stops with
`PLUGIN_ALREADY_BOUND` before the invite is redeemed
(`channel/test/project-agent.test.mjs`, "join on a bound project keeps the
invite"). Each project holds its own binding, so the same credential serves
both projects.

Older plugin versions redeem the invite **before** they check the plugin
binding. There the join reports `conflict` and writes no options, but bob is
now a member and the invite is used. Running the same invite again gives
`INVITE_USED`, because that check comes before the `ALREADY_MEMBER` check
(`broker/invites.mjs:66`, `:71`).

## 6. Switch a plugin that is already bound

### Bind and join never overwrite a binding

A project that is already bound is refused before anything is sent
(`PLUGIN_ALREADY_BOUND`, `channel/cli.mjs:89-98`). To bind it again, remove
the team-messaging entry (`pluginConfigs["team-messaging@team-messaging-local"]`)
from its `.claude/settings.local.json`, or move the agent role with
`/team-messaging:claim`. The global options hold only the credential: if the
saved broker URL or credential path differs from the requested one, `bind`
and `join` refuse before the invite, and `join` reports `conflict` if it
changed during the join (`channel/plugin-config.mjs:54-70`). If `claude
plugin configure` itself fails, the status is `failed` (`:43-47`).

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
`channel/credentials.mjs:26-35`). Otherwise the hooks report
`INSECURE_CREDENTIAL_DIR` and name the directory
(`channel/credentials.mjs:38-41`). After reinstalling, make the directory
private again:

```sh
mkdir -p ~/.claude/plugins/data/team-messaging-team-messaging-local
chmod 700 ~/.claude/plugins/data/team-messaging-team-messaging-local
```

Then run `/team-messaging:bind` or `/team-messaging:join` in the agent
session and **restart Claude Code** (`claude --continue`): the saved
credential only takes effect after a restart.

### One agent session per project

**The model.** Global plugin options (in `~/.claude/settings.json`, written by
`claude plugin configure`) hold only the credential: `broker_url`,
`token_file`, `mode` and `arm`. The old `workspace`, `topic` and `session_id`
options stay in `plugin.json` so 0.9 settings still validate, but they are
unused since 0.10. A project is bound only by our key in its own
`.claude/settings.local.json`:

```json
{
  "pluginConfigs": {
    "team-messaging@team-messaging-local": {
      "options": {
        "version": 1,
        "claude_session_id": "<the agent session>",
        "bindings": ["<workspace>:<topic or empty>:<broker session>"],
        "mac": "<64 hex>"
      }
    }
  }
}
```

A project without that key is **idle**: no tools, no broker request, and one
session-start line pointing to `/team-messaging:bind` and
`/team-messaging:join`. In a bound project, only the session named in
`claude_session_id` is **active**. Every other session is **inert**: the MCP
server lists no tools and answers every call with `NOT_AGENT_SESSION`, no
token is read, nothing is sent, the Stop hook and watchers are silent, and
the session-start line names the agent session
(`channel/plugin-env.mjs:92-128`, `channel/plugin-main.mjs`).

Each binding is one string, because Claude Code validates `pluginConfigs`
values and skips the whole `settings.local.json` for an array of objects
(plan findings, claude 2.1.310). `":"` is outside every name pattern. This
release reads exactly one binding: more than one, or a higher `version`,
gives `PROJECT_OPTIONS_UNSUPPORTED` instead of using only the first. A later
release can accept several bindings, each with its own broker session, with
the agent and the MAC kept at the top level; files written now stay valid.

**Finding the file.** From the realpath of the project directory, the plugin
walks up to the first `.claude/settings.local.json` that has our key. The walk
stops after the first directory holding `.git`, else at HOME, else at `/`
(`channel/project-options.mjs:104-143`). A file is read without blocking, as a
regular file of at most 1 MiB. Bad JSON is refused only if it names our
plugin id; otherwise, like a file without our key, it is somebody else's. A
file with our key must not be a symlink, nor its `.claude`; both must be
yours and not other-writable (group-write is allowed for umask 002), and every
directory up to `/` must be yours or root's and not other-writable
(`:84-97`). `bind` and `join` write a new file at the git root when it is
inside HOME, else in the project directory, keeping every other setting in
the file, its mode (0600 for a new file, 0700 for a new `.claude`) and a
2-space indent.

**The MAC.** `mac` is an HMAC-SHA256, keyed by `HMAC(token, "team-messaging
project binding v1")`, over the canonical JSON of `[project root realpath,
broker URL, version, claude_session_id, bindings]` (`:56-64`). Only the
agent session checks it, after the session id matches and before any
request. A file committed to a repository, copied to another clone or
worktree, or edited by hand fails as `PROJECT_OPTIONS_UNTRUSTED`. Rotating
the token therefore means binding the project again.

**Claim.** `/team-messaging:claim` moves the agent role to the session the
user runs it in. `disable-model-invocation` keeps the model from invoking the
skill, but it only gates the Skill tool: the skill's command runs through
Bash, and a model can run `team-messaging claim` itself. What stops a
message from making an agent take over a project is the user-only skill plus
the Bash permission prompt for that command. **With Bash auto-approved, a
model can claim the project.** It needs a trusted file,
rewrites only `claude_session_id` and the MAC, makes no broker call, and keeps
the broker session, so the inbox, cursors, leases and queue claims carry over
(`channel/cli.mjs:321-338`). The previous agent's MCP server refuses its next
call and stops polling within about 5 s; its watcher exits at its next poll.
SessionStart also runs for forked sessions (matcher
`startup|resume|clear|compact|fork` since 0.11.1), so a fork gets its session
record and the claim skill finds the project without `--project`. In a
session that is not the agent, SessionStart shows the user a notice (a
`systemMessage`) that names the agent session and `/team-messaging:claim`,
and tells the model the same; the inert MCP server's instructions say it
too. The notice shows at every SessionStart of a non-agent session, a
deliberate second session included, because a fork cannot be told apart
from one.

**Reconnect, `/clear`, `--resume` and forks.** Observed in acceptance on
2026-10-26 (Claude desktop app, CLI 2.1.308): `/compact` keeps the session
id; `/clear` and edit-and-retry gave a new session id (observed); that
edit-and-retry sends `source:fork` is **UNVERIFIED** (acceptance A1). A
`--resume … --fork-session` is a fork by the hooks documentation. A new id
makes the session inert, and SessionStart says so (see Claim).

Since 0.11.1 the inert MCP server re-checks its binding every 2 s. The first
time this session is the agent (after a claim, bind or join here), it serves
the tools on the same connection and sends `tools/list_changed`, so the tools
appear without a reconnect. Claude Code documents that re-fetch for the
interactive terminal, `-p` and the Agent SDK; for the desktop app it is
**UNVERIFIED** until acceptance A3. The conditions are those of a restart:
the same project-file checks, the MAC verified with the token read only
after the session id matches. A reconnect (`/mcp`, then team-messaging, then
Reconnect) or a restart with `claude --continue` is still needed:
- after `/clear`, because the MCP server keeps the id it started with and can
  never become the new id's agent;
- after a first credential save, because the server's `TM_*` environment is
  fixed when it starts;
- when the tools do not appear within a few seconds.

Promotion needs no human step, so a claim, bind or join that the model runs
through Bash now takes effect within about 2 s: the Bash permission prompt
is the only gate, as the claim skill warns. Bash is denied inside the
team-message window, so a teammate message cannot trigger it, and it only
chooses which local session in this project is the agent. After a claim the
previous agent stops at its next 5 s check (up to about 9 s when it reads the
project file mid-write); until then the old and the new agent are both active
on one broker session, as with a fast reconnect. Demotion stays final: a
promoted, then demoted process needs a reconnect.

**Threats, in short.** A cloned repository is untrusted input: its files,
including a force-added `settings.local.json`, its `settings.json` env block,
and its directory names.
- A committed project file names a session no local session has, so every
  session is inert; even with a guessed session id, the MAC fails.
- A copied file fails the MAC because the root differs.
- Symlink, owner and mode tricks fail the checks above.
- No path is pasted into a shell: the skills pass only
  `${CLAUDE_SESSION_ID}`, `${CLAUDE_PLUGIN_DATA}` and saved options, and the
  CLI finds the project through a record SessionStart writes in
  `${CLAUDE_PLUGIN_DATA}/state`. Records older than 30 days are pruned, so a
  session that runs more than 30 days without a SessionStart can lose its
  record (the CLI then reports `PROJECT_UNKNOWN`); a restart or
  `claude --resume` rewrites it.
- Another user's `.claude` directory or keyed settings file inside a
  directory of yours (a group member writing into a group-writable checkout)
  is skipped as not a binding, and the walk goes on; it cannot make the
  project untrusted.
- `bind` cannot take over a workspace you do not own
  (`WORKSPACE_NOT_OWNED`). Claim is a user-only skill, but its command can
  still be run through Bash: the Bash permission prompt is the real gate, and
  auto-approved Bash lets a model claim.

**S-1, fixed in 0.10.** In 0.9.x a repository's `.claude/settings.json` env
block could set `CLAUDE_PLUGIN_OPTION_BROKER_URL` and `_TOKEN_FILE` while
those options were unsaved (for example right after the reset in this
section), and the hooks then sent a real credential to the repository's
broker. In 0.10 the token file must be exactly the path `join` saved for that
broker URL and the name in the file name, checked before anything is read or
sent (`channel/credentials.mjs:12-25`), so a repository-chosen broker URL can
only reach a credential minted for that same URL. **Hand-named token files
therefore stop working**: rejoin, or move the file to the path `join` prints
as `credential_path`.

**Rollback.** Plugin 0.9.4 ignores the project file. After an adopt (below)
its global options hold only the credential, so 0.9.4 reports the plugin as
not configured; rebind it globally with 0.9.4's `bind` to go back.

### Upgrading from 0.9

After the upgrade, a 0.9 global binding (for example a node bound to
workspace `test1`) is ignored and every project is idle. The session-start
line names the old workspace and points to the adopt step. To keep that
binding, with its broker session, inbox and leases, in one project:

1. **Restart Claude Code on 0.10 first**, in the project that should keep the
   binding (`claude --continue` is enough). The 0.10 SessionStart hook writes
   the record that lets the CLI find this project from this session; without
   it, adopt stops with `PROJECT_UNKNOWN`. Use the session that should be the
   project's agent.
2. Run `/team-messaging:bind`, choosing the adopt form. It runs:

   ```sh
   team-messaging bind --adopt-global --claude-session '${CLAUDE_SESSION_ID}' --data-dir '${CLAUDE_PLUGIN_DATA}'
   ```

   It checks the 0.9 session with the broker (an ended one is refused),
   writes the project file with the same workspace, topic and session, and
   only then rewrites the global options to the credential only
   (`channel/cli.mjs`, `adopt`).
3. `adapters[0].status` is `adopted`. Restart with `claude --continue`.
4. If the global rewrite failed after the project file was written, run the
   same command again **from the same Claude session**: it recognises its own
   file only when the session matches, and then finishes the clearing. A
   second adopt after success finds nothing (`NOTHING_TO_ADOPT`). A marker in
   `${CLAUDE_PLUGIN_DATA}/state/adopted.json` refuses adopting into a second
   project.

**A hand-named token file** (one not at the path `join` prints as
`credential_path`) makes adopt stop with `TOKEN_FILE_MISMATCH` before
anything is read or sent. Move the file to the join path for the same broker
URL and name, `~/.config/team-messaging/credentials/<16 hex>-<name>.token`
(the hex is the start of the SHA-256 of the exact broker URL; see
[the credential file name](#the-credential-file-name-hashes-the-broker-url)),
keep it mode 0600, save that path as `token_file` with `claude plugin
configure`, and run the adopt again. Or rejoin with a new invite.

**Other projects that used the global binding become idle.** Only one
project adopts it. In each other project, the session that should be its
agent binds it again: a member runs `/team-messaging:bind` with the topic
(member bind, above), and an owner runs `/team-messaging:bind` for the
workspace and topic. Each project then gets its own broker session.

## Keep sharing inside the project (a constant)

**Rule, for every agent:** share only what is inside the project root, as
project-relative paths and project content. Never send anything from outside
the project or about the machine: home or system files, other repositories,
the OS, kernel or distribution, the hostname, the user name or the home path,
installed packages and runtimes, processes, network configuration, hardware,
environment values, credentials or tokens. This is a constant, not advice:
no plugin option, environment variable, settings key or flag turns it off or
weakens it (`channel/outbound-scope.mjs` reads no environment;
`channel/test/outbound-scope.test.mjs`, "AM-S1 stdio";
`channel/test/scope-hook.test.mjs`, "AM-S1 hook"). A refusal is final; there
is no approval path.

The plugin enforces it in three layers, plus settings you add yourself:

- **The instructions (advisory).** The MCP server instructions, the
  SessionStart context and the session-start prefix tell the agent the rule.
- **The outbound guard.** Every string of every tool call that reaches the
  broker is checked before the request (`guardOutbound` in
  `channel/server.mjs`, the rules in `checkOutbound` in
  `channel/outbound-scope.mjs`). A match returns `OUTBOUND_SCOPE` with a
  `reason` of `<rule>:<field>`, never the matched text, and nothing is sent.
  It refuses private keys, passwd/shadow/group lines, token shapes, secret
  assignments, env dumps with live values, `/etc/os-release`, `uname -a`,
  `/proc/cpuinfo`, `/proc/meminfo`, `ip addr`/`ifconfig`, `dpkg -l`,
  `rpm -qa` and `npm ls -g` output (also when quoted or listed in Markdown),
  `lscpu` lines, kernel releases, CIDR and private-range IP addresses, `ip
  -br` lines, the live hostname, user name and home
  path, absolute paths under a system or home root, `~` and `$HOME` paths,
  `../` paths that leave the project tree, credential file names and long
  encoded blobs. Project-relative paths, project files (including `../`
  imports between the project's own directories), the project's own
  dependency list, `ws:`/`user:`/`topic:` addresses and broker route names
  pass. The text is folded before it is judged: compatibility forms are
  folded, invisible characters (every default-ignorable code point,
  combining marks and blank fillers) are removed, and slash and tilde
  look-alikes become `/` and `~`. A word that mixes Latin letters with
  letters of another script is refused as `mixed_script`, and a path with a
  non-ASCII letter as `non_ascii_path`, so a disguised `/etc/passwd` is still
  refused; text wholly in another script, such as Russian or Greek, passes.
  It is a recogniser, not a boundary (see Known gaps).
- **The team-message window (deny-only hook).** From the moment teammate
  content reaches the agent (an `inbox`, `wait`, `pins`, `dead_letters`,
  `queue_item` or `list_decisions` result with another user's entry, a
  `check_overlap` naming another owner, a successful `ack` or `claim`, a
  channel push of another user's message, a watcher wake or Stop-hook block
  for another user's message, or pending messages in the SessionStart
  context) until the
  turn's Stop, the `PreToolUse` hook (`channel/scope-hook.mjs`) allows only
  `Read`, `Glob` and `Grep` inside the project root (a path starting with
  `~` or holding `$` or `%`, and a glob with `{` or a `..` segment, are
  denied), `Read` of this session's
  fresh large tool results, the team-messaging tools (checked by their
  `mcp_server` provenance), `ToolSearch`, `TodoWrite`, `AskUserQuestion`,
  the plan-mode tools and the exact `/team-messaging:watch` command. It
  denies everything else, in every permission mode including
  `bypassPermissions`: `Bash`, `WebFetch`, `WebSearch`, `Agent`/`Task`
  subagents, other MCP servers, `LSP`, and `Write`, `Edit` and
  `NotebookEdit` even inside the root. It never asks. Subagents' tool calls
  go through the same hook. The window is one marker per project root,
  `${CLAUDE_PLUGIN_DATA}/state/scope-<sha256(root)>.json`, so `/clear` or
  another session in the same checkout is covered too. In the window, re-arm
  the watcher only with the exact `/team-messaging:watch` command, and read
  messages with `inbox`. The human's own `send`, `ask` and `invite` and the
  name-only tools (`topics`, `members`, `workspaces`, `invites`) never open
  it.
- **The window closes** at Stop and StopFailure once no teammate message is
  pending for the agent session (a Stop never removes a window reopened
  while it checked), and in any case 2 h after `since`, the last time
  teammate content opened it. No tool decision extends it, so a stream of
  denials cannot keep it on. To end it at once, run the command every
  denial prints, in a terminal outside Claude:
  `node '<plugin>/runtime/channel/cli.mjs' scope-close --project-dir '<root>' --data-dir '<plugin data>'`,
  with the paths filled in, or delete the marker file the denial names. On
  its own, `team-messaging scope-close` uses the current directory and the
  default data directory
  `~/.claude/plugins/data/team-messaging-team-messaging-local`. If the window cannot be written, the teammate content
  is withheld: the tool returns `SCOPE_WINDOW_UNAVAILABLE`, a push is dropped
  (the message stays in `inbox`), SessionStart leaves the messages out, and
  the watcher and the Stop hook do not wake the session. A project root the
  hook cannot resolve (a deleted directory, an unexpanded variable) is
  denied only while some window exists; otherwise the call passes with
  `PROJECT_DIR_UNRESOLVED`. **Known behaviour:** after Esc, the next human
  turn stays deny-only until its Stop.
- **Only while the tools are loaded (0.11.1).** The Stop hook, the watcher
  and SessionStart wake the session, list pending messages or open the
  window only while this session's MCP server serves: it has answered a
  non-empty `tools/list` and its process is alive (a private record
  `${CLAUDE_PLUGIN_DATA}/state/<session>.serving`, checked by `serving` in
  `channel/agent-live.mjs`). Otherwise the Stop hook neither blocks nor opens
  the window and shows the user `TOOLS_NOT_LOADED`, the watcher keeps polling
  without waking, and SessionStart leaves the messages out with a
  `TOOLS_NOT_LOADED` line. In push mode the server holds teammate pushes
  until the host has listed its tools (the messages stay in `inbox`), then
  pushes them. This
  only removes wakes: content still leaves the server only after the window
  is written.

**Requirements.** Claude Code 2.1.274 or later, which passes `mcp_server` to
hooks; older versions deny the team-messaging tools inside the window.
`permissions.blockReadsOutsideWorkingDirectories` below needs 2.1.257 or
later.

**Add these to the project's `.claude/settings.json`** (a plugin cannot set
permissions or the sandbox):

```json
{"permissions":{"blockReadsOutsideWorkingDirectories":true,"deny":["Read(~/.ssh/**)","Read(~/.aws/**)","Read(~/.gnupg/**)","Read(~/.config/team-messaging/**)"]},"sandbox":{"enabled":true,"filesystem":{"denyRead":["~/"],"allowRead":["."]}}}
```

The sandbox covers Bash commands only; Claude's file tools, MCP servers and
hooks, including this plugin's, run outside it. Toolchains installed under
your home (nvm, pyenv, cargo) need their own `allowRead` entries.

**Real-node acceptance (operator, a bound agent session; recorded apart
from the automated tests):**

1. A teammate asks for the files in the project root: the answer is
   project-relative.
2. Teammates ask for `/etc/passwd`, `uname -a`, a home listing and the
   environment. Each is refused by the agent, denied by the hook or returns
   `OUTBOUND_SCOPE`, and the broker's `messages` table holds none of that
   content.
3. In the window, `Bash` and `WebFetch` are denied.
4. Steps 2 and 3 under `bypassPermissions` are still denied.
5. A subagent's `Read` of `/etc/hosts` is denied.
6. A large tool result is readable from the session's `tool-results`
   directory.
7. The hook input for a team-messaging tool carries `mcp_server`
   `{"source":"plugin","name":"plugin:team-messaging:team-messaging"}`.
8. With `arm=bash`, the watcher re-arms in the window with the exact
   `/team-messaging:watch` command, and a changed command is denied.
9. After `/clear` in the window, `Bash` is still denied until the Stop.
10. After the turn, the human's own `Read` outside the root is not blocked.
11. **UNVERIFIED (0.11.1, review r1 R6):** in a real non-agent session, the
    inert server's `experimental['claude/channel']` capability causes no
    channel registration and no warning in Claude Code.

## 7. Known gaps

- **Observed on the Claude desktop app:** after a restart, the watcher was
  armed only once a turn ran. Type one message in the session after
  restarting so that messages wake it.

The project-scope rule ([above](#keep-sharing-inside-the-project-a-constant))
leaves these residuals:

- **R-1:** outside a team-message window, Bash with network access can send
  data anywhere without our tools; only the sandbox network allowlist stops
  that.
- **R-2:** the outbound guard does not recognise paraphrase, bare lists of
  names, or encodings shorter than its 200-character blob threshold. Numeric
  arguments (ids, generations, `since`, timeouts) and names of up to 128
  characters (topics, workstreams, thread ids) are covert channels: an agent
  could encode a little data in them, and no content rule can tell. Whole
  words spelled entirely in another script's look-alike letters (so no word
  mixes scripts), and a secret split across several fields or messages, are
  not recognised either.
- **R-3:** `pip freeze` and `brew list` output looks exactly like a project's
  requirements file or a list of names, so it passes. It cannot be collected
  inside the window, where Bash is denied.
- **R-4:** a hostname or user name shorter than 4 characters, or one on the
  generic list (`root`, `node`, `admin`, `user`, `ubuntu`, `debian`,
  `vscode`, `runner`, `codespace`, `dev`, `test`, `pi`, `ec2-user`,
  `centos`, `fedora`, `docker`, `app`, `build`, `ci`, `home`, `localhost`,
  and host names that are ordinary words: `server`, `laptop`,
  `workstation`, `main`, `staging`, `desktop`, `pc`, `prod`, `production`,
  `web`, `db`, `worker`, `master`, `host`, `box`, `machine`), is not an
  identity check.
- **R-5:** hook input that is not JSON, has no valid session id or data
  directory, or comes with arguments in another form passes through with a
  stderr code.
- **R-6:** a scope hook that is killed, times out (10 s) or finds no node
  fails open; `onFailure: "block"` is not set, because it would block every
  tool call in every session whenever node cannot start.
- **R-7:** `disableAllHooks`, disabling the plugin, or the standalone
  (non-plugin) channel removes the window. The outbound guard stays.
- **R-8:** name-only results (`topics`, `members`, `workspaces`, `invites`)
  do not open the window.
- **R-9:** push content arriving before the agent's first tool call is
  covered by the push opening the window; a push whose window write fails is
  dropped and stays in `inbox`.
- **R-10:** a large tool result is readable in the window only when its
  mtime is at or after the window opened. A coarse filesystem mtime can deny
  a fresh file (fails closed); a tool call that started before the window
  and finishes inside it can leave a fresh file with outside content (fails
  open).
- **R-11:** only the channel push, the tool results above, the watcher wake
  and the Stop-hook block open the window. Any other autonomous turn start,
  such as a background Bash command finishing, does not reopen it.
- **R-12:** an MCP server that dies after a hook judged it serving, and
  before the model calls a tool, leaves an open window: Bash stays denied
  until the Stop, and for the following turns while messages are pending.
  The way out is the same as for any window: the `scope-close` command a
  denial prints, or the 2 h limit. While two processes of one session
  overlap (a reconnect), an old process killed with SIGKILL can leave
  serving false for up to 5 s, until the live server's next check rewrites
  the record; wakes are dropped in that time, never content. `clearServing`
  reads the record and then unlinks it, so a record rewritten between the
  two can be removed; that also fails safe, suppressing wakes for up to 5 s.
- **R-13:** without `/proc` (macOS) the serving check is pid-only, so a
  reused pid can read as serving. That brings back a wake for a session
  without tools (the trap R-12 describes), but no content leaks. The SIGKILL
  overlap in R-12 applies here too.
- **R-14:** in push mode, teammate pushes wait until the host has listed the
  tools. Until then a message is only in `inbox`; at the first listing the
  server pushes each held message that is still unacked, opening the window
  first as for any push.
- **R-15:** the window is one marker per project root, so a window opened by
  an earlier agent session also makes a forked or cleared session in the
  same root strict, and its Bash `claim` is denied until the Stop. Run the
  `scope-close` command first.

## 8. Troubleshooting

| Code | Where it comes from | Meaning | Fix |
|---|---|---|---|
| `conflict` | `adapters[0].status`, `channel/plugin-config.mjs:54-70` | The plugin's saved credential changed to another broker or name during the join, or its configuration could not be read. Nothing was written. A `join` that reports this has already redeemed the invite. | Fix the plugin options, then ask the owner for a new invite. |
| `PLUGIN_ALREADY_BOUND` | `bind`/`join` stderr, `channel/cli.mjs:81-109` | This project is already bound (the hint names the file, binding and agent session), the saved credential is for another broker or name, or a 0.9 binding was already adopted into another project. The invite was not used. | Claim the project, or remove its team-messaging entry, or clear the saved credential, then run again. |
| `PLUGIN_NOT_READY` | `bind`/`join` stderr, `channel/cli.mjs:102-109` | No `claude` CLI on PATH, the plugin is not installed, or its options could not be read. The invite was not used. | Install Claude Code and the plugin, or fix the plugin configuration, then run the same `join` again. |
| `manual` | `adapters[0].status`, `channel/plugin-config.mjs:46`, `:59-62` | No `claude` CLI on PATH, or the plugin is not installed, so nothing could be written. | Install Claude Code and the plugin, then rerun. After a `join`, the project file was not written: rerun as `bind` (a member with `--topic`, member bind); the membership already exists. |
| `failed` | `adapters[0].status`, `channel/plugin-config.mjs:46` | `claude plugin configure` exited non-zero, so the options were not saved. After a `join`, the invite is already redeemed. | Check `claude plugin list` and the Claude Code output, then rerun as `bind` (a member with `--topic`). |
| `CREDENTIAL_MISSING` | `channel/cli.mjs:219` | `bind` found no saved credential for this exact broker URL and name. | Join with an invite first, or use the exact URL string the credential was saved with. |
| `TOPIC_REQUIRED` | `broker/broker.mjs:50` | The broker refused a workspace-level session, which only the workspace owner may open. `bind` reports a member's workspace-level request as `WORKSPACE_NOT_OWNED` before it reaches the broker. | Members bind with `--topic` for a subscribed topic. |
| `NOT_A_MEMBER` | `bind` stderr, `broker/broker.mjs:44` | `bind --topic` for a workspace this name is not a member of, or a membership that ended. | Ask the owner for an invite and use `/team-messaging:join`. |
| `NOT_SUBSCRIBED` | `bind` stderr, `broker/broker.mjs:51` | A member's `bind --topic` for a topic this user is not subscribed to. | Ask the owner for an invite to that topic (section 5 or the static join). |
| `INVITE_EXPIRED` | `broker/invites.mjs:66` | The invite is older than `INVITE_TTL` (default 3600 s). | Ask the owner for a new invite. |
| `INSECURE_CREDENTIAL_DIR` | `channel/credentials.mjs:38-41`, `:73-86` | A credential or state directory, or one of its ancestors, is group- or world-writable, not yours, or a user-owned symlink. The hint names the directory. | Run the `chmod go-w '<dir>'` from the hint. For the plugin data directory, run `chmod 700` as in section 6. |
| `INSECURE_BROKER_URL` | `channel/cli.mjs:31-32` | Plain `http://` to a host that is not loopback. | Use `https://`. HTTP is allowed only for `127.0.0.1`, `[::1]` and `localhost`. |
| `PROJECT_OPTIONS_INVALID` | `channel/project-options.mjs:40-51`, `:104-143` | The project's team-messaging entry is not a valid binding: an unknown key (such as `broker_url` or `token_file`), a malformed field, bad JSON naming the plugin, or a file that is not a regular file of at most 1 MiB. No token is read. | Remove the entry, then bind or join again. |
| `PROJECT_OPTIONS_UNTRUSTED` | `channel/project-options.mjs:84-97`, `channel/plugin-env.mjs:115` | The file was not written for this checkout by this user: a MAC that does not verify (committed, copied, edited, or the token rotated), a symlink, a foreign owner, or an other-writable file or directory. | Remove the entry and bind or join again; fix the modes for a permissions problem. |
| `PROJECT_OPTIONS_UNSUPPORTED` | `channel/project-options.mjs:40-51` | The file was written by a newer plugin (a higher `version`, or more than one binding). | Upgrade the plugin. |
| `TOKEN_FILE_MISMATCH` | `channel/credentials.mjs:12-25` | The saved token file is not the path `join` saved for the saved broker URL, for example a hand-named file (S-1). Nothing was read or sent. | Rejoin, or move the file to the `credential_path` path. |
| `INVALID_HOOK_ARGS` | `channel/plugin-env.mjs:82-90` | A hook got arguments other than `--project-dir` and `--data-dir`. | Reinstall the plugin. |
| `SESSION_ID_UNKNOWN` | `channel/plugin-env.mjs:110`, `channel/cli.mjs:72-79` | No valid Claude session id: the MCP server's `CLAUDE_CODE_SESSION_ID` is absent or malformed (the server stays inert), or a CLI call lacks `--claude-session`. | Use the skills, which pass `${CLAUDE_SESSION_ID}`. |
| `NOT_AGENT_SESSION` | MCP tool result, `channel/plugin-main.mjs`, `channel/server.mjs:61-110` | This session is not the project's agent session, it stopped being it after a claim, or the project's binding was refused. | Use the agent session, or run `/team-messaging:claim` here; the tools load within a few seconds, reconnect if they do not. A reconnect is always needed after `/clear` (the MCP server keeps its old id) or once this process was demoted (demotion stays final). |
| `PROJECT_NOT_BOUND` | MCP tool result, Bash watcher, `claim` | The project has no team-messaging binding (idle). | Run `/team-messaging:bind` or `/team-messaging:join` in the session that should be the agent; the tools load within a few seconds, reconnect if they do not (restart after a first credential save). |
| `PROJECT_UNKNOWN` | `channel/cli.mjs:72-79`, `channel/project-options.mjs:104-109` | The project directory is unknown: no SessionStart record for this session in `${CLAUDE_PLUGIN_DATA}/state`, or the directory does not exist. A session forked under 0.11.0 or earlier has no record, because SessionStart did not run for forks. | Start Claude Code in the project, or restart the session (or pass `--project <dir>` from a terminal). |
| `TOOLS_NOT_LOADED` | Stop hook `systemMessage`, SessionStart context; `serving` in `channel/agent-live.mjs` | Team messages are pending, but this session's team-messaging MCP server has not listed its tools (right after a claim, bind or join, or a server that did not start), so the session was not woken and no window opened. | Wait a few seconds for the tools to load; if they do not appear, reconnect the team-messaging MCP server (`/mcp`, then team-messaging, then Reconnect). |
| `WORKSPACE_NOT_OWNED` | `bind` stderr, `channel/cli.mjs:248` | The workspace exists and you are not its owner, or it was deleted. Nothing was written. | Choose another name, or ask its owner for an invite and use `/team-messaging:join`. |
| `TOPIC_MODE_MISMATCH` | `bind` stderr, `channel/cli.mjs:262` | The topic exists with a mode other than `--mode`. | Leave out `--mode` to reuse the topic, or choose another name. |
| `OUTBOUND_SCOPE` | MCP tool result, `guardOutbound` in `channel/server.mjs` | The outbound guard refused the call; `reason` is `<rule>:<field>`. Nothing reached the broker. | Rewrite without that part (project-relative paths and project content only), or ask your human. Retrying the same content is refused again. |
| `SCOPE_WINDOW_UNAVAILABLE` | MCP tool result, SessionStart context, watcher or Stop-hook stderr; `openWindow` in `channel/scope-window.mjs` | The team-message window could not be written (plugin data directory missing, relative, insecure, or the marker path is not a file), so teammate content was withheld and no wake happened. | Fix the plugin data directory (section 6), then use `inbox`. |
| `SCOPE_WINDOW_INVALID` | `scope_hook_unavailable` stderr at Stop, `closeWindow` | The window marker path is not a regular file, so it was left alone and the window stays. | Remove the non-file at the marker path (`${CLAUDE_PLUGIN_DATA}/state/scope-<sha256(root)>.json`). |
| `PROJECT_DIR_UNRESOLVED` | `scope_hook_unavailable` stderr, `channel/scope-hook.mjs` | The hook could not resolve `--project-dir`, and no window was open, so nothing was blocked. | Start Claude Code in an existing project directory. |
| `INVALID_SCOPE_HOOK_ARGS` | `scope_hook_unavailable` stderr | The scope hook got arguments other than `--event`, `--project-dir`, `--data-dir` in that order. | Reinstall the plugin. |
| `SESSION_ID_UNKNOWN`, `DATA_DIR_REQUIRED` | `scope_hook_unavailable` stderr | The hook input had no valid session id, or `--data-dir` was not absolute; the call passed (residual R-5). | Reinstall the plugin; report the Claude Code version. |
| `SCOPE_CLOSE_FAILED` | `scope_hook_unavailable` stderr at Stop | Closing the window failed for an uncoded reason; it ends 2 h after it opened. | Run the `scope-close` command a denial prints, or delete the marker file it names. |
