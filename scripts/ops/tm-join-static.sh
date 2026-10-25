#!/bin/bash
# Usage: tm-join-static.sh <static-user> <code-file>
# Needs bash, curl and jq; TM_URL and TM_TOKENS are as in tm.sh.
# Redeems an invite for a STATIC token-file user with its own bearer. With a
# valid bearer the broker adds the topic to the existing membership and mints
# no token. The CLI `join` cannot do this for static users: it only reuses a
# credential under ~/.config/team-messaging/credentials.
# The code travels in a 0600 body file, never in argv. The body file is always
# removed. The code file is removed only after HTTP 201: on a network error,
# a 4xx or a 429 the invite may still be usable, so it is kept for a retry.
# Exits as tm.sh does: 0 only on success.
set -euo pipefail
[ $# -eq 2 ] || { echo 'usage: tm-join-static.sh <static-user> <code-file>' >&2; exit 2; }
user=$1 codefile=$2
umask 077
here=$(dirname "$(readlink -f "$0")")
body=$(mktemp "${TMPDIR:-/tmp}/tm-join.XXXXXX")
trap 'rm -f "$body"' EXIT
jq -n --rawfile c "$codefile" '{code:($c|rtrimstr("\n"))}' > "$body"
rc=0
out=$("$here/tm.sh" "static:$user" POST /join "@$body") || rc=$?
printf '%s\n' "$out"
if [ "$rc" -eq 0 ] && [ "${out##*$'\n'}" = "HTTP 201" ]; then rm -f "$codefile"; fi
exit "$rc"
