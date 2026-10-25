#!/bin/bash
# Usage: tm.sh <token-source> <method> <path> [json | @json-file]
# Authenticated team-broker call for operators. Needs bash, curl and jq.
#
# token-source: "static:<user>" reads that user's entry from $TM_TOKENS
# (the broker token file); "file:<path>" reads a mode-0600 minted credential
# file. The token is only ever expanded inside `-H @<(printf ...)` on the curl
# line, so it never lands in argv, in output or in a file we create.
# Headers come from the environment: TM_URL (required), TM_TOKENS (for
# static:), TM_SESSION, TM_WORKSPACE, TM_TOPIC (optional).
#
# A body starting with @ is read by curl from that file. Use that form for
# anything secret (an invite code): an inline body is visible in the process
# list while curl runs.
#
# Prints the response body, then "HTTP <status>". Exits 0 only on a 2xx
# status, 1 on any other HTTP status, or curl's own code when the request
# never completed (the status line then reads "HTTP 000").
set -euo pipefail
usage='usage: tm.sh <static:<user>|file:<path>> <method> <path> [json | @json-file]'
[ $# -ge 3 ] && [ $# -le 4 ] || { echo "$usage" >&2; exit 2; }
src=$1 method=$2 path=$3 body=${4:-}
case $src in
  static:*) token=$(jq -r --arg u "${src#static:}" '.[$u] // empty' "$TM_TOKENS") ;;
  file:*)   token=$(cat "${src#file:}") ;;
  *) echo "bad token source" >&2; echo "$usage" >&2; exit 2 ;;
esac
# Read the token before calling curl so that an unknown user or an empty file
# fails here with a clear message instead of sending "Bearer null". The message
# names the source only, never the token.
[ -n "$token" ] && [ "$token" != null ] || { echo "tm.sh: no token for $src" >&2; exit 2; }
args=(-sS -m 10 -X "$method" -w "\nHTTP %{http_code}\n")
[ -n "${TM_SESSION:-}" ]   && args+=(-H "X-Session-Id: $TM_SESSION")
[ -n "${TM_WORKSPACE:-}" ] && args+=(-H "X-Workspace: $TM_WORKSPACE")
[ -n "${TM_TOPIC:-}" ]     && args+=(-H "X-Topic: $TM_TOPIC")
[ -n "$body" ] && args+=(-H 'Content-Type: application/json' --data-binary "$body")
# The process substitution must sit on the curl line itself: stored in an
# array, its /dev/fd/N is already closed when curl opens it (seen in rehearsal).
rc=0
out=$(curl "${args[@]}" -H @<(printf 'Authorization: Bearer %s\n' "$token") "$TM_URL$path") || rc=$?
printf '%s\n' "$out"
[ "$rc" -eq 0 ] || exit "$rc"
case ${out##*$'\n'} in "HTTP 2"??) exit 0 ;; *) exit 1 ;; esac
