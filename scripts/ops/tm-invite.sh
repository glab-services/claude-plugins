#!/bin/bash
# Usage: tm-invite.sh <token-source> <topic> <out-file>
# Needs bash, curl and jq; token-source and the TM_* variables are as in tm.sh.
# Creates a single-use invite for <topic> in the current TM_SESSION/TM_WORKSPACE
# scope (the caller must be that workspace's owner). Only the code goes to
# <out-file> (mode 0600); invite_id, expiry, the file's mode and HTTP status
# are printed. The code itself is never printed. INVITE_TTL defaults to 3600 s.
# <out-file> must not exist yet: a pre-existing file would keep its own mode,
# and a symlink would send the code wherever it points.
set -euo pipefail
[ $# -eq 3 ] || { echo 'usage: tm-invite.sh <static:<user>|file:<path>> <topic> <out-file>' >&2; exit 2; }
src=$1 topic=$2 out=$3
umask 077
if [ -e "$out" ] || [ -L "$out" ]; then echo "tm-invite.sh: $out already exists; choose a new path" >&2; exit 2; fi
here=$(dirname "$(readlink -f "$0")")
resp=$(mktemp "${TMPDIR:-/tmp}/tm-invite.XXXXXX")
# Created next to <out-file> so the final link stays on one filesystem.
tmp=$(mktemp "$(dirname "$out")/.tm-invite.XXXXXX")
trap 'rm -f "$resp" "$tmp"' EXIT
"$here/tm.sh" "$src" POST /invites "$(jq -cn --arg t "$topic" '{topics:[$t]}')" > "$resp" || true
code=$(tail -1 "$resp")
[ "$code" = "HTTP 201" ] || { head -1 "$resp"; echo "$code"; exit 1; }
head -1 "$resp" | jq -r .code > "$tmp"
# ln refuses an existing name, symlinks included, so a file created at <out>
# after the check above is never overwritten or followed.
ln "$tmp" "$out" || { echo "tm-invite.sh: could not create $out; the invite stays unused" >&2; head -1 "$resp" | jq -c 'del(.code)'; exit 1; }
head -1 "$resp" | jq -c 'del(.code)'
# find -perm is POSIX, unlike GNU `stat -c`, so this runs on BSD/macOS too.
[ -n "$(find "$out" -prune -perm 0600)" ] || { echo "tm-invite.sh: $out is not mode 0600" >&2; exit 1; }
echo "600 $out"
