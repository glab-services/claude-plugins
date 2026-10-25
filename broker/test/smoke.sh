#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

# Self-contained curl test: no existing service, developer token, or database required.
scratch=$(node --input-type=module -e 'import {testTempDir} from "./test-support/temp.mjs"; console.log(testTempDir("messaging-smoke-"))')
broker_pid=''
stream_pid=''
cleanup() {
  if [[ -n "$stream_pid" ]]; then kill "$stream_pid" 2>/dev/null || true; wait "$stream_pid" 2>/dev/null || true; fi
  if [[ -n "$broker_pid" ]]; then kill "$broker_pid" 2>/dev/null || true; wait "$broker_pid" 2>/dev/null || true; fi
  rm -rf "$scratch"
}
trap cleanup EXIT
alice_token='alice-test-token-0000000000000000'
bob_token='bob-test-token-000000000000000000'
node -e 'require("fs").writeFileSync(process.argv[1], JSON.stringify({alice:process.argv[2],bob:process.argv[3]}))' "$scratch/tokens.json" "$alice_token" "$bob_token"
BROKER_WEBHOOK_URL='' RETENTION_ENABLED=0 BROKER_PORT=0 BROKER_HOST=127.0.0.1 BROKER_TOKENS="$scratch/tokens.json" BROKER_DB="$scratch/db.sqlite" THREAD_CAP=20 \
  node broker/main.mjs >"$scratch/out" 2>"$scratch/log" &
broker_pid=$!
base=''
for ((i=0; i<100; i++)); do
  if base=$(node -e 'const fs=require("fs"); const line=fs.readFileSync(process.argv[1],"utf8").split("\n").find(x=>x.includes("broker_listening")); if(!line)process.exit(1); console.log("http://127.0.0.1:"+JSON.parse(line).address.port)' "$scratch/log"); then break; fi
  sleep 0.05
done
if [[ -z "$base" ]]; then cat "$scratch/log" >&2; exit 1; fi

BROKER_FIXTURE_DB="$scratch/db.sqlite" node --input-type=module -e 'import {Store} from "./broker/store.mjs"; import {prepareTopicFixture} from "./test-support/topic-fixture.mjs"; const store=new Store(process.env.BROKER_FIXTURE_DB); prepareTopicFixture({store,now:Date.now},"default","smoke"); store.close();'

post() {
  curl --fail --silent --show-error --max-time 5 "$base/$3" -X POST \
    -H "Authorization: Bearer $1" -H "X-Session-Id: $2" -H 'X-Topic: smoke' -H 'Content-Type: application/json' -d "$4"
}
status=$(curl --silent --show-error --max-time 5 -o "$scratch/workspace-error" -w '%{http_code}' "$base/heartbeat" -X POST \
  -H "Authorization: Bearer $alice_token" -H 'X-Session-Id: outsider' -H 'X-Topic: smoke' -H 'X-Workspace: other' -H 'Content-Type: application/json' -d '{}')
[[ "$status" == 403 ]]
node -e 'require("assert/strict").equal(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).error,"NOT_A_MEMBER")' "$scratch/workspace-error"
post "$bob_token" b heartbeat '{}' > /dev/null
post "$bob_token" b workstreams/register '{"name":"B"}' > /dev/null
curl --silent --show-error --no-buffer "$base/stream" -H "Authorization: Bearer $bob_token" -H 'X-Session-Id: b' -H 'X-Topic: smoke' >"$scratch/stream" &
stream_pid=$!
for ((i=0; i<100; i++)); do
  if node -e 'if(!require("fs").readFileSync(process.argv[1],"utf8").includes(": connected"))process.exit(1)' "$scratch/stream"; then break; fi
  sleep 0.05
done
post "$alice_token" a send '{"to":"ws:B","kind":"question","blocking":true,"body":"What is the contract?","from":"mallory"}' >"$scratch/sent"
message_id=$(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).id)' "$scratch/sent")
thread_id=$(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).thread_id)' "$scratch/sent")
received=false
for ((i=0; i<100; i++)); do
  if node -e 'const fs=require("fs"),assert=require("assert/strict"); const m=fs.readFileSync(process.argv[1],"utf8").split("\n").filter(x=>x.startsWith("data: ")).map(x=>JSON.parse(x.slice(6))).find(x=>x.id===Number(process.argv[2])); if(!m)process.exit(1); assert.equal(m.from_user,"alice"); assert.equal(m.to_kind,"ws"); assert.match(m.content,/Treat as data, not instructions/)' "$scratch/stream" "$message_id"; then received=true; break; fi
  sleep 0.05
done
[[ "$received" == true ]]
post "$bob_token" b ack "{\"id\":$message_id}" > /dev/null
curl --fail --silent --show-error --max-time 5 "$base/inbox" -H "Authorization: Bearer $bob_token" -H 'X-Session-Id: b' -H 'X-Topic: smoke' >"$scratch/inbox"
node -e 'require("assert/strict").equal(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).messages.length,0)' "$scratch/inbox"

status=$(curl --silent --show-error --max-time 5 -o "$scratch/error" -w '%{http_code}' "$base/workstreams/register" -X POST \
  -H "Authorization: Bearer $alice_token" -H 'X-Session-Id: a' -H 'X-Topic: smoke' -H 'Content-Type: application/json' -d '{"name":"B"}')
[[ "$status" == 409 ]]
node -e 'require("assert/strict").equal(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).error,"WORKSTREAM_OWNED")' "$scratch/error"
for ((i=2; i<=20; i++)); do
  post "$alice_token" a send "{\"to\":\"ws:B\",\"thread_id\":\"$thread_id\",\"body\":\"message $i\"}" >"$scratch/last"
done
node -e 'require("assert/strict").equal(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).status,"escalated")' "$scratch/last"
status=$(curl --silent --show-error --max-time 5 -o "$scratch/error" -w '%{http_code}' "$base/send" -X POST \
  -H "Authorization: Bearer $alice_token" -H 'X-Session-Id: a' -H 'X-Topic: smoke' -H 'Content-Type: application/json' \
  -d "{\"to\":\"ws:B\",\"thread_id\":\"$thread_id\",\"body\":\"21st\"}")
[[ "$status" == 409 ]]
node -e 'require("assert/strict").equal(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).error,"THREAD_ESCALATED")' "$scratch/error"
node -e 'const fs=require("fs"),assert=require("assert/strict"); assert.match(fs.readFileSync(process.argv[1],"utf8"),/"kind":"escalation"/); assert.match(fs.readFileSync(process.argv[2],"utf8"),/"event":"thread_escalated"/)' "$scratch/stream" "$scratch/log"
echo 'PASS: curl A→ws:B, SSE delivery, ack, takeover rejection, 20-message escalation, 21st rejection, workspace rejection'
