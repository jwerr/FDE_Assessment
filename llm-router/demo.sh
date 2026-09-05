#!/usr/bin/env bash
# Boots two mock providers + the gateway, then walks through every routing
# path: healthy, 429 failover, 3 s timeout failover, both down, and tenant
# rate limiting. Plain JSON literals only (macOS bash 3.2 / zsh safe).
set -eu
cd "$(dirname "$0")"

P1=9101; P2=9102; GP=8082
rm -f demo.sqlite3 demo.sqlite3-wal demo.sqlite3-shm

PROVIDER_NAME=primary   PORT=$P1 python mock_provider.py 2>primary.log &   A=$!
PROVIDER_NAME=secondary PORT=$P2 python mock_provider.py 2>secondary.log & B=$!
DB_PATH=demo.sqlite3 PORT=$GP PRIMARY_URL=http://127.0.0.1:$P1/v1/chat/completions \
  SECONDARY_URL=http://127.0.0.1:$P2/v1/chat/completions python gateway.py 2>gateway.log & C=$!
trap 'kill $A $B $C 2>/dev/null' EXIT

for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -s -o /dev/null http://127.0.0.1:$GP/health && break; sleep 0.5
done
if ! curl -s -o /dev/null http://127.0.0.1:$GP/health; then
  echo "!! gateway failed to start (port in use?)"; cat gateway.log; exit 1
fi

GW=http://127.0.0.1:$GP/v1/chat/completions
CT='Content-Type: application/json'
BODY='{"model":"gpt-x","max_tokens":50,"messages":[{"role":"user","content":"Summarise the quarterly report in one sentence."}]}'

call () {  # call <api-key> <label>
  echo "▶ $2"
  curl -s -D - -o /tmp/demo_body.json "$GW" -H "$CT" -H "Authorization: Bearer $1" -d "$BODY" \
    | grep -iE '^(HTTP|X-Served-By|X-Fallback|X-Attempts|X-RateLimit-Remaining|Retry-After)' | sed 's/^/   /'
  echo -n "   body: "; head -c 200 /tmp/demo_body.json; echo; echo
}
mode () { curl -s -o /dev/null -X POST http://127.0.0.1:$1/admin/mode -H "$CT" -d "{\"mode\":\"$2\"}"; }

call sk-tenant-alpha "1. Both providers healthy → primary"

mode $P1 429
call sk-tenant-alpha "2. Primary returns 429 → automatic failover to secondary"

mode $P1 slow   # primary answers after 5 s; gateway gives up at 3 s
echo "   (primary now answers after 5 s; router deadline is 3 s — this takes ~3 s)"
call sk-tenant-alpha "3. Primary times out → failover to secondary"

mode $P2 500
call sk-tenant-alpha "4. Primary slow AND secondary 500 → standardised 503, nothing leaked"

mode $P1 ok; mode $P2 ok
echo "▶ 5. Tenant rate limit: 'tiny' has 300 tokens/min. Firing 7 requests (each ≈51 tokens):"
for i in 1 2 3 4 5 6 7; do
  curl -s -D - -o /dev/null "$GW" -H "$CT" -H "Authorization: Bearer sk-tenant-tiny" -d "$BODY" \
    | grep -iE '^(HTTP|X-RateLimit-Remaining|Retry-After)' | tr -d '\r' | tr '\n' ' ' | sed 's/^/   /'; echo
done
echo
echo "▶ Usage as stored in SQLite (demo.sqlite3):"
curl -s http://127.0.0.1:$GP/v1/usage -H "Authorization: Bearer sk-tenant-tiny"; echo; echo
echo "▶ Audit log (request_log table, newest first):"
python - <<'PY'
import sqlite3
c = sqlite3.connect("demo.sqlite3")
for r in c.execute("SELECT request_id, tenant_id, provider, fallback, status, error_code, latency_ms, tokens "
                   "FROM request_log ORDER BY ts_ms DESC LIMIT 8"):
    print("   ", r)
PY
