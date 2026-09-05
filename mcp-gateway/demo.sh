#!/usr/bin/env bash
# One-shot demo: boots the mock downstream + gateway, fires four requests, cleans up.
set -euo pipefail
cd "$(dirname "$0")"

python mock_mcp_server.py 2>mock.log &  MOCK=$!
DOWNSTREAM_URL=http://127.0.0.1:9001/mcp PORT=8080 python gateway.py 2>gateway.log &  GW=$!
trap 'kill $MOCK $GW 2>/dev/null' EXIT
sleep 1.5

H='Content-Type: application/json'
GW_URL=http://127.0.0.1:8080/mcp

echo "▶ 1. viewer → tools/list        (forwarded transparently)"
curl -s "$GW_URL" -H "$H" -H 'Authorization: Bearer tok_viewer_456' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'; echo; echo

echo "▶ 2. viewer → admin_reset_key   (intercepted: -32001, downstream never called)"
curl -s -w '   [HTTP %{http_code}]\n' "$GW_URL" -H "$H" -H 'Authorization: Bearer tok_viewer_456' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"key_id":"master"}}}'; echo

echo "▶ 3. admin  → admin_reset_key   (allowed, forwarded)"
curl -s -w '   [HTTP %{http_code}]\n' "$GW_URL" -H "$H" -H 'Authorization: Bearer tok_admin_123' \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"key_id":"master"}}}'; echo

echo "▶ 4. no token → 401 / -32002"
curl -s -w '   [HTTP %{http_code}]\n' "$GW_URL" -H "$H" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/list"}'; echo

echo "▶ What the downstream actually executed (only the admin's call; client token stripped):"
curl -s http://127.0.0.1:9001/calls; echo; echo

echo "▶ Gateway decision log:"
grep -E 'ALLOW|DENY|401' gateway.log | cut -c25-
