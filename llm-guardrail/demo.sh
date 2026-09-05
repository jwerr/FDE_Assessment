#!/usr/bin/env bash
# Boots the mock provider + gateway, streams a PII-laden reply through the
# gateway with 1-character chunks and 15 ms pacing, and prints what the
# client sees as it arrives.
set -euo pipefail
cd "$(dirname "$0")"

python mock_llm_provider.py 2>provider.log &  P=$!
UPSTREAM_URL=http://127.0.0.1:9002/v1/chat/completions PORT=8081 python llm_gateway.py 2>gateway.log &  G=$!
trap 'kill $P $G 2>/dev/null' EXIT
sleep 1.5

TEXT='Sure! Reach me at john.doe@example.com, my card is 4111 1111 1111 1111 and SSN 123-45-6789. Order 1234567890123456 is not a card.'

echo "▶ Streaming through the gateway (chunk_size=1, 15 ms per chunk). Output as the client receives it:"
echo
STREAM_BODY=$(python - "$TEXT" <<'PY'
import json, sys
print(json.dumps({"model": "mock-1", "stream": True,
                  "messages": [{"role": "user", "content": sys.argv[1]}],
                  "mock": {"chunk_size": 1, "delay_ms": 15}}))
PY
)
PLAIN_BODY=$(python - "$TEXT" <<'PY'
import json, sys
print(json.dumps({"model": "mock-1", "stream": False,
                  "messages": [{"role": "user", "content": sys.argv[1]}]}))
PY
)

curl -sN http://127.0.0.1:8081/v1/chat/completions -H 'Content-Type: application/json' -d "$STREAM_BODY" \
  | python -u -c '
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line.startswith("data:"): continue
    data = line[5:].strip()
    if data == "[DONE]": break
    c = json.loads(data)["choices"][0]["delta"].get("content")
    if c: sys.stdout.write(c); sys.stdout.flush()
print()'
echo
echo "▶ Gateway metrics for that stream:"
curl -s http://127.0.0.1:8081/stats | python -m json.tool
echo
echo "▶ Non-streaming request, same text:"
curl -s http://127.0.0.1:8081/v1/chat/completions -H 'Content-Type: application/json' -d "$PLAIN_BODY" \
  | python -c 'import sys,json; print(json.load(sys.stdin)["choices"][0]["message"]["content"])'
echo
echo "▶ Upstream 500 → sanitised gateway error (provider said: \"internal provider explosion at /srv/llm/worker.py:123\"):"
curl -s -w '   [HTTP %{http_code}]\n' http://127.0.0.1:8081/v1/chat/completions -H 'Content-Type: application/json' \
  -d '{"model":"mock-1","messages":[{"role":"user","content":"x"}],"mock":{"status":500}}'
