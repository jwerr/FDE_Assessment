# MCP Security Gateway (Task 2)

An HTTP/JSON-RPC reverse proxy that sits between an AI-agent client and a
downstream MCP server, authenticates the caller from a Bearer token, and
enforces tool-level authorization before anything reaches the downstream.

```
agent client ──POST /mcp──▶  gateway :8080  ──POST /mcp──▶  mock MCP :9001
   Bearer tok_…               │  authn → parse → authz → forward
                              └─ viewer calling admin_* ⇒ -32001, never forwarded
```

## Run

```bash
pip install -r requirements.txt

# terminal 1 – downstream mock MCP server (no auth of its own)
python mock_mcp_server.py

# terminal 2 – the gateway
python gateway.py
```

Or run everything with one script (boots both, fires the four canonical
requests, prints the downstream's call log and the gateway's decision log):

```bash
./demo.sh
```

Default tokens (override with `GATEWAY_TOKENS="token:role:subject,…"`):

| Token            | Role   |
|------------------|--------|
| `tok_admin_123`  | admin  |
| `tok_viewer_456` | viewer |

```bash
# viewer: tools/list forwarded transparently
curl localhost:8080/mcp -H 'Content-Type: application/json' -H 'Authorization: Bearer tok_viewer_456' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

# viewer: admin tool intercepted → HTTP 403, JSON-RPC -32001
curl localhost:8080/mcp -H 'Content-Type: application/json' -H 'Authorization: Bearer tok_viewer_456' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"key_id":"master"}}}'
# → {"jsonrpc":"2.0","id":2,"error":{"code":-32001,"message":"Unauthorized Tool Call",
#      "data":{"tool":"admin_reset_key","required_role":"admin","role":"viewer"}}}

# admin: same call forwarded → 200 with the downstream's result
curl localhost:8080/mcp -H 'Content-Type: application/json' -H 'Authorization: Bearer tok_admin_123' \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"key_id":"master"}}}'
```

`GET /calls` on the mock shows exactly which tool calls got through, and
which `Authorization` header (if any) the downstream saw.

## Test

```bash
python -m pytest test_gateway.py -v      # 42 tests, ~0.3 s
```

The suite boots a real gateway and a real mock downstream on ephemeral ports
and talks to them over HTTP. The most important assertion appears in several
tests: after a denied request, `mock.calls == []` — the downstream was never
invoked.

## Configuration

| Env var              | Default                          | Meaning                                       |
|----------------------|----------------------------------|-----------------------------------------------|
| `PORT`               | `8080`                           | gateway listen port                           |
| `DOWNSTREAM_URL`     | `http://127.0.0.1:9001/mcp`      | downstream MCP endpoint                       |
| `GATEWAY_TOKENS`     | two demo tokens                  | `token:role:subject` list, comma-separated    |
| `GATEWAY_JWT_SECRET` | *(unset)*                        | if set, HS256 JWTs with a `role` claim are accepted too |
| `PROTECTED_PREFIXES` | `admin_`                         | tool-name prefixes that require `admin`       |
| `UPSTREAM_TOKEN`     | *(unset)*                        | credential the gateway presents downstream    |
| `UPSTREAM_TIMEOUT_S` | `10`                             | downstream request timeout                    |

## Design notes (mapped to the evaluation criteria)

### Parsing the JSON-RPC wire format

`validate_envelope` checks the whole envelope, not just `method`: `jsonrpc`
must be exactly `"2.0"`, `method` a non-empty string, `id` a string/number/null
if present, `params` an object or array if present. Errors follow the spec's
rules for the `id` field: a parse error or unusable `id` yields `"id": null`,
otherwise the caller's `id` is echoed so the client can correlate.

Batch requests (JSON arrays) are supported and every element is validated and
authorized individually. Notifications (no `id`) are handled: a denied
notification still gets an error object with `"id": null`, and allowed ones are
forwarded.

The gateway never re-serialises the request. After inspecting a parsed copy,
it forwards the **original bytes**, so the downstream sees precisely what the
client sent (whitespace, key order, unicode — `test_body_forwarded_byte_for_byte`).

### Proxy construction and forwarding

* Built on `aiohttp` — the server and the upstream client share one event
  loop, and a single pooled `ClientSession` lives for the app's lifetime.
* Hop-by-hop headers (`Connection`, `Transfer-Encoding`, `Host`, …) are
  stripped in both directions per RFC 7230 §6.1; everything else, including
  `Mcp-Session-Id` and `Content-Type`, is relayed.
* The **client's `Authorization` header is never forwarded**. The gateway
  substitutes its own `UPSTREAM_TOKEN`, so the downstream trusts the gateway,
  not end-user credentials (`test_client_token_is_stripped_…`). `X-Forwarded-For`
  is added for audit.
* Response bodies are **streamed** chunk-by-chunk with `iter_any()`, so a
  Streamable-HTTP downstream that answers with `text/event-stream` works
  without buffering.
* Downstream unreachable or timing out → HTTP 502 with JSON-RPC `-32003`. The
  client sees a stable message; the exception detail goes to the gateway log
  only (`test_upstream_down_returns_502_not_stack_trace`).

### Method-level authorization

```
authenticate()  →  Principal(subject, role)        401 / -32002 on failure
validate_envelope()                                400 / -32600 on failure
authorize()     →  tools/call + name.startswith(protected prefix) + role != admin
                   ⇒ 403 / -32001 "Unauthorized Tool Call", not forwarded
forward()       →  everything else
```

Points worth calling out:

* **Only `tools/call` is policy-checked.** `initialize`, `tools/list`, `ping`
  pass through — the task says `tools/list` is forwarded transparently, and
  the viewer *seeing* `admin_reset_key` in the list while being unable to
  call it is the intended behaviour (and what the tests assert).
* **Prefix match is literal.** `Admin_reset_key`, `xadmin_reset_key` and
  `administer` are not protected — they are forwarded and the downstream
  rejects them as unknown tools. Tests pin this down so a future "helpful"
  case-insensitive change is a deliberate decision, not an accident.
* **Fail closed on batches.** If any message in a batch is denied, the whole
  batch is rejected and nothing is forwarded. Forwarding the allowed subset
  would let a caller smuggle a denied call alongside legitimate ones and
  learn from timing/ordering what got through.
* **`tools/call` with no `name`** is not "authorized by default": there is no
  tool to protect, so it is forwarded and the downstream returns `-32602`.
* Two token formats: opaque API keys from a registry (what the task literally
  asks for) and HS256 JWTs verified with `hmac` from the standard library
  (signature, `alg`, `exp`, required `role` claim). Forged, expired, and
  role-less JWTs are all covered by tests.

### Error-code map

| HTTP | JSON-RPC | When                                    |
|------|----------|-----------------------------------------|
| 401  | -32002   | missing / unknown / invalid bearer token |
| 400  | -32700   | body is not JSON                         |
| 400  | -32600   | JSON but not a valid JSON-RPC envelope   |
| 403  | -32001   | `tools/call` on a protected tool without `admin` |
| 502  | -32003   | downstream unreachable / timed out       |
| *relayed* | *relayed* | anything the downstream returns  |

## Files

```
gateway.py           the proxy (≈300 lines, one dependency: aiohttp)
mock_mcp_server.py   downstream mock with echo / get_server_time / admin_* tools + GET /calls
test_gateway.py      42 end-to-end + unit tests
demo.sh              boots both, runs the four canonical requests
requirements.txt
```

### Pairing with Task 1

The Task 1 server speaks stdio, not HTTP. To put it behind this gateway,
wrap it with a stdio→HTTP bridge (e.g. `mcp-proxy`) and point
`DOWNSTREAM_URL` at the bridge; the gateway is transport-agnostic on the
client side and only assumes Streamable-HTTP on the downstream side.
