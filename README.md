# Forward Deployed Engineer Assessment — Submission

Four self-contained Python projects, one per task. Each folder has its own
`README.md` (design notes mapped to the evaluation criteria), a test suite,
a `demo.sh`, and a pinned `requirements.txt`.

| Task | Folder | What it is | Tests |
|------|--------|------------|-------|
| 1 | [`mcp-customer-server/`](mcp-customer-server/) | stdio MCP server with two tools, Pydantic validation, JSON-RPC error codes, stdout guarded for pure JSON-RPC | 27 |
| 2 | [`mcp-gateway/`](mcp-gateway/) | HTTP/JSON-RPC reverse proxy: Bearer-token roles, `tools/call` authorization, `-32001` interception, byte-for-byte forwarding | 42 |
| 3 | [`llm-guardrail/`](llm-guardrail/) | LLM gateway that redacts emails / SSNs / cards from an SSE token stream with bounded holdback (no buffering, ~1 token added latency) | 113 |
| 4 | [`llm-router/`](llm-router/) | SQLite sliding-window token limiter (reserve → settle) + primary/secondary failover on 429 / 3 s timeout / 5xx, standardised errors | 34 |

## Running any task

```bash
cd llm-guardrail          # or any other task folder
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m pytest -v                   # the tests
./demo.sh                             # boots the servers and walks the scenarios
```

Requires Python 3.10+. All 216 tests run in about 25 seconds total; every
suite boots real servers on ephemeral ports rather than mocking HTTP.

## Design choices common to all four

* **Python + `aiohttp`** (Tasks 2–4) so the HTTP server and upstream client
  share one event loop; Task 1 uses the official `mcp` SDK. No other runtime
  dependencies; SQLite and the JWT/Luhn/regex logic are standard library.
* **Errors are sanitised at a single choke point** in each gateway: a fixed
  table maps internal codes to client-facing payloads, and upstream bodies or
  exception text only ever reach the log, keyed by request ID.
* **Tests assert the property, not the happy path**: stdout is parsed as
  JSON-RPC line by line (T1); the downstream records what actually reached it
  (T2); streaming output must equal whole-text redaction under thousands of
  random chunkings (T3); 50 racing coroutines must get exactly N reservations
  and a timed-out attempt must be observed cancelled from the provider's side
  (T4).
* **Trade-offs are written down** in each README rather than hidden.

## Where the interesting decisions are

* T1 `server.py` — why the `tools/call` handler bypasses the SDK decorator
  (the decorator turns every exception into `isError: true`; the task wants
  JSON-RPC error codes).
* T2 `gateway.py::authorize` / `handle_mcp` — fail-closed batches, literal
  prefix match, client token stripped before forwarding.
* T3 `redactor.py::holdback_length` — the two subtleties the fuzz test found
  (digit-led email tails; lookarounds must stay inside the digit run).
* T4 `router.py::_try` — a single `wait_for` deadline that cancels the
  attempt, so a late primary can't race the secondary;
  `ratelimiter.py::reserve` — `BEGIN IMMEDIATE` check-and-insert.
