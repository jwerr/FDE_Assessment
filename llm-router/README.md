# Rate-Limiting & Model-Fallback Router (Task 4)

An LLM gateway module that enforces a per-tenant **token-aware sliding-window
rate limit** (default 50 000 tokens/min per API key), routes each completion
to a primary provider and **fails over to a secondary** on `429`, a 3 s
timeout, `5xx` or a network error, and returns one **standardised error
payload** for every failure. All limiter state lives in **on-disk SQLite**.

```
client ─Bearer <tenant key>─▶ gateway.py
                               ├─ ratelimiter.py   reserve tokens  ──┐  SQLite
                               ├─ router.py        primary ─429/3s/5xx─▶ secondary
                               └─ ratelimiter.py   settle with real usage ─┘  (gateway.sqlite3)
```

## Run

```bash
pip install -r requirements.txt
PROVIDER_NAME=primary   PORT=9101 python mock_provider.py      # terminal 1
PROVIDER_NAME=secondary PORT=9102 python mock_provider.py      # terminal 2
python gateway.py                                              # terminal 3 (:8082, gateway.sqlite3)
./demo.sh                                                      # or: boots all three, walks every path
```

Demo tenants (`Authorization: Bearer …`): `sk-tenant-alpha` / `sk-tenant-beta`
(50 000 tokens/min) and `sk-tenant-tiny` (300 tokens/min, trips in a few calls).
Mock provider behaviour is switchable at runtime:
`curl -X POST localhost:9101/admin/mode -d '{"mode":"429"}'` — modes `ok · 429 · 500 · slow · hang`.

Real providers: `PRIMARY_URL=https://api.openai.com/v1/chat/completions PRIMARY_API_KEY=sk-… SECONDARY_URL=… SECONDARY_API_KEY=… SECONDARY_MODEL=…`

`./demo.sh` output, abridged:

```
▶ 2. Primary returns 429 → automatic failover to secondary
   HTTP/1.1 200 OK   X-Served-By: secondary   X-Fallback: true
   X-Attempts: primary=rate_limited(0ms);secondary=ok(2ms)

▶ 3. Primary times out → failover to secondary
   X-Attempts: primary=timeout(3003ms);secondary=ok(1ms)

▶ 4. Primary slow AND secondary 500 → standardised 503, nothing leaked
   {"error": {"code": "upstream_unavailable", "message": "No model provider could serve the request", "request_id": "req_b7210ee201f3"}}

▶ 5. Tenant rate limit: 'tiny' has 300 tokens/min
   200 OK  X-RateLimit-Remaining: 249 … 45
   429 Too Many Requests  Retry-After: 60  X-RateLimit-Remaining: 45
```

## Test

```bash
python -m pytest -v        # 34 tests, ~2 s
```

* `test_ratelimiter.py` (10) — injectable clock: window slides and rows are
  physically evicted, `Retry-After` computed from the oldest rows, reserve →
  settle → release, tenant isolation, **50 concurrent reservations against a
  1 000-token budget in 100-token bites: exactly 10 succeed**, state survives
  reopening the DB file.
* `test_gateway.py` (24) — real HTTP with two live mock providers: failover on
  429 / 500 / timeout / connection-refused, **timed-out primary attempt is
  cancelled** (provider observes the disconnect), **primary answering inside
  the deadline wins with no fallback**, both-down → 503, both-429 → 429,
  reservation released on failure, 429 with `Retry-After` through the HTTP
  layer, usage settles to the provider's real `usage.total_tokens`, 20
  parallel requests against a 300-token tenant → exactly 2 succeed, every
  error has the identical shape and leaks nothing, audit row written.

## Design notes (mapped to the evaluation criteria)

### Async concurrency & timeout race conditions

Each provider attempt is `asyncio.wait_for(_call(), timeout_s)`. When the
deadline fires, `wait_for` **cancels** the inner task; because the HTTP body
is read inside that task, cancellation closes the socket. Two consequences
the tests pin down:

* A primary that answers at 3.2 s cannot race the secondary's answer — its
  task is gone and its bytes are never read
  (`test_timed_out_primary_attempt_is_cancelled_not_left_running`, verified
  from the provider's side with `handler_cancellation`).
* A primary that answers at 2.9 s wins and the secondary is never contacted
  (`test_slow_but_within_timeout_is_served_by_primary`).

aiohttp's own `ClientTimeout` is set to `None` for the attempt so there is
exactly one deadline, owned by the router, rather than two that can
disagree.

The rate limiter's check-and-reserve is a single `BEGIN IMMEDIATE`
transaction. SQLite's write lock makes it atomic across coroutines *and*
across processes, which is why 50 racing coroutines get exactly 10 wins
(`test_concurrent_reservations_never_exceed_limit`) and 20 racing HTTP
requests get exactly 2 (`test_concurrent_requests_respect_limit`). Within
one process the DB is additionally serialised through an `asyncio.Lock` +
worker thread so the event loop is never blocked on disk I/O.

### Rate-limiter state eviction & token tracking

* **Sliding log, not fixed window.** Usage at any instant = Σ tokens of rows
  with `ts_ms ≥ now − window`. There is no burst of 2× the limit at a minute
  boundary, which fixed windows allow.
* **Eviction is physical.** Every `reserve` first `DELETE`s that tenant's
  rows older than the window (indexed range scan, cheap); a background
  sweeper runs a global sweep every 30 s. `test_window_slides_and_evicts`
  asserts the row count, not just the sum.
* **Reserve → settle.** Token cost isn't known until the provider answers,
  so the gateway reserves `estimate(prompt) + max_tokens` pessimistically,
  then overwrites the row with `usage.total_tokens` from the response, or
  deletes it if the request failed. A caller cannot exhaust the budget by
  sending requests that fail, and a caller who sets `max_tokens: 5000` but
  gets a 40-token answer is charged 40 (`test_usage_settles_to_actual_not_estimate`).
* **`Retry-After` is computed, not guessed.** The limiter walks the tenant's
  rows oldest-first until enough tokens would be freed and reports that
  timestamp + window. A request that could never fit reports a full window.
* **Persistence.** Restarting the gateway does not reset anyone's budget
  (`test_state_survives_restart`); several gateway processes on one host can
  share the file.

### Graceful fallback & standardised error sanitisation

| Primary outcome              | Router action                                           |
|------------------------------|---------------------------------------------------------|
| 2xx                          | return                                                  |
| 429                          | fail over                                               |
| no response in 3 s           | cancel attempt, fail over                               |
| 5xx / connection / bad JSON  | fail over                                               |
| other 4xx                    | **no** fail over — the request is wrong; return 400     |

The router raises `RouterError(code)`; only the gateway turns codes into
text, and it has a fixed table:

| HTTP | `error.code`            | when                                          |
|------|-------------------------|-----------------------------------------------|
| 401  | `unauthorized`          | missing / unknown API key                     |
| 400  | `invalid_request`       | bad body, or a provider rejected it (4xx)     |
| 429  | `rate_limited`          | tenant budget exhausted (+ `Retry-After`, `X-RateLimit-*`) |
| 429  | `upstream_rate_limited` | every provider returned 429                   |
| 503  | `upstream_unavailable`  | every provider failed (timeout / 5xx / network) |

Every error body is exactly `{"error": {"code", "message", "request_id"}}`.
Provider response bodies (which in the mocks deliberately contain a key
fragment, a log path and a Java stack frame), exception reprs and hostnames
are written to the gateway log keyed by `request_id`, never to the client
(`test_every_error_has_identical_shape`, `test_failover_on_429`).

Successful responses carry `X-Served-By`, `X-Fallback`, `X-Attempts`
(per-provider outcome and latency) and `X-RateLimit-Limit/Remaining`; every
request, success or failure, gets a row in `request_log` (provider chain,
fallback flag, status, error code, latency, tokens) — `GET /admin/requests`
or open the SQLite file.

### Trade-offs worth stating

* The pre-flight token estimate is `len(text)/4`. It only needs to be
  pessimistic enough to stop a burst; the settle step corrects it. Swapping
  in `tiktoken` is a one-line change in `estimate_tokens`.
* Failover is sequential (try primary, then secondary), so worst case adds
  one full timeout. Hedged requests (fire the secondary at, say, 1.5 s and
  take whichever answers first) cut tail latency at the cost of double
  spend; the router's structure supports adding that.
* SQLite is the right call for one host or a small fleet sharing a disk;
  for a multi-node gateway the same reserve/settle protocol maps directly
  onto Redis `MULTI`/Lua.
* Streaming responses are downgraded to non-streaming here; Task 3's
  gateway shows how the streaming path works and the two compose (limiter
  in front, redactor behind).

## Files

```
ratelimiter.py     SQLite schema, Database wrapper, SlidingWindowTokenLimiter (stdlib sqlite3)
router.py          ModelRouter: wait_for deadline, failover policy, RouterError codes
gateway.py         HTTP layer: tenant auth, reserve → route → settle, error table, audit log
mock_provider.py   OpenAI-shaped mock with runtime-switchable modes and cancellation counting
test_ratelimiter.py, test_gateway.py
demo.sh            boots everything and walks the five scenarios
requirements.txt
```
