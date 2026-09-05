# LLM Gateway Streaming Guardrail — PII Redaction (Task 3)

An LLM gateway that proxies chat-completion requests to a provider and
redacts emails, SSNs and credit-card numbers **from the token stream as it
flows through**, without buffering the response and without hurting
time-to-first-token.

```
client ──POST /v1/chat/completions──▶ llm_gateway :8081 ──▶ provider :9002
        ◀── SSE, PII → [REDACTED] ─────┘  StreamRedactor      (OpenAI SSE)
```

## Run

```bash
pip install -r requirements.txt
python mock_llm_provider.py            # terminal 1: mock provider (echoes the prompt, streams it)
python llm_gateway.py                  # terminal 2: gateway
./demo.sh                              # or: boots both and shows live redaction + metrics
```

To point at a real provider: `UPSTREAM_URL=https://api.openai.com/v1/chat/completions UPSTREAM_API_KEY=sk-… python llm_gateway.py`.
The gateway speaks the OpenAI chat-completions wire format on both sides.

Example (1-char upstream chunks, 15 ms apart — PII is guaranteed to be split):

```
$ ./demo.sh
Sure! Reach me at [REDACTED], my card is [REDACTED] and SSN [REDACTED]. Order 1234567890123456 is not a card.

{ "ttft_s": 0.079, "total_s": 1.977, "events_in": 132, "events_out": 22,
  "redactions": 3, "max_buffer_chars": 21 }
```

TTFT 79 ms on a ~2 s stream; peak buffer 21 characters.

## Test

```bash
python -m pytest -v          # 113 tests, ~4 s
```

* `test_redactor.py` (91) — pattern behaviour, **the chunking invariant**
  (every split point of every sample, plus 3000 fuzzed texts × random
  chunkings), latency (word released when the next delimiter arrives),
  memory bound on a 330 KB stream, cap behaviour.
* `test_gateway.py` (22) — end-to-end over real HTTP/SSE with a live mock
  provider: PII split at chunk sizes 1/2/3/7/random/500, TTFT measured
  against total stream time, 100 KB stream with buffer-size assertion,
  client disconnect, non-stream mode, upstream 5xx/429/401 sanitisation,
  SSE parser edge cases.

## Design

### The core idea: bounded holdback

A sensitive value can straddle any number of deltas:

```
"Contact jo" | "hn.doe@exa" | "mple.com or" | " 4111 1111 1" | "111 1111."
```

Buffering everything until `[DONE]` would catch it but destroys TTFT.
Emitting each delta after a regex pass misses everything that straddles a
boundary.

`StreamRedactor` does neither. After each delta it inspects only the **tail**
of what it holds and asks: *could this tail be the beginning of a sensitive
value?* Two candidate classes answer that:

| tail looks like…                          | example held     | released when…              |
|-------------------------------------------|------------------|-----------------------------|
| a run of email-legal chars `[A-Za-z0-9._%+@-]+` | `john.doe@exa` | a non-email char arrives (space, comma, `!` …) |
| a digit-led run of digits/space/dash `\d[\d -]*` | `4111 1111 1` | a non-number char arrives    |

Everything *before* the tail cannot participate in a future match, so it is
scanned and emitted immediately. The held tail is at most one word / one
number group, and is capped at `MAX_HOLD = 96` characters so a
delimiter-free blob (a hash, a URL, base64) never stalls the stream.

One subtlety the fuzz tests caught: the email class contains digits but not
spaces, the number class contains spaces but not letters. For
`"4111 1111 1111 1111."` the email tail is only `"1111."` — cutting there
would split the card. So when an email-class tail *starts with a digit*, the
hold extends backwards through any number run that precedes it.

A second subtlety: every pattern's lookarounds only inspect **digits**
(`(?<!\d)…(?!\d)`), never other context like a preceding dash. The holdback
guarantees a digit run is never split, so a pattern that only looks at
digits behaves identically on the whole text and on the run alone. A
lookbehind on a dash would break that (the dash may already be on the wire).
`test_fuzz_random_chunkings` found this too.

### Guarantees, and the tests that prove them

| Property        | Statement                                                                    | Test                                          |
|-----------------|------------------------------------------------------------------------------|-----------------------------------------------|
| Correctness     | `"".join(stream_output) == redact(full_text)` for **any** chunking            | `test_every_single_split_point`, `test_fuzz_random_chunkings` |
| Latency         | prose is released one token behind (when the following delimiter arrives)     | `test_plain_prose_latency_is_one_token`, `test_ttft_is_not_end_of_stream` |
| Memory          | buffer ≤ `MAX_HOLD + len(largest delta)` regardless of response length        | `test_buffer_bounded_on_long_stream`, `test_stream_does_not_accumulate_memory` |

### Patterns

* **Email** — `local@domain.tld`.
* **SSN** — `AAA-GG-SSSS` with consistent separator (`-`, space, or none) and
  the SSA structural rules: area ≠ 000/666/9xx, group ≠ 00, serial ≠ 0000.
  `000-12-3456` is left alone.
* **Card** — 13–19 digits with optional space/dash separators, **Luhn
  validated**. `1234 5678 9012 3456` (fails Luhn) and 13-digit epoch
  timestamps are left alone; `4111 1111 1111 1111`, `378282246310005`
  (Amex) are redacted. Longer digit runs are not mined for embedded PANs.

Adding a pattern is one `Pattern(name, regex, validator)` entry. If it uses a
character class the two candidates don't cover, add a third candidate regex
in `holdback_length`; the fuzz test will tell you if you got it wrong.

### The gateway

* **SSE parsing on raw bytes.** `iter_sse_events` accumulates only until the
  next blank line, so at most one partial event lives in memory; works on any
  TCP chunk boundary, `\r\n` or `\n`, multi-line `data:`. Events larger than
  `MAX_EVENT_BYTES` abort the stream rather than growing the buffer.
* **Delta rewriting.** For each event with `choices[0].delta.content`, the
  text goes through the redactor and the event is re-emitted with whatever
  was released (or not emitted at all if nothing was — the event count drops,
  the text doesn't). Role, `finish_reason`, tool-call, comment and keep-alive
  events are relayed untouched. On `[DONE]` the redactor is flushed into a
  final delta, then `[DONE]` is forwarded.
* **Headers before first byte.** The 200 + `text/event-stream` headers are
  sent before the first upstream event, so client TTFB is bounded by our
  connect time.
* **Non-streaming** requests are redacted in `choices[*].message.content`
  and returned with an `X-Redactions` count.
* **Errors are sanitised.** Upstream 5xx → 502 `upstream_error`; 429 → 429
  `rate_limited`; 4xx → 400 `invalid_request`; connect failure → 502
  `upstream_unavailable`; connect timeout → 504 `upstream_timeout`. The
  client always gets `{"error": {"type", "message", "request_id"}}`; the
  provider's body and the exception go to the gateway log. A failure
  mid-stream (status already sent) becomes an in-band `data: {"error":…}`
  event followed by a clean close.
* **Client disconnect** cancels the upstream read (aiohttp propagates
  `CancelledError`), so an abandoned request doesn't keep pulling tokens.
* `GET /stats` exposes the last stream's `ttft_s`, `total_s`, `events_in/out`,
  `redactions`, `max_buffer_chars`.

### Trade-offs worth stating

* The redactor is character-class based, so it holds back any trailing word
  (it might become `word@host.tld`). That is one token of latency; the
  alternative — releasing words eagerly — would miss every email whose local
  part arrived in an earlier chunk.
* Runs longer than 96 chars without a delimiter are released in pieces;
  PII inside such a run (e.g. an email glued to a 100-char base64 string)
  may be missed. That is a deliberate liveness-over-recall choice and the
  limit is configurable.
* Redaction is regex-based. For names, addresses, or free-form PII an
  NER model would be needed; the holdback structure would stay the same but
  the "could this be a prefix" question becomes harder.

## Files

```
redactor.py            StreamRedactor + patterns (≈200 lines, stdlib only)
llm_gateway.py         the proxy (aiohttp)
mock_llm_provider.py   OpenAI-format mock with chunk_size / delay_ms / status controls
test_redactor.py       91 unit tests incl. fuzzed chunking invariant
test_gateway.py        22 end-to-end tests
demo.sh                live demo with metrics
requirements.txt
```
