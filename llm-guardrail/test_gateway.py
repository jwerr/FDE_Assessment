"""End-to-end tests: real gateway + real mock provider over HTTP/SSE.

Run: python -m pytest test_gateway.py -v
"""

from __future__ import annotations

import asyncio
import json
import time

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import unused_port

import llm_gateway as gw
import mock_llm_provider as provider
from redactor import redact

PII_TEXT = ("Sure! Contact john.doe@example.com, card 4111 1111 1111 1111, "
            "SSN 123-45-6789. Reference 1234567890123456 is just an order id.")


@pytest.fixture
async def stack():
    p_port, g_port = unused_port(), unused_port()

    p_runner = web.AppRunner(provider.create_app())
    await p_runner.setup()
    await web.TCPSite(p_runner, "127.0.0.1", p_port).start()

    cfg = gw.Config(upstream_url=f"http://127.0.0.1:{p_port}/v1/chat/completions",
                    upstream_api_key="sk-gateway-owned", connect_timeout_s=2, read_timeout_s=5,
                    max_event_bytes=64 * 1024)
    g_app = gw.create_app(cfg)
    g_runner = web.AppRunner(g_app)
    await g_runner.setup()
    await web.TCPSite(g_runner, "127.0.0.1", g_port).start()

    async with ClientSession(base_url=f"http://127.0.0.1:{g_port}") as http:
        yield type("S", (), {"http": http, "app": g_app, "p_runner": p_runner})()

    await g_runner.cleanup()
    await p_runner.cleanup()


def _req(text: str, stream=True, **mock):
    return {"model": "mock-1", "stream": stream,
            "messages": [{"role": "user", "content": text}], "mock": mock}


async def collect_sse(resp):
    """Read an SSE response; return (content_pieces, all_events, first_content_time)."""
    pieces, events, t_first = [], [], None
    buf = b""
    async for chunk in resp.content.iter_any():
        buf += chunk
        while b"\n\n" in buf:
            ev, buf = buf.split(b"\n\n", 1)
            for line in ev.split(b"\n"):
                if not line.startswith(b"data:"):
                    continue
                data = line[5:].strip()
                events.append(data)
                if data == b"[DONE]":
                    continue
                obj = json.loads(data)
                c = obj.get("choices", [{}])[0].get("delta", {}).get("content")
                if c:
                    if t_first is None:
                        t_first = time.perf_counter()
                    pieces.append(c)
    return pieces, events, t_first


# --------------------------------------------------------------------------- #
# Streaming redaction
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("chunk_size", [1, 2, 3, 7, "random", 500])
async def test_stream_redacts_pii_split_across_chunks(stack, chunk_size):
    async with stack.http.post("/v1/chat/completions", json=_req(PII_TEXT, chunk_size=chunk_size)) as r:
        assert r.status == 200
        assert r.headers["Content-Type"].startswith("text/event-stream")
        pieces, events, _ = await collect_sse(r)
    assert "".join(pieces) == redact(PII_TEXT)
    joined = "".join(pieces)
    assert "john.doe" not in joined and "4111" not in joined and "123-45-6789" not in joined
    assert "1234567890123456" in "".join(pieces)       # non-Luhn number left alone
    assert events[-1] == b"[DONE]"


async def test_stream_events_are_well_formed_openai_chunks(stack):
    async with stack.http.post("/v1/chat/completions", json=_req("hello world, a@b.co!", chunk_size=1)) as r:
        pieces, events, _ = await collect_sse(r)
    objs = [json.loads(e) for e in events if e != b"[DONE]"]
    assert all(o["object"] == "chat.completion.chunk" for o in objs)
    assert objs[0]["choices"][0]["delta"].get("role") == "assistant"     # role event relayed
    assert objs[-1]["choices"][0]["finish_reason"] == "stop"             # finish event relayed
    assert "".join(pieces) == "hello world, [REDACTED]!"


async def test_ttft_is_not_end_of_stream(stack):
    """With 60 chunks × 25 ms, the first token must arrive long before the last."""
    text = "word " * 60 + "then a@b.co at the end."
    t0 = time.perf_counter()
    async with stack.http.post("/v1/chat/completions", json=_req(text, chunk_size=5, delay_ms=25)) as r:
        pieces, _, t_first = await collect_sse(r)
    t_end = time.perf_counter()
    ttft, total = t_first - t0, t_end - t0
    assert total > 1.0                       # the stream really took a while
    assert ttft < total / 4, f"ttft={ttft:.2f}s total={total:.2f}s – gateway is buffering"
    assert "".join(pieces) == redact(text)
    stats = stack.app[gw.STATS]
    assert stats["ttft_s"] is not None and stats["ttft_s"] < total / 4


async def test_stream_does_not_accumulate_memory(stack):
    text = ("lorem ipsum 4111 1111 1111 1111 dolor a@b.co sit. " * 2000)   # ~100 KB
    async with stack.http.post("/v1/chat/completions", json=_req(text, chunk_size=13)) as r:
        pieces, _, _ = await collect_sse(r)
    assert "".join(pieces) == redact(text)
    stats = stack.app[gw.STATS]
    assert stats["redactions"] == 4000
    assert stats["max_buffer_chars"] <= gw_max_buffer_bound(13)
    assert stats["events_in"] > 7000


def gw_max_buffer_bound(chunk: int) -> int:
    from redactor import MAX_HOLD
    return MAX_HOLD + chunk


async def test_stream_with_no_pii_passes_text_unchanged(stack):
    text = "The quick brown fox jumps over the lazy dog. Numbers: 2024, 3.14, (555) 123-4567."
    async with stack.http.post("/v1/chat/completions", json=_req(text, chunk_size="random")) as r:
        pieces, _, _ = await collect_sse(r)
    assert "".join(pieces) == text


async def test_client_disconnect_mid_stream_does_not_crash_gateway(stack):
    text = "word " * 400
    async with stack.http.post("/v1/chat/completions", json=_req(text, chunk_size=5, delay_ms=10)) as r:
        await r.content.read(200)          # read a little…
    # …then the context manager closes the connection. Gateway must stay healthy:
    await asyncio.sleep(0.1)
    async with stack.http.get("/health") as h:
        assert h.status == 200


# --------------------------------------------------------------------------- #
# Non-streaming
# --------------------------------------------------------------------------- #
async def test_non_stream_response_is_redacted(stack):
    async with stack.http.post("/v1/chat/completions", json=_req(PII_TEXT, stream=False)) as r:
        assert r.status == 200
        body = await r.json()
        assert r.headers["X-Redactions"] == "3"
    assert body["choices"][0]["message"]["content"] == redact(PII_TEXT)
    assert body["usage"]["total_tokens"] > 0       # rest of the body untouched


# --------------------------------------------------------------------------- #
# Error sanitisation & proxy hygiene
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("upstream_status, expect_status, expect_type", [
    (500, 502, "upstream_error"),
    (503, 502, "upstream_error"),
    (429, 429, "rate_limited"),
    (401, 400, "invalid_request"),
])
async def test_upstream_errors_are_sanitised(stack, upstream_status, expect_status, expect_type):
    async with stack.http.post("/v1/chat/completions", json=_req("x", status=upstream_status)) as r:
        assert r.status == expect_status
        body = await r.json()
    assert body["error"]["type"] == expect_type
    assert body["error"]["request_id"].startswith("req_")
    assert "worker.py" not in json.dumps(body) and "explosion" not in json.dumps(body)


async def test_upstream_down_is_502(stack):
    await stack.p_runner.cleanup()
    async with stack.http.post("/v1/chat/completions", json=_req("x")) as r:
        assert r.status == 502
        assert (await r.json())["error"]["type"] == "upstream_unavailable"


async def test_bad_request_body(stack):
    async with stack.http.post("/v1/chat/completions", data=b"not json") as r:
        assert r.status == 400
        assert (await r.json())["error"]["type"] == "invalid_request"


async def test_request_id_is_propagated(stack):
    async with stack.http.post("/v1/chat/completions", json=_req("hi there."),
                               headers={"X-Request-Id": "trace-123"}) as r:
        assert r.headers["X-Request-Id"] == "trace-123"
        await r.read()


# --------------------------------------------------------------------------- #
# SSE parser unit tests
# --------------------------------------------------------------------------- #
class _FakeContent:
    def __init__(self, chunks): self._chunks = chunks
    async def iter_any(self):
        for c in self._chunks:
            yield c


async def test_sse_parser_handles_arbitrary_chunk_boundaries():
    raw = b"data: {\"a\":1}\n\ndata: {\"b\":2}\n\n: keep-alive\n\ndata: [DONE]\n\n"
    for n in (1, 2, 3, 5, 100):
        chunks = [raw[i:i + n] for i in range(0, len(raw), n)]
        events = [e async for e in gw.iter_sse_events(_FakeContent(chunks), 1024)]
        assert [gw.sse_data(e) for e in events] == ['{"a":1}', '{"b":2}', None, "[DONE]"]


async def test_sse_parser_crlf_and_multiline_data():
    raw = b"event: x\r\ndata: line1\r\ndata: line2\r\n\r\n"
    events = [e async for e in gw.iter_sse_events(_FakeContent([raw]), 1024)]
    assert gw.sse_data(events[0]) == "line1\nline2"


async def test_sse_parser_enforces_event_size_limit():
    with pytest.raises(ValueError):
        async for _ in gw.iter_sse_events(_FakeContent([b"data: " + b"x" * 5000]), 1024):
            pass
