#!/usr/bin/env python3
"""
LLM Gateway with a streaming PII guardrail
==========================================

    client ──POST /v1/chat/completions──▶ gateway ──▶ LLM provider
             ◀── SSE, PII replaced by [REDACTED] ──┘   (SSE deltas)

The gateway forwards the request upstream and pipes the response back.  For
streaming responses it parses the SSE events *as they arrive*, extracts each
``choices[0].delta.content`` fragment, pushes it through a
``StreamRedactor`` (see ``redactor.py``), and re-emits an event carrying
whatever text is now provably safe.  Nothing is accumulated: memory per
stream is ``O(max SSE event + redactor holdback)``.

Latency design
--------------
* Upstream connection is opened immediately; the first upstream byte is
  processed the moment it arrives (``iter_any``), not per-line or per-event
  batch.
* The redactor releases text as soon as a delimiter proves it can't be part
  of a sensitive value, so typical added latency is one token, not one
  response.
* Response headers are sent to the client before the first upstream event so
  the client's TTFB is bounded by our connect time, and the client sees the
  first token as soon as the redactor releases it.

Error handling
--------------
Provider errors and network failures are mapped to a stable gateway payload
``{"error": {"type", "message", "request_id"}}``; the upstream body and
exception text go to the gateway log only, never to the client.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, AsyncIterator

from aiohttp import ClientError, ClientSession, ClientTimeout, web

from redactor import StreamRedactor

log = logging.getLogger("llm-gateway")

CFG: web.AppKey["Config"] = web.AppKey("cfg")
HTTP: web.AppKey[ClientSession] = web.AppKey("http")
STATS: web.AppKey[dict] = web.AppKey("stats")   # last-stream metrics, for tests/observability

_HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer",
               "upgrade", "host", "content-length", "content-encoding"}


@dataclass(frozen=True)
class Config:
    upstream_url: str
    upstream_api_key: str | None
    connect_timeout_s: float
    read_timeout_s: float
    max_event_bytes: int

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            upstream_url=os.environ.get("UPSTREAM_URL", "http://127.0.0.1:9002/v1/chat/completions"),
            upstream_api_key=os.environ.get("UPSTREAM_API_KEY"),
            connect_timeout_s=float(os.environ.get("CONNECT_TIMEOUT_S", "5")),
            read_timeout_s=float(os.environ.get("READ_TIMEOUT_S", "60")),
            max_event_bytes=int(os.environ.get("MAX_EVENT_BYTES", str(256 * 1024))),
        )


# --------------------------------------------------------------------------- #
# Error payloads: stable shape, nothing from upstream leaks through
# --------------------------------------------------------------------------- #
def gateway_error(status: int, type_: str, message: str, request_id: str) -> web.Response:
    return web.json_response(
        {"error": {"type": type_, "message": message, "request_id": request_id}}, status=status
    )


# --------------------------------------------------------------------------- #
# SSE parsing: bytes in, complete events out, bounded memory
# --------------------------------------------------------------------------- #
async def iter_sse_events(content, max_event_bytes: int) -> AsyncIterator[bytes]:
    """Yield raw SSE events (without the trailing blank line) from a byte stream.

    Works on whatever chunk boundaries the transport gives us; keeps at most
    one partial event in memory.
    """
    buf = bytearray()
    async for chunk in content.iter_any():
        buf += chunk
        while True:
            # Events are delimited by a blank line: \n\n (or \r\n\r\n).
            idx = buf.find(b"\n\n")
            if idx < 0:
                break
            event = bytes(buf[:idx]).replace(b"\r", b"")
            del buf[:idx + 2]
            if event:
                yield event
        if len(buf) > max_event_bytes:
            raise ValueError("SSE event exceeds max_event_bytes")
    if buf.strip():
        yield bytes(buf).replace(b"\r", b"")


def sse_data(event: bytes) -> str | None:
    """Extract the concatenated ``data:`` payload of an SSE event (or None)."""
    parts = []
    for line in event.split(b"\n"):
        if line.startswith(b"data:"):
            parts.append(line[5:].lstrip(b" "))
    return b"\n".join(parts).decode("utf-8", "replace") if parts else None


# --------------------------------------------------------------------------- #
# Delta rewriting
# --------------------------------------------------------------------------- #
def _delta_content(obj: dict[str, Any]) -> str | None:
    try:
        c = obj["choices"][0]["delta"].get("content")
        return c if isinstance(c, str) else None
    except (KeyError, IndexError, TypeError, AttributeError):
        return None


def _with_content(obj: dict[str, Any], text: str) -> dict[str, Any]:
    obj = json.loads(json.dumps(obj))  # cheap deep copy of a small object
    obj["choices"][0]["delta"]["content"] = text
    return obj


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #
async def chat_completions(request: web.Request) -> web.StreamResponse:
    cfg: Config = request.app[CFG]
    rid = request.headers.get("X-Request-Id") or f"req_{uuid.uuid4().hex[:12]}"
    t0 = time.perf_counter()

    body = await request.read()
    try:
        req_obj = json.loads(body)
        if not isinstance(req_obj, dict):
            raise ValueError
    except ValueError:
        return gateway_error(400, "invalid_request", "Request body must be a JSON object", rid)
    streaming = bool(req_obj.get("stream"))

    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in _HOP_BY_HOP and k.lower() != "authorization"}
    if cfg.upstream_api_key:
        headers["Authorization"] = f"Bearer {cfg.upstream_api_key}"
    headers["Content-Type"] = "application/json"
    headers["Accept"] = "text/event-stream" if streaming else "application/json"

    timeout = ClientTimeout(connect=cfg.connect_timeout_s, sock_read=cfg.read_timeout_s, total=None)
    try:
        upstream = await request.app[HTTP].post(cfg.upstream_url, data=body, headers=headers, timeout=timeout)
    except asyncio.TimeoutError:
        log.error("[%s] upstream connect timeout", rid)
        return gateway_error(504, "upstream_timeout", "The model provider did not respond in time", rid)
    except (ClientError, OSError) as exc:
        log.error("[%s] upstream connection error: %r", rid, exc)
        return gateway_error(502, "upstream_unavailable", "The model provider is unavailable", rid)

    async with upstream:
        if upstream.status >= 400:
            detail = (await upstream.read())[:2000]
            log.error("[%s] upstream %d: %s", rid, upstream.status, detail)  # log only
            if upstream.status == 429:
                return gateway_error(429, "rate_limited", "The model provider is rate limiting requests", rid)
            if upstream.status in (400, 401, 403, 404, 422):
                return gateway_error(400, "invalid_request", "The model provider rejected the request", rid)
            return gateway_error(502, "upstream_error", "The model provider returned an error", rid)

        ctype = upstream.headers.get("Content-Type", "")
        if not streaming or "text/event-stream" not in ctype:
            return await _non_stream(request, upstream, rid)
        return await _stream(request, upstream, cfg, rid, t0)


async def _non_stream(request: web.Request, upstream, rid: str) -> web.Response:
    """Non-streaming: redact choices[*].message.content in the full body."""
    try:
        obj = await upstream.json(content_type=None)
    except Exception as exc:
        log.error("[%s] bad upstream JSON: %r", rid, exc)
        return gateway_error(502, "upstream_error", "The model provider returned malformed data", rid)
    total = 0
    for choice in obj.get("choices", []) if isinstance(obj, dict) else []:
        msg = choice.get("message") or {}
        if isinstance(msg.get("content"), str):
            r = StreamRedactor()
            msg["content"] = r.feed(msg["content"]) + r.flush()
            total += r.redactions
    log.info("[%s] non-stream response, %d redactions", rid, total)
    return web.json_response(obj, headers={"X-Request-Id": rid, "X-Redactions": str(total)})


async def _stream(request: web.Request, upstream, cfg: Config, rid: str, t0: float) -> web.StreamResponse:
    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "X-Request-Id": rid,
    })
    await resp.prepare(request)  # headers out before the first upstream event

    redactor = StreamRedactor()
    ttft: float | None = None
    last_obj: dict[str, Any] | None = None
    events_in = events_out = 0

    async def emit(obj: dict[str, Any]) -> None:
        nonlocal events_out
        await resp.write(b"data: " + json.dumps(obj, separators=(",", ":")).encode() + b"\n\n")
        events_out += 1

    try:
        async for event in iter_sse_events(upstream.content, cfg.max_event_bytes):
            events_in += 1
            data = sse_data(event)
            if data is None:            # comment / keep-alive: pass through
                await resp.write(event + b"\n\n")
                continue
            if data.strip() == "[DONE]":
                break
            try:
                obj = json.loads(data)
            except ValueError:
                await resp.write(event + b"\n\n")   # not JSON: relay untouched
                continue

            content = _delta_content(obj)
            if not content:
                await emit(obj)                      # role / finish_reason / tool / empty events
                continue

            last_obj = obj
            safe = redactor.feed(content)
            if safe:
                if ttft is None:
                    ttft = time.perf_counter() - t0
                await emit(_with_content(obj, safe))
            # else: held back – nothing to send yet, but we did NOT block.

        # End of stream: release whatever is still held.
        tail = redactor.flush()
        if tail:
            if ttft is None:
                ttft = time.perf_counter() - t0
            await emit(_with_content(last_obj or _blank_delta(), tail))
        await resp.write(b"data: [DONE]\n\n")
    except asyncio.CancelledError:
        log.info("[%s] client disconnected; upstream cancelled", rid)
        raise
    except (ClientError, ValueError, OSError) as exc:
        # Mid-stream failure: we can't change the status any more, so send an
        # in-band error event and terminate cleanly.
        log.error("[%s] stream error after %d events: %r", rid, events_in, exc)
        await resp.write(b'data: {"error":{"type":"upstream_error","message":"Stream interrupted","request_id":"'
                         + rid.encode() + b'"}}\n\n')
    finally:
        elapsed = time.perf_counter() - t0
        stats = {"request_id": rid, "ttft_s": ttft, "total_s": elapsed, "events_in": events_in,
                 "events_out": events_out, "redactions": redactor.redactions,
                 "max_buffer_chars": redactor.max_buffer_seen}
        request.app[STATS].clear(); request.app[STATS].update(stats)
        log.info("[%s] stream done ttft=%.1fms total=%.1fms in=%d out=%d redactions=%d max_buf=%d",
                 rid, (ttft or 0) * 1000, elapsed * 1000, events_in, events_out,
                 redactor.redactions, redactor.max_buffer_seen)
    await resp.write_eof()
    return resp


def _blank_delta() -> dict[str, Any]:
    return {"object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {}, "finish_reason": None}]}


async def stats(request: web.Request) -> web.Response:
    return web.json_response(request.app[STATS])


def create_app(cfg: Config | None = None) -> web.Application:
    app = web.Application(client_max_size=8 * 1024 * 1024)
    app[CFG] = cfg or Config.from_env()
    app[STATS] = {}

    async def _client(app):
        app[HTTP] = ClientSession()
        yield
        await app[HTTP].close()

    app.cleanup_ctx.append(_client)
    app.router.add_post("/v1/chat/completions", chat_completions)
    app.router.add_get("/stats", stats)
    app.router.add_get("/health", lambda r: web.json_response({"status": "ok"}))
    return app


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    cfg = Config.from_env()
    port = int(os.environ.get("PORT", "8081"))
    log.info("LLM gateway on :%d → %s", port, cfg.upstream_url)
    web.run_app(create_app(cfg), host="0.0.0.0", port=port, print=None)
