#!/usr/bin/env python3
"""
Mock LLM provider speaking the OpenAI chat-completions wire format.

POST /v1/chat/completions
  * ``stream: true``  → SSE (``data: {...}\\n\\n`` … ``data: [DONE]\\n\\n``)
  * ``stream: false`` → one JSON body

It "generates" by echoing the last user message, which lets tests put PII
exactly where they want it.  Streaming granularity and pacing are
controllable per request so PII can be forced across chunk boundaries and
TTFT can be measured:

    "mock": {"chunk_size": 1 | 3 | "random", "delay_ms": 20, "status": 500}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sys
import time
from typing import Any

from aiohttp import web

log = logging.getLogger("mock-llm")


def _chunks(text: str, size: Any):
    i = 0
    while i < len(text):
        n = random.randint(1, 6) if size == "random" else int(size)
        yield text[i:i + n]
        i += n


def _event(id_: str, delta: dict[str, Any], finish: str | None = None) -> bytes:
    payload = {
        "id": id_, "object": "chat.completion.chunk", "created": int(time.time()),
        "model": "mock-1", "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    return f"data: {json.dumps(payload)}\n\n".encode()


async def completions(request: web.Request) -> web.StreamResponse:
    body = await request.json()
    mock = body.get("mock") or {}
    if "status" in mock:  # simulate an upstream failure
        return web.json_response(
            {"error": {"message": "internal provider explosion at /srv/llm/worker.py:123", "type": "server_error"}},
            status=int(mock["status"]),
        )

    text = next((m["content"] for m in reversed(body.get("messages", [])) if m.get("role") == "user"), "")
    delay = float(mock.get("delay_ms", 0)) / 1000
    size = mock.get("chunk_size", "random")
    id_ = f"chatcmpl-mock-{random.randint(1000, 9999)}"

    if not body.get("stream"):
        await asyncio.sleep(delay)
        return web.json_response({
            "id": id_, "object": "chat.completion", "model": "mock-1",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": len(text.split()), "total_tokens": 1 + len(text.split())},
        })

    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
    })
    await resp.prepare(request)
    await resp.write(_event(id_, {"role": "assistant", "content": ""}))
    n = 0
    for piece in _chunks(text, size):
        if delay:
            await asyncio.sleep(delay)
        await resp.write(_event(id_, {"content": piece}))
        n += 1
    await resp.write(_event(id_, {}, finish="stop"))
    await resp.write(b"data: [DONE]\n\n")
    await resp.write_eof()
    log.info("streamed %d chunks (%d chars)", n, len(text))
    return resp


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/v1/chat/completions", completions)
    return app


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    port = int(os.environ.get("PORT", "9002"))
    log.info("mock LLM provider on :%d", port)
    web.run_app(create_app(), host="127.0.0.1", port=port, print=None)
