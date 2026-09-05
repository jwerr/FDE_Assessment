#!/usr/bin/env python3
"""
Mock model provider (OpenAI chat-completions shape) with switchable behaviour.

    POST /v1/chat/completions      → completion, per current mode
    POST /admin/mode {"mode": M}   → change mode at runtime
    GET  /admin/stats              → {"mode", "requests", "completed", "cancelled"}

Modes:  ok · 429 · 500 · slow (answers after SLOW_S, default 5 s) · hang (never answers)

``cancelled`` counts requests whose client went away before we answered —
used by the tests to prove the router really tears down a timed-out attempt.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

from aiohttp import web

log = logging.getLogger("mock-provider")
STATE: web.AppKey[dict] = web.AppKey("state")


async def completions(request: web.Request) -> web.Response:
    st = request.app[STATE]
    NAME = st["name"]
    st["requests"] += 1
    body = await request.json()
    mode = st["mode"]
    try:
        if mode == "429":
            return web.json_response({"error": {"message": f"{NAME}: quota exhausted, key ending 8f3a, "
                                                            "see /var/log/provider/quota.log"}}, status=429,
                                     headers={"Retry-After": "7"})
        if mode == "500":
            return web.json_response({"error": {"message": f"{NAME}: NullPointerException at Worker.java:88"}},
                                     status=500)
        if mode == "slow":
            await asyncio.sleep(st["slow_s"])
        elif mode == "hang":
            await asyncio.sleep(3600)

        prompt = " ".join(m.get("content", "") for m in body.get("messages", []) if isinstance(m.get("content"), str))
        prompt_tokens = max(1, len(prompt) // 4)
        completion_tokens = min(int(body.get("max_tokens") or 256), 40)
        st["completed"] += 1
        return web.json_response({
            "id": f"chatcmpl-{NAME}-{st['requests']}", "object": "chat.completion", "created": int(time.time()),
            "model": body.get("model", f"{NAME}-model"), "served_by": NAME,
            "choices": [{"index": 0, "message": {"role": "assistant",
                                                 "content": f"Hello from {NAME} (request #{st['requests']})."},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
                      "total_tokens": prompt_tokens + completion_tokens},
        })
    except (asyncio.CancelledError, ConnectionResetError):
        st["cancelled"] += 1
        log.info("request cancelled by client (mode=%s)", mode)
        raise


async def set_mode(request: web.Request) -> web.Response:
    body = await request.json()
    request.app[STATE]["mode"] = body["mode"]
    if "slow_s" in body:
        request.app[STATE]["slow_s"] = float(body["slow_s"])
    log.info("mode → %s", body["mode"])
    return web.json_response({"ok": True, "mode": body["mode"]})


async def stats(request: web.Request) -> web.Response:
    return web.json_response(request.app[STATE])


def create_app(mode: str = "ok", slow_s: float = 5.0, name: str = "mock") -> web.Application:
    app = web.Application()
    app[STATE] = {"name": name, "mode": mode, "slow_s": slow_s, "requests": 0, "completed": 0, "cancelled": 0}
    app.router.add_post("/v1/chat/completions", completions)
    app.router.add_post("/admin/mode", set_mode)
    app.router.add_get("/admin/stats", stats)
    return app


if __name__ == "__main__":
    NAME = os.environ.get("PROVIDER_NAME", "mock")
    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format=f"%(asctime)s %(levelname)s [{NAME}] %(message)s")
    port = int(os.environ.get("PORT", "9101"))
    log.info("mock provider '%s' on :%d mode=%s", NAME, port, os.environ.get("MODE", "ok"))
    web.run_app(create_app(os.environ.get("MODE", "ok"), float(os.environ.get("SLOW_S", "5")), NAME),
                host="127.0.0.1", port=port, print=None,
                # abort the handler when the client closes the socket, so a router
                # that gave up on us really stops our work (and we can count it)
                handler_cancellation=True)
