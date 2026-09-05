#!/usr/bin/env python3
"""
LLM Gateway: per-tenant token rate limiting + resilient model routing.

    client ──POST /v1/chat/completions (Bearer <tenant key>)──▶ gateway
              1. authenticate tenant (SQLite `tenants`)
              2. reserve estimated tokens in the sliding window (SQLite `token_usage`)
              3. route: primary → secondary on 429 / 3 s timeout / 5xx
              4. settle the reservation with the provider's real `usage`
              5. audit row in `request_log`

Every error the client can receive has the same shape:

    {"error": {"code": "<stable_code>", "message": "<human text>", "request_id": "req_…"}}

Codes: unauthorized (401) · invalid_request (400) · rate_limited (429, with
Retry-After) · upstream_rate_limited (429) · upstream_unavailable (503).
Upstream bodies, exception text, hostnames and stack traces never appear in a
response; they go to the gateway log keyed by request_id.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from typing import Any

from aiohttp import ClientSession, web

from ratelimiter import (Database, RateLimitExceeded, SlidingWindowTokenLimiter, Tenant,
                         estimate_tokens)
from router import ModelRouter, Provider, RouterError

log = logging.getLogger("gateway")

DB: web.AppKey[Database] = web.AppKey("db")
LIMITER: web.AppKey[SlidingWindowTokenLimiter] = web.AppKey("limiter")
ROUTER: web.AppKey[ModelRouter] = web.AppKey("router")
HTTP: web.AppKey[ClientSession] = web.AppKey("http")

DEFAULT_TENANTS = [
    Tenant("sk-tenant-alpha", "alpha", 50_000, 60),
    Tenant("sk-tenant-beta", "beta", 50_000, 60),
    Tenant("sk-tenant-tiny", "tiny", 300, 60),        # for demos: trips quickly
]

_ERROR_TEXT = {
    "unauthorized": (401, "Missing or invalid API key"),
    "invalid_request": (400, "The request was rejected"),
    "rate_limited": (429, "Token rate limit exceeded for this API key"),
    "upstream_rate_limited": (429, "All model providers are rate limiting requests"),
    "upstream_unavailable": (503, "No model provider could serve the request"),
}


def error_response(code: str, request_id: str, **extra_headers: str) -> web.Response:
    status, message = _ERROR_TEXT[code]
    return web.json_response(
        {"error": {"code": code, "message": message, "request_id": request_id}},
        status=status, headers={"X-Request-Id": request_id, **extra_headers},
    )


def _prompt_text(body: dict[str, Any]) -> str:
    parts = []
    for m in body.get("messages", []) or []:
        c = m.get("content") if isinstance(m, dict) else None
        if isinstance(c, str):
            parts.append(c)
        elif isinstance(c, list):  # multimodal content blocks
            parts.extend(str(b.get("text", "")) for b in c if isinstance(b, dict))
    return "\n".join(parts)


async def chat_completions(request: web.Request) -> web.Response:
    app = request.app
    rid = request.headers.get("X-Request-Id") or f"req_{uuid.uuid4().hex[:12]}"
    t0 = time.perf_counter()

    # 1. Tenant --------------------------------------------------------------
    scheme, _, key = request.headers.get("Authorization", "").partition(" ")
    tenant = await app[DB].tenant_for_key(key.strip()) if scheme.lower() == "bearer" and key.strip() else None
    if tenant is None:
        await app[DB].log_request(request_id=rid, tenant_id=None, status=401, error_code="unauthorized",
                                  latency_ms=_ms(t0))
        return error_response("unauthorized", rid)

    # 2. Parse ---------------------------------------------------------------
    try:
        body = json.loads(await request.read())
        if not isinstance(body, dict) or not isinstance(body.get("messages"), list) or not body["messages"]:
            raise ValueError
    except ValueError:
        return error_response("invalid_request", rid)
    if body.get("stream"):
        body["stream"] = False   # this router is request/response; streaming is Task 3's job

    # 3. Reserve tokens ------------------------------------------------------
    estimate = estimate_tokens(_prompt_text(body)) + int(body.get("max_tokens") or 256)
    try:
        reservation = await app[LIMITER].reserve(tenant, estimate)
    except RateLimitExceeded as exc:
        log.warning("[%s] tenant %s rate limited: %d/%d, retry in %ds", rid, tenant.tenant_id,
                    exc.used, exc.limit, exc.retry_after_s)
        await app[DB].log_request(request_id=rid, tenant_id=tenant.tenant_id, status=429,
                                  error_code="rate_limited", latency_ms=_ms(t0), tokens=estimate)
        return error_response("rate_limited", rid, **{
            "Retry-After": str(exc.retry_after_s),
            "X-RateLimit-Limit": str(exc.limit),
            "X-RateLimit-Remaining": str(max(0, exc.limit - exc.used)),
        })

    # 4. Route ---------------------------------------------------------------
    try:
        result = await app[ROUTER].complete(body, rid)
    except RouterError as exc:
        await app[LIMITER].settle(reservation, None)          # nothing consumed: release
        await app[DB].log_request(request_id=rid, tenant_id=tenant.tenant_id, status=_ERROR_TEXT[exc.code][0],
                                  error_code=exc.code, latency_ms=_ms(t0),
                                  provider=",".join(f"{a.provider}:{a.outcome}" for a in exc.attempts))
        return error_response(exc.code, rid)
    except asyncio.CancelledError:
        await app[LIMITER].settle(reservation, None)
        raise

    # 5. Settle with real usage -----------------------------------------------
    usage = result.body.get("usage") or {}
    actual = usage.get("total_tokens") if isinstance(usage.get("total_tokens"), int) else estimate
    await app[LIMITER].settle(reservation, actual)
    remaining = tenant.token_limit - await app[LIMITER].usage(tenant)
    await app[DB].log_request(request_id=rid, tenant_id=tenant.tenant_id, status=200, provider=result.provider,
                              fallback=int(result.fallback), latency_ms=_ms(t0), tokens=actual)
    log.info("[%s] %s served by %s%s in %dms, %d tokens (remaining %d)", rid, tenant.tenant_id, result.provider,
             " (FALLBACK)" if result.fallback else "", _ms(t0), actual, remaining)

    return web.json_response(result.body, headers={
        "X-Request-Id": rid,
        "X-Served-By": result.provider,
        "X-Fallback": "true" if result.fallback else "false",
        "X-Attempts": ";".join(f"{a.provider}={a.outcome}({a.latency_ms}ms)" for a in result.attempts),
        "X-RateLimit-Limit": str(tenant.token_limit),
        "X-RateLimit-Remaining": str(max(0, remaining)),
    })


def _ms(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)


async def usage_endpoint(request: web.Request) -> web.Response:
    """GET /v1/usage – the caller's current window consumption."""
    scheme, _, key = request.headers.get("Authorization", "").partition(" ")
    tenant = await request.app[DB].tenant_for_key(key.strip()) if scheme.lower() == "bearer" else None
    if tenant is None:
        return error_response("unauthorized", f"req_{uuid.uuid4().hex[:12]}")
    used = await request.app[LIMITER].usage(tenant)
    return web.json_response({"tenant": tenant.tenant_id, "window_s": tenant.window_s,
                              "limit": tenant.token_limit, "used": used, "remaining": max(0, tenant.token_limit - used)})


async def recent_requests(request: web.Request) -> web.Response:
    def _do(c):
        return [dict(r) for r in c.execute("SELECT * FROM request_log ORDER BY ts_ms DESC LIMIT 20")]
    return web.json_response(await request.app[DB].run(_do))


def create_app(db_path: str, providers: list[Provider], timeout_s: float = 3.0,
               tenants: list[Tenant] | None = None) -> web.Application:
    app = web.Application()

    async def _setup(app: web.Application):
        app[DB] = Database(db_path)
        for t in (tenants if tenants is not None else DEFAULT_TENANTS):
            await app[DB].upsert_tenant(t)
        app[LIMITER] = SlidingWindowTokenLimiter(app[DB])
        app[HTTP] = ClientSession()
        app[ROUTER] = ModelRouter(app[HTTP], providers, timeout_s=timeout_s)

        async def _sweeper():
            while True:
                await asyncio.sleep(30)
                n = await app[LIMITER].evict()
                if n:
                    log.info("evicted %d expired usage rows", n)
        sweeper = asyncio.create_task(_sweeper())
        yield
        sweeper.cancel()
        await app[HTTP].close()
        app[DB].close()

    app.cleanup_ctx.append(_setup)
    app.router.add_post("/v1/chat/completions", chat_completions)
    app.router.add_get("/v1/usage", usage_endpoint)
    app.router.add_get("/admin/requests", recent_requests)
    app.router.add_get("/health", lambda r: web.json_response({"status": "ok"}))
    return app


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    providers = [
        Provider("primary", os.environ.get("PRIMARY_URL", "http://127.0.0.1:9101/v1/chat/completions"),
                 os.environ.get("PRIMARY_API_KEY"), os.environ.get("PRIMARY_MODEL")),
        Provider("secondary", os.environ.get("SECONDARY_URL", "http://127.0.0.1:9102/v1/chat/completions"),
                 os.environ.get("SECONDARY_API_KEY"), os.environ.get("SECONDARY_MODEL")),
    ]
    db_path = os.environ.get("DB_PATH", "gateway.sqlite3")
    timeout = float(os.environ.get("PROVIDER_TIMEOUT_S", "3.0"))
    port = int(os.environ.get("PORT", "8082"))
    log.info("gateway on :%d, db=%s, timeout=%.1fs, providers=%s", port, db_path, timeout,
             [p.url for p in providers])
    web.run_app(create_app(db_path, providers, timeout), host="0.0.0.0", port=port, print=None)
