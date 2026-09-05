"""End-to-end tests: gateway + two mock providers over real HTTP.

The router timeout is set to 0.4 s here (production default 3.0 s) so the
timeout paths run in well under a second each.

Run: python -m pytest test_gateway.py -v
"""

from __future__ import annotations

import asyncio
import json
import time

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import unused_port

import gateway as gw
import mock_provider
from ratelimiter import Tenant
from router import Provider

TIMEOUT = 0.4
ALPHA = {"Authorization": "Bearer sk-alpha"}
TINY = {"Authorization": "Bearer sk-tiny"}


async def _serve(app, port, handler_cancellation=False):
    runner = web.AppRunner(app, handler_cancellation=handler_cancellation)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner


@pytest.fixture
async def stack(tmp_path):
    p1, p2, gp = unused_port(), unused_port(), unused_port()
    primary_app, secondary_app = mock_provider.create_app("ok", slow_s=TIMEOUT * 3, name="primary"), mock_provider.create_app("ok", name="secondary")
    r1, r2 = await _serve(primary_app, p1, True), await _serve(secondary_app, p2, True)

    tenants = [Tenant("sk-alpha", "alpha", 50_000, 60), Tenant("sk-tiny", "tiny", 300, 60)]
    g_app = gw.create_app(
        str(tmp_path / "gw.sqlite3"),
        [Provider("primary", f"http://127.0.0.1:{p1}/v1/chat/completions"),
         Provider("secondary", f"http://127.0.0.1:{p2}/v1/chat/completions")],
        timeout_s=TIMEOUT, tenants=tenants,
    )
    rg = await _serve(g_app, gp)

    async with ClientSession(base_url=f"http://127.0.0.1:{gp}") as http:
        class S:
            pass
        s = S()
        s.http, s.primary, s.secondary, s.gw_app = http, primary_app, secondary_app, g_app
        s.primary_runner = r1
        yield s

    await rg.cleanup(); await r1.cleanup(); await r2.cleanup()


def _mode(app, mode, **kw):
    app[mock_provider.STATE]["mode"] = mode
    app[mock_provider.STATE].update(kw)


def _req(text="hi", **kw):
    return {"model": "gpt-x", "messages": [{"role": "user", "content": text}], "max_tokens": 50, **kw}


async def post(s, body, headers=ALPHA):
    async with s.http.post("/v1/chat/completions", json=body, headers=headers) as r:
        return r.status, await r.json(), r.headers


# --------------------------------------------------------------------------- #
# Routing & failover
# --------------------------------------------------------------------------- #
async def test_primary_healthy(stack):
    st, body, h = await post(stack, _req())
    assert st == 200 and body["served_by"] == "primary"
    assert h["X-Served-By"] == "primary" and h["X-Fallback"] == "false"
    assert stack.secondary[mock_provider.STATE]["requests"] == 0


async def test_failover_on_429(stack):
    _mode(stack.primary, "429")
    st, body, h = await post(stack, _req())
    assert st == 200 and body["served_by"] == "secondary"
    assert h["X-Fallback"] == "true"
    assert h["X-Attempts"].startswith("primary=rate_limited(")
    assert "8f3a" not in json.dumps(body)        # upstream 429 detail not leaked


async def test_failover_on_500(stack):
    _mode(stack.primary, "500")
    st, body, h = await post(stack, _req())
    assert st == 200 and body["served_by"] == "secondary"
    assert "Worker.java" not in json.dumps(body)


async def test_failover_on_timeout(stack):
    _mode(stack.primary, "slow")                 # answers after 3×TIMEOUT
    t0 = time.perf_counter()
    st, body, h = await post(stack, _req())
    elapsed = time.perf_counter() - t0
    assert st == 200 and body["served_by"] == "secondary"
    assert "primary=timeout(" in h["X-Attempts"]
    assert TIMEOUT <= elapsed < TIMEOUT * 2      # waited exactly one timeout, then fell over fast


async def test_timed_out_primary_attempt_is_cancelled_not_left_running(stack):
    _mode(stack.primary, "hang")
    await post(stack, _req())
    await asyncio.sleep(0.05)
    st = stack.primary[mock_provider.STATE]
    assert st["requests"] == 1 and st["completed"] == 0 and st["cancelled"] == 1


async def test_slow_but_within_timeout_is_served_by_primary(stack):
    """Race check: a primary answering at 0.6×timeout must win, no fallback."""
    _mode(stack.primary, "slow", slow_s=TIMEOUT * 0.6)
    st, body, h = await post(stack, _req())
    assert body["served_by"] == "primary" and h["X-Fallback"] == "false"
    assert stack.secondary[mock_provider.STATE]["requests"] == 0


async def test_failover_on_connection_refused(stack):
    await stack.primary_runner.cleanup()         # primary process "dies"
    st, body, h = await post(stack, _req())
    assert st == 200 and body["served_by"] == "secondary"
    assert "primary=network_error(" in h["X-Attempts"]


async def test_both_down_is_standard_503(stack):
    _mode(stack.primary, "500"); _mode(stack.secondary, "hang")
    st, body, h = await post(stack, _req())
    assert st == 503
    assert body == {"error": {"code": "upstream_unavailable",
                              "message": "No model provider could serve the request",
                              "request_id": h["X-Request-Id"]}}


async def test_both_rate_limited_is_429_upstream(stack):
    _mode(stack.primary, "429"); _mode(stack.secondary, "429")
    st, body, _ = await post(stack, _req())
    assert st == 429 and body["error"]["code"] == "upstream_rate_limited"
    assert "quota.log" not in json.dumps(body)


async def test_failed_request_releases_reservation(stack):
    _mode(stack.primary, "500"); _mode(stack.secondary, "500")
    await post(stack, _req("x" * 400))
    async with stack.http.get("/v1/usage", headers=ALPHA) as r:
        assert (await r.json())["used"] == 0


# --------------------------------------------------------------------------- #
# Rate limiting through the HTTP layer
# --------------------------------------------------------------------------- #
async def test_tenant_rate_limit_429_with_headers(stack):
    # tiny tenant: 300 tokens/min. Each request reserves ~len/4 + 50 and settles to real usage (~len/4 + 40).
    st, body, h = await post(stack, _req("a" * 400), TINY)      # ~100 + 40 = 140 used
    assert st == 200
    st, body, h = await post(stack, _req("a" * 400), TINY)      # 280 used
    assert st == 200 and int(h["X-RateLimit-Remaining"]) == 20
    st, body, h = await post(stack, _req("a" * 400), TINY)      # 280 + 150 > 300
    assert st == 429
    assert body["error"]["code"] == "rate_limited"
    assert int(h["Retry-After"]) >= 1 and h["X-RateLimit-Limit"] == "300"
    assert stack.primary[mock_provider.STATE]["requests"] == 2   # third never reached a provider


async def test_usage_settles_to_actual_not_estimate(stack):
    await post(stack, _req("a" * 400, max_tokens=5000))         # estimate 5100, actual 140
    async with stack.http.get("/v1/usage", headers=ALPHA) as r:
        assert (await r.json())["used"] == 140


async def test_concurrent_requests_respect_limit(stack):
    """20 parallel requests for the 300-token tenant: exactly 2 can fit."""
    results = await asyncio.gather(*(post(stack, _req("a" * 400), TINY) for _ in range(20)))
    statuses = [s for s, _, _ in results]
    assert statuses.count(200) == 2 and statuses.count(429) == 18


async def test_unknown_tenants_are_isolated(stack):
    for _ in range(2):
        await post(stack, _req("a" * 400), TINY)
    st, _, _ = await post(stack, _req("a" * 400), ALPHA)         # alpha unaffected by tiny's exhaustion
    assert st == 200


# --------------------------------------------------------------------------- #
# Auth & validation & sanitisation
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer nope"}, {"Authorization": "Basic x"}])
async def test_unauthorized(stack, headers):
    st, body, _ = await post(stack, _req(), headers)
    assert st == 401 and body["error"]["code"] == "unauthorized"


@pytest.mark.parametrize("body", [b"not json", b"[]", b'{"messages": []}', b'{"messages": "x"}'])
async def test_invalid_request(stack, body):
    async with stack.http.post("/v1/chat/completions", data=body, headers=ALPHA) as r:
        assert r.status == 400 and (await r.json())["error"]["code"] == "invalid_request"


async def test_every_error_has_identical_shape(stack):
    _mode(stack.primary, "500"); _mode(stack.secondary, "500")
    cases = [
        (post(stack, _req(), {}), 401),
        (post(stack, _req()), 503),
    ]
    for coro, expect in cases:
        st, body, h = await coro
        assert st == expect
        assert set(body) == {"error"} and set(body["error"]) == {"code", "message", "request_id"}
        assert body["error"]["request_id"] == h["X-Request-Id"]
        text = json.dumps(body)
        assert "127.0.0.1" not in text and "Traceback" not in text and ".java" not in text


async def test_request_id_propagated_and_logged(stack):
    _mode(stack.primary, "429")
    st, _, h = await post(stack, _req(), {**ALPHA, "X-Request-Id": "trace-xyz"})
    assert h["X-Request-Id"] == "trace-xyz"
    async with stack.http.get("/admin/requests") as r:
        rows = await r.json()
    row = next(x for x in rows if x["request_id"] == "trace-xyz")
    assert row["provider"] == "secondary" and row["fallback"] == 1 and row["status"] == 200
    assert row["tokens"] > 0 and row["latency_ms"] >= 0


async def test_stream_flag_is_downgraded(stack):
    st, body, _ = await post(stack, _req(stream=True))
    assert st == 200 and body["object"] == "chat.completion"
