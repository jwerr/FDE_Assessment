#!/usr/bin/env python3
"""
End-to-end tests: real gateway + real mock downstream on localhost ports.

Run:  python -m pytest test_gateway.py -v
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import unused_port

import gateway as gw
import mock_mcp_server as mock

ADMIN = {"Authorization": "Bearer tok_admin_123"}
VIEWER = {"Authorization": "Bearer tok_viewer_456"}
JWT_SECRET = b"test-secret"


def rpc(method: str, params=None, id_=1):
    m = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        m["params"] = params
    if id_ is not None:
        m["id"] = id_
    return m


def call(name: str, args=None, id_=1):
    return rpc("tools/call", {"name": name, "arguments": args or {}}, id_)


def make_jwt(claims: dict, secret: bytes = JWT_SECRET) -> str:
    b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
    h = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    p = b64(json.dumps(claims).encode())
    s = b64(hmac.new(secret, f"{h}.{p}".encode(), hashlib.sha256).digest())
    return f"{h}.{p}.{s}"


# --------------------------------------------------------------------------- #
# Fixtures: boot both servers
# --------------------------------------------------------------------------- #
@pytest.fixture
async def stack():
    mock_port, gw_port = unused_port(), unused_port()

    mock_app = mock.create_app()
    mock_runner = web.AppRunner(mock_app)
    await mock_runner.setup()
    await web.TCPSite(mock_runner, "127.0.0.1", mock_port).start()

    cfg = gw.GatewayConfig(
        downstream_url=f"http://127.0.0.1:{mock_port}/mcp",
        tokens={
            "tok_admin_123": gw.Principal("alice", "admin"),
            "tok_viewer_456": gw.Principal("bob", "viewer"),
        },
        jwt_secret=JWT_SECRET,
        protected_prefixes=("admin_",),
        upstream_token="gateway-internal-secret",
        upstream_timeout_s=5,
    )
    gw_runner = web.AppRunner(gw.create_app(cfg))
    await gw_runner.setup()
    await web.TCPSite(gw_runner, "127.0.0.1", gw_port).start()

    async with ClientSession(base_url=f"http://127.0.0.1:{gw_port}") as http:
        yield type("Stack", (), {"http": http, "mock": mock_app, "mock_runner": mock_runner, "cfg": cfg})()

    await gw_runner.cleanup()
    await mock_runner.cleanup()


async def post(stack, body, headers=None):
    data = body if isinstance(body, bytes) else json.dumps(body)
    async with stack.http.post("/mcp", data=data, headers={"Content-Type": "application/json", **(headers or {})}) as r:
        text = await r.text()
        try:
            return r.status, json.loads(text), r.headers
        except ValueError:
            return r.status, text, r.headers


# --------------------------------------------------------------------------- #
# Authentication
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("headers", [
    {},                                          # no header
    {"Authorization": "Basic abc"},              # wrong scheme
    {"Authorization": "Bearer"},                 # empty token
    {"Authorization": "Bearer nope"},            # unknown token
])
async def test_unauthenticated(stack, headers):
    status, body, hdrs = await post(stack, rpc("tools/list"), headers)
    assert status == 401
    assert body["error"]["code"] == gw.UNAUTHENTICATED
    assert body["id"] is None
    assert "WWW-Authenticate" in hdrs
    assert stack.mock[mock.CALLS] == []


async def test_jwt_admin_and_viewer(stack):
    admin = make_jwt({"sub": "carol", "role": "admin", "exp": time.time() + 60})
    viewer = make_jwt({"sub": "dave", "role": "viewer"})
    s, b, _ = await post(stack, call("admin_reset_key", {"key_id": "k1"}), {"Authorization": f"Bearer {admin}"})
    assert s == 200 and b["result"]["structuredContent"]["rotated"] is True
    s, b, _ = await post(stack, call("admin_reset_key", {"key_id": "k1"}), {"Authorization": f"Bearer {viewer}"})
    assert s == 403 and b["error"]["code"] == gw.UNAUTHORIZED_TOOL_CALL


@pytest.mark.parametrize("token", [
    make_jwt({"sub": "x", "role": "admin"}, secret=b"WRONG"),     # forged signature
    make_jwt({"sub": "x", "role": "admin", "exp": time.time() - 1}),  # expired
    make_jwt({"sub": "x"}),                                        # no role claim
    "a.b.c",                                                       # garbage
])
async def test_jwt_rejected(stack, token):
    s, b, _ = await post(stack, rpc("tools/list"), {"Authorization": f"Bearer {token}"})
    assert s == 401 and b["error"]["code"] == gw.UNAUTHENTICATED


# --------------------------------------------------------------------------- #
# Authorization: the core requirement
# --------------------------------------------------------------------------- #
async def test_tools_list_forwarded_for_viewer(stack):
    s, b, _ = await post(stack, rpc("tools/list", id_=7), VIEWER)
    assert s == 200 and b["id"] == 7
    assert {t["name"] for t in b["result"]["tools"]} >= {"echo", "admin_reset_key"}


async def test_viewer_can_call_normal_tool(stack):
    s, b, _ = await post(stack, call("echo", {"message": "hi"}, id_="req-abc"), VIEWER)
    assert s == 200
    assert b["id"] == "req-abc"                      # string ids preserved
    assert b["result"]["structuredContent"] == {"echo": "hi"}
    assert stack.mock[mock.CALLS][-1]["name"] == "echo"


async def test_viewer_blocked_from_admin_tool_and_downstream_never_called(stack):
    s, b, _ = await post(stack, call("admin_reset_key", {"key_id": "master"}, id_=42), VIEWER)
    assert s == 403
    assert b == {
        "jsonrpc": "2.0", "id": 42,
        "error": {"code": -32001, "message": "Unauthorized Tool Call",
                  "data": {"tool": "admin_reset_key", "required_role": "admin", "role": "viewer"}},
    }
    assert stack.mock[mock.CALLS] == [], "downstream must not have executed the admin tool"


async def test_admin_can_call_admin_tool(stack):
    s, b, _ = await post(stack, call("admin_reset_key", {"key_id": "master"}), ADMIN)
    assert s == 200 and b["result"]["structuredContent"]["rotated"] is True
    assert stack.mock[mock.CALLS][-1]["name"] == "admin_reset_key"


@pytest.mark.parametrize("name", ["admin_delete_tenant", "admin_", "admin_x"])
async def test_prefix_match_is_exact_prefix(stack, name):
    s, b, _ = await post(stack, call(name), VIEWER)
    assert s == 403 and b["error"]["code"] == -32001


@pytest.mark.parametrize("name", ["Admin_reset_key", "xadmin_reset_key", "administer", "admin-reset"])
async def test_lookalike_names_are_not_protected(stack, name):
    """Policy is literal `startswith("admin_")`; these go downstream (which 404s them)."""
    s, b, _ = await post(stack, call(name), VIEWER)
    assert s == 200 and b["error"]["code"] == -32602   # downstream's "unknown tool", not -32001


async def test_tools_call_without_name_passes_to_downstream(stack):
    s, b, _ = await post(stack, rpc("tools/call", {"arguments": {}}), VIEWER)
    assert s == 200 and b["error"]["code"] == -32602   # downstream decides


# --------------------------------------------------------------------------- #
# JSON-RPC wire-format handling
# --------------------------------------------------------------------------- #
async def test_parse_error(stack):
    s, b, _ = await post(stack, b'{"jsonrpc": "2.0", "method": ', VIEWER)
    assert s == 400 and b["error"]["code"] == -32700 and b["id"] is None


@pytest.mark.parametrize("bad", [
    {"method": "tools/list", "id": 1},                     # missing jsonrpc
    {"jsonrpc": "1.0", "method": "tools/list", "id": 1},   # wrong version
    {"jsonrpc": "2.0", "id": 1},                           # no method
    {"jsonrpc": "2.0", "method": 5, "id": 1},              # method not string
    {"jsonrpc": "2.0", "method": "x", "id": {"a": 1}},     # object id
    {"jsonrpc": "2.0", "method": "x", "id": 1, "params": "str"},  # params not structured
    "just a string",
    42,
    [],
])
async def test_invalid_request(stack, bad):
    s, b, _ = await post(stack, bad, VIEWER)
    assert s == 400 and b["error"]["code"] == -32600
    assert stack.mock[mock.CALLS] == []


async def test_invalid_request_preserves_id_when_possible(stack):
    s, b, _ = await post(stack, {"jsonrpc": "2.0", "id": "keep-me"}, VIEWER)
    assert b["id"] == "keep-me"


async def test_batch_all_allowed_is_forwarded(stack):
    batch = [rpc("tools/list", id_=1), call("echo", {"message": "a"}, id_=2), rpc("notifications/initialized", id_=None)]
    s, b, _ = await post(stack, batch, VIEWER)
    assert s == 200 and isinstance(b, list)
    assert {m["id"] for m in b} == {1, 2}


async def test_batch_with_one_denied_is_blocked_entirely(stack):
    batch = [call("echo", {"message": "a"}, id_=1), call("admin_reset_key", {"key_id": "k"}, id_=2)]
    s, b, _ = await post(stack, batch, VIEWER)
    assert s == 403 and isinstance(b, list) and len(b) == 1
    assert b[0]["id"] == 2 and b[0]["error"]["code"] == -32001
    assert stack.mock[mock.CALLS] == [], "fail-closed: nothing in the batch may reach downstream"


async def test_notification_denied_returns_error_with_null_id(stack):
    s, b, _ = await post(stack, call("admin_reset_key", id_=None), VIEWER)
    assert s == 403 and b["id"] is None and b["error"]["code"] == -32001


# --------------------------------------------------------------------------- #
# Proxy behaviour
# --------------------------------------------------------------------------- #
async def test_client_token_is_stripped_and_gateway_credential_injected(stack):
    await post(stack, rpc("tools/list"), VIEWER)
    assert stack.mock[mock.LAST_AUTH] == "Bearer gateway-internal-secret"


async def test_downstream_headers_relayed(stack):
    s, b, hdrs = await post(stack, rpc("tools/list"), VIEWER)
    assert hdrs.get("Mcp-Session-Id") == "mock-session-0001"
    assert hdrs.get("Content-Type", "").startswith("application/json")


async def test_body_forwarded_byte_for_byte(stack):
    # Unusual-but-valid JSON (extra whitespace, unicode) must reach downstream intact.
    raw = '{"jsonrpc":"2.0",  "id": 9, "method":"tools/call", "params":{"name":"echo","arguments":{"message":"héllo ✓"}}}'
    s, b, _ = await post(stack, raw.encode(), VIEWER)
    assert s == 200 and b["result"]["structuredContent"] == {"echo": "héllo ✓"}


async def test_upstream_down_returns_502_not_stack_trace(stack):
    await stack.mock_runner.cleanup()   # kill the downstream
    s, b, _ = await post(stack, rpc("tools/list"), VIEWER)
    assert s == 502 and b["error"]["code"] == gw.UPSTREAM_UNAVAILABLE
    assert "Traceback" not in json.dumps(b) and "127.0.0.1" not in json.dumps(b)


async def test_health(stack):
    async with stack.http.get("/health") as r:
        assert r.status == 200 and (await r.json())["status"] == "ok"


# --------------------------------------------------------------------------- #
# Unit tests on the pure functions
# --------------------------------------------------------------------------- #
def test_is_protected_tool():
    assert gw.is_protected_tool("admin_reset_key", ("admin_",))
    assert not gw.is_protected_tool("reset_key", ("admin_",))
    assert gw.is_protected_tool("sys_reboot", ("admin_", "sys_"))


def test_config_from_env(monkeypatch):
    monkeypatch.setenv("GATEWAY_TOKENS", "t1:admin:alice, t2:viewer:bob")
    monkeypatch.setenv("PROTECTED_PREFIXES", "admin_,danger_")
    cfg = gw.GatewayConfig.from_env()
    assert cfg.tokens["t1"] == gw.Principal("alice", "admin")
    assert cfg.tokens["t2"].role == "viewer"
    assert cfg.protected_prefixes == ("admin_", "danger_")
