#!/usr/bin/env python3
"""
Mock downstream MCP server (Streamable-HTTP style, single POST /mcp endpoint).

Deliberately performs **no authorization of its own** – it will happily run
``admin_reset_key`` for anyone.  That is the point: the tests prove the
gateway is the sole control that stops a viewer from reaching it.

It records every ``tools/call`` it executes in ``app[CALLS]`` and exposes
``GET /calls`` so tests (and demos) can verify what actually got through.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

from aiohttp import web

log = logging.getLogger("mock-mcp")

CALLS: web.AppKey[list] = web.AppKey("calls")
LAST_AUTH: web.AppKey[object] = web.AppKey("last_auth_header")

TOOLS = [
    {"name": "echo", "description": "Echo a message back.",
     "inputSchema": {"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]}},
    {"name": "get_server_time", "description": "Return the server's clock.",
     "inputSchema": {"type": "object", "properties": {}}},
    {"name": "admin_reset_key", "description": "Rotate the master API key. ADMIN ONLY.",
     "inputSchema": {"type": "object", "properties": {"key_id": {"type": "string"}}, "required": ["key_id"]}},
    {"name": "admin_delete_tenant", "description": "Delete a tenant and all data. ADMIN ONLY.",
     "inputSchema": {"type": "object", "properties": {"tenant": {"type": "string"}}, "required": ["tenant"]}},
]


def _result(id_: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_, "result": result}


def _error(id_: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": code, "message": message}}


def _text(payload: Any) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(payload)}], "structuredContent": payload, "isError": False}


def dispatch(app: web.Application, msg: dict[str, Any]) -> dict[str, Any] | None:
    id_ = msg.get("id")
    method = msg.get("method")
    params = msg.get("params") or {}

    if method == "initialize":
        return _result(id_, {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "mock-downstream", "version": "1.0.0"},
        })
    if method == "notifications/initialized":
        return None  # notification: no response
    if method == "ping":
        return _result(id_, {})
    if method == "tools/list":
        return _result(id_, {"tools": TOOLS})
    if method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        app[CALLS].append({"name": name, "arguments": args, "auth": msg.get("_auth_seen")})
        log.info("EXECUTING tool %s args=%s", name, args)
        if name == "echo":
            return _result(id_, _text({"echo": args.get("message")}))
        if name == "get_server_time":
            import time
            return _result(id_, _text({"epoch": time.time()}))
        if name == "admin_reset_key":
            return _result(id_, _text({"rotated": True, "key_id": args.get("key_id"), "new_key": "sk-NEW-SECRET-KEY"}))
        if name == "admin_delete_tenant":
            return _result(id_, _text({"deleted": args.get("tenant")}))
        return _error(id_, -32602, f"Unknown tool: {name}")
    return _error(id_, -32601, f"Method not found: {method}")


async def handle_mcp(request: web.Request) -> web.Response:
    # Record what Authorization header (if any) reached us, so tests can prove
    # the gateway strips the client's token.
    request.app[LAST_AUTH] = request.headers.get("Authorization")
    try:
        payload = await request.json()
    except Exception:
        return web.json_response(_error(None, -32700, "Parse error"), status=400)

    if isinstance(payload, list):
        out = [r for m in payload if (r := dispatch(request.app, m)) is not None]
        return web.json_response(out) if out else web.Response(status=202)
    resp = dispatch(request.app, payload)
    if resp is None:
        return web.Response(status=202)
    headers = {"Mcp-Session-Id": "mock-session-0001"}
    return web.json_response(resp, headers=headers)


async def handle_calls(request: web.Request) -> web.Response:
    return web.json_response({"calls": request.app[CALLS], "last_auth_header": request.app[LAST_AUTH]})


def create_app() -> web.Application:
    app = web.Application()
    app[CALLS] = []
    app[LAST_AUTH] = None
    app.router.add_post("/mcp", handle_mcp)
    app.router.add_get("/calls", handle_calls)
    return app


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    port = int(os.environ.get("PORT", "9001"))
    log.info("mock MCP server listening on :%d (NO auth of its own)", port)
    web.run_app(create_app(), host="127.0.0.1", port=port, print=None)
