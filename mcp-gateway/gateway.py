#!/usr/bin/env python3
"""
MCP Security Gateway
====================

An HTTP/JSON-RPC reverse proxy that sits between an AI-agent client and a
downstream MCP server (Streamable-HTTP transport) and enforces
method-level authorization.

    client ──POST /mcp (Bearer token)──▶ gateway ──POST──▶ downstream MCP
                                           │
                                           └─ intercepts unauthorized
                                              tools/call and answers
                                              with JSON-RPC -32001

Pipeline for every request
--------------------------
1. **Authenticate**  Read ``Authorization: Bearer <token>``; resolve it to a
   principal ``{subject, role}``.  Two token formats are accepted:
     * opaque API keys from the ``GATEWAY_TOKENS`` registry, and
     * HS256 JWTs signed with ``GATEWAY_JWT_SECRET`` (``role`` claim).
   Missing/invalid token → HTTP 401 + JSON-RPC error -32002.
2. **Parse**  Decode the body as JSON-RPC 2.0.  Bad JSON → -32700; a
   well-formed JSON value that is not a valid request envelope → -32600.
   Batch arrays are supported: each element is authorized independently.
3. **Authorize**  ``tools/call`` whose ``params.name`` matches a protected
   prefix (default ``admin_``) requires ``role == "admin"``.  Denied calls are
   answered locally with -32001 *Unauthorized Tool Call* and never reach the
   downstream.  ``tools/list`` and everything else is forwarded as-is.
4. **Forward**  The original body bytes are relayed to the downstream (so the
   downstream sees exactly what the client sent).  The client's
   ``Authorization`` header is stripped and replaced with the gateway's own
   upstream credential, if configured.  Status, content-type and
   ``Mcp-Session-Id`` are relayed back; streaming (SSE) bodies are piped
   chunk-by-chunk.  Downstream unreachable → HTTP 502 + -32003.

Error codes
-----------
    -32700  Parse error            (standard)
    -32600  Invalid Request        (standard)
    -32001  Unauthorized Tool Call (assessment-specified, server-defined range)
    -32002  Unauthenticated        (server-defined)
    -32003  Upstream unavailable   (server-defined)
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Iterable

from aiohttp import ClientError, ClientSession, ClientTimeout, web

log = logging.getLogger("mcp-gateway")

CFG: web.AppKey["GatewayConfig"] = web.AppKey("cfg")
HTTP: web.AppKey[ClientSession] = web.AppKey("http")

# --------------------------------------------------------------------------- #
# Error codes
# --------------------------------------------------------------------------- #
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
UNAUTHORIZED_TOOL_CALL = -32001
UNAUTHENTICATED = -32002
UPSTREAM_UNAVAILABLE = -32003

JSONRPC = "2.0"


def jsonrpc_error(id_: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": JSONRPC, "id": id_, "error": err}


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Principal:
    subject: str
    role: str


@dataclass(frozen=True)
class GatewayConfig:
    downstream_url: str
    tokens: dict[str, Principal]              # opaque token -> principal
    jwt_secret: bytes | None                  # HS256 secret for JWT tokens
    protected_prefixes: tuple[str, ...]       # tool-name prefixes requiring admin
    upstream_token: str | None                # credential the gateway presents downstream
    upstream_timeout_s: float

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        # GATEWAY_TOKENS="tok_admin:admin:alice,tok_view:viewer:bob"
        tokens: dict[str, Principal] = {}
        raw = os.environ.get("GATEWAY_TOKENS", "tok_admin_123:admin:alice,tok_viewer_456:viewer:bob")
        for entry in filter(None, (e.strip() for e in raw.split(","))):
            token, role, subject = (entry.split(":", 2) + [None, None])[:3]
            tokens[token] = Principal(subject=subject or token, role=role or "viewer")
        secret = os.environ.get("GATEWAY_JWT_SECRET")
        return cls(
            downstream_url=os.environ.get("DOWNSTREAM_URL", "http://127.0.0.1:9001/mcp"),
            tokens=tokens,
            jwt_secret=secret.encode() if secret else None,
            protected_prefixes=tuple(
                p for p in os.environ.get("PROTECTED_PREFIXES", "admin_").split(",") if p
            ),
            upstream_token=os.environ.get("UPSTREAM_TOKEN"),
            upstream_timeout_s=float(os.environ.get("UPSTREAM_TIMEOUT_S", "10")),
        )


# --------------------------------------------------------------------------- #
# Authentication
# --------------------------------------------------------------------------- #
class AuthError(Exception):
    pass


def _b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _verify_hs256_jwt(token: str, secret: bytes) -> dict[str, Any]:
    """Minimal HS256 JWT verification (no external dependency)."""
    try:
        h64, p64, s64 = token.split(".")
        header = json.loads(_b64url_decode(h64))
        if header.get("alg") != "HS256":
            raise AuthError("unsupported alg")
        expected = hmac.new(secret, f"{h64}.{p64}".encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _b64url_decode(s64)):
            raise AuthError("bad signature")
        payload = json.loads(_b64url_decode(p64))
    except AuthError:
        raise
    except Exception as exc:  # malformed base64 / json
        raise AuthError("malformed jwt") from exc
    exp = payload.get("exp")
    if exp is not None and time.time() >= float(exp):
        raise AuthError("token expired")
    return payload


def authenticate(request: web.Request, cfg: GatewayConfig) -> Principal:
    header = request.headers.get("Authorization", "")
    scheme, _, token = header.partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        raise AuthError("missing or malformed Authorization header")

    # 1. Opaque API key
    principal = cfg.tokens.get(token)
    if principal is not None:
        return principal

    # 2. HS256 JWT
    if cfg.jwt_secret and token.count(".") == 2:
        claims = _verify_hs256_jwt(token, cfg.jwt_secret)
        role = claims.get("role")
        if not isinstance(role, str) or not role:
            raise AuthError("jwt missing role claim")
        return Principal(subject=str(claims.get("sub", "jwt-user")), role=role)

    raise AuthError("unknown token")


# --------------------------------------------------------------------------- #
# JSON-RPC parsing & authorization
# --------------------------------------------------------------------------- #
class InvalidRequest(Exception):
    def __init__(self, id_: Any, reason: str):
        super().__init__(reason)
        self.id = id_
        self.reason = reason


def _envelope_id(msg: Any) -> Any:
    """Best-effort id extraction for error responses (spec: null if unknown)."""
    if isinstance(msg, dict):
        id_ = msg.get("id")
        if isinstance(id_, (str, int)) or id_ is None:
            return id_
    return None


def validate_envelope(msg: Any) -> dict[str, Any]:
    """Validate one JSON-RPC 2.0 request/notification object."""
    if not isinstance(msg, dict):
        raise InvalidRequest(None, "request must be a JSON object")
    if msg.get("jsonrpc") != JSONRPC:
        raise InvalidRequest(_envelope_id(msg), 'missing or invalid "jsonrpc": "2.0"')
    method = msg.get("method")
    if not isinstance(method, str) or not method:
        raise InvalidRequest(_envelope_id(msg), '"method" must be a non-empty string')
    if "id" in msg and not (msg["id"] is None or isinstance(msg["id"], (str, int))):
        raise InvalidRequest(None, '"id" must be a string, number or null')
    if "params" in msg and not isinstance(msg["params"], (dict, list)):
        raise InvalidRequest(_envelope_id(msg), '"params" must be an object or array')
    return msg


def is_protected_tool(name: str, prefixes: Iterable[str]) -> bool:
    return any(name.startswith(p) for p in prefixes)


def authorize(msg: dict[str, Any], principal: Principal, cfg: GatewayConfig) -> dict[str, Any] | None:
    """Return a JSON-RPC error object if *msg* must be blocked, else None.

    Only ``tools/call`` is subject to tool-level policy; everything else
    (``initialize``, ``tools/list``, ``ping`` …) passes through.
    """
    if msg["method"] != "tools/call":
        return None

    params = msg.get("params")
    name = params.get("name") if isinstance(params, dict) else None
    if not isinstance(name, str) or not name:
        # Let the downstream produce its own -32602; nothing to authorize.
        return None

    if is_protected_tool(name, cfg.protected_prefixes) and principal.role != "admin":
        log.warning(
            "DENY tools/call %s by %s (role=%s)", name, principal.subject, principal.role
        )
        return jsonrpc_error(
            msg.get("id"),
            UNAUTHORIZED_TOOL_CALL,
            "Unauthorized Tool Call",
            {"tool": name, "required_role": "admin", "role": principal.role},
        )
    log.info("ALLOW %s %s by %s (role=%s)", msg["method"], name, principal.subject, principal.role)
    return None


# --------------------------------------------------------------------------- #
# Forwarding
# --------------------------------------------------------------------------- #
# Hop-by-hop headers must not be relayed (RFC 7230 §6.1).
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length",
}


def _upstream_headers(request: web.Request, cfg: GatewayConfig) -> dict[str, str]:
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP and k.lower() != "authorization"
    }
    if cfg.upstream_token:
        headers["Authorization"] = f"Bearer {cfg.upstream_token}"
    headers.setdefault("Accept", "application/json, text/event-stream")
    headers.setdefault("Content-Type", "application/json")
    headers["X-Forwarded-For"] = request.remote or ""
    return headers


async def forward(request: web.Request, body: bytes, cfg: GatewayConfig) -> web.StreamResponse:
    session: ClientSession = request.app[HTTP]
    try:
        upstream = await session.post(
            cfg.downstream_url,
            data=body,
            headers=_upstream_headers(request, cfg),
            timeout=ClientTimeout(total=cfg.upstream_timeout_s),
        )
    except (ClientError, TimeoutError, OSError) as exc:
        log.error("upstream error: %s", exc)
        return web.json_response(
            jsonrpc_error(None, UPSTREAM_UNAVAILABLE, "Upstream MCP server unavailable"),
            status=502,
        )

    async with upstream:
        resp = web.StreamResponse(status=upstream.status)
        for k, v in upstream.headers.items():
            if k.lower() not in _HOP_BY_HOP:
                resp.headers[k] = v
        await resp.prepare(request)
        # Pipe the body through without buffering – handles SSE streams too.
        async for chunk in upstream.content.iter_any():
            await resp.write(chunk)
        await resp.write_eof()
        return resp


# --------------------------------------------------------------------------- #
# Request handler
# --------------------------------------------------------------------------- #
async def handle_mcp(request: web.Request) -> web.StreamResponse:
    cfg: GatewayConfig = request.app[CFG]

    # 1. Authenticate --------------------------------------------------------
    try:
        principal = authenticate(request, cfg)
    except AuthError as exc:
        log.warning("401 from %s: %s", request.remote, exc)
        return web.json_response(
            jsonrpc_error(None, UNAUTHENTICATED, "Unauthenticated", {"reason": str(exc)}),
            status=401,
            headers={"WWW-Authenticate": 'Bearer realm="mcp-gateway"'},
        )

    # 2. Parse ---------------------------------------------------------------
    body = await request.read()
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return web.json_response(jsonrpc_error(None, PARSE_ERROR, "Parse error"), status=400)

    is_batch = isinstance(payload, list)
    messages = payload if is_batch else [payload]
    if is_batch and not messages:
        return web.json_response(
            jsonrpc_error(None, INVALID_REQUEST, "Invalid Request", {"reason": "empty batch"}),
            status=400,
        )

    # 3. Authorize -----------------------------------------------------------
    denials: list[dict[str, Any]] = []
    for msg in messages:
        try:
            validate_envelope(msg)
        except InvalidRequest as exc:
            return web.json_response(
                jsonrpc_error(exc.id, INVALID_REQUEST, "Invalid Request", {"reason": exc.reason}),
                status=400,
            )
        if (denied := authorize(msg, principal, cfg)) is not None:
            denials.append(denied)

    if denials:
        # Fail closed: if *any* message in a batch is unauthorized, block the
        # whole batch.  Partial forwarding would let an attacker smuggle a
        # denied call alongside allowed ones and infer behaviour from timing.
        payload_out: Any = denials if is_batch else denials[0]
        return web.json_response(payload_out, status=403)

    # 4. Forward -------------------------------------------------------------
    return await forward(request, body, cfg)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "downstream": request.app[CFG].downstream_url})


# --------------------------------------------------------------------------- #
# App factory
# --------------------------------------------------------------------------- #
def create_app(cfg: GatewayConfig | None = None) -> web.Application:
    app = web.Application(client_max_size=4 * 1024 * 1024)
    app[CFG] = cfg or GatewayConfig.from_env()

    async def _client(app: web.Application):
        app[HTTP] = ClientSession()
        yield
        await app[HTTP].close()

    app.cleanup_ctx.append(_client)
    app.router.add_post("/mcp", handle_mcp)
    app.router.add_get("/health", handle_health)
    return app


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stderr, level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    cfg = GatewayConfig.from_env()
    port = int(os.environ.get("PORT", "8080"))
    log.info("MCP gateway listening on :%d → %s (protected prefixes: %s)",
             port, cfg.downstream_url, cfg.protected_prefixes)
    web.run_app(create_app(cfg), host="0.0.0.0", port=port, print=None)
