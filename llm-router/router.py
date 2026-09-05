"""
Resilient model router: primary → secondary failover on 429 / timeout / 5xx.

    result = await router.complete(request_body, request_id)

Failover policy
---------------
| Primary outcome                         | Action                                  |
|-----------------------------------------|-----------------------------------------|
| 2xx                                     | return it                               |
| 429 Too Many Requests                   | fail over                               |
| no response within ``timeout_s`` (3.0)  | cancel the attempt, fail over           |
| 5xx / connection error / bad JSON       | fail over                               |
| other 4xx (400/401/404/422…)            | **do not** fail over – the request is   |
|                                         | wrong, the backup would reject it too   |

Every attempt is wrapped in ``asyncio.wait_for``.  When it fires, the
underlying HTTP request is cancelled (aiohttp closes the socket), so a
primary that answers at t=3.2 s cannot race the secondary's answer: its task
is already gone and its bytes are never read.  ``Attempt`` records per-provider
outcomes for the audit log and the ``X-Fallback`` response headers.

Nothing here builds client-facing error text.  It raises ``RouterError``
with a *code*; the gateway maps codes to a fixed payload.  Provider bodies
and exception reprs are logged, never returned.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout

log = logging.getLogger("router")


@dataclass(frozen=True)
class Provider:
    name: str
    url: str
    api_key: str | None = None
    model: str | None = None      # override the model name when forwarding


@dataclass
class Attempt:
    provider: str
    outcome: str                  # ok | rate_limited | timeout | upstream_error | network_error | client_error
    status: int | None = None
    latency_ms: int = 0


@dataclass
class RouteResult:
    body: dict[str, Any]
    provider: str
    fallback: bool
    attempts: list[Attempt] = field(default_factory=list)


class RouterError(Exception):
    def __init__(self, code: str, attempts: list[Attempt], status: int | None = None):
        super().__init__(code)
        self.code, self.attempts, self.status = code, attempts, status


_FAILOVER = {"rate_limited", "timeout", "upstream_error", "network_error"}


class ModelRouter:
    def __init__(self, session: ClientSession, providers: list[Provider], timeout_s: float = 3.0):
        if not providers:
            raise ValueError("at least one provider required")
        self.session, self.providers, self.timeout_s = session, providers, timeout_s

    async def complete(self, body: dict[str, Any], request_id: str) -> RouteResult:
        attempts: list[Attempt] = []
        for i, p in enumerate(self.providers):
            attempt, result = await self._try(p, body, request_id)
            attempts.append(attempt)
            if attempt.outcome == "ok":
                return RouteResult(result, p.name, fallback=i > 0, attempts=attempts)
            if attempt.outcome not in _FAILOVER:
                raise RouterError("invalid_request", attempts, status=attempt.status)
            if i < len(self.providers) - 1:
                log.warning("[%s] %s → %s (%s), failing over to %s",
                            request_id, p.name, attempt.outcome, attempt.status, self.providers[i + 1].name)
        # All providers exhausted.
        if all(a.outcome == "rate_limited" for a in attempts):
            raise RouterError("upstream_rate_limited", attempts)
        raise RouterError("upstream_unavailable", attempts)

    async def _try(self, p: Provider, body: dict[str, Any], request_id: str) -> tuple[Attempt, dict | None]:
        t0 = time.perf_counter()
        payload = dict(body)
        if p.model:
            payload["model"] = p.model
        headers = {"Content-Type": "application/json", "X-Request-Id": request_id}
        if p.api_key:
            headers["Authorization"] = f"Bearer {p.api_key}"

        async def _call() -> tuple[int, Any]:
            async with self.session.post(
                p.url, json=payload, headers=headers,
                timeout=ClientTimeout(total=None),  # the deadline is enforced by wait_for below
            ) as resp:
                # Read the body inside the task so cancellation drops the connection too.
                return resp.status, await resp.json(content_type=None)

        try:
            status, data = await asyncio.wait_for(_call(), timeout=self.timeout_s)
        except asyncio.TimeoutError:
            ms = int((time.perf_counter() - t0) * 1000)
            log.error("[%s] %s timed out after %dms", request_id, p.name, ms)
            return Attempt(p.name, "timeout", None, ms), None
        except (ClientError, OSError, ValueError) as exc:
            ms = int((time.perf_counter() - t0) * 1000)
            log.error("[%s] %s network/parse error: %r", request_id, p.name, exc)   # log only
            return Attempt(p.name, "network_error", None, ms), None

        ms = int((time.perf_counter() - t0) * 1000)
        if 200 <= status < 300 and isinstance(data, dict):
            return Attempt(p.name, "ok", status, ms), data
        log.error("[%s] %s returned %d: %s", request_id, p.name, status, str(data)[:500])  # log only
        if status == 429:
            return Attempt(p.name, "rate_limited", status, ms), None
        if status >= 500:
            return Attempt(p.name, "upstream_error", status, ms), None
        return Attempt(p.name, "client_error", status, ms), None
