"""
Token-aware sliding-window rate limiter, persisted in on-disk SQLite.

Model
-----
Each tenant (API key) has a budget of ``limit`` tokens per ``window_s``
seconds.  The window *slides*: at any instant the tenant's usage is the sum
of tokens consumed in the last ``window_s`` seconds, not "since the top of
the minute".  This is the *sliding log* algorithm — exact, no burst at the
window edge — implemented as one SQLite row per request.

Reserve → settle
----------------
The exact token cost of a completion is only known after the provider
answers.  So the limiter works in two phases:

1. ``reserve(tenant, estimate)`` — before calling the model, atomically check
   ``usage + estimate <= limit`` and, if so, insert a row for ``estimate``
   tokens.  Concurrent requests cannot both squeeze through because the check
   and the insert happen inside one ``BEGIN IMMEDIATE`` transaction, which
   takes SQLite's write lock.
2. ``settle(reservation, actual)`` — after the response, replace the estimate
   with the real ``usage.total_tokens`` (or release it entirely on failure).

Eviction
--------
Rows older than the window are deleted at the start of every ``reserve``
(cheap: indexed range delete) and by ``evict()`` which the gateway runs
periodically, so the table never grows beyond one window of traffic.

Persistence
-----------
Because the state is on disk, a gateway restart does not reset anyone's
budget, and multiple gateway processes on one host share the same limits.
"""

from __future__ import annotations

import asyncio
import math
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS tenants (
    api_key      TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    token_limit  INTEGER NOT NULL,          -- tokens per window
    window_s     INTEGER NOT NULL DEFAULT 60
);

CREATE TABLE IF NOT EXISTS token_usage (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id  TEXT    NOT NULL,
    ts_ms      INTEGER NOT NULL,            -- reservation time
    tokens     INTEGER NOT NULL,            -- reserved, later replaced by actual
    settled    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_usage_tenant_ts ON token_usage (tenant_id, ts_ms);

CREATE TABLE IF NOT EXISTS request_log (
    request_id   TEXT PRIMARY KEY,
    tenant_id    TEXT,
    ts_ms        INTEGER NOT NULL,
    provider     TEXT,
    fallback     INTEGER NOT NULL DEFAULT 0,
    status       INTEGER,
    error_code   TEXT,
    latency_ms   INTEGER,
    tokens       INTEGER
);
"""


@dataclass(frozen=True)
class Tenant:
    api_key: str
    tenant_id: str
    token_limit: int
    window_s: int


@dataclass(frozen=True)
class Reservation:
    id: int
    tenant_id: str
    tokens: int


class RateLimitExceeded(Exception):
    def __init__(self, tenant_id: str, used: int, limit: int, retry_after_s: int):
        super().__init__(f"{tenant_id}: {used}/{limit} tokens used")
        self.tenant_id, self.used, self.limit, self.retry_after_s = tenant_id, used, limit, retry_after_s


class Database:
    """Thin wrapper: one connection, all calls serialised through a thread + lock.

    SQLite is fast enough that a single writer per process is not the
    bottleneck; ``BEGIN IMMEDIATE`` also makes it safe across *processes*.
    """

    def __init__(self, path: str | Path, clock: Callable[[], float] = time.time):
        self.path = str(path)
        self.clock = clock
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._lock = asyncio.Lock()

    async def run(self, fn, *args):
        async with self._lock:
            return await asyncio.to_thread(fn, self._conn, *args)

    def close(self) -> None:
        self._conn.close()

    # ---- tenants ----------------------------------------------------------
    async def upsert_tenant(self, t: Tenant) -> None:
        def _do(c):
            c.execute("INSERT OR REPLACE INTO tenants VALUES (?,?,?,?)",
                      (t.api_key, t.tenant_id, t.token_limit, t.window_s))
        await self.run(_do)

    async def tenant_for_key(self, api_key: str) -> Tenant | None:
        def _do(c):
            r = c.execute("SELECT * FROM tenants WHERE api_key=?", (api_key,)).fetchone()
            return Tenant(r["api_key"], r["tenant_id"], r["token_limit"], r["window_s"]) if r else None
        return await self.run(_do)

    # ---- request log ------------------------------------------------------
    async def log_request(self, **row) -> None:
        def _do(c):
            c.execute(
                "INSERT OR REPLACE INTO request_log (request_id, tenant_id, ts_ms, provider, fallback, status, "
                "error_code, latency_ms, tokens) VALUES (:request_id,:tenant_id,:ts_ms,:provider,:fallback,"
                ":status,:error_code,:latency_ms,:tokens)",
                {"provider": None, "fallback": 0, "status": None, "error_code": None,
                 "latency_ms": None, "tokens": None, "ts_ms": int(self.clock() * 1000), **row})
        await self.run(_do)


class SlidingWindowTokenLimiter:
    def __init__(self, db: Database):
        self.db = db

    def _now_ms(self) -> int:
        return int(self.db.clock() * 1000)

    async def reserve(self, tenant: Tenant, estimate: int) -> Reservation:
        """Atomically check-and-reserve. Raises RateLimitExceeded."""
        now = self._now_ms()
        cutoff = now - tenant.window_s * 1000
        estimate = max(1, int(estimate))

        def _do(c):
            c.execute("BEGIN IMMEDIATE")  # write lock: check + insert are one unit
            try:
                c.execute("DELETE FROM token_usage WHERE tenant_id=? AND ts_ms<?", (tenant.tenant_id, cutoff))
                used = c.execute("SELECT COALESCE(SUM(tokens),0) FROM token_usage WHERE tenant_id=?",
                                 (tenant.tenant_id,)).fetchone()[0]
                if used + estimate > tenant.token_limit:
                    retry = _retry_after_s(c, tenant, used + estimate - tenant.token_limit, now)
                    c.execute("ROLLBACK")
                    raise RateLimitExceeded(tenant.tenant_id, used, tenant.token_limit, retry)
                cur = c.execute("INSERT INTO token_usage (tenant_id, ts_ms, tokens) VALUES (?,?,?)",
                                (tenant.tenant_id, now, estimate))
                c.execute("COMMIT")
                return Reservation(cur.lastrowid, tenant.tenant_id, estimate)
            except Exception:
                if c.in_transaction:
                    c.execute("ROLLBACK")
                raise

        return await self.db.run(_do)

    async def settle(self, res: Reservation, actual_tokens: int | None) -> None:
        """Replace the estimate with the real cost; None releases the reservation."""
        def _do(c):
            if actual_tokens is None:
                c.execute("DELETE FROM token_usage WHERE id=?", (res.id,))
            else:
                c.execute("UPDATE token_usage SET tokens=?, settled=1 WHERE id=?", (max(0, int(actual_tokens)), res.id))
        await self.db.run(_do)

    async def usage(self, tenant: Tenant) -> int:
        cutoff = self._now_ms() - tenant.window_s * 1000

        def _do(c):
            return c.execute("SELECT COALESCE(SUM(tokens),0) FROM token_usage WHERE tenant_id=? AND ts_ms>=?",
                             (tenant.tenant_id, cutoff)).fetchone()[0]
        return await self.db.run(_do)

    async def evict(self, max_window_s: int = 3600) -> int:
        """Global sweep: drop rows older than any tenant's window could need."""
        cutoff = self._now_ms() - max_window_s * 1000

        def _do(c):
            return c.execute("DELETE FROM token_usage WHERE ts_ms<?", (cutoff,)).rowcount
        return await self.db.run(_do)


def _retry_after_s(c, tenant: Tenant, need: int, now_ms: int) -> int:
    """Seconds until at least *need* tokens fall out of the window."""
    freed = 0
    for row in c.execute("SELECT ts_ms, tokens FROM token_usage WHERE tenant_id=? ORDER BY ts_ms",
                         (tenant.tenant_id,)):
        freed += row["tokens"]
        if freed >= need:
            return max(1, math.ceil((row["ts_ms"] + tenant.window_s * 1000 - now_ms) / 1000))
    return tenant.window_s  # request alone exceeds the budget; can't do better than "a full window"


def estimate_tokens(text: str) -> int:
    """Cheap pre-flight estimate (~4 chars/token for English). Settled with real usage later."""
    return max(1, len(text) // 4)
