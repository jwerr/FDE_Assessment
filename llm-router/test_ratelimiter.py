"""Rate limiter unit tests. Run: python -m pytest test_ratelimiter.py -v"""

from __future__ import annotations

import asyncio

import pytest

from ratelimiter import Database, RateLimitExceeded, SlidingWindowTokenLimiter, Tenant, estimate_tokens


class Clock:
    def __init__(self, t=1_000_000.0): self.t = t
    def __call__(self): return self.t
    def advance(self, s): self.t += s


@pytest.fixture
def clock(): return Clock()


@pytest.fixture
async def db(tmp_path, clock):
    d = Database(tmp_path / "rl.sqlite3", clock=clock)
    yield d
    d.close()


@pytest.fixture
def limiter(db): return SlidingWindowTokenLimiter(db)


T = Tenant("k", "acme", token_limit=1000, window_s=60)


async def test_reserve_within_limit(limiter):
    r = await limiter.reserve(T, 400)
    assert r.tokens == 400
    assert await limiter.usage(T) == 400
    await limiter.reserve(T, 600)
    assert await limiter.usage(T) == 1000


async def test_reserve_over_limit_raises_with_retry_after(limiter, clock):
    await limiter.reserve(T, 700)
    clock.advance(10)
    await limiter.reserve(T, 200)
    with pytest.raises(RateLimitExceeded) as ei:
        await limiter.reserve(T, 200)          # 900 + 200 > 1000
    assert ei.value.used == 900 and ei.value.limit == 1000
    # Need 100 tokens freed; the 700-token row expires at t0+60 → 50 s from now.
    assert ei.value.retry_after_s == 50
    assert await limiter.usage(T) == 900        # failed reserve did not insert


async def test_window_slides_and_evicts(limiter, clock, db):
    await limiter.reserve(T, 900)
    clock.advance(59)
    with pytest.raises(RateLimitExceeded):
        await limiter.reserve(T, 200)
    clock.advance(2)                            # 61 s after the first reservation
    await limiter.reserve(T, 200)               # old row evicted → allowed
    assert await limiter.usage(T) == 200
    rows = await db.run(lambda c: c.execute("SELECT COUNT(*) FROM token_usage").fetchone()[0])
    assert rows == 1                            # the expired row was physically deleted


async def test_settle_replaces_estimate_with_actual(limiter):
    r = await limiter.reserve(T, 500)           # pessimistic estimate
    await limiter.settle(r, 120)                # provider said 120
    assert await limiter.usage(T) == 120
    r2 = await limiter.reserve(T, 800)          # fits now
    await limiter.settle(r2, None)              # request failed → release entirely
    assert await limiter.usage(T) == 120


async def test_tenants_are_isolated(limiter):
    other = Tenant("k2", "globex", 1000, 60)
    await limiter.reserve(T, 1000)
    await limiter.reserve(other, 1000)          # unaffected by acme
    with pytest.raises(RateLimitExceeded):
        await limiter.reserve(T, 1)


async def test_concurrent_reservations_never_exceed_limit(limiter):
    """50 coroutines race for a 1000-token budget in 100-token bites: exactly 10 win."""
    async def one():
        try:
            await limiter.reserve(T, 100)
            return True
        except RateLimitExceeded:
            return False
    results = await asyncio.gather(*(one() for _ in range(50)))
    assert sum(results) == 10
    assert await limiter.usage(T) == 1000


async def test_state_survives_restart(tmp_path, clock):
    path = tmp_path / "persist.sqlite3"
    d1 = Database(path, clock=clock)
    await d1.upsert_tenant(T)
    await SlidingWindowTokenLimiter(d1).reserve(T, 950)
    d1.close()

    d2 = Database(path, clock=clock)            # "gateway restarted"
    assert await d2.tenant_for_key("k") == T
    with pytest.raises(RateLimitExceeded):
        await SlidingWindowTokenLimiter(d2).reserve(T, 100)
    d2.close()


async def test_global_evict(limiter, clock, db):
    for _ in range(5):
        await limiter.reserve(T, 10)
    clock.advance(4000)
    assert await limiter.evict(max_window_s=3600) == 5


async def test_retry_after_when_single_request_exceeds_budget(limiter):
    with pytest.raises(RateLimitExceeded) as ei:
        await limiter.reserve(T, 5000)
    assert ei.value.retry_after_s == 60         # can't ever fit; report a full window


def test_estimate_tokens():
    assert estimate_tokens("") == 1
    assert estimate_tokens("a" * 400) == 100
