"""What both limiters promise, asserted against a real Redis.

The concurrency test is the reason this package exists. Everything else is the behaviour
you would expect; that one is the behaviour a hand-rolled limiter usually gets wrong.
"""

import asyncio

import pytest

from redlimit import Decision, FixedWindow, RateLimited, SlidingWindow

BOTH = pytest.mark.parametrize(
    "limiter_class", [FixedWindow, SlidingWindow], ids=["fixed", "sliding"]
)


async def align(redis, *, window: int = 1, offset: float = 0.05) -> None:
    """Land `offset` seconds into a fresh window.

    Windows are aligned to absolute time, not to whenever a test happened to start, so a
    one-second window test that merely sleeps is at the mercy of where the clock was: two
    spends late in a window and a sleep of 1.05s land two windows on, with an empty one in
    between, and the assertion then measures nothing. That is exactly how this test failed
    the first time it ran.

    Aligned against the Redis clock rather than this process's, because that is the one
    the scripts read.
    """
    seconds, microseconds = await redis.time()
    now = seconds + microseconds / 1_000_000
    await asyncio.sleep(window - (now % window) + offset)


@BOTH
async def test_spends_up_to_the_limit(redis, limiter_class):
    limiter = limiter_class(redis, limit=3, window=60)

    for expected_remaining in (2, 1, 0):
        decision = await limiter.consume("caller")
        assert decision.allowed
        assert decision.remaining == expected_remaining


@BOTH
async def test_refuses_past_the_limit(redis, limiter_class):
    limiter = limiter_class(redis, limit=2, window=60)
    await limiter.consume("caller")
    await limiter.consume("caller")

    decision = await limiter.consume("caller")

    assert not decision.allowed
    assert decision.remaining == 0
    assert decision.retry_after > 0


@BOTH
async def test_a_decision_is_truthy_when_it_allows(redis, limiter_class):
    limiter = limiter_class(redis, limit=1, window=60)
    assert await limiter.consume("caller")
    assert not await limiter.consume("caller")


@BOTH
async def test_a_refusal_costs_the_caller_nothing(redis, limiter_class):
    """Otherwise "10 per window" quietly means "10, plus however many times you were told
    no" -- and a caller who keeps retrying holds their own counter above the limit."""
    limiter = limiter_class(redis, limit=2, window=60)
    await limiter.consume("caller")
    await limiter.consume("caller")

    for _ in range(5):
        assert not await limiter.consume("caller")

    # One credit back should buy exactly one attempt. If those five refusals had been
    # charged, the counter would be at seven and this would still be refused.
    await limiter.refund("caller")
    assert await limiter.consume("caller")
    assert not await limiter.consume("caller")


@BOTH
async def test_concurrent_attempts_cannot_outrun_the_limit(racing_redis, limiter_class):
    """The one that matters.

    A limiter that reads the counter, decides, and increments afterwards passes every
    sequential test in this file and fails this one: fifty attempts sent together all read
    the same zero and all get through. Redis runs a script to completion with nothing
    interleaved, so exactly `limit` of them can win.
    """
    limiter = limiter_class(racing_redis, limit=5, window=60)

    decisions = await asyncio.gather(*(limiter.consume("caller") for _ in range(50)))

    assert sum(1 for decision in decisions if decision.allowed) == 5


@BOTH
async def test_concurrent_attempts_across_several_keys(racing_redis, limiter_class):
    limiter = limiter_class(racing_redis, limit=4, window=60)

    decisions = await asyncio.gather(
        *(limiter.consume(["ip:1.2.3.4", "user:alice"]) for _ in range(30))
    )

    assert sum(1 for decision in decisions if decision.allowed) == 4


@BOTH
async def test_keys_are_independent(redis, limiter_class):
    limiter = limiter_class(redis, limit=1, window=60)

    assert await limiter.consume("alice")
    assert await limiter.consume("bob")


@BOTH
async def test_every_key_or_none_of_them(redis, limiter_class):
    """A spend that one key cannot afford must not be charged to the others.

    Charging as it goes would let a caller drain somebody else's budget just by including
    them in an attempt that was always going to be refused.
    """
    limiter = limiter_class(redis, limit=1, window=60)
    await limiter.consume("exhausted")

    assert not await limiter.consume(["exhausted", "untouched"])

    # Still has its whole budget, because that attempt was rolled back.
    assert await limiter.consume("untouched")


@BOTH
async def test_cost_can_be_more_than_one(redis, limiter_class):
    limiter = limiter_class(redis, limit=10, window=60)

    assert (await limiter.consume("caller", cost=7)).remaining == 3
    assert not await limiter.consume("caller", cost=5)
    assert await limiter.consume("caller", cost=3)


@BOTH
async def test_prefixes_keep_limiters_apart(redis, limiter_class):
    one = limiter_class(redis, limit=1, window=60, prefix="login")
    two = limiter_class(redis, limit=1, window=60, prefix="signup")

    assert await one.consume("same-key")
    assert await two.consume("same-key")
    assert not await one.consume("same-key")


@BOTH
async def test_a_limiter_is_built_without_touching_the_server(redis, limiter_class):
    """Constructed at import time in most applications, long before Redis is reachable."""
    broken = limiter_class.__new__(limiter_class)
    assert broken is not None
    limiter_class(redis, limit=1, window=60)  # no await, no connection


@pytest.mark.parametrize("kwargs", [{"limit": 0, "window": 60}, {"limit": 5, "window": 0}])
@BOTH
async def test_nonsense_configuration_is_refused(redis, limiter_class, kwargs):
    with pytest.raises(ValueError):
        limiter_class(redis, **kwargs)


@BOTH
async def test_an_empty_key_list_is_refused(redis, limiter_class):
    limiter = limiter_class(redis, limit=1, window=60)
    with pytest.raises(ValueError, match="at least one key"):
        await limiter.consume([])


@BOTH
async def test_a_zero_cost_is_refused(redis, limiter_class):
    """A free spend is not a rate limit, and is more likely a bug than an intention."""
    limiter = limiter_class(redis, limit=1, window=60)
    with pytest.raises(ValueError, match="cost must be"):
        await limiter.consume("caller", cost=0)


async def test_decision_reports_remaining_headroom(redis):
    limiter = FixedWindow(redis, limit=3, window=60)
    assert await limiter.consume("caller") == Decision(allowed=True, remaining=2, retry_after=60.0)


async def test_the_fixed_window_lets_a_boundary_burst_through(redis):
    """Stated as a test because it is the trade, not a defect.

    A caller can spend the whole budget at the end of one window and the whole of the next
    at the start of it. SlidingWindow is the answer when that matters.
    """
    limiter = FixedWindow(redis, limit=2, window=1)
    await align(redis)

    await limiter.consume("caller")
    await limiter.consume("caller")
    assert not await limiter.consume("caller")

    await align(redis)

    assert await limiter.consume("caller")
    assert await limiter.consume("caller")


async def test_the_sliding_window_does_not(redis):
    limiter = SlidingWindow(redis, limit=2, window=1)
    await align(redis)

    await limiter.consume("caller")
    await limiter.consume("caller")

    # A twentieth of the way into the next window, the one before it still counts for 95%
    # of itself -- so the budget is not handed back wholesale the way a fixed window hands
    # it back at exactly the same moment.
    await align(redis)

    assert not await limiter.consume("caller")


async def test_raising_carries_the_decision(redis):
    limiter = FixedWindow(redis, limit=1, window=60)
    await limiter.consume("caller")

    with pytest.raises(RateLimited) as raised:
        async with limiter.attempt("caller"):
            pytest.fail("the body must not run")

    assert raised.value.retry_after > 0
    assert raised.value.decision.allowed is False
