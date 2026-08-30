"""Refunds: the half that lets "only failures cost anything" be written safely.

Checking a budget, doing slow work, and charging only on failure is the shape people reach
for, and it is the shape concurrency defeats. Spending first and crediting back afterwards
is the same policy with none of the hole.
"""

import asyncio

import pytest

from redlimit import FixedWindow, RateLimited, SlidingWindow

BOTH = pytest.mark.parametrize(
    "limiter_class", [FixedWindow, SlidingWindow], ids=["fixed", "sliding"]
)


@BOTH
async def test_a_refund_gives_the_spend_back(redis, limiter_class):
    limiter = limiter_class(redis, limit=1, window=60)
    await limiter.consume("caller")
    assert not await limiter.consume("caller")

    assert await limiter.refund("caller") == 1

    assert await limiter.consume("caller")


@BOTH
async def test_a_refund_cannot_manufacture_quota(redis, limiter_class):
    """Refunding more than was spent is a caller mistake, not a way to buy attempts."""
    limiter = limiter_class(redis, limit=2, window=60)
    await limiter.consume("caller")

    for _ in range(10):
        await limiter.refund("caller")

    assert await limiter.consume("caller")
    assert await limiter.consume("caller")
    assert not await limiter.consume("caller")


@BOTH
async def test_a_refund_does_not_resurrect_an_expired_window(redis, limiter_class):
    """A bare DECRBY would recreate the key holding a negative number and carrying no TTL,
    and it would then sit there absorbing the next window's traffic until someone noticed
    a limiter that had quietly stopped limiting."""
    limiter = limiter_class(redis, limit=1, window=60)

    assert await limiter.refund("nobody-spent-anything-here") == 0

    assert await redis.keys("redlimit:nobody-spent-anything-here*") == []


@BOTH
async def test_refunding_reports_what_it_could_credit(redis, limiter_class):
    limiter = limiter_class(redis, limit=5, window=60)
    await limiter.consume(["present", "also-present"])

    assert await limiter.refund(["present", "also-present", "never-seen"]) == 2


@BOTH
async def test_a_refund_returns_the_whole_cost(redis, limiter_class):
    limiter = limiter_class(redis, limit=10, window=60)
    await limiter.consume("caller", cost=6)

    await limiter.refund("caller", cost=6)

    assert (await limiter.consume("caller", cost=10)).allowed


@BOTH
async def test_the_attempt_block_spends_and_can_hand_it_back(redis, limiter_class):
    limiter = limiter_class(redis, limit=1, window=60)

    async with limiter.attempt("caller") as attempt:
        attempt.refund()

    # Refunded, so the budget is untouched and the next attempt still fits.
    async with limiter.attempt("caller"):
        pass

    with pytest.raises(RateLimited):
        async with limiter.attempt("caller"):
            pass


@BOTH
async def test_the_attempt_block_keeps_the_cost_when_nobody_asks(redis, limiter_class):
    limiter = limiter_class(redis, limit=1, window=60)

    async with limiter.attempt("caller"):
        pass

    with pytest.raises(RateLimited):
        async with limiter.attempt("caller"):
            pass


@BOTH
async def test_a_refund_survives_the_body_raising(redis, limiter_class):
    """Whether the work succeeded is the caller's business. Whether the attempt should
    have cost anything is what they told us, and a failure elsewhere does not revoke it."""
    limiter = limiter_class(redis, limit=1, window=60)

    with pytest.raises(ZeroDivisionError):
        async with limiter.attempt("caller") as attempt:
            attempt.refund()
            raise ZeroDivisionError("something in the body went wrong")

    assert await limiter.consume("caller")


async def test_only_failures_cost_anything(redis):
    """The whole motivating shape, end to end.

    Ten guesses per window, and signing in correctly forty times in a row costs nothing --
    which is what stops anyone locking an account out by failing at it on purpose.
    """
    limiter = FixedWindow(redis, limit=10, window=900)

    async def sign_in(password: str) -> bool:
        async with limiter.attempt(["ip:1.2.3.4", "email:alice"]) as attempt:
            if password != "correct":
                return False
            attempt.refund()
            return True

    for _ in range(40):
        assert await sign_in("correct") is True

    for _ in range(10):
        assert await sign_in("wrong") is False

    with pytest.raises(RateLimited):
        await sign_in("correct")


async def test_the_motivating_shape_survives_a_burst(racing_redis):
    """Sequentially, ten guesses is ten guesses. Sent together at a limiter that checks
    before it spends, all of them read the same zero and all of them get a try."""
    limiter = FixedWindow(racing_redis, limit=10, window=900)

    async def guess() -> bool:
        try:
            async with limiter.attempt("email:alice"):
                return True  # reached the password check
        except RateLimited:
            return False

    reached = await asyncio.gather(*(guess() for _ in range(100)))

    assert sum(reached) == 10
