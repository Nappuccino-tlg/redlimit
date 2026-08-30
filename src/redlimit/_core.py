"""Limiters, and the decision they hand back."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from importlib import resources
from types import TracebackType

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

DEFAULT_PREFIX = "redlimit"


class RateLimited(Exception):
    """Raised by `attempt()` when a limiter refuses.

    Carries the decision, so a handler can put `retry_after` in a header rather than
    guessing at one.
    """

    def __init__(self, decision: Decision) -> None:
        self.decision = decision
        super().__init__(f"rate limited, retry in {decision.retry_after:.1f}s")

    @property
    def retry_after(self) -> float:
        return self.decision.retry_after


@dataclass(frozen=True)
class Decision:
    """What a limiter said, and what a caller needs to act on it."""

    allowed: bool
    remaining: int
    #: Seconds until the attempt is worth repeating. A hint, not a promise: the sliding
    #: window decays continuously and often allows another attempt before this elapses.
    retry_after: float

    def __bool__(self) -> bool:
        return self.allowed


def _load(name: str) -> str:
    return resources.files(__package__).joinpath("lua", name).read_text(encoding="utf-8")


class Limiter:
    """Common machinery. Use FixedWindow or SlidingWindow.

    Both spend across every key in one atomic script. That is the whole point: a limiter
    that reads a counter, does slow work, and writes afterwards can be walked straight
    past by sending the attempts together instead of in turn -- they all read the same
    number and they all get through. Redis runs a script to completion with nothing
    interleaved, so a hundred simultaneous attempts get a hundred distinct answers.
    """

    _script_name: str

    def __init__(
        self,
        redis: Redis,
        *,
        limit: int,
        window: int,
        prefix: str = DEFAULT_PREFIX,
    ) -> None:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if window < 1:
            raise ValueError("window must be at least 1 second")

        self.redis = redis
        self.limit = limit
        self.window = window
        self.prefix = prefix
        # register_script computes the SHA locally, so building a limiter never touches
        # the server -- which matters when one is constructed at import time.
        self._spend: AsyncScript = redis.register_script(_load(self._script_name))
        self._give_back: AsyncScript = redis.register_script(_load("refund.lua"))

    def _keys(self, keys: Sequence[str]) -> list[str]:
        if not keys:
            raise ValueError("at least one key is required")
        return [f"{self.prefix}:{key}" for key in keys]

    async def consume(self, keys: Sequence[str] | str, *, cost: int = 1) -> Decision:
        """Spend `cost` from every key, or from none, and say whether it was allowed.

        All of them or none of them, because a caller limited on two keys at once -- a
        login throttled per address and per account -- wants one answer, not two that can
        disagree halfway through.
        """
        if isinstance(keys, str):
            keys = [keys]
        if cost < 1:
            raise ValueError("cost must be at least 1")

        allowed, remaining, retry_ms = await self._spend(
            keys=self._keys(keys), args=[self.limit, self.window, cost]
        )
        return Decision(
            allowed=bool(allowed),
            remaining=int(remaining),
            retry_after=max(0.0, int(retry_ms) / 1000),
        )

    async def refund(self, keys: Sequence[str] | str, *, cost: int = 1) -> int:
        """Give back what an attempt spent. Returns how many keys were credited.

        Fewer than you asked for means some window expired in between, which is not an
        error -- there is simply nothing left to credit.
        """
        if isinstance(keys, str):
            keys = [keys]
        return int(await self._give_back(keys=self._keys(keys), args=[self.window, cost]))

    def attempt(self, keys: Sequence[str] | str, *, cost: int = 1) -> Attempt:
        """Spend on the way in, and let the body decide whether to hand it back.

            async with limiter.attempt([f"ip:{ip}", f"email:{email}"]) as attempt:
                if not verify(password):
                    raise Unauthorized      # the attempt keeps its cost
                attempt.refund()            # it was the owner, so it was free

        Spending first and refunding after is what separates this from checking first and
        spending later. The check-first shape is the one everybody writes and it does not
        survive concurrency.
        """
        return Attempt(self, keys if not isinstance(keys, str) else [keys], cost)


class Attempt:
    """One trip through a limiter. Created by `Limiter.attempt`."""

    def __init__(self, limiter: Limiter, keys: Sequence[str], cost: int) -> None:
        self._limiter = limiter
        self._keys = keys
        self._cost = cost
        self._refund = False
        self.decision: Decision | None = None

    def refund(self) -> None:
        """Mark this attempt free. The credit happens when the block exits."""
        self._refund = True

    async def __aenter__(self) -> Attempt:
        self.decision = await self._limiter.consume(self._keys, cost=self._cost)
        if not self.decision.allowed:
            raise RateLimited(self.decision)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Refunded even when the body raised, if it asked to be. Whether the work
        # succeeded is the caller's business; whether the attempt should have cost
        # anything is what they told us.
        if self._refund:
            await self._limiter.refund(self._keys, cost=self._cost)


class FixedWindow(Limiter):
    """Counts per clock-aligned window. Cheapest, and lets twice the limit through a
    boundary if a caller spends its budget just before one and again just after."""

    _script_name = "fixed_window.lua"


class SlidingWindow(Limiter):
    """Counts this window plus what is still inside the last one. No boundary burst, at
    the cost of one extra read per key and an approximation -- see lua/sliding_window.lua."""

    _script_name = "sliding_window.lua"
