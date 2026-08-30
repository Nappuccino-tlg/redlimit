"""A FastAPI dependency, for the case where the limit is about the request itself.

    from redlimit import SlidingWindow
    from redlimit.fastapi import limit

    links = SlidingWindow(redis, limit=30, window=3600)

    @app.post("/links", dependencies=[Depends(limit(links))])
    async def create_link(...): ...

That covers "this endpoint, this often" and nothing more. As soon as the decision depends
on something the handler learns -- whether the password was right, whether the upload was
accepted -- the dependency is the wrong shape, because it has to answer before the handler
runs. Use `limiter.attempt()` inside the handler instead; refunds only make sense there.

FastAPI is not a dependency of this package. Importing this module without it raises, and
that is the only place it is mentioned.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable, Sequence

from redlimit._core import Limiter

try:
    from fastapi import Depends, HTTPException, Request, status
except ModuleNotFoundError as exc:  # pragma: no cover - exercised by import, not by tests
    raise ModuleNotFoundError(
        "redlimit.fastapi needs FastAPI installed: pip install fastapi"
    ) from exc

__all__ = ["client_ip", "limit"]

KeyFunc = Callable[[Request], Sequence[str] | str]


def client_ip(request: Request) -> str:
    """The peer address, and only that.

    Deliberately not X-Forwarded-For. That header is caller-supplied, and a rate limit
    keyed on something the caller chooses is not a rate limit -- a fresh value per request
    is an unlimited quota. Behind a proxy, read it yourself with the number of hops you
    actually run and pass your own key function; only you know how many there are.
    """
    return request.client.host if request.client else "unknown"


def _default_key(request: Request) -> str:
    return f"{request.url.path}:{client_ip(request)}"


def limit(
    limiter: Limiter,
    key: KeyFunc = _default_key,
    *,
    cost: int = 1,
) -> Callable[[Request], Awaitable[None]]:
    """Build a dependency that spends from `limiter` before the handler runs.

    `key` maps a request to one key or several. The default is the path and the peer
    address, so two endpoints sharing a limiter still get separate budgets.

    A refusal becomes a 429 carrying `Retry-After`, because a client that is told to slow
    down and not told for how long will simply retry immediately.
    """

    async def dependency(request: Request) -> None:
        keys = key(request)
        decision = await limiter.consume(keys, cost=cost)
        if not decision.allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Try again later.",
                headers={"Retry-After": str(max(1, round(decision.retry_after)))},
            )

    return dependency


def hashed(value: str) -> str:
    """A fixed-width, log-safe stand-in for a caller-supplied identifier.

    Emails and usernames make good limiter keys and poor Redis keys: unbounded in length,
    and personal data sitting in a keyspace that any `KEYS *` will print. The hash is not
    a security measure -- anyone can compute it -- it just keeps the address out of the
    dump and the key a predictable size.
    """
    return hashlib.sha256(value.strip().lower().encode()).hexdigest()[:32]


# Re-exported so `from redlimit.fastapi import Depends` is not needed alongside this one.
__all__ += ["Depends", "hashed"]
