"""The FastAPI dependency, driven through a real app.

Skipped rather than failed where FastAPI is absent: it is an optional extra, and the rest
of the package must not need it.
"""

import asyncio

import pytest

from redlimit import FixedWindow

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi import Depends, FastAPI  # noqa: E402

from redlimit.fastapi import client_ip, hashed, limit  # noqa: E402


def build(limiter, **kwargs):
    app = FastAPI()

    @app.get("/thing", dependencies=[Depends(limit(limiter, **kwargs))])
    async def thing():
        return {"ok": True}

    @app.get("/other", dependencies=[Depends(limit(limiter, **kwargs))])
    async def other():
        return {"ok": True}

    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver")


async def test_allows_up_to_the_limit_then_refuses(redis):
    limiter = FixedWindow(redis, limit=2, window=60)

    async with build(limiter) as client:
        assert (await client.get("/thing")).status_code == 200
        assert (await client.get("/thing")).status_code == 200

        refused = await client.get("/thing")

    assert refused.status_code == 429
    assert int(refused.headers["Retry-After"]) >= 1


async def test_endpoints_sharing_a_limiter_keep_separate_budgets(redis):
    """The default key includes the path, so one busy endpoint does not close another."""
    limiter = FixedWindow(redis, limit=1, window=60)

    async with build(limiter) as client:
        assert (await client.get("/thing")).status_code == 200
        assert (await client.get("/other")).status_code == 200
        assert (await client.get("/thing")).status_code == 429


async def test_a_custom_key_replaces_the_default(redis):
    """One budget for the whole app rather than one per path."""
    limiter = FixedWindow(redis, limit=1, window=60)

    async with build(limiter, key=lambda request: "everything") as client:
        assert (await client.get("/thing")).status_code == 200
        assert (await client.get("/other")).status_code == 429


async def test_a_key_function_may_return_several_keys(redis):
    limiter = FixedWindow(redis, limit=1, window=60)

    def by_ip_and_tenant(request):
        return [f"ip:{client_ip(request)}", "tenant:acme"]

    async with build(limiter, key=by_ip_and_tenant) as client:
        assert (await client.get("/thing")).status_code == 200
        assert (await client.get("/other")).status_code == 429


async def test_cost_is_passed_through(redis):
    limiter = FixedWindow(redis, limit=10, window=60)

    async with build(limiter, cost=6) as client:
        assert (await client.get("/thing")).status_code == 200
        assert (await client.get("/thing")).status_code == 429


async def test_a_burst_through_the_dependency_still_cannot_outrun_it(racing_redis):
    limiter = FixedWindow(racing_redis, limit=5, window=60)

    async with build(limiter) as client:
        responses = await asyncio.gather(*(client.get("/thing") for _ in range(40)))

    assert sum(1 for r in responses if r.status_code == 200) == 5


async def test_client_ip_ignores_a_forwarded_header(redis):
    """Keyed on something the caller sets, a limit is not a limit: a fresh value per
    request is an unlimited quota."""
    limiter = FixedWindow(redis, limit=1, window=60)

    async with build(limiter) as client:
        assert (await client.get("/thing")).status_code == 200
        spoofed = await client.get("/thing", headers={"x-forwarded-for": "9.9.9.9"})

    assert spoofed.status_code == 429


def test_hashed_is_stable_and_bounded():
    assert hashed(" Alice@Example.COM ") == hashed("alice@example.com")
    assert len(hashed("a" * 5000)) == 32
