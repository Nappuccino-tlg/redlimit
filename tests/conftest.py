"""Fixtures backed by a real Redis.

Every guarantee this library makes is a guarantee about what Redis does with a script
while other clients are waiting. A fake would be asserting that the fake is atomic.
"""

import asyncio
import os

import pytest
from redis.asyncio import Redis

# Assigned, not defaulted from REDIS_URL: these tests flush the database they point at
# between every test, so what they are allowed to destroy is decided here and not by
# whatever the surrounding shell happens to export.
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
async def redis():
    client = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


async def warm(client: Redis, connections: int) -> None:
    """Fill the connection pool before a test races anything through it.

    Without this the concurrency tests are theatre. N commands issued together on a cold
    pool queue behind N TCP handshakes, so the first attempt finishes its whole read,
    decide, write cycle before the last one has a socket -- and a check-then-increment
    limiter, which is exactly what these tests exist to catch, sails through. Measured on
    a cold pool it let 50 of 50 past a limit of 5, and the test still passed.

    A round of concurrent pings forces the pool to open the sockets and hand them back.
    """
    await asyncio.gather(*(client.ping() for _ in range(connections)))


@pytest.fixture
async def racing_redis():
    """Like `redis`, but with a pool warm enough for a real race."""
    client = Redis.from_url(TEST_REDIS_URL, decode_responses=True, max_connections=128)
    await warm(client, 64)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()
