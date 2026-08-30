"""What each limiter costs, measured rather than asserted.

    python benchmarks/compare.py --attempts 20000 --callers 2000

Reports latency per attempt and the memory Redis actually holds per caller, and finishes
by racing the check-then-increment shape this package exists to replace -- because the
interesting number is not how fast redlimit is, it is that the cheap alternative is wrong.

Point REDIS_URL at something disposable: this writes thousands of keys and flushes the
database on the way out.
"""

import argparse
import asyncio
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from redis.asyncio import Redis

from redlimit import FixedWindow, SlidingWindow

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/14")


async def warm(client: Redis, connections: int) -> None:
    """Open the sockets before timing anything, so the numbers are the limiter's and not
    the connection pool's."""
    await asyncio.gather(*(client.ping() for _ in range(connections)))


async def used_memory(client: Redis, pattern: str) -> int:
    """Bytes Redis reports for every key matching `pattern`.

    MEMORY USAGE per key rather than the server total: the point is what one caller costs,
    and the total moves for reasons that have nothing to do with this.
    """
    total = 0
    async for key in client.scan_iter(match=pattern, count=1000):
        total += await client.memory_usage(key) or 0
    return total


async def measure(client: Redis, limiter, callers: int, attempts: int) -> dict:
    await client.flushdb()

    latencies: list[float] = []
    allowed = 0
    for index in range(attempts):
        key = f"caller:{index % callers}"
        started = time.perf_counter()
        decision = await limiter.consume(key)
        latencies.append((time.perf_counter() - started) * 1000)
        allowed += decision.allowed

    latencies.sort()
    memory = await used_memory(client, f"{limiter.prefix}:*")
    return {
        "median_ms": statistics.median(latencies),
        "p99_ms": latencies[int(len(latencies) * 0.99)],
        "bytes_per_caller": memory / callers,
        "allowed": allowed,
    }


async def race_the_naive_one(client: Redis, attempts: int, limit: int) -> int:
    """The shape this package replaces, given every chance to work."""
    await client.flushdb()
    window = 60

    async def naive() -> bool:
        slot = int(time.time()) // window
        key = f"naive:caller:{slot}"
        if int(await client.get(key) or 0) >= limit:
            return False
        await client.incr(key)
        await client.expire(key, window)
        return True

    return sum(await asyncio.gather(*(naive() for _ in range(attempts))))


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attempts", type=int, default=20_000)
    parser.add_argument("--callers", type=int, default=2_000)
    parser.add_argument("--connections", type=int, default=64)
    args = parser.parse_args()

    client = Redis.from_url(REDIS_URL, decode_responses=True, max_connections=256)
    await warm(client, args.connections)

    info = await client.info("server")
    print(f"redis {info['redis_version']} at {REDIS_URL}")
    print(f"{args.attempts:,} attempts spread over {args.callers:,} callers\n")

    rows = []
    for name, cls in (("FixedWindow", FixedWindow), ("SlidingWindow", SlidingWindow)):
        limiter = cls(client, limit=1_000_000, window=3600, prefix=name.lower())
        rows.append((name, await measure(client, limiter, args.callers, args.attempts)))

    width = max(len(name) for name, _ in rows)
    print(f"{'':<{width}}  {'median':>9}  {'p99':>9}  {'bytes/caller':>13}")
    for name, result in rows:
        print(
            f"{name:<{width}}  {result['median_ms']:>8.3f}ms  {result['p99_ms']:>8.3f}ms  "
            f"{result['bytes_per_caller']:>13.0f}"
        )

    print()
    limit, burst = 5, 200
    got_through = await race_the_naive_one(client, burst, limit)
    print(f"check-then-increment, {burst} attempts at once, limit {limit}: {got_through} allowed")

    await client.flushdb()
    fixed = FixedWindow(client, limit=limit, window=60, prefix="race")
    decisions = await asyncio.gather(*(fixed.consume("caller") for _ in range(burst)))
    print(
        f"redlimit,             {burst} attempts at once, limit {limit}: "
        f"{sum(1 for d in decisions if d.allowed)} allowed"
    )

    await client.flushdb()
    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
