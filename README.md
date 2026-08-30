# redlimit

Rate limiting for Redis that survives concurrency. Atomic spends across several keys at
once, and refunds — so "only failed attempts cost anything" can be written without leaving
a hole in it.

[![CI](https://github.com/Nappuccino-tlg/redlimit/actions/workflows/ci.yml/badge.svg)](https://github.com/Nappuccino-tlg/redlimit/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%20--%203.13-blue)
![License](https://img.shields.io/badge/license-MIT-green)

```bash
pip install redlimit
```

## The limiter everybody writes

```python
current = await redis.get(key)          # read
if current >= limit:                    # decide
    return False
await redis.incr(key)                   # write, too late
return True
```

Sent one at a time it is correct, and every sequential test of it passes. Sent together,
every attempt reads the same number before any of them writes, and every attempt gets
through. Measured against a real Redis with a warm connection pool, that code let **50 of
50 simultaneous attempts past a limit of 5**.

The gap between the read and the write is the whole problem, and no amount of care in
Python closes it — the fix has to happen inside Redis, which runs a script to completion
with nothing interleaved. That is all this library is.

```python
from redis.asyncio import Redis
from redlimit import SlidingWindow

limiter = SlidingWindow(Redis.from_url("redis://localhost"), limit=10, window=900)

decision = await limiter.consume(f"login:{email}")
if not decision:
    raise TooManyRequests(retry_after=decision.retry_after)
```

## Only failures cost anything

Throttling sign-ins means counting failures, not attempts — otherwise anyone can lock an
account out by failing at it on purpose. The obvious way to write that is to check the
budget, verify the password, and charge only when it was wrong, which puts a password
check in the gap and makes the race easier to win, not harder.

Spend first, hand it back if the attempt turns out to have been legitimate:

```python
from redlimit import FixedWindow, RateLimited

limiter = FixedWindow(redis, limit=10, window=900)

async def sign_in(email: str, password: str, ip: str) -> User:
    async with limiter.attempt([f"ip:{ip}", f"email:{email}"]) as attempt:
        user = await users.find(email)
        if user is None or not verify(password, user.hash):
            raise Unauthorized          # the attempt keeps its cost
        attempt.refund()                # it was the owner, so it was free
        return user
```

Signing in correctly forty times in a row costs nothing. Guessing gets ten tries, however
the guesses are arranged.

## Several keys, one answer

Per-IP alone lets an attacker spread guesses for one account across a botnet. Per-account
alone lets one host walk a password list through every account. Both together need one
decision, not two that can disagree halfway through — so a spend either happens on every
key or on none of them:

```python
await limiter.consume([f"ip:{ip}", f"email:{email}"])
```

If any key is out of budget the whole attempt is refused **and rolled back**, so a caller
cannot drain someone else's quota by naming them in an attempt that was always going to
fail. It also means a refusal costs nothing: "ten per window" means ten spends, not ten
plus however many times you were told no.

## The two limiters

| | `FixedWindow` | `SlidingWindow` |
|---|---|---|
| Redis keys per limiter key | 1 | 2 |
| Reads per attempt | 0 | 1 |
| Boundary burst | up to 2× the limit | no |
| Exact | yes, within the window | approximate |

`FixedWindow` counts per clock-aligned window. A caller can spend the whole budget just
before a boundary and the whole of the next just after, so twice the limit passes in a
moment straddling the two. Cheapest, and fine when the limit is a guard rail.

`SlidingWindow` adds what is still inside the previous window, weighted by how far the
current one has run. No boundary burst, at the cost of one extra read and an
approximation: it assumes the previous window's traffic was evenly spread. The exact
answer needs every timestamp in a sorted set per key, which grows with the traffic being
limited — the busier a caller, the more it costs to say no to them. Two integers per key,
whatever they do, is the better trade for deciding whether someone may try a password
again.

Windows are aligned to the **Redis** clock, not the caller's. Application processes on
slightly skewed clocks would otherwise disagree about which window they are in and let
more than the limit through between them at every boundary.

## When you do not need this

If you want "100 requests an hour per IP" and it does not matter that a burst occasionally
gets 105 through, write the ten lines yourself. Nothing here will pay for itself.

It earns its place where going over has a real cost and attempts arrive together: sign-in,
password reset, payment, anything one client can fire in parallel.

## Requirements

Redis 6.0 or newer (`SET ... KEEPTTL`), Python 3.10 or newer, and `redis>=5.0`.

On Redis Cluster, every key in one `consume()` must live in the same slot — wrap the
shared part in `{braces}`, e.g. `f"{{{user_id}}}:ip:{ip}"`.

## Tests

The suite runs against a real Redis, because every guarantee here is a guarantee about
what Redis does with a script while other clients wait. A fake would only prove the fake
is atomic.

```bash
docker run -d -p 6379:6379 redis:7-alpine
pytest
```

The concurrency tests warm the connection pool before they race. Without that they are
theatre: fifty commands issued together on a cold pool queue behind fifty TCP handshakes,
so the first attempt finishes its whole cycle before the last one has a socket — and the
naive limiter above passes. It was written that way first, and it passed.

## License

MIT
