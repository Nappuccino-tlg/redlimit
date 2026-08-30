"""Rate limiting for Redis that survives concurrency.

The limiter everybody writes reads a counter, decides, and increments afterwards. Sent
one at a time, it works. Sent together, every attempt reads the same number and every
attempt gets through, and the configured limit stops describing anything. Everything here
spends first, atomically, inside a Lua script -- and hands the spend back if the caller
says the attempt should have been free.
"""

from redlimit._core import (
    Attempt,
    Decision,
    FixedWindow,
    Limiter,
    RateLimited,
    SlidingWindow,
)

__all__ = [
    "Attempt",
    "Decision",
    "FixedWindow",
    "Limiter",
    "RateLimited",
    "SlidingWindow",
]

__version__ = "0.1.0"
