"""Process-wide request rate limiting."""

from __future__ import annotations

import threading
import time


class TokenBucket:
    """A shared token bucket, so the cap is global rather than per worker.

    Dividing a budget across N workers undershoots the target, because a worker
    stalled on a slow response cannot donate its unused allowance to the others.
    A shared bucket lets the remaining workers absorb that slack.

    It also gives :meth:`penalise` a single place to slow *everyone* down when
    PyPI pushes back with 429/503 -- per-worker limiters have no way to
    coordinate that.
    """

    def __init__(self, rate_per_minute: float, burst: float | None = None) -> None:
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be positive")
        self.base_rate = rate_per_minute / 60.0
        self._rate = self.base_rate
        # A modest burst smooths over scheduling jitter without letting the
        # crawler open with a stampede.
        self.capacity = burst if burst is not None else max(1.0, self._rate)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    @property
    def rate_per_minute(self) -> float:
        with self._lock:
            return self._rate * 60.0

    def acquire(self) -> None:
        """Block until a token is available, then consume it."""
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self.capacity, self._tokens + (now - self._last) * self._rate
                )
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            # Sleep outside the lock so other workers can still make progress.
            time.sleep(wait)

    def penalise(self, factor: float = 0.5, floor_per_minute: float = 6.0) -> float:
        """Halve the global rate after server pushback. Returns the new rate."""
        with self._lock:
            self._rate = max(self._rate * factor, floor_per_minute / 60.0)
            self.capacity = max(1.0, self._rate)
            return self._rate * 60.0

    def recover(self, factor: float = 1.2) -> float:
        """Ease the rate back toward the configured baseline."""
        with self._lock:
            self._rate = min(self._rate * factor, self.base_rate)
            self.capacity = max(1.0, self._rate)
            return self._rate * 60.0
