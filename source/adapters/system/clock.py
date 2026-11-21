"""SystemClock: the real ClockPort, backed by the operating system's clocks."""

from __future__ import annotations

import time

from source.application.ports.clock import ClockPort
from source.domain.canonical import is_int


class SystemClock(ClockPort):
    """Real wall-clock and monotonic time, and real sleeping."""

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def monotonic_ms(self) -> int:
        return time.monotonic_ns() // 1_000_000

    def sleep_ms(self, duration_ms: int) -> None:
        if not is_int(duration_ms) or duration_ms < 0:
            raise ValueError(
                "duration must be a non-negative integer number of milliseconds"
            )
        time.sleep(duration_ms / 1000)
