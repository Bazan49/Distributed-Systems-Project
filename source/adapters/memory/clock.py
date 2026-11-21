"""ManualClock: a ClockPort whose time only moves when told to.

Used in tests so that nothing ever really waits: a test can let 14 days pass
(message expiry) or replay a failure at an exact moment, instantly and
deterministically. Standard library only.
"""

from __future__ import annotations

from source.application.ports.clock import ClockPort
from source.domain.canonical import is_int

DEFAULT_START_WALL_MS = 1_700_000_000_000  # 2023-11-14, a plausible "now"


def _check_duration(duration_ms: int) -> None:
    if not is_int(duration_ms) or duration_ms < 0:
        raise ValueError(
            "duration must be a non-negative integer number of milliseconds"
        )


class ManualClock(ClockPort):
    """A controllable clock with a wall clock and a monotonic clock.

    ``sleep_ms`` does not wait: it advances both clocks, exactly like real time
    passing, and records the call so tests can assert on retry/backoff schedules.
    """

    def __init__(
        self, wall_ms: int = DEFAULT_START_WALL_MS, monotonic_ms: int = 0
    ) -> None:
        if (
            not is_int(wall_ms)
            or wall_ms < 0
            or not is_int(monotonic_ms)
            or monotonic_ms < 0
        ):
            raise ValueError("initial times must be non-negative integers")
        self._wall_ms = wall_ms
        self._monotonic_ms = monotonic_ms
        self._sleeps: list[int] = []

    # ClockPort

    def now_ms(self) -> int:
        return self._wall_ms

    def monotonic_ms(self) -> int:
        return self._monotonic_ms

    def sleep_ms(self, duration_ms: int) -> None:
        _check_duration(duration_ms)
        self._sleeps.append(duration_ms)
        self.advance(duration_ms)

    # test controls

    def advance(self, duration_ms: int) -> None:
        """Let time pass for both clocks without recording a sleep call."""
        _check_duration(duration_ms)
        self._wall_ms += duration_ms
        self._monotonic_ms += duration_ms

    def set_wall_clock(self, wall_ms: int) -> None:
        """Make the wall clock jump (NTP correction, user changing the time).

        It may go backwards. The monotonic clock is NOT affected, which is
        exactly why code must use it, and not the wall clock, for durations.
        """
        if not is_int(wall_ms) or wall_ms < 0:
            raise ValueError("wall clock must be a non-negative integer")
        self._wall_ms = wall_ms

    @property
    def sleeps(self) -> tuple[int, ...]:
        """Durations passed to ``sleep_ms``, in call order."""
        return tuple(self._sleeps)
