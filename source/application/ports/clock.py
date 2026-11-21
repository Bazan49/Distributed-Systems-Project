"""ClockPort: time and waiting, injected so that tests can control them.

Nothing in the domain or application layers may call ``time.time`` or
``time.sleep`` directly. Going through this port is what lets a test jump
forward 14 days (message expiry), or replay a failure at an exact moment,
without waiting.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ClockPort(Protocol):
    """Wall-clock time, monotonic time and sleeping."""

    def now_ms(self) -> int:
        """Wall-clock time in milliseconds since 1970-01-01 UTC.

        For timestamps that are shown or stored. It can jump (NTP, manual
        changes) and differs between machines, so NEVER use it to order
        messages or to measure elapsed time.
        """
        ...

    def monotonic_ms(self) -> int:
        """Milliseconds from an arbitrary origin; never goes backwards.

        Use it for deadlines, timeouts and elapsed time. The origin is
        meaningless: only differences between two readings carry information.
        """
        ...

    def sleep_ms(self, duration_ms: int) -> None:
        """Block the caller for ``duration_ms`` milliseconds (``0`` returns immediately).

        A controllable clock advances its own time instead of really waiting.

        Raises:
            ValueError: ``duration_ms`` is negative or not an integer.
        """
        ...
