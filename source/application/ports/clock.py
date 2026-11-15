from typing import Protocol, runtime_checkable


@runtime_checkable
class ClockPort(Protocol):
    """Time source and timers.

    Business code must never call ``time.time()`` or ``time.sleep()``. Tests
    inject a controllable clock, so expiry, backoff and failure-detection
    timeouts run instantly and deterministically.
    """

    def now(self) -> float:
        """Current wall-clock time in seconds since the epoch."""
        ...

    def sleep(self, seconds: float) -> None:
        """Block for ``seconds``. A fake clock just advances its time."""
        ...
