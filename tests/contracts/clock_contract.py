"""Contract of ClockPort: behaviour EVERY implementation must have.

How to use it: subclass ``ClockContract`` in a test module and override the
``clock`` fixture. pytest then runs every test below against your adapter.

    class TestMyClock(ClockContract):
        @pytest.fixture
        def clock(self) -> ClockPort:
            return MyClock()

The class is not collected by itself (its name does not start with "Test"), so
the tests only run for concrete subclasses.
"""

from typing import Any

import pytest

from source.application.ports.clock import ClockPort

# Real sleeping has timer granularity, so the elapsed time may be 1 ms short.
TOLERANCE_MS = 1


class ClockContract:
    @pytest.fixture
    def clock(self) -> ClockPort:
        raise NotImplementedError(
            "override the 'clock' fixture in the concrete test class"
        )

    # -- wall clock -----------------------------------------------------------

    def test_now_ms_is_an_integer_not_a_bool(self, clock: ClockPort) -> None:
        value = clock.now_ms()
        assert isinstance(value, int) and not isinstance(value, bool)

    def test_now_ms_looks_like_unix_time_in_milliseconds(
        self, clock: ClockPort
    ) -> None:
        # Catches the classic bug of returning seconds (~1.7e9) or
        # microseconds (~1.7e15).
        assert 1_600_000_000_000 < clock.now_ms() < 4_000_000_000_000

    # -- monotonic clock ------------------------------------------------------

    def test_monotonic_ms_is_an_integer_not_a_bool(self, clock: ClockPort) -> None:
        value = clock.monotonic_ms()
        assert isinstance(value, int) and not isinstance(value, bool)

    def test_monotonic_ms_never_goes_backwards(self, clock: ClockPort) -> None:
        readings = [clock.monotonic_ms() for _ in range(200)]
        assert readings == sorted(readings)

    # -- sleeping -------------------------------------------------------------

    def test_sleep_returns_none(self, clock: ClockPort) -> None:
        result: object = clock.sleep_ms(0)  # type: ignore[func-returns-value]
        assert result is None

    def test_sleeping_zero_does_not_move_monotonic_time_backwards(
        self, clock: ClockPort
    ) -> None:
        before = clock.monotonic_ms()
        clock.sleep_ms(0)
        assert clock.monotonic_ms() >= before

    def test_sleeping_advances_monotonic_time_by_at_least_the_duration(
        self, clock: ClockPort
    ) -> None:
        before = clock.monotonic_ms()
        clock.sleep_ms(20)
        assert clock.monotonic_ms() - before >= 20 - TOLERANCE_MS

    def test_consecutive_sleeps_accumulate(self, clock: ClockPort) -> None:
        before = clock.monotonic_ms()
        for _ in range(3):
            clock.sleep_ms(10)
        assert clock.monotonic_ms() - before >= 30 - 3 * TOLERANCE_MS

    @pytest.mark.parametrize("bad", [-1, -1000])
    def test_sleep_rejects_negative_durations(self, clock: ClockPort, bad: int) -> None:
        with pytest.raises(ValueError):
            clock.sleep_ms(bad)

    @pytest.mark.parametrize("bad", [1.5, 0.1, "10", None, True])
    def test_sleep_rejects_non_integer_durations(
        self, clock: ClockPort, bad: Any
    ) -> None:
        with pytest.raises(ValueError):
            clock.sleep_ms(bad)

    def test_a_rejected_sleep_does_not_move_time(self, clock: ClockPort) -> None:
        before = clock.monotonic_ms()
        with pytest.raises(ValueError):
            clock.sleep_ms(-5)
        assert clock.monotonic_ms() - before < 20  # nothing slept
