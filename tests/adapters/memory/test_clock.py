from typing import Any

import pytest

from source.adapters.memory.clock import DEFAULT_START_WALL_MS, ManualClock
from source.application.ports.clock import ClockPort
from tests.contracts.clock_contract import ClockContract


class TestManualClockContract(ClockContract):
    @pytest.fixture
    def clock(self) -> ClockPort:
        return ManualClock()


# ----------------------------- behaviour specific to the manual clock -------


def test_it_satisfies_the_port() -> None:
    assert isinstance(ManualClock(), ClockPort)


def test_time_never_moves_on_its_own() -> None:
    clock = ManualClock()
    assert (clock.now_ms(), clock.monotonic_ms()) == (
        clock.now_ms(),
        clock.monotonic_ms(),
    )
    assert clock.now_ms() == DEFAULT_START_WALL_MS and clock.monotonic_ms() == 0


def test_sleeping_advances_both_clocks_by_exactly_the_duration() -> None:
    clock = ManualClock(wall_ms=5_000, monotonic_ms=100)
    clock.sleep_ms(250)
    assert (clock.now_ms(), clock.monotonic_ms()) == (5_250, 350)


def test_advance_moves_both_clocks_without_recording_a_sleep() -> None:
    clock = ManualClock(wall_ms=1_000, monotonic_ms=0)
    clock.advance(14 * 24 * 3600 * 1000)  # 14 days, instantly
    assert clock.monotonic_ms() == 14 * 24 * 3600 * 1000
    assert clock.sleeps == ()


def test_sleep_calls_are_recorded_in_order_for_backoff_assertions() -> None:
    clock = ManualClock()
    for delay in (100, 200, 400):
        clock.sleep_ms(delay)
    assert clock.sleeps == (100, 200, 400)


def test_the_wall_clock_can_jump_backwards_without_touching_monotonic_time() -> None:
    clock = ManualClock(wall_ms=10_000, monotonic_ms=500)
    clock.set_wall_clock(2_000)  # e.g. an NTP correction
    assert clock.now_ms() == 2_000
    assert clock.monotonic_ms() == 500
    clock.sleep_ms(10)
    assert clock.monotonic_ms() == 510 and clock.now_ms() == 2_010


@pytest.mark.parametrize("bad", [-1, 1.5, "1", None, True])
def test_controls_reject_invalid_values(bad: Any) -> None:
    clock = ManualClock()
    with pytest.raises(ValueError):
        clock.advance(bad)
    with pytest.raises(ValueError):
        clock.set_wall_clock(bad)


@pytest.mark.parametrize(
    "kwargs", [{"wall_ms": -1}, {"monotonic_ms": -1}, {"wall_ms": 1.5}]
)
def test_constructor_rejects_invalid_start_times(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        ManualClock(**kwargs)


def test_a_rejected_sleep_is_not_recorded() -> None:
    clock = ManualClock()
    with pytest.raises(ValueError):
        clock.sleep_ms(-1)
    assert clock.sleeps == ()


def test_two_clocks_are_independent() -> None:
    a, b = ManualClock(), ManualClock()
    a.advance(1_000)
    assert b.monotonic_ms() == 0
