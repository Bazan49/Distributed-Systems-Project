import pytest

from source.adapters.system.clock import SystemClock
from source.application.ports.clock import ClockPort
from tests.contracts.clock_contract import ClockContract


class TestSystemClockContract(ClockContract):
    """The tests in the clock contract are run against the real system clock."""

    @pytest.fixture
    def clock(self) -> ClockPort:
        return SystemClock()


def test_it_satisfies_the_port() -> None:
    assert isinstance(SystemClock(), ClockPort)
