"""Tests for the pure presence rules (task 1B.8, domain part)."""

import pytest

from source.domain.presence import next_version, renewal_due

NOW = 1_700_000_000_000


# ---------------- next_version ----------------
def test_the_first_version_is_the_current_time() -> None:
    assert next_version(0, NOW) == NOW


def test_later_versions_follow_the_clock() -> None:
    assert next_version(NOW, NOW + 10_000) == NOW + 10_000


def test_two_publishes_in_the_same_millisecond_get_different_versions() -> None:
    assert next_version(NOW, NOW) == NOW + 1


def test_versions_keep_growing_if_the_clock_goes_back() -> None:
    assert next_version(NOW, NOW - 60_000) == NOW + 1


@pytest.mark.parametrize(
    ("previous", "now"), [(-1, NOW), (0, -1), (1.5, NOW), (0, "x")]
)
def test_invalid_inputs_are_rejected(previous: object, now: object) -> None:
    with pytest.raises(ValueError):
        next_version(previous, now)


# ---------------- renewal_due ----------------
def test_without_an_accepted_presence_publishing_is_due() -> None:
    assert renewal_due(NOW, None, 20_000)


@pytest.mark.parametrize(
    ("now", "due"),
    [
        (NOW, False),  # just published (lease ends at NOW + 30 s)
        (NOW + 9_999, False),
        (NOW + 10_000, True),  # 20 s left: renew
        (NOW + 30_000, True),  # lease over
        (NOW + 99_000, True),
    ],
)
def test_renewal_is_due_when_the_margin_is_reached(now: int, due: bool) -> None:
    assert renewal_due(now, NOW + 30_000, 20_000) is due
