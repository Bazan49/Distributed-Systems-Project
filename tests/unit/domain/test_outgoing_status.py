"""Unit tests for the OutgoingStatus state machine."""

import itertools

import pytest

from source.domain.outgoing_status import OutgoingStatus as S
from source.domain.outgoing_status import advance, can_transition

# Independent specification of the legal transition table.
# If _ALLOWED changes, this must change too, or the test fails.
LEGAL = {
    (S.QUEUED, S.ACCEPTED),
    (S.QUEUED, S.DELIVERED),
    (S.QUEUED, S.EXPIRED),
    (S.ACCEPTED, S.DELIVERED),
    (S.ACCEPTED, S.EXPIRED),
    (S.EXPIRED, S.DELIVERED),
}


@pytest.mark.parametrize("a,b", list(itertools.product(S, S)))
def test_transition_table_is_exhaustive(a: S, b: S) -> None:
    """Every (state, state) pair matches the hand-written spec."""
    assert can_transition(a, b) == ((a, b) in LEGAL)


def test_delivered_is_terminal() -> None:
    """DELIVERED cannot transition to any state."""
    assert not any(can_transition(S.DELIVERED, s) for s in S)


def test_advance_moves_on_legal_transitions() -> None:
    """advance() applies legal transitions."""
    assert advance(S.QUEUED, S.ACCEPTED) is S.ACCEPTED
    assert advance(S.ACCEPTED, S.DELIVERED) is S.DELIVERED


def test_advance_is_idempotent() -> None:
    """Applying the same event twice does not change the state."""
    for s in S:
        assert advance(s, s) == s


def test_advance_ignores_late_or_illegal_events() -> None:
    """Out-of-order or illegal events are ignored, not raised."""
    assert advance(S.DELIVERED, S.ACCEPTED) is S.DELIVERED  # late accepted
    assert advance(S.DELIVERED, S.EXPIRED) is S.DELIVERED
    assert advance(S.ACCEPTED, S.QUEUED) is S.ACCEPTED


def test_real_confirmation_wins_over_expired() -> None:
    """EXPIRED -> DELIVERED is allowed: reality wins over local pessimism."""
    assert advance(S.EXPIRED, S.DELIVERED) is S.DELIVERED


def test_any_event_order_ends_delivered_if_delivery_happened() -> None:
    """If a DELIVERED event appeared at some point, the final state is
    DELIVERED, regardless of the order of the other events."""
    for order in itertools.permutations([S.ACCEPTED, S.DELIVERED, S.EXPIRED]):
        state = S.QUEUED
        for event in order:
            state = advance(state, event)
        assert state is S.DELIVERED, order
