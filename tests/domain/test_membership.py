"""Tests for the pure membership rules (no clock involved)."""

import pytest

from source.domain.membership import MembershipView, NodeState
from source.domain.node_id import NodeId

N1, N2, N3 = NodeId("node-1"), NodeId("node-2"), NodeId("node-3")


@pytest.fixture
def view() -> MembershipView:
    return MembershipView(suspect_timeout_ms=100, dead_timeout_ms=300)


@pytest.mark.parametrize(
    ("suspect", "dead"), [(0, 300), (100, 0), (-1, 300), (300, 300), (400, 300)]
)
def test_invalid_timeouts_are_rejected(suspect: int, dead: int) -> None:
    with pytest.raises(ValueError):
        MembershipView(suspect_timeout_ms=suspect, dead_timeout_ms=dead)


def test_unknown_node_is_dead(view: MembershipView) -> None:
    assert view.state_of(N1, now_ms=0) == NodeState.DEAD


@pytest.mark.parametrize(
    ("elapsed", "state"),
    [
        (0, NodeState.ALIVE),
        (99, NodeState.ALIVE),
        (100, NodeState.SUSPECT),
        (299, NodeState.SUSPECT),
        (300, NodeState.DEAD),
    ],
)
def test_state_boundaries(view: MembershipView, elapsed: int, state: NodeState) -> None:
    view.record_success(N1, now_ms=1_000)
    assert view.state_of(N1, now_ms=1_000 + elapsed) == state


def test_dead_node_revives_on_success(view: MembershipView) -> None:
    view.record_success(N1, now_ms=0)
    assert view.state_of(N1, now_ms=500) == NodeState.DEAD
    view.record_success(N1, now_ms=500)
    assert view.state_of(N1, now_ms=500) == NodeState.ALIVE


def test_active_nodes_keeps_alive_and_suspect_in_order(view: MembershipView) -> None:
    view.record_success(N1, now_ms=0)  # SUSPECT at 200
    view.record_success(N2, now_ms=150)  # ALIVE at 200
    assert view.active_nodes([N3, N2, N1], now_ms=200) == [N2, N1]


def test_prune_dead_forgets_only_dead_nodes(view: MembershipView) -> None:
    view.record_success(N1, now_ms=0)
    view.record_success(N2, now_ms=350)
    assert view.prune_dead(now_ms=400) == 1
    assert view.state_of(N2, now_ms=400) == NodeState.ALIVE
    assert view.prune_dead(now_ms=400) == 0
