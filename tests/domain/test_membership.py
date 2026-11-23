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


# ---------------- never contacted ----------------
def test_a_node_never_contacted_is_suspect_not_dead(view: MembershipView) -> None:
    assert view.state_of(N1, now_ms=0) == NodeState.SUSPECT
    assert (
        view.state_of(N1, now_ms=10**9) == NodeState.SUSPECT
    )  # time alone changes nothing


def test_a_new_client_still_tries_every_node(view: MembershipView) -> None:
    assert view.active_nodes([N1, N2, N3], now_ms=0) == [N1, N2, N3]


# ---------------- successes and time ----------------
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


# ---------------- failures ----------------
def test_a_failure_makes_an_alive_node_suspect_at_once(view: MembershipView) -> None:
    view.record_success(N1, now_ms=0)
    view.record_failure(N1, now_ms=10)
    assert view.state_of(N1, now_ms=10) == NodeState.SUSPECT


def test_dead_is_counted_from_the_last_success_not_from_the_failures(
    view: MembershipView,
) -> None:
    view.record_success(N1, now_ms=0)
    view.record_failure(N1, now_ms=50)
    view.record_failure(N1, now_ms=250)  # repeated failures do not move the clock
    assert view.state_of(N1, now_ms=299) == NodeState.SUSPECT
    assert view.state_of(N1, now_ms=300) == NodeState.DEAD


def test_a_node_that_never_answered_dies_after_failing_for_the_dead_timeout(
    view: MembershipView,
) -> None:
    view.record_failure(N1, now_ms=1_000)
    view.record_failure(N1, now_ms=1_200)
    assert view.state_of(N1, now_ms=1_299) == NodeState.SUSPECT
    assert view.state_of(N1, now_ms=1_300) == NodeState.DEAD


def test_a_success_clears_previous_failures(view: MembershipView) -> None:
    view.record_success(N1, now_ms=0)
    view.record_failure(N1, now_ms=10)
    view.record_success(N1, now_ms=20)
    assert view.state_of(N1, now_ms=20) == NodeState.ALIVE
    view.record_failure(N1, now_ms=30)  # a new failure counts from here on
    assert view.state_of(N1, now_ms=30) == NodeState.SUSPECT
    assert view.state_of(N1, now_ms=319) == NodeState.SUSPECT
    assert view.state_of(N1, now_ms=320) == NodeState.DEAD


# ---------------- active nodes and pruning ----------------
def test_active_nodes_keeps_everything_but_dead_in_order(view: MembershipView) -> None:
    view.record_success(N1, now_ms=0)  # SUSPECT at 200
    view.record_success(N2, now_ms=150)  # ALIVE at 200
    view.record_failure(N3, now_ms=-200)  # failing for 400 ms: DEAD at 200
    assert view.active_nodes([N3, N2, N1], now_ms=200) == [N2, N1]


def test_prune_dead_forgets_only_dead_nodes(view: MembershipView) -> None:
    view.record_success(N1, now_ms=0)
    view.record_success(N2, now_ms=350)
    view.record_failure(N3, now_ms=0)
    assert view.prune_dead(now_ms=400) == 2
    assert view.state_of(N2, now_ms=400) == NodeState.ALIVE
    assert view.prune_dead(now_ms=400) == 0


def test_a_pruned_node_counts_as_never_contacted(view: MembershipView) -> None:
    view.record_success(N1, now_ms=0)
    view.prune_dead(now_ms=400)
    assert view.state_of(N1, now_ms=400) == NodeState.SUSPECT
