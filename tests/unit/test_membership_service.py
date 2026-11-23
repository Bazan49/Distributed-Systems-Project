"""Tests for the MembershipService (task 1B.2), with a manual clock."""

import pytest

from source.adapters.memory.clock import ManualClock
from source.application.use_cases.membership import MembershipService
from source.domain.membership import NodeState
from source.domain.node_id import NodeId

NODE = NodeId("node-1")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def membership(clock: ManualClock) -> MembershipService:
    return MembershipService(clock, suspect_timeout_ms=100, dead_timeout_ms=300)


def test_a_node_never_contacted_is_suspect_and_still_tried(
    membership: MembershipService,
) -> None:
    nodes = [NodeId("node-1"), NodeId("node-2"), NodeId("node-3")]
    assert membership.get_state(NODE) == NodeState.SUSPECT
    assert membership.get_active_nodes(nodes) == nodes


def test_node_goes_through_all_states(
    membership: MembershipService, clock: ManualClock
) -> None:
    membership.report_success(NODE)
    assert membership.get_state(NODE) == NodeState.ALIVE
    clock.advance(150)
    assert membership.get_state(NODE) == NodeState.SUSPECT
    clock.advance(200)  # 350 ms without an answer
    assert membership.get_state(NODE) == NodeState.DEAD


def test_a_failure_makes_the_node_suspect_without_waiting(
    membership: MembershipService,
) -> None:
    membership.report_success(NODE)
    membership.report_failure(NODE)
    assert membership.get_state(NODE) == NodeState.SUSPECT


def test_a_node_that_keeps_failing_dies_and_revives_when_it_answers(
    membership: MembershipService, clock: ManualClock
) -> None:
    for _ in range(4):
        membership.report_failure(NODE)
        clock.advance(100)
    assert membership.get_state(NODE) == NodeState.DEAD
    assert membership.get_active_nodes([NODE]) == []
    membership.report_success(NODE)
    assert membership.get_state(NODE) == NodeState.ALIVE


def test_uses_the_monotonic_clock_not_the_wall_clock(
    membership: MembershipService, clock: ManualClock
) -> None:
    membership.report_success(NODE)
    clock.set_wall_clock(clock.now_ms() + 10**9)  # NTP jump: must not matter
    assert membership.get_state(NODE) == NodeState.ALIVE


def test_prune_dead_removes_nodes(
    membership: MembershipService, clock: ManualClock
) -> None:
    membership.report_success(NODE)
    clock.advance(400)
    assert membership.prune_dead() == 1
    assert membership.prune_dead() == 0
