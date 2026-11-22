"""Tests for the MembershipService."""

import pytest

from source.adapters.memory.clock import ManualClock
from source.application.use_cases.membership import MembershipService
from source.domain.membership import NodeState
from source.domain.node_id import NodeId


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def membership(clock: ManualClock) -> MembershipService:
    return MembershipService(clock, suspect_timeout_ms=100, dead_timeout_ms=300)


def test_unknown_node_is_dead(membership: MembershipService) -> None:
    assert membership.get_state(NodeId("node-1")) == NodeState.DEAD


def test_node_goes_through_all_states(
    membership: MembershipService, clock: ManualClock
) -> None:
    node = NodeId("node-1")

    # 1. El nodo responde -> ALIVE
    membership.report_success(node)
    assert membership.get_state(node) == NodeState.ALIVE

    # 2. Avanzamos 150ms -> SUSPECT
    clock.advance(150)
    assert membership.get_state(node) == NodeState.SUSPECT

    # 3. Avanzamos otros 200ms (Total 350ms) -> DEAD
    clock.advance(200)
    assert membership.get_state(node) == NodeState.DEAD


def test_node_revives(membership: MembershipService, clock: ManualClock) -> None:
    node = NodeId("node-1")

    clock.advance(400)
    assert membership.get_state(node) == NodeState.DEAD

    membership.report_success(node)
    assert membership.get_state(node) == NodeState.ALIVE


def test_get_active_nodes_filters_dead(
    membership: MembershipService, clock: ManualClock
) -> None:
    node1 = NodeId("node-1")
    node2 = NodeId("node-2")

    membership.report_success(node1)
    membership.report_success(node2)

    clock.advance(200)  # node1 y node2 pasan a SUSPECT

    nodes = [node1, node2, NodeId("node-3")]

    assert set(membership.get_active_nodes(nodes)) == {node1, node2}


def test_prune_dead_removes_nodes(
    membership: MembershipService, clock: ManualClock
) -> None:
    node = NodeId("node-1")
    membership.report_success(node)

    clock.advance(400)  # Ahora esta muerto

    assert membership.prune_dead() == 1
    assert membership.get_state(node) == NodeState.DEAD
