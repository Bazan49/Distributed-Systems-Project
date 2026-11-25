"""Tests for the MembershipService (task 1B.2), with a manual clock."""

import logging

import pytest

from source.adapters.memory.clock import ManualClock
from source.application.observability import FIELDS_ATTR
from source.application.use_cases.membership import MembershipService
from source.domain.membership import NodeState
from source.domain.node_id import NodeId
from tests.support.logs import events, has_event

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


# ================= logs: node.state_changed (docs/logging.md) =================
def changes(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str]]:
    return [
        (NodeState(f["from_state"]).value, NodeState(f["to_state"]).value)
        for name, f in events(caplog)
        if name == "node.state_changed"
    ]


def test_every_change_of_state_is_logged_once(
    membership: MembershipService, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    membership.report_success(NODE)
    membership.report_success(NODE)  # still alive: nothing new
    clock.advance(150)
    membership.get_state(NODE)
    clock.advance(200)
    membership.get_active_nodes([NODE])
    membership.get_state(NODE)  # still dead: nothing new
    membership.report_success(NODE)  # it comes back

    assert changes(caplog) == [
        ("suspect", "alive"),  # never contacted -> answered
        ("alive", "suspect"),
        ("suspect", "dead"),
        ("dead", "alive"),
    ]
    assert has_event(
        caplog,
        "node.state_changed",
        node=NODE,
        from_state=NodeState.DEAD,
        to_state=NodeState.ALIVE,
    )


def test_becoming_dead_is_a_warning_the_rest_is_info(
    membership: MembershipService, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    membership.report_success(NODE)
    membership.report_failure(NODE)
    clock.advance(300)
    membership.get_state(NODE)
    levels = {
        getattr(r, FIELDS_ATTR)["to_state"]: r.levelno
        for r in caplog.records
        if r.getMessage() == "node.state_changed"
    }
    assert levels == {
        NodeState.ALIVE: logging.INFO,
        NodeState.SUSPECT: logging.INFO,
        NodeState.DEAD: logging.WARNING,
    }


def test_querying_a_node_never_contacted_logs_nothing(
    membership: MembershipService, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    membership.get_state(NODE)
    membership.get_active_nodes([NodeId("node-2")])
    assert changes(caplog) == []


def test_a_pruned_node_is_forgotten_without_a_false_change(
    membership: MembershipService, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    membership.report_failure(NODE)
    clock.advance(300)
    membership.get_state(NODE)  # dead
    caplog.set_level(logging.DEBUG, logger="p08")
    caplog.clear()  # only what happens from here on
    assert membership.prune_dead() == 1
    membership.get_state(NODE)  # never contacted again: suspect, not a change
    assert changes(caplog) == []
