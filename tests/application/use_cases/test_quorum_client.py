"""Tests for the QuorumClient (task 1B.7), on the in-memory gateway.

Acceptance criterion: with a fake gateway, the loss of 1 node does not stop a
message from being accepted; with 2 nodes down the message is not accepted and
stays in the send queue.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace

import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.application.use_cases.membership import MembershipService
from source.application.use_cases.quorum import QuorumClient, QuorumPolicy
from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.membership import NodeState
from source.domain.node_id import NodeId
from source.domain.outgoing_status import OutgoingStatus
from source.domain.placement import rank_nodes
from source.domain.records import DirectoryRecord, NodeReply
from tests.support.builders import (
    ALICE,
    BOB,
    ack_request,
    collect_request,
    make_outgoing,
    signed_record,
)

THREE = [NodeId("node-1"), NodeId("node-2"), NodeId("node-3")]
FIVE = [NodeId(f"node-{i}") for i in range(1, 6)]
DEAD_AFTER_MS = 300


class Cluster:
    """A client (QuorumClient) talking to an in-memory cluster."""

    def __init__(self, nodes: list[NodeId]) -> None:
        self.clock = ManualClock()
        self.gateway = InMemoryNodeGateway(nodes, self.clock, FakeCrypto())
        self.membership = MembershipService(
            self.clock, suspect_timeout_ms=100, dead_timeout_ms=DEAD_AFTER_MS
        )
        self.client = QuorumClient(
            self.gateway, self.membership, FakeCrypto(), self.clock
        )
        self.nodes = nodes

    def ranked_for(self, user: UserId) -> list[NodeId]:
        return rank_nodes(user, self.nodes)

    def declare_dead(self, node: NodeId) -> None:
        """Make the client's membership see ``node`` as DEAD."""
        self.gateway.set_down(node)
        self.membership.report_failure(node)
        self.clock.advance(DEAD_AFTER_MS)
        assert self.membership.get_state(node) is NodeState.DEAD

    def bob_collects(self) -> list[Envelope]:
        return list(
            self.client.collect(collect_request(BOB, self.clock.now_ms())).envelopes
        )


@pytest.fixture
def cluster() -> Cluster:
    return Cluster(THREE)


# ================= acceptance criterion =================
def test_one_node_down_does_not_stop_the_message_from_being_accepted(
    cluster: Cluster,
) -> None:
    store = MemoryLocalStore()
    message = make_outgoing(sender=ALICE, receiver=BOB)
    store.enqueue_outgoing(message)
    cluster.gateway.set_down(cluster.ranked_for(BOB.user_id)[0])

    sent = cluster.client.deposit_outgoing(message)
    store.update_outgoing(sent)

    assert sent.status is OutgoingStatus.ACCEPTED
    assert store.list_due_outgoing(cluster.clock.now_ms()) == []  # left the queue


def test_with_two_nodes_down_the_message_is_not_accepted_and_stays_queued(
    cluster: Cluster,
) -> None:
    store = MemoryLocalStore()
    message = make_outgoing(sender=ALICE, receiver=BOB)
    store.enqueue_outgoing(message)
    for node in cluster.ranked_for(BOB.user_id)[:2]:
        cluster.gateway.set_down(node)

    sent = cluster.client.deposit_outgoing(message)
    store.update_outgoing(sent)

    assert sent.status is OutgoingStatus.QUEUED
    assert sent.attempts == 1
    [due] = store.list_due_outgoing(cluster.clock.now_ms())
    assert due.message_id == message.message_id  # still in the send queue


def test_the_queued_message_is_accepted_on_retry_once_nodes_are_back(
    cluster: Cluster,
) -> None:
    message = make_outgoing(sender=ALICE, receiver=BOB)
    for node in THREE[:2]:
        cluster.gateway.set_down(node)
    first = cluster.client.deposit_outgoing(message)
    for node in THREE[:2]:
        cluster.gateway.set_up(node)

    retried = cluster.client.deposit_outgoing(first)

    assert retried.status is OutgoingStatus.ACCEPTED
    assert retried.attempts == 2
    assert retried.envelope == message.envelope  # the very same bytes
    assert cluster.bob_collects() == [message.envelope]  # no duplicate


# ================= deposit =================
def test_deposit_with_every_node_up_is_accepted(cluster: Cluster) -> None:
    result = cluster.client.deposit(make_outgoing().envelope)
    assert result.accepted
    assert len(result.confirmed_by) >= 2
    assert set(result.asked) == set(THREE)


def test_a_node_that_does_not_answer_becomes_suspect(cluster: Cluster) -> None:
    down = cluster.ranked_for(BOB.user_id)[0]
    cluster.gateway.set_down(down)
    result = cluster.client.deposit(make_outgoing().envelope)
    assert down not in result.confirmed_by
    assert cluster.membership.get_state(down) is NodeState.SUSPECT
    for node in result.confirmed_by:
        assert cluster.membership.get_state(node) is NodeState.ALIVE


def test_only_queued_messages_are_sent(cluster: Cluster) -> None:
    for node in THREE:
        cluster.gateway.set_down(node)  # any call would fail
    for status in (
        OutgoingStatus.ACCEPTED,
        OutgoingStatus.DELIVERED,
        OutgoingStatus.EXPIRED,
    ):
        message = make_outgoing(status=status)
        assert cluster.client.deposit_outgoing(message) is message
    assert all(cluster.membership.get_state(n) is NodeState.SUSPECT for n in THREE)


def test_a_dead_node_is_skipped_and_the_next_node_takes_its_place() -> None:
    cluster = Cluster(FIVE)
    ranked = cluster.ranked_for(BOB.user_id)
    cluster.declare_dead(ranked[0])

    result = cluster.client.deposit(make_outgoing().envelope)

    assert result.accepted
    assert result.asked == tuple(ranked[1:4])


def test_with_every_node_dead_nothing_is_called(cluster: Cluster) -> None:
    for node in THREE:
        cluster.declare_dead(node)
    message = make_outgoing()

    result = cluster.client.deposit(message.envelope)

    assert not result.accepted
    assert result.asked == () and result.confirmed_by == ()
    assert cluster.client.deposit_outgoing(message).status is OutgoingStatus.QUEUED
    assert (
        cluster.client.collect(collect_request(BOB, cluster.clock.now_ms())).asked == ()
    )
    assert (
        cluster.client.acknowledge(
            ack_request(BOB, cluster.clock.now_ms(), [message.message_id])
        )
        == ()
    )
    assert (
        cluster.client.publish_record(
            signed_record(ALICE, cluster.clock.now_ms())
        ).asked
        == ()
    )
    assert cluster.client.resolve(ALICE.user_id) is None


# ================= collect / acknowledge =================
def test_collect_joins_what_each_node_has_without_duplicates(
    cluster: Cluster,
) -> None:
    first, second = make_outgoing(seq=1), make_outgoing(seq=2)
    cluster.client.deposit(first.envelope)
    cluster.gateway.set_down(THREE[0])  # the second goes to other nodes
    cluster.client.deposit(second.envelope)
    cluster.gateway.set_up(THREE[0])

    envelopes = cluster.bob_collects()

    assert sorted(e.message_id.value for e in envelopes) == sorted(
        [first.message_id.value, second.message_id.value]
    )


def test_an_accepted_message_survives_the_loss_of_any_one_node(
    cluster: Cluster,
) -> None:
    message = make_outgoing()
    result = cluster.client.deposit(message.envelope)
    for lost in result.confirmed_by:
        cluster.gateway.set_down(lost)
        assert cluster.bob_collects() == [message.envelope]
        cluster.gateway.set_up(lost)


def test_collect_reaches_a_copy_on_the_replacement_node() -> None:
    # Two clients with different views. The sender saw every node alive and
    # wrote to Bob's normal nodes. Bob's client believes the first one is dead,
    # so it asks its replacement too, and finds the message on another copy.
    cluster = Cluster(FIVE)
    ranked = cluster.ranked_for(BOB.user_id)
    message = make_outgoing()
    assert cluster.client.deposit(message.envelope).accepted

    bob_view = MembershipService(
        cluster.clock, suspect_timeout_ms=100, dead_timeout_ms=DEAD_AFTER_MS
    )
    bob_view.report_failure(ranked[0])
    cluster.clock.advance(DEAD_AFTER_MS)
    bob = QuorumClient(cluster.gateway, bob_view, FakeCrypto(), cluster.clock)

    result = bob.collect(collect_request(BOB, cluster.clock.now_ms()))

    assert result.asked == tuple(ranked[1:4])
    assert list(result.envelopes) == [message.envelope]


def test_acknowledge_deletes_the_messages_from_every_live_node(
    cluster: Cluster,
) -> None:
    message = make_outgoing()
    cluster.client.deposit(message.envelope)

    confirmed = cluster.client.acknowledge(
        ack_request(BOB, cluster.clock.now_ms(), [message.message_id])
    )

    assert set(confirmed) == set(THREE)
    assert cluster.bob_collects() == []


def test_a_node_that_refuses_a_request_stays_alive(cluster: Cluster) -> None:
    request = collect_request(BOB, cluster.clock.now_ms())
    cluster.client.collect(request)
    cluster.clock.advance(50)

    replayed = cluster.client.collect(request)  # same nonce: every node refuses

    assert replayed.answered_by == ()
    assert all(cluster.membership.get_state(n) is NodeState.ALIVE for n in THREE)


def test_collect_and_acknowledge_check_the_kind_of_request(
    cluster: Cluster,
) -> None:
    now = cluster.clock.now_ms()
    ack = ack_request(BOB, now, [make_outgoing().message_id])
    with pytest.raises(ValueError):
        cluster.client.collect(ack)
    with pytest.raises(ValueError):
        cluster.client.acknowledge(collect_request(BOB, now))


# ================= directory =================
def test_publish_is_accepted_with_one_node_down_but_not_with_two(
    cluster: Cluster,
) -> None:
    cluster.gateway.set_down(THREE[0])
    assert cluster.client.publish_record(
        signed_record(ALICE, cluster.clock.now_ms(), version=1)
    ).accepted
    cluster.gateway.set_down(THREE[1])
    assert not cluster.client.publish_record(
        signed_record(ALICE, cluster.clock.now_ms(), version=2)
    ).accepted


def test_resolve_returns_the_highest_version_even_if_a_node_is_behind(
    cluster: Cluster,
) -> None:
    now = cluster.clock.now_ms()
    behind = cluster.ranked_for(ALICE.user_id)[0]
    for node in THREE:  # v1 everywhere (a write may stop after 2 nodes)
        cluster.gateway.publish_record([node], signed_record(ALICE, now), 1, 1_000)
    cluster.gateway.set_down(behind)
    v2 = signed_record(ALICE, now, version=2)
    assert cluster.client.publish_record(v2).accepted
    cluster.gateway.set_up(behind)  # still holds v1

    assert cluster.client.resolve(ALICE.user_id) == v2


def test_resolve_of_an_unknown_user_is_none(cluster: Cluster) -> None:
    assert cluster.client.resolve(BOB.user_id) is None


def test_resolve_with_every_node_down_is_none(cluster: Cluster) -> None:
    cluster.client.publish_record(signed_record(ALICE, cluster.clock.now_ms()))
    for node in THREE:
        cluster.gateway.set_down(node)
    assert cluster.client.resolve(ALICE.user_id) is None


class _LyingGateway(InMemoryNodeGateway):
    """Every node answers ``resolve_record`` with the same given record."""

    def __init__(
        self, nodes: list[NodeId], clock: ManualClock, lie: DirectoryRecord
    ) -> None:
        super().__init__(nodes, clock, FakeCrypto())
        self._lie = lie

    def resolve_record(
        self, nodes: Sequence[NodeId], user_id: UserId, timeout_ms: int
    ) -> list[NodeReply[DirectoryRecord | None]]:
        return [NodeReply(node=n, ok=True, value=self._lie) for n in nodes]


Lie = Callable[[DirectoryRecord], DirectoryRecord]


def _resolve_with_lie(make_lie: Lie | None) -> DirectoryRecord | None:
    clock = ManualClock()
    now = clock.now_ms()
    real = signed_record(ALICE, now, version=1)
    lie = make_lie(real) if make_lie is not None else real
    gateway = _LyingGateway(THREE, clock, lie)
    client = QuorumClient(gateway, MembershipService(clock), FakeCrypto(), clock)
    return client.resolve(ALICE.user_id)


def test_the_client_accepts_a_genuine_record_from_the_nodes() -> None:
    assert _resolve_with_lie(None) is not None


@pytest.mark.parametrize(
    "make_lie",
    [
        pytest.param(lambda r: replace(r, version=99), id="altered version"),
        pytest.param(lambda r: replace(r, signature=b"forged"), id="bad signature"),
        pytest.param(
            lambda r: signed_record(BOB, r.expires_at_ms - 60_000, version=9),
            id="another user's record",
        ),
        pytest.param(
            lambda r: replace(r, expires_at_ms=r.expires_at_ms - 60_000),
            id="expired (and so altered)",
        ),
    ],
)
def test_the_client_ignores_records_a_node_cannot_prove(make_lie: Lie) -> None:
    assert _resolve_with_lie(make_lie) is None


def test_the_client_ignores_a_genuine_but_expired_record() -> None:
    clock = ManualClock()
    expired = signed_record(ALICE, clock.now_ms() - 120_000)  # lease ended 60 s ago
    gateway = _LyingGateway(THREE, clock, expired)
    client = QuorumClient(gateway, MembershipService(clock), FakeCrypto(), clock)
    assert client.resolve(ALICE.user_id) is None


# ================= membership upkeep =================
def test_refresh_membership_revives_a_dead_node_that_came_back(
    cluster: Cluster,
) -> None:
    node = THREE[0]
    cluster.declare_dead(node)
    assert node not in cluster.membership.get_active_nodes(THREE)
    cluster.gateway.set_up(node)

    alive = cluster.client.refresh_membership()

    assert set(alive) == set(THREE)
    assert cluster.membership.get_state(node) is NodeState.ALIVE


@pytest.mark.parametrize(
    "kwargs",
    [
        {"replicas": 0},
        {"write_quorum": 0},
        {"timeout_ms": 0},
        {"replicas": 3, "write_quorum": 4},
        {"timeout_ms": 1.5},
    ],
)
def test_invalid_policies_are_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        QuorumPolicy(**kwargs)
