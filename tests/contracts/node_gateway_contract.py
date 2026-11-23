"""Contract every NodeGatewayPort implementation must satisfy.

The port has no way to kill a node, so each adapter's test provides a
``GatewayHarness`` that does (in memory: a flag; with Ray: ``ray.kill``).
The harness also exposes the cluster's clock, so requests and records are
signed "now" from the nodes' point of view. Nodes use ``FakeCrypto`` in tests.
"""

from dataclasses import replace
from typing import Protocol

from source.application.ports import ClockPort, NodeGatewayPort
from source.domain.identifiers import MessageId, UserId
from source.domain.mailbox_access import MailboxRequest
from source.domain.node_id import NodeId
from source.domain.records import DirectoryRecord, PutOutcome
from tests.contracts.mailbox_store_contract import make_envelope
from tests.support.builders import (
    ALICE,
    BOB,
    CARLA,
    ack_request,
    collect_request,
    signed_record,
)

TIMEOUT_MS = 1_000


class GatewayHarness(Protocol):
    gateway: NodeGatewayPort
    nodes: list[NodeId]  # exactly 3 nodes

    clock: ClockPort

    def set_down(self, node: NodeId) -> None: ...
    def set_up(self, node: NodeId) -> None: ...


def bob_collects(harness: GatewayHarness) -> MailboxRequest:
    return collect_request(BOB, harness.clock.now_ms())


def bob_acks(harness: GatewayHarness, ids: list[MessageId]) -> MailboxRequest:
    return ack_request(BOB, harness.clock.now_ms(), ids)


def make_record(harness: GatewayHarness, version: int = 1) -> DirectoryRecord:
    return signed_record(ALICE, harness.clock.now_ms(), version=version)


class NodeGatewayContract:
    """Subclass it and override the ``harness`` fixture."""

    def test_list_nodes_returns_all_nodes(self, harness: GatewayHarness) -> None:
        assert sorted(harness.gateway.list_nodes()) == sorted(harness.nodes)

    # ---- ping ----
    def test_ping_all_alive(self, harness: GatewayHarness) -> None:
        replies = harness.gateway.ping(harness.nodes, TIMEOUT_MS)
        assert {r.node for r in replies} == set(harness.nodes)
        assert all(r.ok for r in replies)

    def test_ping_reports_a_dead_node_without_raising(
        self, harness: GatewayHarness
    ) -> None:
        dead = harness.nodes[0]
        harness.set_down(dead)
        by_node = {r.node: r for r in harness.gateway.ping(harness.nodes, TIMEOUT_MS)}
        assert not by_node[dead].ok and by_node[dead].error
        assert all(by_node[n].ok for n in harness.nodes if n != dead)

    # ---- deposit ----
    def test_deposit_reaches_the_required_nodes(self, harness: GatewayHarness) -> None:
        replies = harness.gateway.deposit(harness.nodes, make_envelope(), 2, TIMEOUT_MS)
        assert sum(r.ok for r in replies) >= 2
        assert all(r.value is PutOutcome.STORED for r in replies if r.ok)

    def test_at_most_one_reply_per_node_asked(self, harness: GatewayHarness) -> None:
        asked = [harness.nodes[0], harness.nodes[0], harness.nodes[1]]
        replies = harness.gateway.ping(asked, TIMEOUT_MS)
        nodes = [r.node for r in replies]
        assert len(nodes) == len(set(nodes)) and set(nodes) <= set(asked)

    def test_deposit_is_idempotent(self, harness: GatewayHarness) -> None:
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT_MS)
        second = harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT_MS)
        assert all(r.value is PutOutcome.ALREADY_PRESENT for r in second if r.ok)

    def test_deposit_survives_one_dead_node(self, harness: GatewayHarness) -> None:
        harness.set_down(harness.nodes[0])
        replies = harness.gateway.deposit(harness.nodes, make_envelope(), 2, TIMEOUT_MS)
        assert sum(r.ok for r in replies) >= 2

    def test_deposit_with_two_dead_nodes_misses_the_quorum(
        self, harness: GatewayHarness
    ) -> None:
        harness.set_down(harness.nodes[0])
        harness.set_down(harness.nodes[1])
        replies = harness.gateway.deposit(harness.nodes, make_envelope(), 2, TIMEOUT_MS)
        assert sum(r.ok for r in replies) < 2  # and nothing was raised
        assert all(r.error for r in replies if not r.ok)

    def test_unknown_node_is_a_failed_reply_not_an_exception(
        self, harness: GatewayHarness
    ) -> None:
        ghost = NodeId("ghost-node")
        [reply] = harness.gateway.deposit([ghost], make_envelope(), 1, TIMEOUT_MS)
        assert not reply.ok and reply.error

    # ---- collect / acknowledge ----
    def test_deposited_message_is_collectable_from_a_quorum(
        self, harness: GatewayHarness
    ) -> None:
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 2, TIMEOUT_MS)
        replies = harness.gateway.collect(
            harness.nodes, bob_collects(harness), TIMEOUT_MS
        )
        holders = [r for r in replies if r.ok and r.value == [envelope]]
        assert len(holders) >= 2

    def test_collect_reports_a_dead_node_and_still_answers_from_others(
        self, harness: GatewayHarness
    ) -> None:
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT_MS)
        harness.set_down(harness.nodes[2])
        by_node = {
            r.node: r
            for r in harness.gateway.collect(
                harness.nodes, bob_collects(harness), TIMEOUT_MS
            )
        }
        assert not by_node[harness.nodes[2]].ok
        assert by_node[harness.nodes[0]].value == [envelope]

    def test_acknowledge_removes_the_message_from_every_node(
        self, harness: GatewayHarness
    ) -> None:
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT_MS)
        acks = harness.gateway.acknowledge(
            harness.nodes, bob_acks(harness, [envelope.message_id]), TIMEOUT_MS
        )
        assert all(r.ok and r.value == 1 for r in acks)
        after = harness.gateway.collect(
            harness.nodes, bob_collects(harness), TIMEOUT_MS
        )
        assert all(r.ok and r.value == [] for r in after)

    def test_acknowledge_of_unknown_id_deletes_nothing(
        self, harness: GatewayHarness
    ) -> None:
        acks = harness.gateway.acknowledge(
            harness.nodes, bob_acks(harness, [MessageId.new()]), TIMEOUT_MS
        )
        assert all(r.ok and r.value == 0 for r in acks)

    def test_someone_else_cannot_collect_or_acknowledge(
        self, harness: GatewayHarness
    ) -> None:
        """Every node refuses, and nothing is deleted."""
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT_MS)
        now = harness.clock.now_ms()
        spy = replace(collect_request(CARLA, now), receiver=BOB.user_id)
        thief = replace(
            ack_request(CARLA, now, [envelope.message_id]), receiver=BOB.user_id
        )
        refused = harness.gateway.collect(harness.nodes, spy, TIMEOUT_MS)
        refused += harness.gateway.acknowledge(harness.nodes, thief, TIMEOUT_MS)
        assert all(not r.ok and r.error for r in refused)
        after = harness.gateway.collect(
            harness.nodes, bob_collects(harness), TIMEOUT_MS
        )
        assert all(r.ok and r.value == [envelope] for r in after)

    def test_a_replayed_request_is_refused_by_every_node(
        self, harness: GatewayHarness
    ) -> None:
        request = bob_collects(harness)
        first = harness.gateway.collect(harness.nodes, request, TIMEOUT_MS)
        again = harness.gateway.collect(harness.nodes, request, TIMEOUT_MS)
        assert all(r.ok for r in first)
        assert all(not r.ok and r.error for r in again)

    # ---- directory records ----
    def test_published_record_is_resolvable_from_a_quorum(
        self, harness: GatewayHarness
    ) -> None:
        record = make_record(harness)
        harness.gateway.publish_record(harness.nodes, record, 2, TIMEOUT_MS)
        replies = harness.gateway.resolve_record(
            harness.nodes, record.user_id, TIMEOUT_MS
        )
        assert len([r for r in replies if r.ok and r.value == record]) >= 2

    def test_publish_survives_one_dead_node(self, harness: GatewayHarness) -> None:
        harness.set_down(harness.nodes[1])
        replies = harness.gateway.publish_record(
            harness.nodes, make_record(harness), 2, TIMEOUT_MS
        )
        assert sum(r.ok for r in replies) >= 2

    def test_a_forged_record_is_refused_by_every_node(
        self, harness: GatewayHarness
    ) -> None:
        forged = replace(make_record(harness), version=2)  # signature no longer matches
        replies = harness.gateway.publish_record(harness.nodes, forged, 3, TIMEOUT_MS)
        assert replies and all(not r.ok and r.error for r in replies)
        resolved = harness.gateway.resolve_record(
            harness.nodes, forged.user_id, TIMEOUT_MS
        )
        assert all(r.ok and r.value is None for r in resolved)

    def test_resolving_an_unknown_user_is_ok_with_no_record(
        self, harness: GatewayHarness
    ) -> None:
        stranger = UserId.from_public_key(b"stranger")
        replies = harness.gateway.resolve_record(harness.nodes, stranger, TIMEOUT_MS)
        assert all(r.ok and r.value is None for r in replies)
