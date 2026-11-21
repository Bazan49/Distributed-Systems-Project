"""Contract every NodeGatewayPort implementation must satisfy.

The port has no way to kill a node, so each adapter's test provides a
``GatewayHarness`` that does (in memory: a flag; with Ray: ``ray.kill``).
"""

from typing import Protocol

from source.application.ports import NodeGatewayPort
from source.domain.identifiers import MessageId, UserId
from source.domain.node_id import NodeId
from source.domain.records import PutOutcome
from tests.contracts.directory_store_contract import make_record
from tests.contracts.mailbox_store_contract import BOB, make_envelope

TIMEOUT = 1.0


class GatewayHarness(Protocol):
    gateway: NodeGatewayPort
    nodes: list[NodeId]  # exactly 3 nodes

    def set_down(self, node: NodeId) -> None: ...
    def set_up(self, node: NodeId) -> None: ...


class NodeGatewayContract:
    """Subclass it and override the ``harness`` fixture."""

    def test_list_nodes_returns_all_nodes(self, harness: GatewayHarness) -> None:
        assert sorted(harness.gateway.list_nodes()) == sorted(harness.nodes)

    # ---- ping ----
    def test_ping_all_alive(self, harness: GatewayHarness) -> None:
        replies = harness.gateway.ping(harness.nodes, TIMEOUT)
        assert {r.node for r in replies} == set(harness.nodes)
        assert all(r.ok for r in replies)

    def test_ping_reports_a_dead_node_without_raising(
        self, harness: GatewayHarness
    ) -> None:
        dead = harness.nodes[0]
        harness.set_down(dead)
        by_node = {r.node: r for r in harness.gateway.ping(harness.nodes, TIMEOUT)}
        assert not by_node[dead].ok and by_node[dead].error
        assert all(by_node[n].ok for n in harness.nodes if n != dead)

    # ---- deposit ----
    def test_deposit_reaches_the_required_nodes(self, harness: GatewayHarness) -> None:
        replies = harness.gateway.deposit(harness.nodes, make_envelope(), 2, TIMEOUT)
        assert sum(r.ok for r in replies) >= 2
        assert all(r.value is PutOutcome.STORED for r in replies if r.ok)

    def test_at_most_one_reply_per_node_asked(self, harness: GatewayHarness) -> None:
        asked = [harness.nodes[0], harness.nodes[0], harness.nodes[1]]
        replies = harness.gateway.ping(asked, TIMEOUT)
        nodes = [r.node for r in replies]
        assert len(nodes) == len(set(nodes)) and set(nodes) <= set(asked)

    def test_deposit_is_idempotent(self, harness: GatewayHarness) -> None:
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT)
        second = harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT)
        assert all(r.value is PutOutcome.ALREADY_PRESENT for r in second if r.ok)

    def test_deposit_survives_one_dead_node(self, harness: GatewayHarness) -> None:
        harness.set_down(harness.nodes[0])
        replies = harness.gateway.deposit(harness.nodes, make_envelope(), 2, TIMEOUT)
        assert sum(r.ok for r in replies) >= 2

    def test_deposit_with_two_dead_nodes_misses_the_quorum(
        self, harness: GatewayHarness
    ) -> None:
        harness.set_down(harness.nodes[0])
        harness.set_down(harness.nodes[1])
        replies = harness.gateway.deposit(harness.nodes, make_envelope(), 2, TIMEOUT)
        assert sum(r.ok for r in replies) < 2  # and nothing was raised
        assert all(r.error for r in replies if not r.ok)

    def test_unknown_node_is_a_failed_reply_not_an_exception(
        self, harness: GatewayHarness
    ) -> None:
        ghost = NodeId("ghost-node")
        [reply] = harness.gateway.deposit([ghost], make_envelope(), 1, TIMEOUT)
        assert not reply.ok and reply.error

    # ---- collect / acknowledge ----
    def test_deposited_message_is_collectable_from_a_quorum(
        self, harness: GatewayHarness
    ) -> None:
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 2, TIMEOUT)
        replies = harness.gateway.collect(harness.nodes, BOB, 10, TIMEOUT)
        holders = [r for r in replies if r.ok and r.value == [envelope]]
        assert len(holders) >= 2

    def test_collect_reports_a_dead_node_and_still_answers_from_others(
        self, harness: GatewayHarness
    ) -> None:
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT)
        harness.set_down(harness.nodes[2])
        by_node = {
            r.node: r for r in harness.gateway.collect(harness.nodes, BOB, 10, TIMEOUT)
        }
        assert not by_node[harness.nodes[2]].ok
        assert by_node[harness.nodes[0]].value == [envelope]

    def test_acknowledge_removes_the_message_from_every_node(
        self, harness: GatewayHarness
    ) -> None:
        envelope = make_envelope()
        harness.gateway.deposit(harness.nodes, envelope, 3, TIMEOUT)
        acks = harness.gateway.acknowledge(
            harness.nodes, BOB, [envelope.message_id], TIMEOUT
        )
        assert all(r.ok and r.value == 1 for r in acks)
        after = harness.gateway.collect(harness.nodes, BOB, 10, TIMEOUT)
        assert all(r.ok and r.value == [] for r in after)

    def test_acknowledge_of_unknown_id_deletes_nothing(
        self, harness: GatewayHarness
    ) -> None:
        acks = harness.gateway.acknowledge(
            harness.nodes, BOB, [MessageId.new()], TIMEOUT
        )
        assert all(r.ok and r.value == 0 for r in acks)

    # ---- directory records ----
    def test_published_record_is_resolvable_from_a_quorum(
        self, harness: GatewayHarness
    ) -> None:
        record = make_record()
        harness.gateway.publish_record(harness.nodes, record, 2, TIMEOUT)
        replies = harness.gateway.resolve_record(harness.nodes, record.user_id, TIMEOUT)
        assert len([r for r in replies if r.ok and r.value == record]) >= 2

    def test_publish_survives_one_dead_node(self, harness: GatewayHarness) -> None:
        harness.set_down(harness.nodes[1])
        replies = harness.gateway.publish_record(
            harness.nodes, make_record(), 2, TIMEOUT
        )
        assert sum(r.ok for r in replies) >= 2

    def test_resolving_an_unknown_user_is_ok_with_no_record(
        self, harness: GatewayHarness
    ) -> None:
        stranger = UserId.from_public_key(b"stranger")
        replies = harness.gateway.resolve_record(harness.nodes, stranger, TIMEOUT)
        assert all(r.ok and r.value is None for r in replies)
