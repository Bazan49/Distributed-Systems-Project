"""Tests for the pure quorum rules (task 1B.7, domain part)."""

from dataclasses import replace

from source.domain.node_id import NodeId
from source.domain.placement import rank_nodes
from source.domain.quorum import (
    confirmations,
    latest_record,
    merge_envelopes,
    quorum_reached,
    read_targets,
    write_targets,
)
from source.domain.records import NodeReply
from tests.contracts.mailbox_store_contract import make_envelope
from tests.support.builders import ALICE, BASE_TIME_MS, BOB, signed_record

NODES = [NodeId(f"node-{i}") for i in range(1, 6)]
USER = BOB.user_id
RANKED = rank_nodes(USER, NODES)  # BOB's preference order over the 5 nodes


def ok(node: NodeId, value: object = None) -> NodeReply[object]:
    return NodeReply(node=node, ok=True, value=value)


def failed(node: NodeId) -> NodeReply[object]:
    return NodeReply(node=node, ok=False, error="node down")


# ---------------- which nodes ----------------
def test_write_targets_are_the_users_three_best_nodes() -> None:
    assert write_targets(USER, NODES, NODES) == RANKED[:3]


def test_a_dead_node_is_replaced_by_the_next_one_in_the_ranking() -> None:
    dead = RANKED[0]
    active = [n for n in NODES if n != dead]
    assert write_targets(USER, NODES, active) == RANKED[1:4]


def test_with_few_active_nodes_there_are_fewer_targets() -> None:
    assert write_targets(USER, NODES, RANKED[:2]) == RANKED[:2]
    assert write_targets(USER, NODES, []) == []


def test_targets_do_not_depend_on_the_order_of_the_lists() -> None:
    assert write_targets(USER, NODES[::-1], NODES) == write_targets(USER, NODES, NODES)


def test_read_targets_without_dead_nodes_are_the_write_targets() -> None:
    assert read_targets(USER, NODES, NODES) == RANKED[:3]


def test_read_targets_include_the_replacement_of_a_dead_node() -> None:
    # This client thinks RANKED[0] is dead, so it would have written to
    # RANKED[1:4]. Another sender (who saw it alive) wrote to RANKED[0:3].
    # The reader must cover both, minus the node it believes dead.
    active = [n for n in NODES if n != RANKED[0]]
    assert read_targets(USER, NODES, active) == RANKED[1:4]


def test_read_targets_never_include_dead_nodes() -> None:
    assert read_targets(USER, NODES, [RANKED[1]]) == [RANKED[1]]
    assert read_targets(USER, NODES, []) == []


# ---------------- counting ----------------
def test_quorum_counts_distinct_ok_nodes() -> None:
    n1, n2, n3 = NODES[:3]
    assert quorum_reached([ok(n1), failed(n2), ok(n3)], 2)
    assert not quorum_reached([ok(n1), failed(n2), failed(n3)], 2)
    assert not quorum_reached([ok(n1), ok(n1)], 2)  # one node counts once
    assert confirmations([ok(n2), failed(n1), ok(n3), ok(n2)]) == [n2, n3]


def test_a_refusal_is_not_a_confirmation() -> None:
    refused: NodeReply[None] = NodeReply(
        node=NODES[0], ok=False, error="stale", refused=True
    )
    assert refused.answered
    assert not quorum_reached([refused, ok(NODES[1])], 2)
    assert not failed(NODES[0]).answered


# ---------------- merging envelopes ----------------
def test_merge_joins_every_node_and_drops_duplicates() -> None:
    a, b, c = make_envelope(), make_envelope(), make_envelope()
    replies = [
        NodeReply(node=NODES[0], ok=True, value=[a, b]),
        NodeReply(node=NODES[1], ok=True, value=[b, c]),
        NodeReply(node=NODES[2], ok=True, value=[a]),
    ]
    assert merge_envelopes(replies) == [a, b, c]


def test_merge_ignores_nodes_that_failed() -> None:
    a = make_envelope()
    replies = [
        NodeReply(node=NODES[0], ok=False, error="down"),
        NodeReply(node=NODES[1], ok=True, value=[a]),
    ]
    assert merge_envelopes(replies) == [a]
    assert merge_envelopes([]) == []


def test_merge_keeps_a_different_envelope_with_the_same_id() -> None:
    # A lying node returns a forged copy. Dropping by id could hide the real
    # one; the receiver will tell them apart by the signature.
    real = make_envelope()
    forged = replace(real, ciphertext=b"forged")
    replies = [
        NodeReply(node=NODES[0], ok=True, value=[forged]),
        NodeReply(node=NODES[1], ok=True, value=[real]),
    ]
    assert merge_envelopes(replies) == [forged, real]


# ---------------- choosing a record ----------------
def test_the_highest_version_wins() -> None:
    old = signed_record(ALICE, BASE_TIME_MS, version=1)
    new = signed_record(ALICE, BASE_TIME_MS, version=3)
    middle = signed_record(ALICE, BASE_TIME_MS, version=2)
    assert latest_record([old, new, middle]) == new
    assert latest_record([]) is None


def test_a_version_tie_is_broken_the_same_way_in_any_order() -> None:
    a = signed_record(ALICE, BASE_TIME_MS, version=5, lease_ms=1_000)
    b = signed_record(ALICE, BASE_TIME_MS, version=5, lease_ms=2_000)
    assert a != b
    assert latest_record([a, b]) == latest_record([b, a])
