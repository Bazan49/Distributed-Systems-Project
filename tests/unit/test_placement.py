"""Tests for source.domain.placement"""

import random

import pytest
from hypothesis import given
from hypothesis import strategies as st

from source.domain.identifiers import UserId
from source.domain.node_id import NodeId
from source.domain.placement import REPLICAS, preferred_nodes, rank_nodes

ALICE = UserId.from_public_key(b"alice")


def nodes_named(count: int) -> list[NodeId]:
    return [NodeId(f"node-{i}") for i in range(1, count + 1)]


# Hypothesis strategies
users = st.binary(min_size=1, max_size=32).map(UserId.from_public_key)
node_sets = st.sets(st.integers(1, 12), min_size=1, max_size=12).map(
    lambda ids: [NodeId(f"node-{i}") for i in sorted(ids)]
)


# ---------------- basic behaviour ----------------
def test_returns_three_nodes_by_default() -> None:
    assert len(preferred_nodes(ALICE, nodes_named(5))) == REPLICAS == 3


def test_returns_distinct_nodes_taken_from_the_input() -> None:
    nodes = nodes_named(5)
    chosen = preferred_nodes(ALICE, nodes)
    assert len(set(chosen)) == len(chosen)
    assert set(chosen) <= set(nodes)


def test_with_fewer_nodes_than_requested_returns_all_of_them() -> None:
    assert sorted(preferred_nodes(ALICE, nodes_named(2))) == nodes_named(2)


def test_with_no_nodes_returns_nothing() -> None:
    assert preferred_nodes(ALICE, []) == []


def test_duplicate_nodes_in_the_input_are_ignored() -> None:
    nodes = nodes_named(4)
    assert preferred_nodes(ALICE, nodes + nodes) == preferred_nodes(ALICE, nodes)


def test_count_below_one_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        preferred_nodes(ALICE, nodes_named(3), count=0)


def test_a_custom_count_is_respected() -> None:
    assert len(preferred_nodes(ALICE, nodes_named(5), count=2)) == 2


def test_first_choice_is_the_head_of_the_full_ranking() -> None:
    nodes = nodes_named(5)
    assert preferred_nodes(ALICE, nodes, count=1) == rank_nodes(ALICE, nodes)[:1]


# ---------------- determinism ----------------
def test_golden_values_never_change() -> None:
    """If this fails, someone changed the algorithm: every stored mailbox would
    be looked up on the wrong nodes. Change it only on purpose, with a migration."""
    expected = {
        "alice": ["node-3", "node-1", "node-5"],
        "bob": ["node-3", "node-2", "node-1"],
    }
    assert [n.value for n in preferred_nodes(ALICE, nodes_named(5))] == expected[
        "alice"
    ]
    bob = UserId.from_public_key(b"bob")
    assert [n.value for n in preferred_nodes(bob, nodes_named(5))] == expected["bob"]


@given(user=users, nodes=node_sets, seed=st.integers())
def test_the_order_of_the_input_list_does_not_matter(
    user: UserId, nodes: list[NodeId], seed: int
) -> None:
    shuffled = list(nodes)
    random.Random(seed).shuffle(shuffled)
    assert preferred_nodes(user, shuffled) == preferred_nodes(user, nodes)


@given(user=users, nodes=node_sets)
def test_same_input_same_output(user: UserId, nodes: list[NodeId]) -> None:
    assert preferred_nodes(user, nodes) == preferred_nodes(user, nodes)


# ---------------- minimal disruption (the property that matters) ----------------
@given(user=users, nodes=node_sets.filter(lambda ns: len(ns) >= 2), data=st.data())
def test_removing_a_node_only_replaces_that_node(
    user: UserId, nodes: list[NodeId], data: st.DataObject
) -> None:
    removed = data.draw(st.sampled_from(nodes))
    remaining = [n for n in nodes if n != removed]
    before = preferred_nodes(user, nodes)
    after = preferred_nodes(user, remaining)
    kept = [n for n in before if n != removed]
    # everyone who was already chosen (except the removed node) keeps their place...
    assert after[: len(kept)] == kept
    # ...and if the removed node was not chosen, nothing changed at all
    if removed not in before:
        assert after == before


@given(user=users, nodes=node_sets, newcomer=st.integers(100, 110))
def test_adding_a_node_never_reshuffles_the_others(
    user: UserId, nodes: list[NodeId], newcomer: int
) -> None:
    new_node = NodeId(f"node-{newcomer}")
    before = preferred_nodes(user, nodes)
    after = preferred_nodes(user, [*nodes, new_node])
    # the previous choices keep their relative order; the newcomer only slots in
    assert [n for n in after if n != new_node] == before[
        : len([n for n in after if n != new_node])
    ]


def test_when_a_node_disappears_only_the_users_who_had_it_change() -> None:
    nodes = nodes_named(5)
    dead = nodes[2]
    survivors = [n for n in nodes if n != dead]
    all_users = [UserId.from_public_key(f"user-{i}".encode()) for i in range(1000)]
    changed = [
        u
        for u in all_users
        if preferred_nodes(u, nodes) != preferred_nodes(u, survivors)
    ]
    had_it = [u for u in all_users if dead in preferred_nodes(u, nodes)]
    assert set(changed) == set(had_it)  # exactly the users that held the dead node
    assert len(changed) < len(
        all_users
    )  # and not everybody (a plain hash % n would move ~all)


# ---------------- fairness ----------------
def test_load_is_spread_evenly_across_nodes() -> None:
    nodes = nodes_named(5)
    totals = dict.fromkeys(nodes, 0)
    for i in range(2000):
        for node in preferred_nodes(
            UserId.from_public_key(f"user-{i}".encode()), nodes
        ):
            totals[node] += 1
    # each user sits on 3 of 5 nodes, so each node should hold about 60% of users
    for node, count in totals.items():
        assert 0.52 * 2000 <= count <= 0.68 * 2000, f"{node} holds {count / 2000:.0%}"
