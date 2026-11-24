"""Discovery with real Ray (task 2.4).

Acceptance criterion: a new client obtains the list of nodes from any initial node.

Every test uses its OWN Ray namespace, so it only sees its own actors and the
lists can be compared exactly.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import ray

from source.adapters.memory.crypto import FakeCrypto
from source.adapters.system.clock import SystemClock
from source.adapters.system.ray_gateway import DiscoveryError, RayNodeGateway
from source.adapters.system.ray_node import (
    actor_name,
    registered_nodes,
    start_service_node,
)
from source.application.use_cases.membership import MembershipService
from source.application.use_cases.quorum import QuorumClient
from source.domain.node_id import NodeId
from source.domain.records import NodeReply
from tests.support.builders import BOB, collect_request, make_outgoing

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_cluster")]

TIMEOUT_MS = 5_000
WAIT_S = 60

A, B, C, D = (NodeId(f"node-{x}") for x in "abcd")


def ids(*nodes: NodeId) -> list[NodeId]:
    return sorted(nodes)


class Cluster:
    """Node actors of one test, in a namespace of their own."""

    def __init__(self, data_dir: Path) -> None:
        self.namespace = f"test-{secrets.token_hex(4)}"
        self._data_dir = data_dir
        self.handles: dict[NodeId, Any] = {}
        self.others: list[Any] = []  # non-node actors created by the test

    def start(self, *nodes: NodeId) -> None:
        for node in nodes:
            handle = start_service_node(
                node.value,
                self._data_dir,
                namespace=self.namespace,
                crypto=FakeCrypto(),
            )
            assert ray.get(handle.ping.remote(), timeout=WAIT_S).ok  # ready
            self.handles[node] = handle

    def kill(self, node: NodeId, *, restart: bool = False) -> None:
        ray.kill(self.handles[node], no_restart=not restart)

    def client(self, *seeds: NodeId) -> RayNodeGateway:
        return RayNodeGateway.from_seeds(seeds, self.namespace, TIMEOUT_MS)

    def close(self) -> None:
        for handle in [*self.handles.values(), *self.others]:
            ray.kill(handle, no_restart=True)


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[Cluster]:
    c = Cluster(tmp_path)
    yield c
    c.close()


def wait_until(condition: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + WAIT_S
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting until {what}")


# ================= acceptance criterion =================
@pytest.mark.parametrize("seed", [A, B, C, D])
def test_a_new_client_gets_the_whole_list_from_any_initial_node(
    cluster: Cluster, seed: NodeId
) -> None:
    cluster.start(A, B, C, D)

    gateway = cluster.client(seed)  # it only knows ONE node

    assert gateway.list_nodes() == ids(A, B, C, D)


def test_one_live_initial_node_is_enough(cluster: Cluster) -> None:
    cluster.start(A, B, C)
    cluster.kill(A)
    ghost = NodeId("node-ghost")  # configured, but it never existed

    gateway = cluster.client(A, ghost, C)

    assert set(gateway.list_nodes()) >= {B, C}
    assert gateway.ping([B], TIMEOUT_MS)[0].ok


def test_without_any_live_initial_node_discovery_fails(cluster: Cluster) -> None:
    cluster.start(A)
    cluster.kill(A)
    wait_until(lambda: A not in _registered(cluster), "A is gone")
    with pytest.raises(DiscoveryError):
        cluster.client(A, NodeId("node-ghost"))


def test_a_lying_initial_node_is_ignored_if_another_one_answers(
    cluster: Cluster,
) -> None:
    cluster.start(B, C)
    liar = NodeId("node-liar")
    cluster.others.append(
        _LyingNode.options(  # type: ignore[attr-defined]
            name=actor_name(liar.value), namespace=cluster.namespace
        ).remote(liar)
    )

    gateway = cluster.client(liar, B)

    assert set(gateway.list_nodes()) >= {B, C}


# ================= what counts as a node =================
def test_only_node_actors_of_the_same_namespace_are_listed(cluster: Cluster) -> None:
    cluster.start(A, B)
    cluster.others.append(  # a named actor that is not a node
        _LyingNode.options(name="echo", namespace=cluster.namespace).remote(A)  # type: ignore[attr-defined]
    )
    elsewhere = start_service_node(
        "node-z",
        cluster._data_dir,
        namespace=f"{cluster.namespace}-other",
        crypto=FakeCrypto(),
    )
    cluster.others.append(elsewhere)

    assert cluster.client(A).list_nodes() == ids(A, B)


def test_a_node_being_restarted_by_ray_is_still_listed(cluster: Cluster) -> None:
    cluster.start(A, B)
    cluster.kill(B, restart=True)
    assert cluster.client(A).list_nodes() == ids(A, B)


def test_a_node_killed_for_good_disappears_from_the_nodes_answer(
    cluster: Cluster,
) -> None:
    cluster.start(A, B)
    cluster.kill(B)
    wait_until(lambda: B not in _registered(cluster), "B is gone")
    assert cluster.client(A).list_nodes() == ids(A)


# ================= keeping the list up to date =================
def test_a_node_that_joins_later_is_learned_on_refresh(cluster: Cluster) -> None:
    cluster.start(A, B)
    gateway = cluster.client(A)
    assert gateway.list_nodes() == ids(A, B)

    cluster.start(C)
    assert gateway.refresh_nodes(TIMEOUT_MS)

    assert gateway.list_nodes() == ids(A, B, C)
    assert gateway.ping([C], TIMEOUT_MS)[0].ok  # and it can be used at once


def test_a_refresh_never_forgets_a_node(cluster: Cluster) -> None:
    """The list decides placement, so it must be stable: a node that is down
    is skipped by membership, not removed from the list."""
    cluster.start(A, B)
    gateway = cluster.client(A)
    cluster.kill(B)
    wait_until(lambda: B not in _registered(cluster), "B is gone")

    assert gateway.refresh_nodes(TIMEOUT_MS)

    assert gateway.list_nodes() == ids(A, B)
    assert not gateway.ping([B], TIMEOUT_MS)[0].ok


def test_a_refresh_with_every_node_down_keeps_the_list(cluster: Cluster) -> None:
    cluster.start(A)
    gateway = cluster.client(A)
    cluster.kill(A)
    wait_until(lambda: A not in _registered(cluster), "A is gone")

    assert not gateway.refresh_nodes(500)
    assert gateway.list_nodes() == ids(A)


# ================= end to end =================
def test_a_client_that_only_knows_one_node_can_send_and_receive(
    cluster: Cluster,
) -> None:
    cluster.start(A, B, C, D)
    clock = SystemClock()
    client = QuorumClient(
        cluster.client(D), MembershipService(clock), FakeCrypto(), clock
    )
    message = make_outgoing()

    assert client.deposit(message.envelope).accepted
    collected = client.collect(collect_request(BOB, clock.now_ms()))
    assert list(collected.envelopes) == [message.envelope]


# ---- helpers ----
@ray.remote
class _LyingNode:
    """Answers ``list_nodes`` with an invalid node id."""

    def __init__(self, node: NodeId) -> None:
        self._node = node

    def list_nodes(self) -> Any:
        return NodeReply(node=self._node, ok=True, value=["Not A Node Id"])


def _registered(cluster: Cluster) -> list[NodeId]:
    """Ask Ray directly which node actors of the test's namespace exist."""
    return registered_nodes(cluster.namespace)
