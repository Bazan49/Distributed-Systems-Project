"""Fault tools with real Ray.

Acceptance criterion: functions the integration tests can call. These tests
use them exactly the way later integration tests will.

The launched nodes use the REAL crypto (NaCl), so Bob has real keys here.
The clients are the fake client actor
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import ray

from source.adapters.memory.crypto import FakeCrypto
from source.adapters.system.clock import SystemClock
from source.adapters.system.crypto import NaClCrypto
from source.adapters.system.ray_gateway import RayNodeGateway
from source.adapters.system.ray_node import registered_nodes
from source.application.use_cases.membership import MembershipService
from source.application.use_cases.quorum import QuorumClient
from source.composition.faults import (
    FaultError,
    client_offline,
    crash_node,
    disconnect_client,
    kill_node,
    node_down,
    reconnect_client,
    restart_node,
)
from source.composition.launcher import (
    RunningCluster,
    client_actor_name,
    launch,
    parse_config,
)
from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.mailbox_access import MailboxAction, MailboxRequest
from source.domain.node_id import NodeId
from source.domain.records import KeyPair
from tests.contracts.mailbox_store_contract import make_envelope
from tests.support.fake_client import events_of, start_fake_client

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_cluster")]

NODES = ["node-1", "node-2", "node-3"]
TIMEOUT_MS = 10_000
CRYPTO = NaClCrypto()


@pytest.fixture
def cluster(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[RunningCluster]:
    namespace = f"faults-{os.getpid()}-{abs(hash(request.node.nodeid)) % 10**8}"
    config = parse_config(
        {
            "namespace": namespace,
            "data_dir": "state",
            "nodes": NODES,
            "clients": [{"id": "alice"}, {"id": "bob"}],
        },
        tmp_path,
    )
    running = launch(config, client_factory=start_fake_client)
    yield running
    running.stop()


@pytest.fixture
def bob() -> KeyPair:
    return CRYPTO.generate_key_pair()


def gateway(cluster: RunningCluster) -> RayNodeGateway:
    return RayNodeGateway(cluster.config.nodes, cluster.config.namespace)


def ping(cluster: RunningCluster, node: str) -> bool:
    [reply] = gateway(cluster).ping([NodeId(node)], TIMEOUT_MS)
    return reply.ok


def envelope_for(keys: KeyPair) -> Envelope:
    return make_envelope(receiver=UserId.from_public_key(keys.public_key))


def collect_from(cluster: RunningCluster, node: str, keys: KeyPair) -> list[Envelope]:
    request = MailboxRequest.build(
        action=MailboxAction.COLLECT,
        public_key=keys.public_key,
        issued_at_ms=SystemClock().now_ms(),
        limit=10,
        sign=lambda data: CRYPTO.sign(keys.private_key, data),
    )
    [reply] = gateway(cluster).collect([NodeId(node)], request, TIMEOUT_MS)
    assert reply.ok, reply.error
    return reply.value or []


def actor_exists(cluster: RunningCluster, name: str) -> bool:
    try:
        ray.get_actor(name, namespace=cluster.config.namespace)
    except ValueError:
        return False
    return True


# ================= nodes =================
def test_a_killed_node_stops_answering(cluster: RunningCluster) -> None:
    kill_node(cluster, "node-2")

    assert not ping(cluster, "node-2")
    assert ping(cluster, "node-1") and ping(cluster, "node-3")
    assert [n.value for n in registered_nodes(cluster.config.namespace)] == [
        "node-1",
        "node-3",
    ]
    assert NodeId("node-2") not in cluster.nodes


def test_a_restarted_node_answers_again_with_its_data(
    cluster: RunningCluster, bob: KeyPair
) -> None:
    envelope = envelope_for(bob)
    assert all(
        r.ok
        for r in gateway(cluster).deposit(cluster.config.nodes, envelope, 3, TIMEOUT_MS)
    )
    kill_node(cluster, "node-2")

    restart_node(cluster, "node-2")

    assert ping(cluster, "node-2")
    assert collect_from(cluster, "node-2", bob) == [envelope]


def test_a_graceful_kill_also_keeps_the_data(
    cluster: RunningCluster, bob: KeyPair
) -> None:
    envelope = envelope_for(bob)
    gateway(cluster).deposit([NodeId("node-1")], envelope, 1, TIMEOUT_MS)
    kill_node(cluster, "node-1", graceful=True)
    restart_node(cluster, "node-1")
    assert collect_from(cluster, "node-1", bob) == [envelope]


def test_a_crashed_node_is_restarted_by_ray_with_its_data(
    cluster: RunningCluster, bob: KeyPair
) -> None:
    envelope = envelope_for(bob)
    gateway(cluster).deposit([NodeId("node-3")], envelope, 1, TIMEOUT_MS)
    handle = cluster.nodes[NodeId("node-3")]
    old_pid = ray.get(handle.info.remote()).pid

    crash_node(cluster, "node-3")

    assert ray.get(handle.info.remote()).pid != old_pid  # a new process
    assert collect_from(cluster, "node-3", bob) == [envelope]


def test_node_down_kills_inside_the_block_and_restarts_after(
    cluster: RunningCluster,
) -> None:
    with node_down(cluster, "node-1"):
        assert not ping(cluster, "node-1")
    assert ping(cluster, "node-1")


def test_node_down_restarts_the_node_even_if_the_block_fails(
    cluster: RunningCluster,
) -> None:
    with pytest.raises(ZeroDivisionError), node_down(cluster, "node-1"):
        _ = 1 / 0
    assert ping(cluster, "node-1")


def test_stop_also_stops_a_node_that_was_restarted(cluster: RunningCluster) -> None:
    kill_node(cluster, "node-1")
    restart_node(cluster, "node-1")
    cluster.stop()
    namespace = cluster.config.namespace
    deadline = time.monotonic() + 60
    while registered_nodes(namespace) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert registered_nodes(namespace) == []


@pytest.mark.parametrize(
    ("action", "node", "message"),
    [
        (kill_node, "node-9", "not a node"),
        (restart_node, "node-1", "already running"),
        (crash_node, "node-9", "not a node"),
    ],
)
def test_node_faults_refuse_to_make_no_sense(
    cluster: RunningCluster, action: Any, node: str, message: str
) -> None:
    with pytest.raises(FaultError, match=message):
        action(cluster, node)


def test_a_node_cannot_be_killed_twice_or_crashed_while_down(
    cluster: RunningCluster,
) -> None:
    kill_node(cluster, "node-1")
    with pytest.raises(FaultError, match="already down"):
        kill_node(cluster, "node-1")
    with pytest.raises(FaultError, match="restart_node"):
        crash_node(cluster, "node-1")


# ================= the quorum under faults =================
def test_the_quorum_survives_one_node_down_but_not_two(
    cluster: RunningCluster,
) -> None:
    clock = SystemClock()
    client = QuorumClient(
        gateway(cluster), MembershipService(clock), FakeCrypto(), clock
    )

    with node_down(cluster, "node-1"):
        assert client.deposit(make_envelope()).accepted
        with node_down(cluster, "node-2"):
            assert not client.deposit(make_envelope()).accepted
    assert client.deposit(make_envelope()).accepted


# ================= clients =================
def test_a_disconnected_client_is_gone_and_comes_back_over_its_data(
    cluster: RunningCluster,
) -> None:
    folder = cluster.config.client_dir("bob")

    disconnect_client(cluster, "bob")
    assert not actor_exists(cluster, client_actor_name("bob"))
    assert actor_exists(cluster, client_actor_name("alice"))  # the others stay

    reconnect_client(cluster, "bob")
    assert actor_exists(cluster, client_actor_name("bob"))
    assert events_of(folder) == ["connect", "connect"]  # same folder, no clean close


def test_a_graceful_disconnect_lets_the_client_close_first(
    cluster: RunningCluster,
) -> None:
    disconnect_client(cluster, "alice", graceful=True)
    assert events_of(cluster.config.client_dir("alice")) == ["connect", "close"]


def test_the_reconnected_client_gets_the_same_arguments(
    cluster: RunningCluster,
) -> None:
    before = ray.get(cluster.clients["bob"].describe.remote())
    disconnect_client(cluster, "bob")
    reconnect_client(cluster, "bob")
    assert ray.get(cluster.clients["bob"].describe.remote()) == before


def test_client_offline_reconnects_after_the_block(cluster: RunningCluster) -> None:
    with client_offline(cluster, "alice"):
        assert not actor_exists(cluster, client_actor_name("alice"))
    assert actor_exists(cluster, client_actor_name("alice"))


@pytest.mark.parametrize(
    ("action", "client", "message"),
    [
        (disconnect_client, "carla", "not a client"),
        (reconnect_client, "carla", "not a client"),
        (reconnect_client, "bob", "already online"),
    ],
)
def test_client_faults_refuse_to_make_no_sense(
    cluster: RunningCluster, action: Any, client: str, message: str
) -> None:
    with pytest.raises(FaultError, match=message):
        action(cluster, client)


def test_a_client_cannot_be_disconnected_twice(cluster: RunningCluster) -> None:
    disconnect_client(cluster, "bob")
    with pytest.raises(FaultError, match="already offline"):
        disconnect_client(cluster, "bob")
