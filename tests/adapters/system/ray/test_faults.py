"""Fault tools with real Ray.

Acceptance criterion: functions the integration tests can call. These tests
use them exactly the way later integration tests will.

The launched nodes use the REAL crypto (NaCl), so Bob has real keys here.
The clients are the real client actor, started by the launcher.
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
from source.adapters.system.ray.ray_peer_transport import inbox_name
from source.adapters.system.ray_gateway import RayNodeGateway
from source.adapters.system.ray_node import registered_nodes
from source.application.use_cases.client_app import Profile
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
from source.domain.outgoing_status import OutgoingStatus
from source.domain.records import KeyPair
from tests.contracts.mailbox_store_contract import make_envelope
from tests.support.client_actors import (
    PASSWORD,
    befriend,
    call,
    client_options,
    received,
    wait_for,
)

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
            "log_dir": "logs",
            "nodes": NODES,
            "clients": [
                {"id": c, "options": client_options()} for c in ("alice", "bob")
            ],
        },
        tmp_path,
    )
    running = launch(config)
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
    bob: Profile = call(cluster.clients["bob"], "register", "Bob", PASSWORD)

    disconnect_client(cluster, "bob")
    assert not actor_exists(cluster, client_actor_name("bob"))
    wait_for(  # Ray removes the inbox with its owner, a moment later
        lambda: actor_exists(cluster, inbox_name(bob.user_id)),
        lambda alive: not alive,
        "Bob's inbox is gone: nobody can reach him directly",
    )
    assert actor_exists(cluster, client_actor_name("alice"))  # the others stay

    reconnect_client(cluster, "bob")
    again = cluster.clients["bob"]
    assert call(again, "logged_in") is False  # back, but logged out
    assert call(again, "login", PASSWORD).user_id == bob.user_id  # same store


@pytest.mark.parametrize("graceful", [False, True])
def test_a_client_keeps_its_identity_after_any_disconnect(
    cluster: RunningCluster, graceful: bool
) -> None:
    alice: Profile = call(cluster.clients["alice"], "register", "Alice", PASSWORD)
    disconnect_client(cluster, "alice", graceful=graceful)
    reconnect_client(cluster, "alice")
    assert call(cluster.clients["alice"], "login", PASSWORD) == alice


def test_an_offline_client_gets_its_message_from_the_mailboxes_on_login(
    cluster: RunningCluster,
) -> None:
    a, b = befriend(cluster.clients["alice"], cluster.clients["bob"])

    with client_offline(cluster, "bob"):
        sent = call(cluster.clients["alice"], "send", b.user_id, "te espero")
        assert sent.status is OutgoingStatus.ACCEPTED  # in the mailboxes

    bob = cluster.clients["bob"]
    call(bob, "login", PASSWORD)  # its first tick collects the mailbox
    wait_for(
        lambda: received(call(bob, "history", a.user_id)),
        lambda texts: texts == ["te espero"],
        "Bob collects the message on logging in",
    )


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
