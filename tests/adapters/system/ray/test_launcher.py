"""The launcher with real Ray.

Acceptance criterion: one command starts 3 nodes and 2 clients.

Nothing is fake: the clients are the real client actor (``start_ray_device``,
the default factory) with PyNaCl, SQLite and their own tick loops.
"""

from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import ray

from source.adapters.system.clock import SystemClock
from source.adapters.system.crypto import NaClCrypto
from source.adapters.system.ray.ray_peer_transport import inbox_name
from source.adapters.system.ray_gateway import RayNodeGateway
from source.adapters.system.ray_node import MAILBOX_DB, registered_nodes
from source.composition.launcher import (
    DEFAULT_CLIENT_FACTORY,
    LaunchConfig,
    LaunchError,
    RunningCluster,
    client_actor_name,
    launch,
    parse_config,
)
from source.domain.identifiers import UserId
from source.domain.mailbox_access import MailboxAction, MailboxRequest
from source.domain.node_id import NodeId
from source.domain.outgoing_status import OutgoingStatus
from tests.contracts.mailbox_store_contract import make_envelope
from tests.support.client_actors import (
    PASSWORD,
    befriend,
    call,
    client_options,
    received,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_cluster")]

REPO = Path(__file__).parents[4]
NODES = ["node-1", "node-2", "node-3"]
WAIT_S = 60


@pytest.fixture
def namespace(request: pytest.FixtureRequest) -> str:
    """One Ray namespace per test: actor names never collide between tests."""
    return f"launcher-{os.getpid()}-{abs(hash(request.node.nodeid)) % 10**8}"


def make_config(tmp_path: Path, namespace: str, **overrides: Any) -> LaunchConfig:
    options = client_options()
    raw: dict[str, Any] = {
        "namespace": namespace,
        "data_dir": "state",
        "log_dir": "logs",
        "nodes": NODES,
        "seeds": ["node-1"],
        "clients": [
            {"id": "alice", "options": options},
            {"id": "bob", "options": options},
        ],
    }
    raw.update(overrides)
    return parse_config(raw, tmp_path)


@pytest.fixture
def running(tmp_path: Path, namespace: str) -> Iterator[RunningCluster]:
    cluster = launch(make_config(tmp_path, namespace))
    yield cluster
    cluster.stop()


def actor_exists(name: str, namespace: str) -> bool:
    try:
        ray.get_actor(name, namespace=namespace)
    except ValueError:
        return False
    return True


def wait_until(condition: Any, what: str) -> None:
    deadline = time.monotonic() + WAIT_S
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting until {what}")


# ================= launch =================
def test_it_starts_every_node_and_every_client(running: RunningCluster) -> None:
    ns = running.config.namespace
    assert [n.value for n in registered_nodes(ns)] == NODES
    for node in running.nodes.values():
        assert ray.get(node.ping.remote(), timeout=WAIT_S).ok
    for client_id in ("alice", "bob"):
        assert actor_exists(client_actor_name(client_id), ns)


def test_the_clients_are_the_real_client_actor_and_start_logged_out(
    running: RunningCluster,
) -> None:
    assert running.config.client_factory == DEFAULT_CLIENT_FACTORY
    for client in running.clients.values():
        assert call(client, "is_registered") is False
        assert call(client, "logged_in") is False


def test_each_client_keeps_its_store_in_its_folder(running: RunningCluster) -> None:
    for client_id in ("alice", "bob"):
        store = running.config.client_dir(client_id) / f"{client_id}.db"
        assert store.exists()


def test_launched_clients_reach_the_nodes_and_each_other(
    running: RunningCluster,
) -> None:
    alice, bob = running.clients["alice"], running.clients["bob"]
    # befriend publishes both records and sends the contact request through
    # the nodes: the clients found them from the seed of the configuration.
    a, b = befriend(alice, bob)

    sent = call(alice, "send", b.user_id, "hola, Bob")

    assert sent.status is OutgoingStatus.DELIVERED  # straight to Bob's actor
    assert received(call(bob, "history", a.user_id)) == ["hola, Bob"]


def test_every_process_writes_its_own_log_in_the_log_folder(
    running: RunningCluster,
) -> None:
    logs = running.config.logs_dir()

    def events_in(name: str) -> list[str]:
        path = logs / f"{name}.jsonl"
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()
        return [json.loads(line)["event"] for line in lines]

    for node in NODES:
        wait_until(lambda n=node: "actor.started" in events_in(n), f"{node} logs")
    for client in ("alice", "bob"):
        wait_until(
            lambda c=client: "logging.configured" in events_in(f"client-{c}"),
            f"{client} logs",
        )


def test_each_node_keeps_its_files_in_its_folder(running: RunningCluster) -> None:
    for node in NODES:
        assert (running.config.node_dir() / node / MAILBOX_DB).exists()


def test_a_client_can_discover_the_launched_nodes_from_a_seed(
    running: RunningCluster,
) -> None:
    gateway = RayNodeGateway.from_seeds([NodeId("node-1")], running.config.namespace)
    assert [n.value for n in gateway.list_nodes()] == NODES


# ================= stop =================
def test_stop_removes_every_actor_and_can_be_repeated(
    running: RunningCluster,
) -> None:
    ns = running.config.namespace
    running.stop()
    running.stop()  # nothing left to stop: no error
    wait_until(lambda: registered_nodes(ns) == [], "the nodes are gone")
    for client_id in ("alice", "bob"):
        wait_until(
            lambda cid=client_id: not actor_exists(client_actor_name(cid), ns),
            f"{client_id} is gone",
        )
    assert running.nodes == {} and running.clients == {}


def test_stop_closes_the_clients_first(running: RunningCluster) -> None:
    ns = running.config.namespace
    bob = running.clients["bob"]
    b = call(bob, "register", "Bob", PASSWORD)
    assert actor_exists(inbox_name(b.user_id), ns)  # Bob is listening

    running.stop()

    wait_until(lambda: not actor_exists(inbox_name(b.user_id), ns), "the inbox is gone")


def test_launching_again_picks_up_the_data(tmp_path: Path, namespace: str) -> None:
    # The launched nodes use the REAL crypto (NaCl), so Bob needs real keys.
    crypto = NaClCrypto()
    bob = crypto.generate_key_pair()
    config = make_config(tmp_path, namespace, clients=[])
    envelope = make_envelope(receiver=UserId.from_public_key(bob.public_key))

    first = launch(config)
    deposited = RayNodeGateway(config.nodes, namespace).deposit(
        config.nodes, envelope, 3, 10_000
    )
    assert all(r.ok for r in deposited)
    first.stop()
    wait_until(lambda: registered_nodes(namespace) == [], "the nodes are gone")

    second = launch(config)
    try:
        request = MailboxRequest.build(
            action=MailboxAction.COLLECT,
            public_key=bob.public_key,
            issued_at_ms=SystemClock().now_ms(),
            limit=10,
            sign=lambda data: crypto.sign(bob.private_key, data),
        )
        held = RayNodeGateway(config.nodes, namespace).collect(
            config.nodes, request, 10_000
        )
        assert all(r.ok and r.value == [envelope] for r in held)
    finally:
        second.stop()


# ================= failures: all or nothing =================
def test_if_a_client_fails_to_start_everything_is_stopped(
    tmp_path: Path, namespace: str
) -> None:
    # Alice starts; Bob cannot (a wrong option): Alice and the nodes go too.
    config = make_config(
        tmp_path,
        namespace,
        clients=[
            {"id": "alice", "options": client_options()},
            {"id": "bob", "options": {"kdf": "slow"}},
        ],
    )
    with pytest.raises(LaunchError, match="kdf of client bob"):
        launch(config)
    wait_until(lambda: registered_nodes(namespace) == [], "the nodes are stopped")
    wait_until(
        lambda: not actor_exists(client_actor_name("alice"), namespace),
        "alice is stopped",
    )


def test_a_client_factory_that_does_not_exist_is_reported_and_starts_nothing(
    tmp_path: Path, namespace: str
) -> None:
    config = make_config(
        tmp_path, namespace, client_factory="source.adapters.system.no_such:start"
    )
    with pytest.raises(LaunchError, match="cannot import the client factory"):
        launch(config)
    assert registered_nodes(namespace) == []


def test_a_node_that_cannot_open_its_files_aborts_the_launch(
    tmp_path: Path, namespace: str
) -> None:
    config = make_config(tmp_path, namespace, clients=[])
    blocked = config.node_dir() / "node-2"
    blocked.parent.mkdir(parents=True)
    blocked.write_text("a file where the node's folder should be")

    with pytest.raises(LaunchError, match="failed to start"):
        launch(config, ready_timeout_ms=30_000)
    wait_until(lambda: registered_nodes(namespace) == [], "the nodes are stopped")


# ================= the command (acceptance criterion) =================
def _read_line_until(proc: subprocess.Popen[str], prefix: str) -> str:
    """Read the command's output until a line starts with ``prefix``."""
    assert proc.stdout is not None
    deadline = time.monotonic() + 180
    seen = []
    while time.monotonic() < deadline:
        ready, _, _ = select.select([proc.stdout], [], [], 1)
        if ready:
            line = proc.stdout.readline()
            if not line:
                break  # the process ended
            seen.append(line)
            if line.startswith(prefix):
                return line
    raise AssertionError(f"no {prefix!r} line; output was: {''.join(seen)}")


def test_one_command_starts_3_nodes_and_2_clients_and_stops_them(
    tmp_path: Path, namespace: str
) -> None:
    config_file = tmp_path / "cluster.json"
    config_file.write_text(
        json.dumps(
            {
                "namespace": namespace,
                "data_dir": "state",
                "log_dir": "logs",
                "nodes": NODES,
                "clients": [
                    {"id": c, "options": client_options()} for c in ("alice", "bob")
                ],
            }
        ),
        encoding="utf-8",
    )
    env = {k: v for k, v in os.environ.items() if k != "RAY_ADDRESS"}
    proc = subprocess.Popen(  # noqa: S603 - our own interpreter and module
        [sys.executable, "-m", "source.composition.launcher", str(config_file)],
        cwd=REPO,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        ready = _read_line_until(proc, "READY")
        assert "nodes=[node-1, node-2, node-3]" in ready
        assert "clients=[client-device-alice, client-device-bob]" in ready
        for node in NODES:
            assert (tmp_path / "state" / "nodes" / node / MAILBOX_DB).exists()
        for client in ("alice", "bob"):
            assert (tmp_path / "state" / "clients" / client / f"{client}.db").exists()

        proc.send_signal(signal.SIGINT)  # Ctrl+C

        _read_line_until(proc, "STOPPED")
        assert proc.wait(timeout=120) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
