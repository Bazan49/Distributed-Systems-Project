"""The CLI with real client actors: ``RemoteClient`` over Ray.

Three service nodes and the client actors of two devices, everything real
(PyNaCl with the cheap key derivation, SQLite, the actors' own loops). The
users are scripted consoles.
"""

from __future__ import annotations

import secrets
import threading
import time
from collections.abc import Callable, Iterator
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
import ray

from source.adapters.cli.app import Cli
from source.adapters.cli.client_api import ClientApi, ClientUnavailableError
from source.adapters.system.ray.ray_node import start_service_node
from source.adapters.system.ray.remote_client import RemoteClient
from source.application.ports.errors import WrongPasswordError
from source.composition.client import ClientSettings, KdfStrength
from source.composition.ray_network import start_ray_client
from source.domain.contacts import ContactState
from tests.adapters.cli.support import PASSWORD, ScriptedConsole

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_cluster")]

WAIT_S = 60


class Cluster:
    def __init__(self, data_dir: Path) -> None:
        self.namespace = f"cli-{secrets.token_hex(4)}"
        prefix = secrets.token_hex(3)
        self.seeds = [f"n{prefix}-{i}" for i in (1, 2, 3)]
        self.data_dir = data_dir
        self.actors: list[Any] = [
            start_service_node(seed, data_dir / "nodes", namespace=self.namespace)
            for seed in self.seeds
        ]
        for node in self.actors:
            ray.get(node.ping.remote(), timeout=WAIT_S)

    def client(self, device: str) -> RemoteClient:
        handle = start_ray_client(
            device,
            self.seeds,
            namespace=self.namespace,
            settings=ClientSettings(
                data_dir=self.data_dir / "devices", kdf=KdfStrength.FAST
            ),
            log_dir=self.data_dir / "logs",
            tick_interval_ms=200,
        )
        self.actors.append(handle)
        return RemoteClient(handle, timeout_s=WAIT_S)

    def close(self) -> None:
        for actor in reversed(self.actors):
            ray.kill(actor, no_restart=True)


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[Cluster]:
    made = Cluster(tmp_path)
    yield made
    made.close()


def wait_until(condition: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + WAIT_S
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting until {what}")
        time.sleep(0.1)


def test_a_remote_client_is_a_client_api_and_answers(cluster: Cluster) -> None:
    alice: ClientApi = cluster.client("alice")
    assert isinstance(alice, ClientApi)
    assert alice.is_registered() is False
    profile = alice.register("Alice", PASSWORD)
    assert alice.logged_in() and alice.profile() == profile


def test_an_error_inside_the_actor_arrives_as_itself(cluster: Cluster) -> None:
    alice = cluster.client("alice")
    alice.register("Alice", PASSWORD)
    alice.logout()
    with pytest.raises(WrongPasswordError):  # not a RayTaskError
        alice.login("mala contraseña")


def test_a_dead_actor_is_unavailable(cluster: Cluster) -> None:
    alice = cluster.client("alice")
    ray.kill(cluster.actors[-1], no_restart=True)
    wait_until(lambda: _raises_unavailable(alice), "the actor is gone")


def _raises_unavailable(client: RemoteClient) -> bool:
    try:
        client.is_registered()
    except ClientUnavailableError:
        return True
    return False


def test_two_people_chat_through_their_interfaces(cluster: Cluster) -> None:
    alice, bob = cluster.client("alice"), cluster.client("bob")
    bob_profile = bob.register("Bob", PASSWORD)

    def bob_accepts_when_the_request_arrives() -> str:
        wait_until(lambda: bool(bob.pending_requests()), "Bob gets the request")
        bob.accept(alice.profile().user_id)
        wait_until(
            lambda: bool(alice.contacts(ContactState.ACTIVE)), "Alice sees Bob active"
        )
        return "1"  # conversations

    console = ScriptedConsole(
        "Alice",
        PASSWORD,
        PASSWORD,
        "2",
        bob_profile.link,
        bob_accepts_when_the_request_arrives,
        "1",
        "hola desde la CLI",
        None,  # Esc
        "0",
    )
    Cli(alice, console, device="alice", tz=timezone.utc).run()

    assert "¡Cuenta creada! Hola, Alice." in console.output
    assert "Solicitud enviada a Bob" in console.output
    assert "Tú: hola desde la CLI   ✓✓ entregado" in console.output
    alice_id = alice.profile().user_id
    wait_until(
        lambda: [e.body for e in bob.history(alice_id)] == ["hola desde la CLI"],
        "Bob has the message",
    )


def test_waiting_for_a_change_wakes_up_when_a_message_arrives(
    cluster: Cluster,
) -> None:
    alice, bob = cluster.client("alice"), cluster.client("bob")
    a, b = alice.register("Alice", PASSWORD), bob.register("Bob", PASSWORD)
    alice.add_contact(b.link)
    wait_until(lambda: bool(bob.pending_requests()), "Bob gets the request")
    bob.accept(a.user_id)
    wait_until(lambda: bool(alice.contacts(ContactState.ACTIVE)), "Bob accepted")

    before = alice.wait_for_change(-1, 0)
    # Nothing happens: the wait ends with the same count, after its timeout.
    assert alice.wait_for_change(before, 0.5) == before

    got: list[int] = []
    waiter = threading.Thread(
        target=lambda: got.append(alice.wait_for_change(before, 30))
    )
    waiter.start()
    time.sleep(0.2)
    started = time.monotonic()
    bob.send(a.user_id, "¿me lees?")
    waiter.join(WAIT_S)

    assert got and got[0] > before
    assert time.monotonic() - started < 10  # woke up for the message, not at 30 s
