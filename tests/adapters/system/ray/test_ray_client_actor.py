"""Client actors on a real Ray cluster.

Another actor hands it a direct envelope and the
message is in its history. A receiver that is
offline gets the message from the mailboxes when it connects again.

Everything is real here: three service-node actors, two client actors in
their own processes, PyNaCl, SQLite, and the clients' own tick loops.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, TypeVar

import pytest
import ray

from source.adapters.system.ray.ray_client import device_actor_name
from source.adapters.system.ray.ray_node import start_service_node
from source.adapters.system.ray.ray_peer_transport import inbox_name
from source.application.use_cases.client_app import EntryKind, HistoryEntry, Profile
from source.composition.client import ClientSettings, KdfStrength
from source.composition.ray_network import RayClientFactory, start_ray_client
from source.domain.contacts import ContactState
from source.domain.outgoing_status import OutgoingStatus

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_cluster")]

PASSWORD = "contraseña larga"
WAIT_S = 60
T = TypeVar("T")


def wait_for(read: Callable[[], T], ok: Callable[[T], bool], what: str) -> T:
    deadline = time.monotonic() + WAIT_S
    while True:
        value = read()
        if ok(value):
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting until {what}: {value!r}")
        time.sleep(0.1)


class Cluster:
    """Three service nodes and the client actors started on them."""

    def __init__(self, data_dir: Path) -> None:
        self.namespace = f"p08-test-{secrets.token_hex(4)}"
        prefix = secrets.token_hex(3)
        self.seeds = [f"n{prefix}-{i}" for i in (1, 2, 3)]
        self._data_dir = data_dir
        self._actors: list[Any] = [
            start_service_node(seed, data_dir / "nodes", namespace=self.namespace)
            for seed in self.seeds
        ]
        for node in self._actors:
            ray.get(node.ping.remote(), timeout=WAIT_S)

    def client(self, device: str) -> Any:
        handle = start_ray_client(
            device,
            self.seeds,
            namespace=self.namespace,
            settings=ClientSettings(
                data_dir=self._data_dir / "devices", kdf=KdfStrength.FAST
            ),
            log_dir=self._data_dir / "logs",
            tick_interval_ms=200,
        )
        self._actors.append(handle)
        return handle

    def close(self) -> None:
        for actor in reversed(self._actors):
            ray.kill(actor, no_restart=True)


@pytest.fixture
def cluster(tmp_path: Path) -> Iterator[Cluster]:
    (tmp_path / "devices").mkdir()
    made = Cluster(tmp_path)
    yield made
    made.close()


def call(handle: Any, method: str, *args: Any) -> Any:
    return ray.get(getattr(handle, method).remote(*args), timeout=WAIT_S)


def befriend(alice: Any, bob: Any) -> tuple[Profile, Profile]:
    a: Profile = call(alice, "register", "Alice", PASSWORD)
    b: Profile = call(bob, "register", "Bob", PASSWORD)
    call(alice, "add_contact", b.link)
    wait_for(lambda: call(bob, "pending_requests"), bool, "Bob gets the request")
    call(bob, "accept", a.user_id)
    wait_for(
        lambda: call(alice, "contacts", ContactState.ACTIVE),
        bool,
        "Alice learns that Bob accepted",
    )
    return a, b


def received(history: list[HistoryEntry]) -> list[str]:
    return [e.body for e in history if e.kind is EntryKind.RECEIVED]


def test_a_direct_envelope_from_another_actor_lands_in_the_history(
    cluster: Cluster,
) -> None:
    alice, bob = cluster.client("alice"), cluster.client("bob")
    a, b = befriend(alice, bob)
    assert ray.get_actor(inbox_name(b.user_id), namespace=cluster.namespace)

    sent = call(alice, "send", b.user_id, "hola por la vía directa")

    assert sent.status is OutgoingStatus.DELIVERED  # straight to Bob's actor
    assert received(call(bob, "history", a.user_id)) == ["hola por la vía directa"]


def test_an_offline_receiver_collects_from_the_mailboxes_when_it_connects(
    cluster: Cluster,
) -> None:
    alice, bob = cluster.client("alice"), cluster.client("bob")
    a, b = befriend(alice, bob)
    call(bob, "logout")

    sent = call(alice, "send", b.user_id, "te espero en el buzón")

    assert sent.status is OutgoingStatus.ACCEPTED  # in the mailboxes, not direct
    call(bob, "login", PASSWORD)
    wait_for(
        lambda: received(call(bob, "history", a.user_id)),
        lambda texts: texts == ["te espero en el buzón"],
        "Bob collects the message on connecting",
    )
    # His receipt travels back: Alice sees it delivered (✓✓).
    wait_for(
        lambda: [e.status for e in call(alice, "history", b.user_id)],
        lambda statuses: statuses == [OutgoingStatus.DELIVERED],
        "Alice learns that Bob got it",
    )


def test_the_device_actor_is_found_by_name_and_started_only_once(
    cluster: Cluster,
) -> None:
    first = cluster.client("carol")
    call(first, "register", "Carol", PASSWORD)
    found = ray.get_actor(device_actor_name("carol"), namespace=cluster.namespace)
    assert call(found, "logged_in") is True
    again = cluster.client("carol")  # get_if_exists: the same actor
    assert call(again, "profile") == call(first, "profile")


def test_the_factory_rejects_bad_settings() -> None:
    with pytest.raises(ValueError):
        RayClientFactory(device="Bad Name", seeds=("n1",))
    with pytest.raises(ValueError):
        RayClientFactory(device="alice", seeds=())
    with pytest.raises(ValueError):
        RayClientFactory(device="alice", seeds=("not a node id!",))
