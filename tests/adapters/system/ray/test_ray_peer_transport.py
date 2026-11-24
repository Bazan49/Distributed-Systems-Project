"""RayPeerTransport against real inbox actors.

Acceptance criterion: it passes the contract of the port (the same
``PeerTransportContract`` the in-memory transport passes) and, with the
receiver switched off, a send is UNREACHABLE, so the sender uses the mailbox.

Plus what the contract cannot see: the deadline, a malformed envelope, and
the inbox dying under the receiver.
"""

from __future__ import annotations

import asyncio
import secrets
import threading
import time
from collections.abc import Iterator

import pytest
import ray

from source.adapters.system.ray.ray_peer_transport import (
    INBOX_PREFIX,
    Inbox,
    RayPeerTransport,
    inbox_name,
)
from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.records import DirectSendResult
from tests.contracts.mailbox_store_contract import ALICE, BOB, make_envelope
from tests.contracts.peer_transport_contract import (
    PeerTransportContract,
    TransportFactory,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_cluster")]

TIMEOUT_MS = 2_000


@pytest.fixture
def namespace() -> str:
    """Inbox names are global inside a namespace: one namespace per test."""
    return f"p08-test-{secrets.token_hex(4)}"


@pytest.fixture
def transports(namespace: str) -> Iterator[list[RayPeerTransport]]:
    """Every transport made by a test; all stop listening at the end."""
    made: list[RayPeerTransport] = []
    yield made
    for transport in made:
        transport.stop_listening()


@pytest.fixture
def make_transport(
    namespace: str, transports: list[RayPeerTransport]
) -> TransportFactory:
    def make(user: UserId) -> RayPeerTransport:
        transport = RayPeerTransport(user, namespace)
        transports.append(transport)
        return transport

    return make


# ================= acceptance criterion: the contract =================
class TestRayPeerTransport(PeerTransportContract):
    pass  # uses the module's ``make_transport`` fixture


# ================= beyond the contract =================
def test_the_inbox_is_named_after_the_user(
    make_transport: TransportFactory, namespace: str
) -> None:
    assert inbox_name(BOB) == f"{INBOX_PREFIX}{BOB.value}"
    make_transport(BOB).start_listening(lambda _e: None)
    assert ray.get_actor(inbox_name(BOB), namespace=namespace)


def test_a_switched_off_receiver_is_unreachable_so_the_mailbox_is_used(
    make_transport: TransportFactory,
) -> None:
    received: list[Envelope] = []
    bob = make_transport(BOB)
    bob.start_listening(received.append)
    alice = make_transport(ALICE)
    assert alice.send_direct(BOB, make_envelope(BOB), TIMEOUT_MS) is (
        DirectSendResult.DELIVERED
    )
    bob.stop_listening()
    assert alice.send_direct(BOB, make_envelope(BOB), TIMEOUT_MS) is (
        DirectSendResult.UNREACHABLE
    )
    assert len(received) == 1


def test_a_slow_receiver_is_unreachable_after_the_deadline(
    make_transport: TransportFactory,
) -> None:
    release = threading.Event()
    make_transport(BOB).start_listening(lambda _e: release.wait(10))
    started = time.monotonic()
    result = make_transport(ALICE).send_direct(BOB, make_envelope(BOB), 300)
    elapsed = time.monotonic() - started
    release.set()
    assert result is DirectSendResult.UNREACHABLE
    assert elapsed < 3


def test_an_inbox_killed_under_the_receiver_makes_it_unreachable(
    make_transport: TransportFactory, namespace: str
) -> None:
    make_transport(BOB).start_listening(lambda _e: None)
    ray.kill(ray.get_actor(inbox_name(BOB), namespace=namespace), no_restart=True)
    result = make_transport(ALICE).send_direct(BOB, make_envelope(BOB), TIMEOUT_MS)
    assert result is DirectSendResult.UNREACHABLE


def test_negative_timeouts_are_rejected(make_transport: TransportFactory) -> None:
    with pytest.raises(ValueError):
        make_transport(ALICE).send_direct(BOB, make_envelope(BOB), -1)


def test_stop_listening_twice_is_harmless(make_transport: TransportFactory) -> None:
    bob = make_transport(BOB)
    bob.stop_listening()
    bob.start_listening(lambda _e: None)
    bob.stop_listening()
    bob.stop_listening()


def test_malformed_bytes_are_not_settled(namespace: str) -> None:
    received: list[Envelope] = []
    bob = RayPeerTransport(BOB, namespace)
    assert bob._hand_over(b"not an envelope", received.append) is False
    assert received == []


# ---- the inbox itself, without Ray ----
async def _deliver_and_settle(inbox: Inbox, settled: bool) -> bool:
    pending = asyncio.ensure_future(inbox.deliver(b"raw", 5))
    item = await inbox.next(5)
    assert item is not None and item[1] == b"raw"
    await inbox.settle(item[0], settled)
    return await pending


def test_inbox_returns_the_receivers_verdict() -> None:
    assert asyncio.run(_deliver_and_settle(Inbox(), True)) is True
    assert asyncio.run(_deliver_and_settle(Inbox(), False)) is False


def test_inbox_without_a_verdict_in_time_is_false_and_next_times_out() -> None:
    async def scenario() -> tuple[bool, object]:
        inbox = Inbox()
        late = await inbox.deliver(b"raw", 0.01)
        await inbox.settle(99, True)  # nobody waits for key 99: ignored
        drained = await inbox.next(0.01)
        empty = await inbox.next(0.01)
        return late, (drained is not None, empty)

    assert asyncio.run(scenario()) == (False, (True, None))
