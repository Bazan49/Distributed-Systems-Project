"""Tests for ClientApp, the client's public API.

The full flow works with in-memory
adapters. Every device here is a real ``ClientApp`` with its own store, and
they share an in-memory cluster (real node services) and an in-memory peer
network, so the whole stack runs: identity, contacts, sending, delivery
(direct and mailbox), receiving, ordering, presence and quorum.
"""

from __future__ import annotations

import random
from collections.abc import Callable

import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.adapters.memory.peer_transport import InMemoryPeerNetwork
from source.application.ports.errors import WrongPasswordError
from source.application.use_cases.client_app import (
    ClientApp,
    ClientPolicy,
    EntryKind,
    HistoryEntry,
    NotLoggedInError,
)
from source.application.use_cases.contacts import AddOutcome, NoPendingRequestError
from source.application.use_cases.identity import (
    AlreadyRegisteredError,
    NotRegisteredError,
)
from source.application.use_cases.receive_messages import GapReport
from source.application.use_cases.send_message import UnknownContactError
from source.domain.contact_link import ContactLink
from source.domain.contacts import ContactState
from source.domain.envelope import Envelope, MessageKind
from source.domain.identifiers import Alias, ConversationId, MessageId, UserId
from source.domain.messages import ReceivedMessage
from source.domain.node_id import NodeId
from source.domain.outgoing_status import OutgoingStatus
from source.domain.placement import rank_nodes
from source.domain.records import DirectSendResult

NODES = [NodeId(f"node-{i}") for i in (1, 2, 3)]
PASSWORD = "contraseña larga"
LEASE_PAST_MS = 60_000  # longer than the 30 s default presence lease


class World:
    """One clock, one in-memory cluster and one peer network for every device."""

    def __init__(self) -> None:
        self.clock = ManualClock()
        self.crypto = FakeCrypto()
        self.gateway = InMemoryNodeGateway(NODES, self.clock, self.crypto)
        self.network = InMemoryPeerNetwork()
        self.stores: dict[str, MemoryLocalStore] = {}

    def device(self, name: str, policy: ClientPolicy | None = None) -> ClientApp:
        """A device (store + ClientApp). Reusing a name reuses its store: a restart."""
        store = self.stores.setdefault(name, MemoryLocalStore())
        return ClientApp(
            crypto=self.crypto,
            store=store,
            clock=self.clock,
            gateway=self.gateway,
            peers_for=self.network.transport_for,
            policy=policy,
            rng=random.Random(0),
        )

    def user(self, name: str) -> ClientApp:
        app = self.device(name)
        app.register(name.capitalize(), PASSWORD)
        return app

    def pass_time(self, ms: int, *apps: ClientApp, step_ms: int = 1_000) -> None:
        """Let time pass while the given apps keep ticking, like their main loops."""
        for _ in range(ms // step_ms):
            self.clock.advance(step_ms)
            for app in apps:
                if app.logged_in:
                    app.tick()


@pytest.fixture
def world() -> World:
    return World()


def befriend(a: ClientApp, b: ClientApp) -> None:
    a.add_contact(b.profile().link)
    b.accept(a.profile().user_id)


def texts(entries: list[HistoryEntry]) -> list[tuple[EntryKind, str]]:
    return [(e.kind, e.body) for e in entries]


# ---------------- account ----------------


def test_register_logs_in_and_shows_the_profile(world: World) -> None:
    app = world.device("alice")
    assert not app.is_registered()

    profile = app.register("Alice", PASSWORD)

    assert app.logged_in and app.is_registered()
    assert profile.alias == Alias("Alice")
    assert ContactLink.from_text(profile.link).user_id == profile.user_id
    assert profile.fingerprint == world.crypto.fingerprint(
        ContactLink.from_text(profile.link).public_key
    )
    assert app.profile() == profile


def test_logging_in_announces_presence(world: World) -> None:
    alice = world.user("alice")
    bob = world.user("bob")
    befriend(alice, bob)

    # Bob is reachable directly: the request and acceptance never touched a mailbox.
    message = alice.send(bob.profile().user_id, "hola")
    assert message.status is OutgoingStatus.DELIVERED


def test_logout_then_login_with_the_password(world: World) -> None:
    app = world.user("alice")
    before = app.profile()
    app.logout()

    assert not app.logged_in
    with pytest.raises(WrongPasswordError):
        app.login("otra contraseña")
    assert app.login(PASSWORD) == before


def test_logout_is_idempotent(world: World) -> None:
    app = world.user("alice")
    app.logout()
    app.logout()
    assert not app.logged_in


def test_the_identity_survives_a_restart_of_the_device(world: World) -> None:
    profile = world.user("alice").profile()

    restarted = world.device("alice")  # same store, new process

    assert restarted.is_registered() and not restarted.logged_in
    assert restarted.login(PASSWORD) == profile


def test_a_device_cannot_register_twice(world: World) -> None:
    app = world.user("alice")
    with pytest.raises(AlreadyRegisteredError):
        app.register("Otra", PASSWORD)


def test_login_without_an_identity_fails(world: World) -> None:
    with pytest.raises(NotRegisteredError):
        world.device("nadie").login(PASSWORD)


@pytest.mark.parametrize(
    "call",
    [
        lambda app: app.profile(),
        lambda app: app.contacts(),
        lambda app: app.pending_requests(),
        lambda app: app.add_contact("p08:contact:x"),
        lambda app: app.accept(UserId("0" * 64)),
        lambda app: app.reject(UserId("0" * 64)),
        lambda app: app.remove_contact(UserId("0" * 64)),
        lambda app: app.send(UserId("0" * 64), "hola"),
        lambda app: app.history(UserId("0" * 64)),
        lambda app: app.tick(),
    ],
)
def test_everything_but_the_account_needs_a_login(
    world: World, call: Callable[[ClientApp], object]
) -> None:
    with pytest.raises(NotLoggedInError):
        call(world.device("alice"))


# ---------------- the full flow ----------------


def test_the_full_flow_with_in_memory_adapters(world: World) -> None:
    """Register, add a contact, accept, chat both ways, read the history."""
    alice, bob = world.user("alice"), world.user("bob")
    alice_id, bob_id = alice.profile().user_id, bob.profile().user_id

    added = alice.add_contact(bob.profile().link)
    assert added.outcome is AddOutcome.REQUEST_SENT
    (request,) = bob.pending_requests()
    assert request.contact.user_id == alice_id
    assert request.fingerprint == alice.profile().fingerprint

    bob.accept(alice_id)
    assert [v.contact.state for v in alice.contacts()] == [ContactState.ACTIVE]

    alice.send(bob_id, "hola Bob")
    world.clock.advance(1_000)
    bob.send(alice_id, "hola Alice")
    world.clock.advance(1_000)
    alice.send(bob_id, "¿nos vemos mañana?")

    expected = [
        (EntryKind.SENT, "hola Bob"),
        (EntryKind.RECEIVED, "hola Alice"),
        (EntryKind.SENT, "¿nos vemos mañana?"),
    ]
    assert texts(alice.history(bob_id)) == expected
    assert texts(bob.history(alice_id)) == [
        (EntryKind.RECEIVED, "hola Bob"),
        (EntryKind.SENT, "hola Alice"),
        (EntryKind.RECEIVED, "¿nos vemos mañana?"),
    ]


def test_an_offline_contact_gets_the_messages_from_the_mailboxes(
    world: World,
) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    bob_id = bob.profile().user_id
    bob.logout()
    world.pass_time(LEASE_PAST_MS, alice)  # Bob's presence expires

    sent = [alice.send(bob_id, "uno"), alice.send(bob_id, "dos")]
    assert [m.status for m in sent] == [OutgoingStatus.ACCEPTED] * 2

    bob.login(PASSWORD)
    report = bob.tick()

    assert report.collected is not None
    assert [m.body for m in report.stored] == ["uno", "dos"]
    assert texts(bob.history(alice.profile().user_id)) == [
        (EntryKind.RECEIVED, "uno"),
        (EntryKind.RECEIVED, "dos"),
    ]


def test_collected_messages_are_acknowledged_on_the_nodes(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    bob_id = bob.profile().user_id
    bob.logout()
    world.pass_time(LEASE_PAST_MS, alice)
    alice.send(bob_id, "uno")

    bob.login(PASSWORD)
    bob.tick()
    world.clock.advance(ClientPolicy().collect_interval_ms)
    again = bob.tick()

    assert again.collected is not None
    assert again.collected.results == ()  # the nodes deleted what was confirmed


def test_a_request_to_an_offline_user_is_answered_when_they_return(
    world: World,
) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    link = bob.profile().link
    bob.logout()
    world.pass_time(LEASE_PAST_MS, alice)

    alice.add_contact(link)
    bob.login(PASSWORD)
    bob.tick()
    bob.accept(alice.profile().user_id)
    world.pass_time(ClientPolicy().collect_interval_ms, alice)

    assert [v.contact.state for v in alice.contacts()] == [ContactState.ACTIVE]


def test_messages_queued_while_the_mailboxes_are_down_go_out_later(
    world: World,
) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    bob_id = bob.profile().user_id
    bob.logout()
    world.pass_time(LEASE_PAST_MS, alice)
    down = rank_nodes(bob_id, NODES)[:2]
    for node in down:
        world.gateway.set_down(node)

    queued = alice.send(bob_id, "hola")
    assert queued.status is OutgoingStatus.QUEUED

    for node in down:
        world.gateway.set_up(node)
    world.pass_time(30_000, alice)
    bob.login(PASSWORD)
    bob.tick()

    assert texts(bob.history(alice.profile().user_id)) == [(EntryKind.RECEIVED, "hola")]


def test_sending_to_a_stranger_fails(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    with pytest.raises(UnknownContactError):
        alice.send(bob.profile().user_id, "hola")


def test_reject_and_remove(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    alice_id = alice.profile().user_id
    alice.add_contact(bob.profile().link)

    bob.reject(alice_id)

    assert bob.pending_requests() == []
    with pytest.raises(NoPendingRequestError):
        bob.accept(alice_id)
    assert alice.remove_contact(bob.profile().user_id) is True
    assert alice.contacts() == []


# ---------------- direct delivery ----------------


def test_a_logged_out_user_does_not_take_direct_envelopes(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    bob_id = bob.profile().user_id
    bob.logout()  # presence still valid for a while, but nobody listens

    message = alice.send(bob_id, "hola")

    assert message.status is OutgoingStatus.ACCEPTED  # fell back to the mailboxes


def test_receive_direct_returns_the_receive_result(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    queued = alice.send(bob.profile().user_id, "hola")
    envelope = world.stores["alice"].get_outgoing(queued.message_id)
    assert envelope is not None

    again = bob.receive_direct(envelope.envelope)

    assert again.acknowledge and again.message_id == queued.message_id


def test_logout_stops_listening_for_direct_envelopes(world: World) -> None:
    """Checked on the transport itself: with Ray, a listener left behind would
    keep the actor reachable even though nobody is logged in."""
    calls: list[str] = []

    class SpyTransport:
        def __init__(self, user: UserId) -> None:
            self._inner = world.network.transport_for(user)

        def send_direct(
            self, receiver: UserId, envelope: Envelope, timeout_ms: int
        ) -> DirectSendResult:
            return self._inner.send_direct(receiver, envelope, timeout_ms)

        def start_listening(self, on_envelope: Callable[[Envelope], None]) -> None:
            calls.append("start")
            self._inner.start_listening(on_envelope)

        def stop_listening(self) -> None:
            calls.append("stop")
            self._inner.stop_listening()

    app = ClientApp(
        crypto=world.crypto,
        store=MemoryLocalStore(),
        clock=world.clock,
        gateway=world.gateway,
        peers_for=SpyTransport,
    )
    app.register("Alice", PASSWORD)
    app.logout()
    app.login(PASSWORD)
    app.login(PASSWORD)  # logging in again replaces the session

    assert calls == ["start", "stop", "start", "stop", "start"]


def test_a_direct_envelope_that_is_not_accepted_falls_back_to_the_mailbox(
    world: World,
) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    alice_id, bob_id = alice.profile().user_id, bob.profile().user_id
    bob.remove_contact(alice_id)  # Bob no longer knows Alice: unknown sender

    message = alice.send(bob_id, "hola")

    assert message.status is OutgoingStatus.ACCEPTED
    assert bob.history(alice_id) == []


# ---------------- history ----------------


def test_history_shows_a_missing_message_and_fills_it_when_it_arrives(
    world: World,
) -> None:
    alice = world.user("alice")
    bob = world.user("bob")
    befriend(alice, bob)
    bob_id = bob.profile().user_id
    conversation = ConversationId.between(alice.profile().user_id, bob_id)
    store = world.stores["alice"]
    last_seq = max(m.seq for m in store.list_received(conversation))

    def arrives(seq: int, body: str) -> None:
        store.save_incoming_message(
            ReceivedMessage(
                MessageId.new(),
                conversation,
                bob_id,
                seq,
                MessageKind.TEXT,
                body,
                seq,
                seq,
            )
        )

    arrives(last_seq + 2, "segundo")
    assert texts(alice.history(bob_id)) == [
        (EntryKind.MISSING, ""),
        (EntryKind.RECEIVED, "segundo"),
    ]

    arrives(last_seq + 1, "primero")  # arrives late: shown in its place
    assert texts(alice.history(bob_id)) == [
        (EntryKind.RECEIVED, "primero"),
        (EntryKind.RECEIVED, "segundo"),
    ]


def test_history_with_someone_who_never_wrote_is_empty(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    alice.add_contact(bob.profile().link)
    assert alice.history(bob.profile().user_id) == []


def test_history_hides_contact_messages(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    assert alice.history(bob.profile().user_id) == []


def test_history_reports_the_delivery_state_of_sent_messages(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    alice.send(bob.profile().user_id, "hola")

    (entry,) = alice.history(bob.profile().user_id)

    assert entry.kind is EntryKind.SENT
    assert entry.status is OutgoingStatus.DELIVERED


def test_history_limit(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    for i in range(5):
        world.clock.advance(10)
        alice.send(bob.profile().user_id, f"m{i}")

    assert [e.body for e in alice.history(bob.profile().user_id, limit=2)] == [
        "m3",
        "m4",
    ]
    with pytest.raises(ValueError):
        alice.history(bob.profile().user_id, limit=0)


# ---------------- tick ----------------


def test_tick_throttles_its_network_work(world: World) -> None:
    alice = world.user("alice")
    policy = ClientPolicy()

    first = alice.tick()
    second = alice.tick()
    world.clock.advance(policy.collect_interval_ms)
    third = alice.tick()

    assert first.collected is not None  # the first tick after login collects
    assert not first.membership_refreshed  # login itself just refreshed
    assert second.collected is None and not second.membership_refreshed
    assert third.collected is not None and third.membership_refreshed


def test_tick_renews_the_presence_before_it_expires(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)

    world.pass_time(5 * LEASE_PAST_MS, bob)  # Bob's loop keeps his presence alive

    assert alice.send(bob.profile().user_id, "sigues ahí?").status is (
        OutgoingStatus.DELIVERED
    )


def test_tick_skips_an_expired_gap_and_the_late_message_takes_its_place(
    world: World,
) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    alice_id, bob_id = alice.profile().user_id, bob.profile().user_id
    alice.logout()
    world.pass_time(LEASE_PAST_MS, bob)  # Alice's presence expires
    for node in NODES:
        world.gateway.set_down(node)
    bob.send(alice_id, "primero")
    bob.send(alice_id, "segundo")
    first, second = (
        m.envelope
        for m in world.stores["bob"].list_outgoing(statuses=(OutgoingStatus.QUEUED,))
    )
    bob.logout()  # both stay in Bob's outbox
    for node in NODES:
        world.gateway.set_up(node)

    alice.login(PASSWORD)
    assert alice.receive_direct(second).status.value == "buffered"
    gaps: list[GapReport] = []
    for _ in range(130):  # the default gap timeout is 2 minutes
        world.clock.advance(1_000)
        gaps.extend(alice.tick().gaps)

    (gap,) = gaps
    assert (gap.sender, len(gap.skipped)) == (bob_id, 1)
    assert texts(alice.history(bob_id)) == [
        (EntryKind.MISSING, ""),
        (EntryKind.RECEIVED, "segundo"),
    ]

    assert alice.receive_direct(first).status.value == "late"
    assert texts(alice.history(bob_id)) == [
        (EntryKind.RECEIVED, "primero"),
        (EntryKind.RECEIVED, "segundo"),
    ]


# ---------------- policy ----------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"membership_refresh_ms": 0},
        {"collect_interval_ms": -1},
        {"collect_limit": 0},
        {"collect_limit": 10_000},
        {"collect_limit": True},
    ],
)
def test_the_client_policy_rejects_invalid_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        ClientPolicy(**kwargs)
