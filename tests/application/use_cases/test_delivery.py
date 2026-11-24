"""Tests for the delivery policy.

With a fake clock and fake transports, a failed
direct send falls back to the mailbox, and retries get further apart.

Most tests use ``FakeMailbox`` and ``FakePresence``: small scriptable doubles
of ``QuorumClient`` and ``DirectoryClient`` that make any
failure easy to stage. The last section runs the policy against the REAL
``QuorumClient`` and ``DirectoryClient`` over the in-memory cluster, which runs
the real node services: that is what checks that both halves fit.
"""

from __future__ import annotations

import dataclasses
import random
from dataclasses import dataclass, field

import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.adapters.memory.peer_transport import InMemoryPeerNetwork
from source.application.ports.errors import StoreError
from source.application.use_cases.client_directory import DirectoryClient
from source.application.use_cases.delivery import (
    DeliveryOutcome,
    DeliveryPolicy,
    DeliveryReport,
    DeliveryService,
    MailboxDepositor,
    PresenceLookup,
)
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.membership import MembershipService
from source.application.use_cases.quorum import QuorumClient
from source.application.use_cases.receive_messages import ReceiveMessagesService
from source.application.use_cases.send_message import SendMessageService
from source.domain.envelope import Envelope
from source.domain.identifiers import ConversationId, UserId
from source.domain.messages import OutgoingMessage
from source.domain.node_id import NodeId
from source.domain.outgoing_status import OutgoingStatus
from source.domain.placement import rank_nodes
from source.domain.records import DirectSendResult
from tests.support.builders import (
    ALICE,
    BOB,
    CARLA,
    Person,
    ack_request,
    collect_request,
    conversation_of,
    make_contact,
    make_identity,
)

DIRECT, MAILBOX, RETRY, EXPIRED, WAITING = (
    DeliveryOutcome.DELIVERED_DIRECT,
    DeliveryOutcome.ACCEPTED_BY_MAILBOX,
    DeliveryOutcome.RETRY_SCHEDULED,
    DeliveryOutcome.EXPIRED,
    DeliveryOutcome.WAITING_FOR_EARLIER,
)
POLICY = DeliveryPolicy(
    direct_timeout_ms=1_500,
    backoff_base_ms=1_000,
    backoff_cap_ms=60_000,
    max_attempts=5,
)
WITH_BOB = conversation_of(ALICE, BOB)
WITH_CARLA = conversation_of(ALICE, CARLA)


def session_of(person: Person) -> UnlockedIdentity:
    return UnlockedIdentity(make_identity(person), person.keys.private_key)


# -- stand-ins for the other half ------------------------------------------------


@dataclass(frozen=True)
class DepositResult:
    """Same shape as ``QuorumClient``'s ``WriteResult``, reduced to what delivery reads."""

    accepted: bool


@dataclass
class FakeMailbox:
    """Scriptable double of ``QuorumClient``, reduced to ``deposit``.

    Deposits for a receiver in ``unreachable`` are always refused (their
    mailbox nodes are down). Otherwise ``script`` answers the next deposits in
    order (True = accepted) and, when it runs out, ``default`` answers.
    """

    default: bool = True
    script: list[bool] = field(default_factory=list)
    unreachable: set[UserId] = field(default_factory=set)
    deposited: list[Envelope] = field(default_factory=list)

    def deposit(self, envelope: Envelope) -> DepositResult:
        if envelope.receiver in self.unreachable:
            return DepositResult(False)
        accepted = self.script.pop(0) if self.script else self.default
        if accepted:
            self.deposited.append(envelope)
        return DepositResult(accepted)


@dataclass
class FakePresence:
    """Scriptable double of ``DirectoryClient``, reduced to ``is_online``."""

    online: set[UserId] = field(default_factory=set)
    lookups: list[UserId] = field(default_factory=list)

    def is_online(self, user_id: UserId) -> bool:
        self.lookups.append(user_id)
        return user_id in self.online


# -- peers --------------------------------------------------------------------


@dataclass
class SpyPeers:
    """A peer transport that records calls and answers from a fixed rule."""

    reachable: set[UserId] = field(default_factory=set)
    calls: list[tuple[UserId, int]] = field(default_factory=list)

    def send_direct(
        self, receiver: UserId, envelope: Envelope, timeout_ms: int
    ) -> DirectSendResult:
        self.calls.append((receiver, timeout_ms))
        return (
            DirectSendResult.DELIVERED
            if receiver in self.reachable
            else DirectSendResult.UNREACHABLE
        )

    def start_listening(self, on_envelope: object) -> None:  # pragma: no cover
        raise NotImplementedError

    def stop_listening(self) -> None:  # pragma: no cover
        raise NotImplementedError


class FailingUpdateStore(MemoryLocalStore):
    def update_outgoing(self, message: OutgoingMessage) -> None:
        raise StoreError("disk full")


# -- fixtures -----------------------------------------------------------------


@dataclass
class World:
    clock: ManualClock
    store: MemoryLocalStore
    peers: SpyPeers
    mailbox: FakeMailbox
    presence: FakePresence
    sender: SendMessageService
    delivery: DeliveryService

    def queue(self, to: Person = BOB, body: str = "hola") -> OutgoingMessage:
        return self.sender.send_text(session_of(ALICE), to.user_id, body)

    def status(self, message: OutgoingMessage) -> OutgoingMessage:
        found = self.store.get_outgoing(message.message_id)
        assert found is not None
        return found

    def advance_to_next_attempt(self) -> None:
        when = self.delivery.next_attempt_ms()
        assert when is not None
        self.clock.advance(when - self.clock.now_ms())


def make_world(store: MemoryLocalStore | None = None, seed: int = 7) -> World:
    clock = ManualClock()
    store = store if store is not None else MemoryLocalStore()
    for person in (BOB, CARLA):
        store.save_contact(make_contact(person))
    peers, mailbox, presence = SpyPeers(), FakeMailbox(), FakePresence()
    sender = SendMessageService(FakeCrypto(), store, clock)
    delivery = DeliveryService(
        store, peers, mailbox, presence, clock, POLICY, random.Random(seed)
    )
    return World(clock, store, peers, mailbox, presence, sender, delivery)


@pytest.fixture
def world() -> World:
    return make_world()


def outcomes(report: DeliveryReport) -> list[DeliveryOutcome]:
    return [outcome for _, outcome in report.outcomes]


# ---------------- choosing the path ----------------


def test_an_online_receiver_gets_it_directly_without_any_mailbox(world: World) -> None:
    world.presence.online.add(BOB.user_id)
    world.peers.reachable.add(BOB.user_id)
    message = world.queue()

    report = world.delivery.deliver_due()

    assert report.of(message.message_id) is DIRECT
    assert world.status(message).status is OutgoingStatus.DELIVERED
    assert world.mailbox.deposited == []
    assert world.peers.calls == [(BOB.user_id, POLICY.direct_timeout_ms)]


def test_a_failed_direct_send_falls_back_to_the_mailbox(world: World) -> None:
    """The plan's first criterion."""
    world.presence.online.add(
        BOB.user_id
    )  # presence says yes, the peer does not answer
    message = world.queue()

    report = world.delivery.deliver_due()

    assert report.of(message.message_id) is MAILBOX
    assert world.peers.calls == [(BOB.user_id, POLICY.direct_timeout_ms)]
    assert world.mailbox.deposited == [message.envelope]
    stored = world.status(message)
    assert stored.status is OutgoingStatus.ACCEPTED
    assert stored.attempts == 0  # a missed direct send is not a failed attempt
    assert stored.next_attempt_at_ms is None


def test_an_offline_receiver_goes_straight_to_the_mailbox(world: World) -> None:
    message = world.queue()

    assert world.delivery.deliver_due().of(message.message_id) is MAILBOX
    assert world.peers.calls == []


def test_presence_is_looked_up_once_per_receiver_per_round(world: World) -> None:
    for i in range(3):
        world.queue(BOB, f"b{i}")
    world.queue(CARLA)

    world.delivery.deliver_due()

    assert sorted(world.presence.lookups) == sorted([BOB.user_id, CARLA.user_id])


def test_after_a_direct_miss_the_rest_of_the_round_skips_the_direct_path(
    world: World,
) -> None:
    world.presence.online.add(BOB.user_id)
    for i in range(3):
        world.queue(BOB, f"b{i}")

    report = world.delivery.deliver_due()

    assert outcomes(report) == [MAILBOX] * 3
    assert len(world.peers.calls) == 1


def test_accepted_messages_are_not_sent_again(world: World) -> None:
    world.queue()
    world.delivery.deliver_due()

    assert world.delivery.deliver_due() == DeliveryReport()
    assert len(world.mailbox.deposited) == 1


# ---------------- retries and backoff ----------------


def test_a_refused_deposit_schedules_a_retry_within_the_first_backoff(
    world: World,
) -> None:
    world.mailbox.default = False
    message = world.queue()
    now = world.clock.now_ms()

    report = world.delivery.deliver_due()

    assert report.of(message.message_id) is RETRY
    stored = world.status(message)
    assert stored.status is OutgoingStatus.QUEUED
    assert stored.attempts == 1
    assert stored.next_attempt_at_ms is not None
    assert now + 500 <= stored.next_attempt_at_ms <= now + 1_000


def test_a_scheduled_retry_waits_for_its_time(world: World) -> None:
    world.mailbox.default = False
    message = world.queue()
    world.delivery.deliver_due()
    retry_at = world.status(message).next_attempt_at_ms
    assert retry_at is not None

    world.clock.advance(retry_at - world.clock.now_ms() - 1)
    assert world.delivery.deliver_due() == DeliveryReport()

    world.clock.advance(1)
    world.mailbox.default = True
    assert world.delivery.deliver_due().of(message.message_id) is MAILBOX


@pytest.mark.parametrize("seed", range(20))
def test_retries_get_further_apart_until_the_cap(seed: int) -> None:
    """The plan's second criterion, for many jitter draws."""
    world = make_world(seed=seed)
    world = dataclasses.replace(
        world,
        delivery=DeliveryService(
            world.store,
            world.peers,
            world.mailbox,
            world.presence,
            world.clock,
            DeliveryPolicy(
                backoff_base_ms=1_000, backoff_cap_ms=16_000, max_attempts=10
            ),
            random.Random(seed),
        ),
    )
    world.mailbox.default = False
    message = world.queue()
    waits = []

    for _ in range(9):
        before = world.clock.now_ms()
        world.delivery.deliver_due()
        retry_at = world.status(message).next_attempt_at_ms
        assert retry_at is not None
        waits.append(retry_at - before)
        world.advance_to_next_attempt()

    ceilings = [1_000, 2_000, 4_000, 8_000, 16_000, 16_000, 16_000, 16_000, 16_000]
    for wait, ceiling in zip(waits, ceilings, strict=True):
        assert ceiling // 2 <= wait <= ceiling
    # The guaranteed minimum doubles until the cap: retries spread out.
    assert waits[4] > waits[0]


def test_the_jitter_spreads_clients_that_failed_together() -> None:
    retry_times = set()
    for seed in range(10):
        world = make_world(seed=seed)
        world.mailbox.default = False
        message = world.queue()
        world.delivery.deliver_due()
        retry_times.add(world.status(message).next_attempt_at_ms)
    assert len(retry_times) > 1


def test_a_message_expires_after_the_attempt_limit(world: World) -> None:
    world.mailbox.default = False
    message = world.queue()
    seen = []

    for _ in range(POLICY.max_attempts):
        seen.append(world.delivery.deliver_due().of(message.message_id))
        if world.status(message).status is OutgoingStatus.QUEUED:
            world.advance_to_next_attempt()

    assert seen == [RETRY] * (POLICY.max_attempts - 1) + [EXPIRED]
    stored = world.status(message)
    assert stored.status is OutgoingStatus.EXPIRED
    assert stored.attempts == POLICY.max_attempts
    assert stored.next_attempt_at_ms is None
    assert world.delivery.next_attempt_ms() is None


def test_a_retry_that_succeeds_clears_the_retry_time(world: World) -> None:
    world.mailbox.script = [False]
    message = world.queue()
    world.delivery.deliver_due()
    world.advance_to_next_attempt()

    world.delivery.deliver_due()

    stored = world.status(message)
    assert (stored.status, stored.attempts, stored.next_attempt_at_ms) == (
        OutgoingStatus.ACCEPTED,
        1,
        None,
    )


# ---------------- per-conversation order (head of line) ----------------


def test_a_failed_message_holds_back_the_rest_of_its_conversation(
    world: World,
) -> None:
    _, second, third = (world.queue(BOB, f"b{i}") for i in range(3))
    world.mailbox.script = [False]

    report = world.delivery.deliver_due()

    assert outcomes(report) == [RETRY, WAITING, WAITING]
    assert world.mailbox.deposited == []
    assert world.status(second).status is OutgoingStatus.QUEUED
    assert world.status(third).attempts == 0


def test_the_held_messages_go_as_soon_as_the_head_does(world: World) -> None:
    messages = [world.queue(BOB, f"b{i}") for i in range(3)]
    world.mailbox.script = [False]
    world.delivery.deliver_due()

    world.advance_to_next_attempt()
    report = world.delivery.deliver_due()

    assert outcomes(report) == [MAILBOX] * 3
    assert world.mailbox.deposited == [m.envelope for m in messages]  # in seq order


def test_a_blocked_conversation_does_not_block_the_others(world: World) -> None:
    to_bob = world.queue(BOB)
    to_carla = world.queue(CARLA)
    world.mailbox.unreachable.add(BOB.user_id)

    report = world.delivery.deliver_due()

    assert report.of(to_bob.message_id) is RETRY
    assert report.of(to_carla.message_id) is MAILBOX


def test_an_expired_head_lets_the_rest_go(world: World) -> None:
    head = world.queue(BOB, "head")
    rest = world.queue(BOB, "rest")
    world.mailbox.script = [False] * POLICY.max_attempts

    while world.status(head).status is OutgoingStatus.QUEUED:
        world.delivery.deliver_due()
        if world.delivery.next_attempt_ms() is not None:
            world.advance_to_next_attempt()

    assert world.status(head).status is OutgoingStatus.EXPIRED
    assert world.status(rest).status is OutgoingStatus.ACCEPTED


def test_next_attempt_ignores_messages_waiting_behind_their_head(
    world: World,
) -> None:
    """Otherwise the caller would wake up "now" for messages it cannot send."""
    world.queue(BOB, "head")
    world.queue(BOB, "behind")
    world.mailbox.script = [False]
    world.delivery.deliver_due()

    head_retry = world.delivery.next_attempt_ms()

    assert head_retry is not None and head_retry > world.clock.now_ms()


# ---------------- scheduling ----------------


def test_nothing_queued_means_no_next_attempt(world: World) -> None:
    assert world.delivery.next_attempt_ms() is None


def test_a_new_message_is_due_now(world: World) -> None:
    world.queue()
    assert world.delivery.next_attempt_ms() == world.clock.now_ms()


def test_a_round_takes_at_most_batch_limit_messages() -> None:
    world = make_world()
    world = dataclasses.replace(
        world,
        delivery=DeliveryService(
            world.store,
            world.peers,
            world.mailbox,
            world.presence,
            world.clock,
            DeliveryPolicy(batch_limit=2),
        ),
    )
    for i in range(5):
        world.queue(BOB, f"b{i}")

    assert len(world.delivery.deliver_due().outcomes) == 2


def test_a_store_failure_propagates_and_the_message_is_retried_later() -> None:
    """At-least-once: the outcome was not saved, so the message stays QUEUED
    and goes out again; the receiver drops the copy as a duplicate."""
    world = make_world(store=FailingUpdateStore())
    message = world.queue()

    with pytest.raises(StoreError):
        world.delivery.deliver_due()

    assert world.status(message).status is OutgoingStatus.QUEUED
    assert world.mailbox.deposited == [message.envelope]


# ---------------- policy and report ----------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"direct_timeout_ms": 0},
        {"backoff_base_ms": -1},
        {"max_attempts": 0},
        {"batch_limit": True},
        {"backoff_cap_ms": 1.5},
        {"backoff_base_ms": 2_000, "backoff_cap_ms": 1_000},
    ],
)
def test_the_policy_rejects_invalid_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        DeliveryPolicy(**kwargs)


def test_backoff_ceilings_double_and_cap() -> None:
    policy = DeliveryPolicy(backoff_base_ms=1_000, backoff_cap_ms=5_000)
    assert [policy.backoff_ceiling_ms(n) for n in range(1, 6)] == [
        1_000,
        2_000,
        4_000,
        5_000,
        5_000,
    ]
    assert policy.backoff_ceiling_ms(10**6) == 5_000  # no huge integers


@pytest.mark.parametrize("attempts", [0, -1, True])
def test_backoff_needs_at_least_one_failed_attempt(attempts: int) -> None:
    with pytest.raises(ValueError):
        DeliveryPolicy().backoff_ceiling_ms(attempts)


def test_report_helpers(world: World) -> None:
    world.mailbox.script = [False]
    first = world.queue(BOB)
    world.queue(BOB)

    report = world.delivery.deliver_due()

    assert report.count(RETRY) == 1
    assert report.count(WAITING) == 1
    assert report.of(first.message_id) is RETRY
    assert world.delivery.policy is POLICY


# ---------------- end to end with the receiver ----------------


def test_end_to_end_direct_and_mailbox_paths() -> None:
    """Alice delivers to Bob over the in-memory peer network. Bob's listener is
    his ReceiveMessagesService: a direct send only counts as delivered when Bob
    acknowledged it. While Bob is offline, messages go to the (fake) mailbox;
    when he comes back he collects them, and his history is complete and in
    order."""
    clock = ManualClock()
    alice_store, bob_store = MemoryLocalStore(), MemoryLocalStore()
    alice_store.save_contact(make_contact(BOB))
    bob_store.save_contact(make_contact(ALICE))
    crypto = FakeCrypto()
    network = InMemoryPeerNetwork()
    bob_session = session_of(BOB)
    bob_receiver = ReceiveMessagesService(crypto, bob_store, clock)

    def bob_listener(envelope: Envelope) -> None:
        if not bob_receiver.receive(bob_session, envelope).acknowledge:
            raise RuntimeError("not accepted")

    mailbox, presence = FakeMailbox(), FakePresence()
    sender = SendMessageService(crypto, alice_store, clock)
    delivery = DeliveryService(
        alice_store,
        network.transport_for(ALICE.user_id),
        mailbox,
        presence,
        clock,
        POLICY,
        random.Random(1),
    )
    alice = session_of(ALICE)
    bob_transport = network.transport_for(BOB.user_id)

    # Bob offline: two messages wait in the mailbox.
    sender.send_text(alice, BOB.user_id, "uno")
    sender.send_text(alice, BOB.user_id, "dos")
    assert outcomes(delivery.deliver_due()) == [MAILBOX, MAILBOX]

    # Bob comes online and announces it.
    bob_transport.start_listening(bob_listener)
    presence.online.add(BOB.user_id)
    sender.send_text(alice, BOB.user_id, "tres")
    assert outcomes(delivery.deliver_due()) == [DIRECT]

    # Bob collects what waited for him.
    bob_receiver.receive_batch(bob_session, mailbox.deposited)

    conversation = ConversationId.between(ALICE.user_id, BOB.user_id)
    assert [m.body for m in bob_store.list_received(conversation)] == [
        "uno",
        "dos",
        "tres",
    ]


# ---------------- integration with the real node side ----------------

NODES = [NodeId(f"node-{i}") for i in (1, 2, 3)]


class LiveSystem:
    """Alice delivers to Bob through the REAL QuorumClient and DirectoryClient.

    The cluster is the in-memory gateway, which runs the real node services
    (mailbox and directory), so signatures, leases and quorums are all
    checked for real. Only Ray is missing.
    """

    def __init__(self) -> None:
        self.clock = ManualClock()
        self.crypto = FakeCrypto()
        self.gateway = InMemoryNodeGateway(NODES, self.clock, self.crypto)
        network = InMemoryPeerNetwork()

        self.alice_store = MemoryLocalStore()
        self.alice_store.save_contact(make_contact(BOB))
        self.alice_quorum = self._quorum()
        self.alice_directory = DirectoryClient(
            session_of(ALICE), self.alice_quorum, self.crypto, self.clock
        )
        self.sender = SendMessageService(self.crypto, self.alice_store, self.clock)
        self.peers = network.transport_for(ALICE.user_id)
        self.delivery = DeliveryService(
            self.alice_store,
            self.peers,
            self.alice_quorum,
            self.alice_directory,
            self.clock,
            POLICY,
            random.Random(3),
        )

        self.bob_store = MemoryLocalStore()
        self.bob_store.save_contact(make_contact(ALICE))
        self.bob_session = session_of(BOB)
        self.bob_quorum = self._quorum()
        self.bob_directory = DirectoryClient(
            self.bob_session, self.bob_quorum, self.crypto, self.clock
        )
        self.bob_receiver = ReceiveMessagesService(
            self.crypto, self.bob_store, self.clock
        )
        self.bob_transport = network.transport_for(BOB.user_id)

    def _quorum(self) -> QuorumClient:
        return QuorumClient(
            self.gateway, MembershipService(self.clock), self.crypto, self.clock
        )

    def app_tick(self) -> None:
        """What both clients' main loops do periodically: ping every node.

        Required: membership declares DEAD a node whose last success is older
        than its dead timeout, even if it never failed, and a DEAD node is
        not asked at all. A client that stayed silent too long would collect
        nothing and deposit nowhere until it pings again.
        """
        self.alice_quorum.refresh_membership()
        self.bob_quorum.refresh_membership()

    def send(self, body: str) -> OutgoingMessage:
        return self.sender.send_text(session_of(ALICE), BOB.user_id, body)

    def status(self, message: OutgoingMessage) -> OutgoingStatus:
        found = self.alice_store.get_outgoing(message.message_id)
        assert found is not None
        return found.status

    def bob_goes_online(self) -> None:
        """Bob listens for direct envelopes and announces his presence."""

        def listener(envelope: Envelope) -> None:
            if not self.bob_receiver.receive(self.bob_session, envelope).acknowledge:
                raise RuntimeError("not accepted")

        self.bob_transport.start_listening(listener)
        assert self.bob_directory.publish().accepted

    def bob_pending_in_mailbox(self) -> int:
        """How many envelopes Bob's nodes still hold for him (read-only)."""
        request = collect_request(BOB, self.clock.now_ms(), limit=100)
        return len(self.bob_quorum.collect(request).envelopes)

    def bob_collects(self) -> None:
        """What Bob's client does on connecting: collect, store, acknowledge."""
        now = self.clock.now_ms()
        envelopes = self.bob_quorum.collect(collect_request(BOB, now, limit=100))
        batch = self.bob_receiver.receive_batch(self.bob_session, envelopes.envelopes)
        if batch.to_acknowledge:
            self.bob_quorum.acknowledge(ack_request(BOB, now, batch.to_acknowledge))

    def bob_history(self) -> list[str]:
        conversation = ConversationId.between(ALICE.user_id, BOB.user_id)
        return [m.body for m in self.bob_store.list_received(conversation)]


@pytest.fixture
def live() -> LiveSystem:
    return LiveSystem()


def test_the_real_collaborators_satisfy_the_interfaces(live: LiveSystem) -> None:
    """Checked by mypy: QuorumClient and DirectoryClient fit what delivery needs."""
    depositor: MailboxDepositor = live.alice_quorum
    presence: PresenceLookup = live.alice_directory
    assert depositor is live.alice_quorum and presence is live.alice_directory


def test_live_an_offline_receiver_collects_from_the_mailboxes(
    live: LiveSystem,
) -> None:
    sent = [live.send("uno"), live.send("dos")]

    assert outcomes(live.delivery.deliver_due()) == [MAILBOX, MAILBOX]
    assert [live.status(m) for m in sent] == [OutgoingStatus.ACCEPTED] * 2
    assert live.bob_pending_in_mailbox() == 2

    live.bob_collects()

    assert live.bob_history() == ["uno", "dos"]
    assert live.bob_pending_in_mailbox() == 0  # acknowledged on every node


def test_live_an_online_receiver_gets_it_directly(live: LiveSystem) -> None:
    live.bob_goes_online()
    message = live.send("hola")

    assert outcomes(live.delivery.deliver_due()) == [DIRECT]
    assert live.status(message) is OutgoingStatus.DELIVERED
    assert live.bob_history() == ["hola"]
    assert live.bob_pending_in_mailbox() == 0  # never touched a mailbox


def test_live_an_expired_presence_means_the_mailbox(live: LiveSystem) -> None:
    """Bob still listens, but stopped renewing: his lease ends, so he looks
    offline and the direct path is not even tried."""
    live.bob_goes_online()
    live.clock.advance(60_000)  # past the 30 s default lease
    live.app_tick()
    live.send("hola")

    assert outcomes(live.delivery.deliver_due()) == [MAILBOX]
    assert live.bob_history() == []
    assert live.bob_pending_in_mailbox() == 1


def test_live_one_mailbox_node_down_is_tolerated(live: LiveSystem) -> None:
    live.gateway.set_down(rank_nodes(BOB.user_id, NODES)[0])
    live.send("hola")

    assert outcomes(live.delivery.deliver_due()) == [MAILBOX]

    live.bob_collects()
    assert live.bob_history() == ["hola"]


def test_live_two_mailbox_nodes_down_retry_until_they_return(
    live: LiveSystem,
) -> None:
    for node in rank_nodes(BOB.user_id, NODES)[:2]:
        live.gateway.set_down(node)
    message = live.send("hola")

    assert outcomes(live.delivery.deliver_due()) == [RETRY]
    assert live.status(message) is OutgoingStatus.QUEUED

    for node in NODES:
        live.gateway.set_up(node)
    live.app_tick()
    retry_at = live.delivery.next_attempt_ms()
    assert retry_at is not None
    live.clock.advance(retry_at - live.clock.now_ms())

    assert outcomes(live.delivery.deliver_due()) == [MAILBOX]
    live.bob_collects()
    assert live.bob_history() == ["hola"]
