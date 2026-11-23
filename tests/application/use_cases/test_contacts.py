"""Tests for the contacts use case.

Most tests run the real handshake between two (or three) simulated devices.
Each ``Device`` has its own store and services, wired as the client will be:
``ReceiveMessagesService`` uses ``ContactsService`` as its ``ContactGate``.
``deliver`` plays the network: it hands one device's queued envelopes to
another and marks them delivered.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field

import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.application.ports.errors import StoreError
from source.application.use_cases.contacts import (
    MAX_PENDING_REQUESTS,
    AddOutcome,
    ContactsService,
    InvalidLinkError,
    NoPendingRequestError,
)
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.receive_messages import (
    ReceiveMessagesService,
    ReceiveResult,
    ReceiveStatus,
)
from source.application.use_cases.send_message import SendMessageService
from source.domain.canonical import b64e, canonical_json
from source.domain.contact_link import LINK_PREFIX, ContactLink
from source.domain.contacts import Contact, ContactState
from source.domain.envelope import Envelope, MessageContent, MessageKind
from source.domain.identifiers import Alias, MessageId
from source.domain.outgoing_status import OutgoingStatus
from tests.support.builders import (
    ALICE,
    BASE_TIME_MS,
    BOB,
    CARLA,
    DAVID,
    Person,
    conversation_of,
    make_contact,
    make_identity,
    make_person,
)

CRYPTO = FakeCrypto()
ACTIVE = ContactState.ACTIVE
OUTGOING = ContactState.OUTGOING_REQUEST
INCOMING = ContactState.INCOMING_REQUEST


class FailingSaveStore(MemoryLocalStore):
    """A store whose next ``save_contact`` fails."""

    fail_next_save = False

    def save_contact(self, contact: Contact) -> None:
        if self.fail_next_save:
            self.fail_next_save = False
            raise StoreError("disk full")
        super().save_contact(contact)


@dataclass
class Device:
    """One user's client: a store and the three services, wired together."""

    person: Person
    clock: ManualClock
    store: FailingSaveStore = field(default_factory=FailingSaveStore)

    def __post_init__(self) -> None:
        self.session = UnlockedIdentity(
            make_identity(self.person), self.person.keys.private_key
        )
        self.sender = SendMessageService(CRYPTO, self.store, self.clock)
        self.contacts = ContactsService(CRYPTO, self.store, self.clock, self.sender)
        self.receiver = ReceiveMessagesService(
            CRYPTO, self.store, self.clock, contacts=self.contacts
        )

    def state_of(self, other: Device) -> ContactState | None:
        contact = self.store.get_contact(other.person.user_id)
        return None if contact is None else contact.state

    def outbox_kinds(self) -> list[MessageKind]:
        return [m.kind for m in self.store.list_outgoing()]

    def history_with(self, other: Device) -> list[tuple[MessageKind, str]]:
        conversation = conversation_of(self.person, other.person)
        return [(m.kind, m.body) for m in self.store.list_received(conversation)]


def deliver(source: Device, target: Device) -> list[ReceiveResult]:
    """Hand every due envelope of ``source`` to ``target``, oldest first."""
    results = []
    for message in source.store.list_due_outgoing(source.clock.now_ms()):
        results.append(target.receiver.receive(target.session, message.envelope))
        source.store.update_outgoing(
            dataclasses.replace(message, status=OutgoingStatus.DELIVERED)
        )
    return results


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def alice(clock: ManualClock) -> Device:
    return Device(ALICE, clock)


@pytest.fixture
def bob(clock: ManualClock) -> Device:
    return Device(BOB, clock)


@pytest.fixture
def carla(clock: ManualClock) -> Device:
    return Device(CARLA, clock)


def befriend(a: Device, b: Device) -> None:
    """The full handshake: a adds b, b accepts."""
    a.contacts.add_contact(a.session, b.contacts.my_link(b.session))
    deliver(a, b)
    b.contacts.accept(b.session, a.person.user_id)
    deliver(b, a)


# ---------------- my link and fingerprint ----------------


def test_my_link_is_a_signed_link_to_me(alice: Device) -> None:
    link = ContactLink.from_text(alice.contacts.my_link(alice.session))

    assert link.user_id == ALICE.user_id
    assert link.alias == Alias("Alice")
    assert CRYPTO.verify(link.public_key, link.signing_bytes(), link.signature)


def test_my_fingerprint_is_the_fingerprint_of_my_key(alice: Device) -> None:
    assert alice.contacts.my_fingerprint(alice.session) == CRYPTO.fingerprint(
        ALICE.keys.public_key
    )


# ---------------- adding: invalid links ----------------


@pytest.mark.parametrize("text", ["", "hola", "p08:contact:!!!"])
def test_garbage_is_an_invalid_link(alice: Device, text: str) -> None:
    with pytest.raises(InvalidLinkError):
        alice.contacts.add_contact(alice.session, text)
    assert alice.store.list_contacts() == []
    assert alice.store.list_outgoing() == []


def test_an_invalid_link_error_is_a_value_error() -> None:
    assert issubclass(InvalidLinkError, ValueError)


def test_a_link_whose_identifier_does_not_match_its_key_is_rejected(
    alice: Device, bob: Device
) -> None:
    """The plan's "done when" criterion, through the use case."""
    payload = {
        "v": 1,
        "id": CARLA.user_id.value,
        "pk": b64e(BOB.keys.public_key),
        "alias": "Bob",
        "sig": b64e(b"\x01" * 32),
    }
    forged = LINK_PREFIX + b64e(canonical_json(payload))

    with pytest.raises(InvalidLinkError, match="does not match"):
        alice.contacts.add_contact(alice.session, forged)
    assert alice.store.list_contacts() == []


def test_a_link_with_a_changed_alias_is_rejected(alice: Device, bob: Device) -> None:
    """Someone renamed Bob's link on the way: the signature no longer matches."""
    link = ContactLink.from_text(bob.contacts.my_link(bob.session))
    renamed = dataclasses.replace(link, alias=Alias("Banco Oficial"))

    with pytest.raises(InvalidLinkError, match="signature"):
        alice.contacts.add_contact(alice.session, renamed.to_text())
    assert alice.store.list_outgoing() == []


def test_your_own_link_is_rejected(alice: Device) -> None:
    with pytest.raises(InvalidLinkError, match="own"):
        alice.contacts.add_contact(alice.session, alice.contacts.my_link(alice.session))


# ---------------- adding: the request ----------------


def test_adding_saves_an_outgoing_request_and_queues_a_contact_request(
    alice: Device, bob: Device, clock: ManualClock
) -> None:
    result = alice.contacts.add_contact(
        alice.session, bob.contacts.my_link(bob.session)
    )

    assert result.outcome is AddOutcome.REQUEST_SENT
    assert result.view.contact == Contact(
        BOB.user_id, BOB.keys.public_key, Alias("Bob"), OUTGOING, clock.now_ms()
    )
    assert result.view.fingerprint == CRYPTO.fingerprint(BOB.keys.public_key)
    assert alice.store.get_contact(BOB.user_id) == result.view.contact
    assert alice.outbox_kinds() == [MessageKind.CONTACT_REQUEST]


def test_the_request_carries_my_link_encrypted_for_them(
    alice: Device, bob: Device
) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))
    (request,) = alice.store.list_outgoing()

    content = MessageContent.from_bytes(
        CRYPTO.decrypt(BOB.keys.private_key, request.envelope.ciphertext)
    )

    assert content.kind is MessageKind.CONTACT_REQUEST
    assert ContactLink.from_text(content.body).user_id == ALICE.user_id


def test_adding_twice_sends_one_request(alice: Device, bob: Device) -> None:
    link = bob.contacts.my_link(bob.session)
    alice.contacts.add_contact(alice.session, link)

    again = alice.contacts.add_contact(alice.session, link)

    assert again.outcome is AddOutcome.ALREADY_REQUESTED
    assert alice.outbox_kinds() == [MessageKind.CONTACT_REQUEST]


def test_adding_an_active_contact_changes_nothing(alice: Device, bob: Device) -> None:
    befriend(alice, bob)
    queued = alice.store.list_outgoing()

    result = alice.contacts.add_contact(
        alice.session, bob.contacts.my_link(bob.session)
    )

    assert result.outcome is AddOutcome.ALREADY_CONTACT
    assert alice.store.list_outgoing() == queued


def test_adding_someone_who_already_asked_accepts_them(
    alice: Device, bob: Device
) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))
    deliver(alice, bob)

    result = bob.contacts.add_contact(
        bob.session, alice.contacts.my_link(alice.session)
    )

    assert result.outcome is AddOutcome.ACCEPTED
    assert bob.state_of(alice) is ACTIVE
    assert bob.outbox_kinds() == [MessageKind.CONTACT_ACCEPT]


def test_the_request_is_queued_before_the_contact_is_saved(
    alice: Device, bob: Device
) -> None:
    """If saving fails, the request is already queued, and simply adding again
    works: the duplicate request is harmless for the receiver."""
    link = bob.contacts.my_link(bob.session)
    alice.store.fail_next_save = True

    with pytest.raises(StoreError):
        alice.contacts.add_contact(alice.session, link)
    assert alice.outbox_kinds() == [MessageKind.CONTACT_REQUEST]
    assert alice.state_of(bob) is None

    assert (
        alice.contacts.add_contact(alice.session, link).outcome
        is AddOutcome.REQUEST_SENT
    )
    deliver(alice, bob)
    assert [v.contact.user_id for v in bob.contacts.pending_requests()] == [
        ALICE.user_id
    ]


# ---------------- look-alikes (the "key change" warning) ----------------


def test_a_newcomer_with_the_alias_of_a_contact_is_flagged(
    alice: Device, bob: Device, clock: ManualClock
) -> None:
    befriend(alice, bob)
    impostor = Device(make_person("bob"), clock)  # same alias, another key

    result = alice.contacts.add_contact(
        alice.session, impostor.contacts.my_link(impostor.session)
    )

    assert [c.user_id for c in result.view.look_alikes] == [BOB.user_id]
    assert result.view.fingerprint != CRYPTO.fingerprint(BOB.keys.public_key)


def test_a_request_from_a_look_alike_is_flagged_in_the_inbox(
    alice: Device, bob: Device, clock: ManualClock
) -> None:
    befriend(alice, bob)
    impostor = Device(make_person("BOB"), clock)
    impostor.contacts.add_contact(
        impostor.session, alice.contacts.my_link(alice.session)
    )
    deliver(impostor, alice)

    (request,) = alice.contacts.pending_requests()

    assert request.contact.user_id == impostor.person.user_id
    assert [c.user_id for c in request.look_alikes] == [BOB.user_id]


def test_contacts_with_different_aliases_are_not_look_alikes(
    alice: Device, bob: Device, carla: Device
) -> None:
    befriend(alice, bob)
    befriend(alice, carla)
    assert all(v.look_alikes == () for v in alice.contacts.contacts())


# ---------------- the requests inbox ----------------


def test_a_request_from_a_stranger_lands_in_the_inbox(
    alice: Device, bob: Device, clock: ManualClock
) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))

    (result,) = deliver(alice, bob)

    assert result.status is ReceiveStatus.DELIVERED
    assert result.acknowledge
    (request,) = bob.contacts.pending_requests()
    assert request.contact == Contact(
        ALICE.user_id, ALICE.keys.public_key, Alias("Alice"), INCOMING, clock.now_ms()
    )
    assert request.fingerprint == CRYPTO.fingerprint(ALICE.keys.public_key)
    assert bob.history_with(alice)[0][0] is MessageKind.CONTACT_REQUEST


def test_copies_of_a_request_create_one_pending_request(
    alice: Device, bob: Device
) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))
    (request,) = alice.store.list_outgoing()

    statuses = [
        bob.receiver.receive(bob.session, request.envelope).status for _ in range(3)
    ]

    assert statuses == [ReceiveStatus.DELIVERED] + [ReceiveStatus.DUPLICATE] * 2
    assert len(bob.contacts.pending_requests()) == 1


def test_the_inbox_lists_the_oldest_request_first(
    alice: Device, bob: Device, carla: Device, clock: ManualClock
) -> None:
    for asker in (carla, alice):
        asker.contacts.add_contact(asker.session, bob.contacts.my_link(bob.session))
        deliver(asker, bob)
        clock.advance(1_000)

    assert [v.contact.user_id for v in bob.contacts.pending_requests()] == [
        CARLA.user_id,
        ALICE.user_id,
    ]


def test_a_full_inbox_leaves_new_requests_waiting(
    bob: Device, carla: Device, clock: ManualClock
) -> None:
    for i in range(MAX_PENDING_REQUESTS):
        stranger = make_person(f"stranger-{i}")
        bob.store.save_contact(make_contact(stranger, state=INCOMING))
    carla.contacts.add_contact(carla.session, bob.contacts.my_link(bob.session))
    (request,) = carla.store.list_outgoing()

    first = bob.receiver.receive(bob.session, request.envelope)

    assert first.status is ReceiveStatus.UNKNOWN_SENDER
    assert not first.acknowledge  # it stays in the mailbox, to be retried
    bob.contacts.reject(bob.contacts.pending_requests()[0].contact.user_id)
    assert bob.receiver.receive(bob.session, request.envelope).status is (
        ReceiveStatus.DELIVERED
    )


# ---------------- accept and reject ----------------


def test_accepting_makes_both_sides_active(alice: Device, bob: Device) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))
    deliver(alice, bob)

    view = bob.contacts.accept(bob.session, ALICE.user_id)
    assert view.contact.state is ACTIVE
    assert bob.outbox_kinds() == [MessageKind.CONTACT_ACCEPT]
    assert alice.state_of(bob) is OUTGOING

    deliver(bob, alice)
    assert alice.state_of(bob) is ACTIVE
    assert bob.contacts.pending_requests() == []


def test_after_the_handshake_they_can_chat(alice: Device, bob: Device) -> None:
    befriend(alice, bob)

    alice.sender.send_text(alice.session, BOB.user_id, "¡hola Bob!")
    bob.sender.send_text(bob.session, ALICE.user_id, "¡hola Alice!")
    deliver(alice, bob)
    deliver(bob, alice)

    assert bob.history_with(alice)[-1] == (MessageKind.TEXT, "¡hola Bob!")
    assert alice.history_with(bob)[-1] == (MessageKind.TEXT, "¡hola Alice!")


def test_rejecting_deletes_the_request_and_sends_nothing(
    alice: Device, bob: Device
) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))
    deliver(alice, bob)

    bob.contacts.reject(ALICE.user_id)

    assert bob.state_of(alice) is None
    assert bob.contacts.pending_requests() == []
    assert bob.store.list_outgoing() == []
    assert alice.state_of(bob) is OUTGOING
    assert bob.history_with(alice) != []  # the received request stays in history


def test_a_rejected_user_may_ask_again(alice: Device, bob: Device) -> None:
    link = bob.contacts.my_link(bob.session)
    alice.contacts.add_contact(alice.session, link)
    deliver(alice, bob)
    bob.contacts.reject(ALICE.user_id)

    alice.contacts.remove(BOB.user_id)
    result = alice.contacts.add_contact(alice.session, link)
    deliver(alice, bob)

    assert result.outcome is AddOutcome.REQUEST_SENT
    assert [v.contact.user_id for v in bob.contacts.pending_requests()] == [
        ALICE.user_id
    ]


@pytest.mark.parametrize("state", [None, OUTGOING, ACTIVE])
def test_accept_and_reject_need_a_pending_request(
    bob: Device, state: ContactState | None
) -> None:
    if state is not None:
        bob.store.save_contact(make_contact(ALICE, state=state))
    with pytest.raises(NoPendingRequestError):
        bob.contacts.accept(bob.session, ALICE.user_id)
    with pytest.raises(NoPendingRequestError):
        bob.contacts.reject(ALICE.user_id)


# ---------------- crossings and races (on_released) ----------------


def test_two_users_adding_each_other_at_once_end_up_active(
    alice: Device, bob: Device
) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))
    bob.contacts.add_contact(bob.session, alice.contacts.my_link(alice.session))

    deliver(alice, bob)
    deliver(bob, alice)

    assert alice.state_of(bob) is ACTIVE
    assert bob.state_of(alice) is ACTIVE


def test_a_text_that_overtakes_the_acceptance_is_not_lost(
    alice: Device, bob: Device
) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))
    deliver(alice, bob)
    bob.contacts.accept(bob.session, ALICE.user_id)
    bob.sender.send_text(bob.session, ALICE.user_id, "¡aceptado!")
    accept, text = bob.store.list_outgoing()

    first = alice.receiver.receive(alice.session, text.envelope)
    second = alice.receiver.receive(alice.session, accept.envelope)

    assert first.status is ReceiveStatus.BUFFERED
    assert second.status is ReceiveStatus.DELIVERED
    assert alice.state_of(bob) is ACTIVE
    assert [kind for kind, _ in alice.history_with(bob)] == [
        MessageKind.CONTACT_ACCEPT,
        MessageKind.TEXT,
    ]


def test_a_contact_who_lost_us_is_accepted_again_automatically(
    alice: Device, bob: Device
) -> None:
    befriend(alice, bob)
    alice.contacts.remove(BOB.user_id)
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))

    deliver(alice, bob)  # Bob still has Alice as ACTIVE
    assert bob.outbox_kinds()[-1] is MessageKind.CONTACT_ACCEPT
    deliver(bob, alice)

    assert alice.state_of(bob) is ACTIVE


def test_on_released_is_idempotent(alice: Device, bob: Device) -> None:
    alice.contacts.add_contact(alice.session, bob.contacts.my_link(bob.session))
    deliver(alice, bob)
    bob.contacts.accept(bob.session, ALICE.user_id)
    deliver(bob, alice)
    stored = alice.store.list_received(conversation_of(ALICE, BOB))

    alice.contacts.on_released(alice.session, stored)
    alice.contacts.on_released(alice.session, stored)

    assert alice.state_of(bob) is ACTIVE
    assert alice.store.list_outgoing()[-1].kind is MessageKind.CONTACT_REQUEST


def test_on_released_ignores_messages_from_unknown_senders(
    alice: Device, bob: Device
) -> None:
    befriend(alice, bob)
    alice.contacts.remove(BOB.user_id)
    queued = alice.store.list_outgoing()

    alice.contacts.on_released(
        alice.session, alice.store.list_received(conversation_of(ALICE, BOB))
    )

    assert alice.state_of(bob) is None
    assert alice.store.list_outgoing() == queued


def test_on_released_ignores_plain_texts(alice: Device, bob: Device) -> None:
    befriend(alice, bob)
    bob.sender.send_text(bob.session, ALICE.user_id, "hola")
    deliver(bob, alice)
    queued = alice.store.list_outgoing()
    texts = [
        m
        for m in alice.store.list_received(conversation_of(ALICE, BOB))
        if m.kind is MessageKind.TEXT
    ]

    alice.contacts.on_released(alice.session, texts)

    assert alice.state_of(bob) is ACTIVE
    assert alice.store.list_outgoing() == queued


# ---------------- introductions that must fail ----------------


def stranger_envelope(
    *,
    sender: Person = CARLA,
    receiver: Person = BOB,
    kind: MessageKind = MessageKind.CONTACT_REQUEST,
    body: str | None = None,
    link_owner: Person | None = None,
    link_alias: str | None = None,
    sign_with: Person | None = None,
    encrypt_for: Person | None = None,
) -> Envelope:
    """A message from a stranger, built by hand to forge every part of it."""
    owner = link_owner or sender
    link = ContactLink.build(
        public_key=owner.keys.public_key,
        alias=Alias(link_alias or owner.name),
        sign=lambda data: CRYPTO.sign(owner.keys.private_key, data),
    )
    content = MessageContent(
        kind,
        conversation_of(sender, receiver),
        1,
        BASE_TIME_MS,
        link.to_text() if body is None else body,
    )
    return Envelope.build(
        message_id=MessageId.new(),
        sender=sender.user_id,
        receiver=receiver.user_id,
        ciphertext=CRYPTO.encrypt_for(
            (encrypt_for or receiver).keys.public_key, content.to_bytes()
        ),
        sign=lambda data: CRYPTO.sign((sign_with or sender).keys.private_key, data),
    )


def test_a_valid_hand_made_request_is_introduced(bob: Device) -> None:
    """Control case for the forgeries below: the helper itself is sound."""
    contact = bob.contacts.introduce(bob.session, stranger_envelope())
    assert contact is not None and contact.state is INCOMING


@pytest.mark.parametrize(
    ("case", "envelope"),
    [
        (
            "a text, not a request",
            stranger_envelope(kind=MessageKind.TEXT, body="hola"),
        ),
        (
            "an acceptance from a stranger",
            stranger_envelope(kind=MessageKind.CONTACT_ACCEPT, body=""),
        ),
        ("a request whose body is not a link", stranger_envelope(body="hola")),
        ("a link of someone else", stranger_envelope(link_owner=DAVID)),
        ("the envelope signed by another key", stranger_envelope(sign_with=DAVID)),
        ("encrypted for someone else", stranger_envelope(encrypt_for=DAVID)),
        ("addressed to someone else", stranger_envelope(receiver=DAVID)),
    ],
)
def test_a_forged_or_misdirected_introduction_creates_nothing(
    bob: Device, case: str, envelope: Envelope
) -> None:
    assert bob.contacts.introduce(bob.session, envelope) is None
    assert bob.store.list_contacts() == []


def test_a_link_with_a_bad_signature_is_not_introduced(bob: Device) -> None:
    """Carla's real link, renamed on the way: its signature no longer matches."""
    link = ContactLink.build(
        public_key=CARLA.keys.public_key,
        alias=Alias("Carla"),
        sign=lambda data: CRYPTO.sign(CARLA.keys.private_key, data),
    )
    renamed = dataclasses.replace(link, alias=Alias("Banco Oficial")).to_text()

    assert bob.contacts.introduce(bob.session, stranger_envelope(body=renamed)) is None


def test_a_forged_introduction_is_reported_as_unknown_and_left_waiting(
    bob: Device,
) -> None:
    result = bob.receiver.receive(bob.session, stranger_envelope(sign_with=DAVID))
    assert result.status is ReceiveStatus.UNKNOWN_SENDER
    assert not result.acknowledge


# ---------------- views ----------------


def test_view_of_an_unknown_user_is_none(alice: Device) -> None:
    assert alice.contacts.view(BOB.user_id) is None


def test_contacts_are_listed_by_alias_and_can_be_filtered(
    alice: Device, bob: Device, carla: Device
) -> None:
    befriend(alice, carla)
    befriend(alice, bob)
    alice.store.save_contact(make_contact(DAVID, state=OUTGOING))

    assert [v.contact.alias.value for v in alice.contacts.contacts()] == [
        "Bob",
        "Carla",
        "David",
    ]
    assert [v.contact.user_id for v in alice.contacts.contacts(OUTGOING)] == [
        DAVID.user_id
    ]
    view = alice.contacts.view(BOB.user_id)
    assert view is not None and view.fingerprint == CRYPTO.fingerprint(
        BOB.keys.public_key
    )


def test_remove_reports_whether_the_contact_existed(alice: Device, bob: Device) -> None:
    befriend(alice, bob)
    assert alice.contacts.remove(BOB.user_id) is True
    assert alice.contacts.remove(BOB.user_id) is False
