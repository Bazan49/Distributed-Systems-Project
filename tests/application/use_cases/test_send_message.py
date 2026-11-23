"""Tests for the send-message use case."""

from __future__ import annotations

import pytest
from nacl import pwhash

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.system.crypto import NaClCrypto
from source.application.ports.crypto import CryptoPort
from source.application.ports.errors import AlreadyExistsError, DecryptionError
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.send_message import (
    ContactNotActiveError,
    SendMessageService,
    UnknownContactError,
)
from source.domain.canonical import FormatError
from source.domain.contacts import Contact, ContactState
from source.domain.envelope import (
    MAX_BODY_BYTES,
    Envelope,
    MessageContent,
    MessageKind,
)
from source.domain.identifiers import Alias, ConversationId, UserId
from source.domain.identity import LocalIdentity
from source.domain.outgoing_status import OutgoingStatus
from source.domain.records import KeyPair
from tests.support.builders import (
    ALICE,
    BOB,
    CARLA,
    Person,
    conversation_of,
    make_contact,
    make_identity,
)

WITH_BOB = conversation_of(ALICE, BOB)


def session_of(person: Person) -> UnlockedIdentity:
    return UnlockedIdentity(make_identity(person), person.keys.private_key)


def open_as_receiver(
    crypto: CryptoPort,
    envelope: Envelope,
    sender_public_key: bytes,
    receiver_private_key: bytes,
) -> MessageContent:
    """What the receiving side does with an envelope it was handed."""
    assert crypto.verify(
        sender_public_key, envelope.signing_bytes(), envelope.signature
    )
    content = MessageContent.from_bytes(
        crypto.decrypt(receiver_private_key, envelope.ciphertext)
    )
    envelope.ensure_content_matches(content)
    return content


class RacingStore(MemoryLocalStore):
    """Another sender used the next seq between our read and our write."""

    def last_outgoing_seq(self, conversation_id: ConversationId) -> int:
        return 0


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def store() -> MemoryLocalStore:
    store = MemoryLocalStore()
    store.save_contact(make_contact(BOB, state=ContactState.ACTIVE))
    return store


@pytest.fixture
def crypto() -> FakeCrypto:
    return FakeCrypto()


@pytest.fixture
def service(
    crypto: FakeCrypto, store: MemoryLocalStore, clock: ManualClock
) -> SendMessageService:
    return SendMessageService(crypto, store, clock)


ALICE_SESSION = session_of(ALICE)


# ---------------- the message that is queued ----------------


def test_a_text_is_queued_ready_to_be_delivered(
    service: SendMessageService, store: MemoryLocalStore, clock: ManualClock
) -> None:
    message = service.send_text(ALICE_SESSION, BOB.user_id, "hola")

    assert message.kind is MessageKind.TEXT
    assert message.body == "hola"
    assert message.conversation_id == WITH_BOB
    assert message.seq == 1
    assert message.sent_at_ms == clock.now_ms()
    assert message.status is OutgoingStatus.QUEUED
    assert message.attempts == 0
    assert message.next_attempt_at_ms is None
    assert store.get_outgoing(message.message_id) == message
    assert store.list_due_outgoing(clock.now_ms()) == [message]


def test_the_envelope_names_sender_and_receiver(service: SendMessageService) -> None:
    envelope = service.send_text(ALICE_SESSION, BOB.user_id, "hola").envelope

    assert envelope.sender == ALICE.user_id
    assert envelope.receiver == BOB.user_id


def test_seq_counts_up_per_conversation(
    service: SendMessageService, store: MemoryLocalStore
) -> None:
    store.save_contact(make_contact(CARLA, state=ContactState.ACTIVE))

    to_bob = [
        service.send_text(ALICE_SESSION, BOB.user_id, f"b{i}").seq for i in range(3)
    ]
    to_carla = service.send_text(ALICE_SESSION, CARLA.user_id, "c").seq

    assert to_bob == [1, 2, 3]
    assert to_carla == 1


def test_every_message_gets_its_own_id(service: SendMessageService) -> None:
    ids = {
        service.send_text(ALICE_SESSION, BOB.user_id, "x").message_id for _ in range(20)
    }
    assert len(ids) == 20


def test_the_same_text_twice_gives_two_different_envelopes(
    service: SendMessageService,
) -> None:
    first = service.send_text(ALICE_SESSION, BOB.user_id, "same")
    second = service.send_text(ALICE_SESSION, BOB.user_id, "same")
    assert first.envelope.ciphertext != second.envelope.ciphertext


# ---------------- the receiver can verify and decrypt it ----------------


def test_the_receiver_verifies_and_decrypts_the_envelope(
    service: SendMessageService, crypto: FakeCrypto, clock: ManualClock
) -> None:
    message = service.send_text(ALICE_SESSION, BOB.user_id, "¿nos vemos mañana?")

    content = open_as_receiver(
        crypto, message.envelope, ALICE.keys.public_key, BOB.keys.private_key
    )

    assert content == MessageContent(
        MessageKind.TEXT, WITH_BOB, 1, clock.now_ms(), "¿nos vemos mañana?"
    )


def test_nobody_else_can_decrypt_it(
    service: SendMessageService, crypto: FakeCrypto
) -> None:
    envelope = service.send_text(ALICE_SESSION, BOB.user_id, "secreto").envelope
    with pytest.raises(DecryptionError):
        crypto.decrypt(CARLA.keys.private_key, envelope.ciphertext)


def test_the_signature_is_the_senders_and_covers_the_ciphertext(
    service: SendMessageService, crypto: FakeCrypto
) -> None:
    envelope = service.send_text(ALICE_SESSION, BOB.user_id, "firmado").envelope
    signed = envelope.signing_bytes()

    assert crypto.verify(ALICE.keys.public_key, signed, envelope.signature)
    assert not crypto.verify(CARLA.keys.public_key, signed, envelope.signature)
    tampered = Envelope(
        envelope.message_id,
        envelope.sender,
        envelope.receiver,
        envelope.ciphertext[:-1] + bytes([envelope.ciphertext[-1] ^ 1]),
        envelope.signature,
    )
    assert not crypto.verify(
        ALICE.keys.public_key, tampered.signing_bytes(), envelope.signature
    )


def test_with_real_cryptography_over_the_wire() -> None:
    """End to end with NaClCrypto: the envelope goes through its wire format
    (as it will through Ray or a mailbox) and the receiver still opens it,
    while the plaintext never appears in the bytes that travel."""
    crypto = NaClCrypto(
        opslimit=pwhash.argon2id.OPSLIMIT_MIN, memlimit=pwhash.argon2id.MEMLIMIT_MIN
    )
    alice, bob = (_real_person(crypto, name) for name in ("Alice", "Bob"))
    store = MemoryLocalStore()
    store.save_contact(
        Contact(bob[0], bob[1].public_key, Alias("Bob"), ContactState.ACTIVE, 0)
    )
    session = UnlockedIdentity(
        LocalIdentity(alice[0], alice[1].public_key, Alias("Alice"), b"locked", 0),
        alice[1].private_key,
    )
    body = "frase conocida para buscar en los nodos"

    message = SendMessageService(crypto, store, ManualClock()).send_text(
        session, bob[0], body
    )
    wire = message.envelope.to_bytes()
    received = Envelope.from_bytes(wire)

    content = open_as_receiver(
        crypto, received, alice[1].public_key, bob[1].private_key
    )
    assert content.body == body
    assert body.encode() not in wire


def _real_person(crypto: NaClCrypto, name: str) -> tuple[UserId, KeyPair]:
    keys = crypto.generate_key_pair()
    return UserId.from_public_key(keys.public_key), keys


# ---------------- who you can write to ----------------


def test_an_unknown_receiver_is_refused(
    service: SendMessageService, store: MemoryLocalStore
) -> None:
    with pytest.raises(UnknownContactError):
        service.send_text(ALICE_SESSION, CARLA.user_id, "hola")
    assert store.list_outgoing() == []


@pytest.mark.parametrize(
    "state", [ContactState.OUTGOING_REQUEST, ContactState.INCOMING_REQUEST]
)
def test_a_contact_not_yet_accepted_is_refused(
    service: SendMessageService, store: MemoryLocalStore, state: ContactState
) -> None:
    store.save_contact(make_contact(CARLA, state=state))
    with pytest.raises(ContactNotActiveError):
        service.send_text(ALICE_SESSION, CARLA.user_id, "hola")
    assert store.list_outgoing() == []


def test_you_cannot_write_to_yourself(service: SendMessageService) -> None:
    with pytest.raises(ValueError):
        service.send_text(ALICE_SESSION, ALICE.user_id, "hola")


# ---------------- invalid bodies consume nothing ----------------


@pytest.mark.parametrize(
    "body", ["", "x" * (MAX_BODY_BYTES + 1), "ñ" * (MAX_BODY_BYTES // 2 + 1)]
)
def test_an_invalid_body_is_refused_before_anything_is_stored(
    service: SendMessageService, store: MemoryLocalStore, body: str
) -> None:
    with pytest.raises(FormatError):
        service.send_text(ALICE_SESSION, BOB.user_id, body)
    assert store.list_outgoing() == []
    assert store.last_outgoing_seq(WITH_BOB) == 0


def test_a_body_at_the_size_limit_is_accepted(service: SendMessageService) -> None:
    message = service.send_text(ALICE_SESSION, BOB.user_id, "x" * MAX_BODY_BYTES)
    assert len(message.body) == MAX_BODY_BYTES


def test_a_failed_send_does_not_burn_a_seq(service: SendMessageService) -> None:
    with pytest.raises(FormatError):
        service.send_text(ALICE_SESSION, BOB.user_id, "")
    assert service.send_text(ALICE_SESSION, BOB.user_id, "ok").seq == 1


# ---------------- the lower-level queue (used by contact requests) ----------------


def test_queue_sends_contact_messages_before_the_contact_is_active(
    service: SendMessageService, crypto: FakeCrypto
) -> None:
    pending = make_contact(CARLA, state=ContactState.OUTGOING_REQUEST)

    message = service.queue(ALICE_SESSION, pending, MessageKind.CONTACT_REQUEST, "")

    content = open_as_receiver(
        crypto, message.envelope, ALICE.keys.public_key, CARLA.keys.private_key
    )
    assert content.kind is MessageKind.CONTACT_REQUEST


def test_contact_messages_and_text_share_the_seq_space(
    service: SendMessageService,
) -> None:
    bob = make_contact(BOB)
    first = service.queue(ALICE_SESSION, bob, MessageKind.CONTACT_ACCEPT, "")
    second = service.send_text(ALICE_SESSION, BOB.user_id, "hola")
    assert (first.seq, second.seq) == (1, 2)


def test_queue_refuses_a_contact_that_is_yourself(service: SendMessageService) -> None:
    me = make_contact(ALICE)
    with pytest.raises(ValueError):
        service.queue(ALICE_SESSION, me, MessageKind.TEXT, "hola")


def test_queue_checks_its_argument_types(service: SendMessageService) -> None:
    with pytest.raises(TypeError):
        service.queue("session", make_contact(BOB), MessageKind.TEXT, "x")
    with pytest.raises(TypeError):
        service.queue(ALICE_SESSION, BOB.user_id, MessageKind.TEXT, "x")


def test_a_malformed_contact_key_is_refused_and_nothing_is_queued(
    service: SendMessageService, store: MemoryLocalStore
) -> None:
    bad_key = b"not a key"
    contact = Contact(
        UserId.from_public_key(bad_key), bad_key, Alias("Bad"), ContactState.ACTIVE, 0
    )
    with pytest.raises(ValueError):
        service.queue(ALICE_SESSION, contact, MessageKind.TEXT, "hola")
    assert store.list_outgoing() == []


# ---------------- concurrency ----------------


def test_a_seq_taken_meanwhile_is_refused_by_the_store(
    crypto: FakeCrypto, clock: ManualClock
) -> None:
    store = RacingStore()
    store.save_contact(make_contact(BOB))
    service = SendMessageService(crypto, store, clock)
    first = service.send_text(ALICE_SESSION, BOB.user_id, "one")

    with pytest.raises(AlreadyExistsError):
        service.send_text(ALICE_SESSION, BOB.user_id, "two")
    assert store.list_outgoing() == [first]
