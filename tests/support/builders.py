"""Small factories that build valid domain objects for tests.

Everything is deterministic (seeded FakeCrypto, counter-based ids) so a failing
test is always reproducible.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass

from source.adapters.memory.crypto import FakeCrypto
from source.domain.contacts import Contact, ContactState
from source.domain.envelope import Envelope, MessageContent, MessageKind
from source.domain.identifiers import Alias, ConversationId, MessageId, UserId
from source.domain.identity import LocalIdentity
from source.domain.messages import OutgoingMessage, ReceivedMessage
from source.domain.outgoing_status import OutgoingStatus
from source.domain.records import KeyPair

BASE_TIME_MS = 1_700_000_000_000

_counter = itertools.count(1)


def next_message_id() -> MessageId:
    return MessageId(f"{next(_counter):032x}")


@dataclass(frozen=True)
class Person:
    """A test user: key pair plus derived id."""

    name: str
    keys: KeyPair
    user_id: UserId


def make_person(name: str) -> Person:
    keys = FakeCrypto().generate_key_pair()
    return Person(name, keys, UserId.from_public_key(keys.public_key))


ALICE = make_person("Alice")
BOB = make_person("Bob")
CARLA = make_person("Carla")
DAVID = make_person("David")


def conversation_of(a: Person, b: Person) -> ConversationId:
    return ConversationId.between(a.user_id, b.user_id)


def make_identity(
    person: Person = ALICE, *, created_at_ms: int = BASE_TIME_MS
) -> LocalIdentity:
    crypto = FakeCrypto()
    return LocalIdentity(
        user_id=person.user_id,
        public_key=person.keys.public_key,
        alias=Alias(person.name),
        protected_private_key=crypto.protect_private_key(
            person.keys.private_key, "password"
        ),
        created_at_ms=created_at_ms,
    )


def make_contact(
    person: Person = BOB,
    *,
    state: ContactState = ContactState.ACTIVE,
    alias: str | None = None,
    created_at_ms: int = BASE_TIME_MS,
) -> Contact:
    return Contact(
        user_id=person.user_id,
        public_key=person.keys.public_key,
        alias=Alias(alias or person.name),
        state=state,
        created_at_ms=created_at_ms,
    )


def make_received(
    *,
    sender: Person = BOB,
    me: Person = ALICE,
    seq: int = 1,
    body: str = "hello",
    kind: MessageKind = MessageKind.TEXT,
    message_id: MessageId | None = None,
    sent_at_ms: int = BASE_TIME_MS,
    received_at_ms: int = BASE_TIME_MS + 10,
) -> ReceivedMessage:
    return ReceivedMessage(
        message_id=message_id or next_message_id(),
        conversation_id=conversation_of(sender, me),
        sender=sender.user_id,
        seq=seq,
        kind=kind,
        body=body,
        sent_at_ms=sent_at_ms,
        received_at_ms=received_at_ms,
    )


def make_outgoing(
    *,
    sender: Person = ALICE,
    receiver: Person = BOB,
    seq: int = 1,
    body: str = "hello",
    kind: MessageKind = MessageKind.TEXT,
    message_id: MessageId | None = None,
    sent_at_ms: int = BASE_TIME_MS,
    status: OutgoingStatus = OutgoingStatus.QUEUED,
    attempts: int = 0,
    next_attempt_at_ms: int | None = None,
) -> OutgoingMessage:
    crypto = FakeCrypto()
    conversation_id = conversation_of(sender, receiver)
    content = MessageContent(kind, conversation_id, seq, sent_at_ms, body)
    envelope = Envelope.build(
        message_id=message_id or next_message_id(),
        sender=sender.user_id,
        receiver=receiver.user_id,
        ciphertext=crypto.encrypt_for(receiver.keys.public_key, content.to_bytes()),
        sign=lambda data: crypto.sign(sender.keys.private_key, data),
    )
    return OutgoingMessage(
        envelope=envelope,
        conversation_id=conversation_id,
        seq=seq,
        kind=kind,
        body=body,
        sent_at_ms=sent_at_ms,
        status=status,
        attempts=attempts,
        next_attempt_at_ms=next_attempt_at_ms,
    )
