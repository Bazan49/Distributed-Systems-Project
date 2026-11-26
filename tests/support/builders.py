"""Small factories that build valid domain objects for tests.

Everything is deterministic (seeded FakeCrypto, counter-based ids) so a failing
test is always reproducible.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from dataclasses import dataclass

from source.adapters.memory.crypto import FakeCrypto
from source.domain.contacts import Contact, ContactState
from source.domain.directory import build_record
from source.domain.envelope import (
    Envelope,
    MessageContent,
    MessageKind,
    receipt_body,
)
from source.domain.identifiers import Alias, ConversationId, MessageId, UserId
from source.domain.identity import LocalIdentity
from source.domain.mailbox_access import MailboxAction, MailboxRequest
from source.domain.messages import OutgoingMessage, ReceivedMessage
from source.domain.outgoing_status import OutgoingStatus
from source.domain.records import DirectoryRecord, KeyPair

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


def default_body(kind: MessageKind) -> str:
    """A valid body for ``kind``: text for TEXT, one confirmed id for RECEIPT."""
    if kind is MessageKind.RECEIPT:
        return receipt_body([MessageId("f" * 32)])
    return "hello"


def make_received(
    *,
    sender: Person = BOB,
    me: Person = ALICE,
    seq: int = 1,
    body: str | None = None,
    kind: MessageKind = MessageKind.TEXT,
    message_id: MessageId | None = None,
    sent_at_ms: int = BASE_TIME_MS,
    received_at_ms: int = BASE_TIME_MS + 10,
    lamport: int | None = None,
) -> ReceivedMessage:
    """A received message; its Lamport timestamp defaults to its ``seq``."""
    return ReceivedMessage(
        message_id=message_id or next_message_id(),
        conversation_id=conversation_of(sender, me),
        sender=sender.user_id,
        seq=seq,
        kind=kind,
        body=default_body(kind) if body is None else body,
        sent_at_ms=sent_at_ms,
        received_at_ms=received_at_ms,
        lamport=seq if lamport is None else lamport,
    )


def make_outgoing(
    *,
    sender: Person = ALICE,
    receiver: Person = BOB,
    seq: int = 1,
    body: str | None = None,
    kind: MessageKind = MessageKind.TEXT,
    message_id: MessageId | None = None,
    sent_at_ms: int = BASE_TIME_MS,
    status: OutgoingStatus = OutgoingStatus.QUEUED,
    attempts: int = 0,
    next_attempt_at_ms: int | None = None,
    lamport: int | None = None,
) -> OutgoingMessage:
    """An outgoing message; its Lamport timestamp defaults to its ``seq``."""
    crypto = FakeCrypto()
    conversation_id = conversation_of(sender, receiver)
    body = default_body(kind) if body is None else body
    lamport = seq if lamport is None else lamport
    content = MessageContent(kind, conversation_id, seq, sent_at_ms, body, lamport)
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
        lamport=lamport,
        status=status,
        attempts=attempts,
        next_attempt_at_ms=next_attempt_at_ms,
    )


def signed_by(person: Person) -> Callable[[bytes], bytes]:
    """A ``sign`` callback that signs with ``person``'s private key (FakeCrypto)."""
    crypto = FakeCrypto()
    return lambda data: crypto.sign(person.keys.private_key, data)


def collect_request(
    person: Person, now_ms: int, *, limit: int = 10, nonce: str | None = None
) -> MailboxRequest:
    """A COLLECT request correctly signed by ``person`` at ``now_ms``."""
    return MailboxRequest.build(
        action=MailboxAction.COLLECT,
        public_key=person.keys.public_key,
        issued_at_ms=now_ms,
        limit=limit,
        nonce=nonce,
        sign=signed_by(person),
    )


def ack_request(
    person: Person,
    now_ms: int,
    message_ids: list[MessageId],
    *,
    nonce: str | None = None,
) -> MailboxRequest:
    """An ACKNOWLEDGE request correctly signed by ``person`` at ``now_ms``."""
    return MailboxRequest.build(
        action=MailboxAction.ACKNOWLEDGE,
        public_key=person.keys.public_key,
        issued_at_ms=now_ms,
        message_ids=message_ids,
        nonce=nonce,
        sign=signed_by(person),
    )


def signed_record(
    person: Person, now_ms: int, *, version: int = 1, lease_ms: int = 60_000
) -> DirectoryRecord:
    """A directory record correctly signed by ``person``, valid for ``lease_ms``."""
    return build_record(
        public_key=person.keys.public_key,
        version=version,
        expires_at_ms=now_ms + lease_ms,
        sign=signed_by(person),
    )
