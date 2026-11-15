"""Plain data types that cross the port boundaries.

Immutable and without behaviour. Business rules (ordering, signature checks,
version comparison, status transitions) live in other domain modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

from source.domain.envelope import Envelope, MessageContent
from source.domain.identifiers import MessageId, UserId
from source.domain.node_id import NodeId
from source.domain.outgoing_status import OutgoingStatus

T = TypeVar("T")


@dataclass(frozen=True)
class KeyPair:
    """An identity key pair. Raw bytes; the format is an adapter detail."""

    public_key: bytes
    private_key: bytes


@dataclass(frozen=True)
class StoredIdentity:
    """The local user's identity as persisted on the device."""

    user_id: UserId
    public_key: bytes
    protected_private_key: bytes  # private key encrypted with the user's password
    created_at: float


class ContactStatus(Enum):
    PENDING_OUTGOING = "pending_outgoing"  # we asked, they have not accepted
    PENDING_INCOMING = "pending_incoming"  # they asked, we have not accepted
    ACCEPTED = "accepted"


@dataclass(frozen=True)
class Contact:
    user_id: UserId
    public_key: bytes
    display_name: str
    status: ContactStatus
    fingerprint_verified: bool


@dataclass(frozen=True)
class StoredMessage:
    """A message in the local history (already decrypted).

    ``content`` is the inner layer of the message (kind, conversation_id, seq,
    sent_at_ms, body). Direction is given by sender / receiver.
    """

    message_id: MessageId
    sender: UserId
    receiver: UserId
    content: MessageContent
    stored_at: float


@dataclass(frozen=True)
class OutboxEntry:
    """A sent message tracked in the local send queue.

    ``status`` is the sender-side state machine (see ``outgoing_status``).
    The queue keeps the entry after it is ACCEPTED, because the sender still
    waits for the DELIVERED confirmation.
    """

    envelope: Envelope
    status: OutgoingStatus
    attempts: int
    next_attempt_at: float


@dataclass(frozen=True)
class DirectoryRecord:
    """A signed record published by a user (presence, current location)."""

    user_id: UserId
    version: int  # the higher version wins
    payload: bytes
    signature: bytes
    expires_at: float


class PutOutcome(Enum):
    STORED = "stored"
    ALREADY_PRESENT = "already_present"  # idempotent repeat, still a success


class DirectSendResult(Enum):
    DELIVERED = "delivered"
    UNREACHABLE = "unreachable"  # timeout, peer offline, or any transport failure


@dataclass(frozen=True)
class NodeReply(Generic[T]):
    """The answer (or failure) of ONE service node to ONE call.

    A failed call is data, not an exception, so quorum logic can count replies
    without try/except around every node.
    """

    node: NodeId
    ok: bool
    value: T | None = None
    error: str | None = None
