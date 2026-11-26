"""Receive-messages use case: from an envelope that arrived to a message in the history.

Envelopes arrive from two paths (direct from the sender, or collected from
the mailboxes), up to once per mailbox replica, in any order. Every one goes
through the same pipeline, cheapest checks first:

1. **Addressed to me?** Otherwise it is not ours to store or to confirm.
2. **Already seen?** (``LocalStorePort.has_received``.) With three mailbox
   replicas most arrivals are copies, so this check runs BEFORE any
   cryptography: a copy costs one index lookup, not a verify and a decrypt.
3. **Sender known?** The public key that verifies the signature comes from
   the local contacts, never from the envelope itself. The one exception is
   delegated to the ``ContactGate``: a stranger's CONTACT_REQUEST carries the
   requester's signed link, from which the gate may create a pending
   request; the envelope then continues through the pipeline as usual.
4. **Signature valid?** Authenticity is checked before the ciphertext is
   even decrypted: nothing forged is ever parsed.
5. **Readable?** Decrypt with our private key, parse the content strictly,
   and check it belongs to the conversation between sender and receiver (so
   a ciphertext cannot be lifted from another conversation and re-signed).
6. **Order.** The message is offered to the sender's ``InboundStream``,
   which releases it (and any buffered followers) in the sender's order,
   buffers it behind a gap, or recognises a duplicate.
7. **Store.** The ``ContactGate`` is told about the released messages (in
   order), then they and the new stream state are committed in ONE
   transaction (``LocalStorePort.commit_inbound``). The gate runs first so
   that a failure anywhere leaves the envelope unconfirmed: it comes back,
   and the gate's reactions are idempotent.
8. **Confirm.** The result says whether the envelope may be acknowledged:
   to the mailboxes (delete it) or to a direct sender (it was delivered).

Because the stream state is committed together with the history, a
BUFFERED message is as durable as a stored one, so it is acknowledged right
away like any other settled message.

What a message MEANS (a contact request, an acceptance, a text) is not
interpreted here: every kind is ordered and stored the same way, and the
``ContactGate`` (the contacts use case) reacts to the released
messages.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from source.application.observability import get_logger
from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.errors import DecryptionError
from source.application.ports.local_store import LocalStorePort
from source.application.use_cases.identity import UnlockedIdentity
from source.domain.canonical import FormatError
from source.domain.contacts import Contact
from source.domain.envelope import Envelope, MessageContent, MessageKind
from source.domain.identifiers import ConversationId, MessageId, UserId
from source.domain.messages import ReceivedMessage
from source.domain.ordering import (
    Arrival,
    Delivery,
    InboundStream,
    OrderingPolicy,
)

__all__ = [
    "BatchResult",
    "ContactGate",
    "GapReport",
    "ReceiptGate",
    "ReceiveMessagesService",
    "ReceiveResult",
    "ReceiveStatus",
]

_StreamKey = tuple[ConversationId, UserId]


class ContactGate(Protocol):
    """What the receive pipeline needs from the contacts use case.

    An interface inside the application layer, so that receiving
    does not depend on how contacts work.
    """

    def introduce(
        self, session: UnlockedIdentity, envelope: Envelope
    ) -> Contact | None:
        """Called for an envelope from an unknown sender.

        Return the contact it introduces (now stored), or None to treat the
        sender as unknown. Must not raise for anything a peer can send.
        """
        ...

    def on_released(
        self, session: UnlockedIdentity, messages: Sequence[ReceivedMessage]
    ) -> None:
        """Called with messages about to be stored, in order. Must be idempotent."""
        ...


class ReceiptGate(Protocol):
    """What the receive pipeline needs from the receipts use case."""

    def on_released(
        self, session: UnlockedIdentity, messages: Sequence[ReceivedMessage]
    ) -> None:
        """Called with messages about to be stored, in order. Must be idempotent."""
        ...


log = get_logger(__name__)


# results


class ReceiveStatus(str, Enum):
    """What happened to one received envelope."""

    # Authentic and new: it went through ordering.
    DELIVERED = "delivered"
    """Stored in the history now (maybe with buffered followers)."""
    BUFFERED = "buffered"
    """Durably buffered behind a gap; stored once the gap closes."""
    LATE = "late"
    """Its gap had been skipped; stored now, out of order."""

    # Nothing new.
    DUPLICATE = "duplicate"
    """Already stored or already buffered."""
    CONFLICT = "conflict"
    """The sender already used this seq for another message; dropped."""

    # Not processed now.
    TOO_FAR_AHEAD = "too_far_ahead"
    """Beyond the ordering buffer; must be received again later."""
    NOT_FOR_ME = "not_for_me"
    """Addressed to another user."""
    UNKNOWN_SENDER = "unknown_sender"
    """The sender is not a contact, so there is no key to verify it with."""

    # Refused for good.
    BAD_SIGNATURE = "bad_signature"
    """Forged or altered in transit."""
    UNREADABLE = "unreadable"
    """Authentic, but cannot be decrypted, is malformed, or belongs to another
    conversation."""

    @property
    def acknowledge(self) -> bool:
        """True if the envelope may be confirmed (mailbox ack / direct "delivered").

        Confirming means "never send me this again". So it is True for
        everything settled: stored, durably buffered, a duplicate, or refused
        for good (retrying a forged envelope can never make it valid). It is
        False when the envelope must come back: it was too far ahead, or it
        cannot be handled YET (an unknown sender may become a contact),
        or it is not ours to confirm at all.
        """
        return self not in _NOT_ACKNOWLEDGED


_NOT_ACKNOWLEDGED = frozenset(
    {
        ReceiveStatus.TOO_FAR_AHEAD,
        ReceiveStatus.NOT_FOR_ME,
        ReceiveStatus.UNKNOWN_SENDER,
    }
)

_FROM_ARRIVAL = {
    Arrival.DELIVERED: ReceiveStatus.DELIVERED,
    Arrival.BUFFERED: ReceiveStatus.BUFFERED,
    Arrival.LATE: ReceiveStatus.LATE,
    Arrival.DUPLICATE: ReceiveStatus.DUPLICATE,
    Arrival.CONFLICT: ReceiveStatus.CONFLICT,
    Arrival.TOO_FAR_AHEAD: ReceiveStatus.TOO_FAR_AHEAD,
}

# Arrivals that change the stream, so its state must be committed.
_CHANGES_STREAM = frozenset({Arrival.DELIVERED, Arrival.BUFFERED, Arrival.LATE})


@dataclass(frozen=True)
class ReceiveResult:
    """Outcome of receiving one envelope.

    ``released`` holds the messages stored in the history by THIS call, in
    the order they were stored (``late`` marks the out-of-order ones). They
    may include earlier messages that were waiting for this one.

    ``sender`` and ``kind`` are those of the envelope itself, known only once
    it was verified and decrypted; None for envelopes refused or skipped
    before that (not for me, duplicate, unknown sender, bad signature,
    unreadable).
    """

    message_id: MessageId
    status: ReceiveStatus
    released: tuple[Delivery[ReceivedMessage], ...] = ()
    sender: UserId | None = None
    kind: MessageKind | None = None

    @property
    def acknowledge(self) -> bool:
        return self.status.acknowledge


@dataclass(frozen=True)
class BatchResult:
    """Outcome of receiving a batch of envelopes (e.g. one mailbox collect)."""

    results: tuple[ReceiveResult, ...]

    @property
    def released(self) -> tuple[Delivery[ReceivedMessage], ...]:
        """Every message stored by the batch, in storage order."""
        return tuple(d for result in self.results for d in result.released)

    @property
    def to_acknowledge(self) -> list[MessageId]:
        """Ids to confirm to the mailboxes, each once, in arrival order."""
        ids = (r.message_id for r in self.results if r.acknowledge)
        return list(dict.fromkeys(ids))


@dataclass(frozen=True)
class GapReport:
    """A gap that ``expire_gaps`` gave up on, and what that unblocked."""

    conversation_id: ConversationId
    sender: UserId
    skipped: range
    released: tuple[Delivery[ReceivedMessage], ...]


# use case


class ReceiveMessagesService:
    """Validate, deduplicate, order and store incoming envelopes.

    It keeps one ``InboundStream`` per ``(conversation, sender)`` in memory,
    loaded lazily from the store. The store stays the source of truth: if a
    commit fails, the in-memory stream is dropped and reloaded next time, so
    memory never runs ahead of the disk.

    Why streams are cached instead of reloaded for every envelope: the gap
    timer lives in the stream. Restoring a stream restarts its timer, so a
    sender who keeps sending would postpone the skip of a gap forever.

    Not thread-safe: the client processes envelopes one at a time.
    """

    def __init__(
        self,
        crypto: CryptoPort,
        store: LocalStorePort,
        clock: ClockPort,
        policy: OrderingPolicy | None = None,
        contacts: ContactGate | None = None,
        receipts: ReceiptGate | None = None,
    ) -> None:
        """Args:
        contacts: the contacts use case. Without it, every unknown sender
            stays unknown and contact messages are only stored.
        receipts: the receipts use case. Without it, receipts are only stored.
        """
        self._crypto = crypto
        self._store = store
        self._clock = clock
        self._policy = policy if policy is not None else OrderingPolicy()
        self._contacts = contacts
        self._receipts = receipts
        self._streams: dict[_StreamKey, InboundStream[ReceivedMessage]] = {}

    # receiving

    def receive(self, session: UnlockedIdentity, envelope: Envelope) -> ReceiveResult:
        """Run one envelope through the pipeline.

        Never raises for anything a peer or a mailbox can send: a bad
        envelope is a status, not an exception.

        Raises:
            StoreError: the local store failed. Nothing about this envelope
                was confirmed, so it is safe to receive it again later.
        """
        result = self._receive(session, envelope)
        _log_result(envelope.sender, result)
        return result

    def _receive(self, session: UnlockedIdentity, envelope: Envelope) -> ReceiveResult:
        """Receive a single envelope and process it."""
        if not isinstance(session, UnlockedIdentity):
            raise TypeError("session must be an UnlockedIdentity")
        if not isinstance(envelope, Envelope):
            raise TypeError("envelope must be an Envelope")
        message_id = envelope.message_id

        def outcome(status: ReceiveStatus) -> ReceiveResult:
            return ReceiveResult(message_id, status)

        # 1-3: cheap checks.
        if envelope.receiver != session.user_id:
            return outcome(ReceiveStatus.NOT_FOR_ME)
        if self._store.has_received(message_id):
            return outcome(ReceiveStatus.DUPLICATE)
        contact = self._store.get_contact(envelope.sender)
        if contact is None and self._contacts is not None:
            contact = self._contacts.introduce(session, envelope)
        if contact is None:
            return outcome(ReceiveStatus.UNKNOWN_SENDER)

        # 4: authenticity, before touching the ciphertext.
        if not self._crypto.verify(
            contact.public_key, envelope.signing_bytes(), envelope.signature
        ):
            return outcome(ReceiveStatus.BAD_SIGNATURE)

        # 5: confidentiality and strict parsing.
        try:
            plaintext = self._crypto.decrypt(session.private_key, envelope.ciphertext)
            content = MessageContent.from_bytes(plaintext)
            envelope.ensure_content_matches(content)
        except (DecryptionError, FormatError):
            return outcome(ReceiveStatus.UNREADABLE)

        message = ReceivedMessage(
            message_id=message_id,
            conversation_id=content.conversation_id,
            sender=envelope.sender,
            seq=content.seq,
            kind=content.kind,
            body=content.body,
            sent_at_ms=content.sent_at_ms,
            received_at_ms=self._clock.now_ms(),
            lamport=content.lamport,
        )

        # 6-7: order, then commit history and stream state together.
        key = (content.conversation_id, envelope.sender)
        stream = self._stream(key)
        result = stream.offer(
            content.seq, message_id, message, self._clock.monotonic_ms()
        )
        if result.arrival in _CHANGES_STREAM:
            self._commit(session, key, stream, result.released)
        # 8: the status carries whether to confirm.
        return ReceiveResult(
            message_id,
            _FROM_ARRIVAL[result.arrival],
            result.released,
            sender=envelope.sender,
            kind=content.kind,
        )

    def receive_batch(
        self, session: UnlockedIdentity, envelopes: Iterable[Envelope]
    ) -> BatchResult:
        """Receive several envelopes in order; collect what to acknowledge.

        Typical use: everything one mailbox collect returned, then one
        ``acknowledge`` call to the nodes with ``to_acknowledge``.

        Raises:
            StoreError: the local store failed. Envelopes handled before the
                failure are committed; none is reported for acknowledgement,
                so they will be collected again and come back as duplicates,
                which are then acknowledged.
        """
        return BatchResult(tuple(self.receive(session, e) for e in envelopes))

    # gap timers

    def resume(self, session: UnlockedIdentity) -> None:
        """Load the stream of every contact, so their gap timers run again.

        Call it once after login. Without it, a stream that had a gap when
        the client stopped would only be looked at again when that sender
        sends something new, and its buffered messages could wait forever.
        """
        me = session.user_id
        for contact in self._store.list_contacts():
            if contact.user_id != me:
                self._stream(
                    (ConversationId.between(me, contact.user_id), contact.user_id)
                )

    def next_deadline_ms(self) -> int | None:
        """The earliest gap deadline (monotonic ms) among loaded streams, or None.

        The caller schedules a timer for it and then calls ``expire_gaps``.
        """
        deadlines = [
            s.deadline_ms for s in self._streams.values() if s.deadline_ms is not None
        ]
        return min(deadlines, default=None)

    def expire_gaps(self, session: UnlockedIdentity) -> list[GapReport]:
        """Skip every gap whose deadline has passed; store what that unblocks.

        Calling it early is harmless: streams whose deadline has not come
        are left alone. The session is needed because the released messages
        go to the ``ContactGate``, which may have to answer them.

        Raises:
            StoreError: the local store failed (see ``receive``).
        """
        now_ms = self._clock.monotonic_ms()
        reports: list[GapReport] = []
        for key, stream in list(self._streams.items()):
            gap = stream.expire(now_ms)
            if gap.skipped is None:
                continue
            self._commit(session, key, stream, gap.released)
            reports.append(GapReport(key[0], key[1], gap.skipped, gap.released))
            log.warning(
                "receive.gap_skipped",
                peer=key[1],
                conv=key[0],
                from_seq=gap.skipped.start,
                to_seq=gap.skipped.stop - 1,
            )
            _log_stored(key[1], gap.released)
        return reports

    # internals

    def _stream(self, key: _StreamKey) -> InboundStream[ReceivedMessage]:
        """The cached stream of ``key``, loading it from the store on first use."""
        stream = self._streams.get(key)
        if stream is None:
            saved = self._store.load_inbound_stream(*key)
            stream = (
                InboundStream(self._policy)
                if saved is None
                else InboundStream.restore(
                    saved, self._clock.monotonic_ms(), self._policy
                )
            )
            self._streams[key] = stream
        return stream

    def _commit(
        self,
        session: UnlockedIdentity,
        key: _StreamKey,
        stream: InboundStream[ReceivedMessage],
        released: Sequence[Delivery[ReceivedMessage]],
    ) -> None:
        """Hand the released messages to the gates, then persist one step of ``stream``.

        ``offer``/``expire`` already changed the stream in memory. If a
        gate or the commit fails, memory is ahead of the disk, so the stream
        is dropped: the next envelope reloads it from the last committed
        state, and the unconfirmed envelope will simply be received again.
        """
        messages = [d.item for d in released]
        try:
            if self._contacts is not None and messages:
                self._contacts.on_released(session, messages)
            if self._receipts is not None and messages:
                self._receipts.on_released(session, messages)
            self._store.commit_inbound(key[0], key[1], messages, stream.snapshot())
        except BaseException:
            self._streams.pop(key, None)
            raise


# logging


_REJECTED = frozenset(
    {
        ReceiveStatus.BAD_SIGNATURE,
        ReceiveStatus.UNREADABLE,
        ReceiveStatus.CONFLICT,
        ReceiveStatus.NOT_FOR_ME,
    }
)
"""Refused for good: forged, altered, malformed or misdirected."""

_DEFERRED = frozenset({ReceiveStatus.TOO_FAR_AHEAD, ReceiveStatus.UNKNOWN_SENDER})
"""Not handled now; the envelope comes back later."""


def _log_result(sender: UserId, result: ReceiveResult) -> None:
    """One event for what happened to the envelope, plus one per stored message."""
    status = result.status
    if status in _REJECTED:
        log.warning(
            "receive.rejected", msg=result.message_id, peer=sender, reason=status
        )
    elif status in _DEFERRED:
        log.info("receive.deferred", msg=result.message_id, peer=sender, reason=status)
    elif status is ReceiveStatus.BUFFERED:
        log.debug("receive.buffered", msg=result.message_id, peer=sender)
    elif status is ReceiveStatus.DUPLICATE:
        log.debug("receive.duplicate", msg=result.message_id, peer=sender)
    _log_stored(sender, result.released)


def _log_stored(sender: UserId, released: Sequence[Delivery[ReceivedMessage]]) -> None:
    for delivery in released:
        log.info(
            "receive.stored",
            msg=delivery.message_id,
            peer=sender,
            seq=delivery.seq,
            lamport=delivery.item.lamport,
            late=delivery.late,
        )
