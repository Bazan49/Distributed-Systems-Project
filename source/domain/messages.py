"""Messages as the local client stores them (history and outbox)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from source.domain.canonical import FormatError, is_int
from source.domain.envelope import Envelope, MessageContent, MessageKind
from source.domain.identifiers import ConversationId, MessageId, UserId
from source.domain.ordering import StreamSnapshot
from source.domain.outgoing_status import OutgoingStatus


@dataclass(frozen=True)
class ReceivedMessage:
    """A message received, validated and decrypted, as saved in the local history.

    Every kind is stored (text and contact messages) because they all share the
    same ``seq`` space, which the ordering logic needs in order to detect gaps.
    """

    message_id: MessageId
    conversation_id: ConversationId
    sender: UserId
    seq: int
    kind: MessageKind
    body: str
    sent_at_ms: int
    received_at_ms: int
    lamport: int

    def __post_init__(self) -> None:
        if not isinstance(self.message_id, MessageId) or not isinstance(
            self.sender, UserId
        ):
            raise FormatError("invalid message_id or sender")
        # Reuse the content rules (seq >= 1, body limits, ...) instead of duplicating them.
        MessageContent(
            self.kind,
            self.conversation_id,
            self.seq,
            self.sent_at_ms,
            self.body,
            self.lamport,
        )
        if not is_int(self.received_at_ms) or self.received_at_ms < 0:
            raise FormatError("received_at_ms must be an integer >= 0")


@dataclass(frozen=True)
class OutgoingMessage:
    """A message the local user sent, with everything needed to (re)deliver it.

    ``envelope`` is the final signed envelope, so a retry resends exactly the
    same bytes with the same MessageId and never re-encrypts. Delivery progress
    lives in ``status``, ``attempts`` and ``next_attempt_at_ms``; update it with
    ``dataclasses.replace`` and persist it with ``LocalStorePort.update_outgoing``.
    """

    envelope: Envelope
    conversation_id: ConversationId
    seq: int
    kind: MessageKind
    body: str
    sent_at_ms: int
    lamport: int
    status: OutgoingStatus = OutgoingStatus.QUEUED
    attempts: int = 0
    next_attempt_at_ms: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.envelope, Envelope):
            raise FormatError("envelope must be an Envelope")
        if not isinstance(self.status, OutgoingStatus):
            raise FormatError("invalid status")
        content = MessageContent(
            self.kind,
            self.conversation_id,
            self.seq,
            self.sent_at_ms,
            self.body,
            self.lamport,
        )
        self.envelope.ensure_content_matches(content)
        if not is_int(self.attempts) or self.attempts < 0:
            raise FormatError("attempts must be an integer >= 0")
        if self.next_attempt_at_ms is not None and (
            not is_int(self.next_attempt_at_ms) or self.next_attempt_at_ms < 0
        ):
            raise FormatError("next_attempt_at_ms must be None or an integer >= 0")

    @property
    def message_id(self) -> MessageId:
        return self.envelope.message_id

    @property
    def receiver(self) -> UserId:
        return self.envelope.receiver


InboundState = StreamSnapshot[ReceivedMessage]
"""The persisted ordering state of one incoming stream: buffered messages are
kept already decrypted, as the ``ReceivedMessage`` they will become."""


def check_inbound_commit(
    conversation_id: ConversationId,
    sender: UserId,
    released: Sequence[ReceivedMessage],
    state: InboundState,
) -> None:
    """Fail unless ``released`` and ``state`` describe ONE step of ONE stream.

    This is the consistency rule behind ``LocalStorePort.commit_inbound``,
    shared by every adapter so they all accept and refuse exactly the same
    input. It only looks at the arguments; the rules that need the stored
    data (seen ids, ``next_seq`` never going back) are checked by the store.

    Rules:

    * every released and every buffered message belongs to
      ``(conversation_id, sender)``;
    * every released message is BEHIND ``state.next_seq``: after a step,
      whatever was released (in order or late) is settled;
    * every buffered ``Pending`` describes its own message (same seq, same id);
    * no ``MessageId`` appears twice across released and buffered messages.

    Raises:
        ValueError: any rule above is broken.
    """
    if not isinstance(conversation_id, ConversationId) or not isinstance(
        sender, UserId
    ):
        raise ValueError("invalid conversation_id or sender")
    if not isinstance(state, StreamSnapshot):
        raise ValueError("state must be a StreamSnapshot")

    def belongs(message: object) -> bool:
        return (
            isinstance(message, ReceivedMessage)
            and message.conversation_id == conversation_id
            and message.sender == sender
        )

    ids: set[MessageId] = set()
    for message in released:
        if not belongs(message):
            raise ValueError("a released message belongs to another stream")
        if message.seq >= state.next_seq:
            raise ValueError(f"released seq {message.seq} is not behind next_seq")
        if message.message_id in ids:
            raise ValueError(f"message {message.message_id} is released twice")
        ids.add(message.message_id)
    for pending in state.pending:
        item = pending.item
        if not belongs(item):
            raise ValueError("a buffered message belongs to another stream")
        if item.seq != pending.seq or item.message_id != pending.message_id:
            raise ValueError("a buffered entry does not match its message")
        if item.message_id in ids:
            raise ValueError(f"message {item.message_id} is released and buffered")
        ids.add(item.message_id)
