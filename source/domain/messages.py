"""Messages as the local client stores them (history and outbox)."""

from __future__ import annotations

from dataclasses import dataclass

from source.domain.canonical import FormatError, is_int
from source.domain.envelope import Envelope, MessageContent, MessageKind
from source.domain.identifiers import ConversationId, MessageId, UserId
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

    def __post_init__(self) -> None:
        if not isinstance(self.message_id, MessageId) or not isinstance(
            self.sender, UserId
        ):
            raise FormatError("invalid message_id or sender")
        # Reuse the content rules (seq >= 1, body limits, ...) instead of duplicating them.
        MessageContent(
            self.kind, self.conversation_id, self.seq, self.sent_at_ms, self.body
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
    status: OutgoingStatus = OutgoingStatus.QUEUED
    attempts: int = 0
    next_attempt_at_ms: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.envelope, Envelope):
            raise FormatError("envelope must be an Envelope")
        if not isinstance(self.status, OutgoingStatus):
            raise FormatError("invalid status")
        content = MessageContent(
            self.kind, self.conversation_id, self.seq, self.sent_at_ms, self.body
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
