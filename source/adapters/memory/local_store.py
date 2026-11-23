"""MemoryLocalStore: an in-memory LocalStorePort, for tests and fast development.

It follows every rule of the port except durability: nothing survives the
process. The contract tests about durability are skipped for it and run
against the SQLite adapter instead. Standard library only.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Collection, Sequence

from source.application.ports.errors import AlreadyExistsError, NotFoundError
from source.domain.contacts import Contact, ContactState
from source.domain.identifiers import ConversationId, MessageId, UserId
from source.domain.identity import LocalIdentity
from source.domain.messages import (
    InboundState,
    OutgoingMessage,
    ReceivedMessage,
    check_inbound_commit,
)
from source.domain.outgoing_status import OutgoingStatus


def _same_apart_from_delivery_state(old: OutgoingMessage, new: OutgoingMessage) -> bool:
    """True if ``new`` differs from ``old`` only in status, attempts or retry time."""
    normalised = dataclasses.replace(
        new,
        status=old.status,
        attempts=old.attempts,
        next_attempt_at_ms=old.next_attempt_at_ms,
    )
    return normalised == old


class MemoryLocalStore:
    """Test double for ``LocalStorePort`` that keeps everything in dictionaries."""

    def __init__(self) -> None:
        self._identity: LocalIdentity | None = None
        self._contacts: dict[UserId, Contact] = {}
        self._received: dict[MessageId, ReceivedMessage] = {}
        self._outgoing: dict[MessageId, OutgoingMessage] = {}
        self._streams: dict[tuple[ConversationId, UserId], InboundState] = {}

    # identity

    def save_identity(self, identity: LocalIdentity) -> None:
        """Persist the local identity."""
        if self._identity is not None:
            raise AlreadyExistsError("an identity is already stored")
        self._identity = identity

    def load_identity(self) -> LocalIdentity | None:
        """Return the local identity, or None if the user never registered."""
        return self._identity

    # contacts

    def save_contact(self, contact: Contact) -> None:
        """Insert or update a contact (keyed by ``user_id``)."""
        self._contacts[contact.user_id] = contact

    def get_contact(self, user_id: UserId) -> Contact | None:
        """Return the contact, or None if unknown."""
        return self._contacts.get(user_id)

    def list_contacts(self, state: ContactState | None = None) -> list[Contact]:
        """Return all contacts, or only those in ``state``, sorted by ``user_id``."""
        contacts = [
            c for c in self._contacts.values() if state is None or c.state == state
        ]
        return sorted(contacts, key=lambda c: c.user_id)

    def delete_contact(self, user_id: UserId) -> bool:
        """Remove a contact. Return ``True`` if it existed, ``False`` otherwise.

        Their messages stay in the history.
        """
        return self._contacts.pop(user_id, None) is not None

    # received messages

    def save_incoming_message(self, message: ReceivedMessage) -> bool:
        """Store the message AND mark its id as seen in ONE atomic transaction.

        Returns:
            True if stored; False if the id was already seen (a duplicate;
            nothing is written).
        """
        if message.message_id in self._received:
            return False
        self._received[message.message_id] = message
        return True

    # incoming ordering state

    def load_inbound_stream(
        self, conversation_id: ConversationId, sender: UserId
    ) -> InboundState | None:
        """Return the last committed ordering state of the stream, or None."""
        return self._streams.get((conversation_id, sender))

    def commit_inbound(
        self,
        conversation_id: ConversationId,
        sender: UserId,
        released: Sequence[ReceivedMessage],
        state: InboundState,
    ) -> None:
        """Append ``released`` to the history and replace the stream state, atomically.

        Every check runs BEFORE anything is modified, which is what makes the
        operation all-or-nothing in memory.

        Raises:
            ValueError: inconsistent arguments, ``next_seq`` going back, or a
                buffered seq changing its message.
            AlreadyExistsError: a released message id was already seen.
        """
        released = tuple(released)
        check_inbound_commit(conversation_id, sender, released, state)
        current = self._streams.get((conversation_id, sender))
        if current is not None:
            if state.next_seq < current.next_seq:
                raise ValueError("next_seq cannot go back")
            before = {p.seq: p for p in current.pending}
            for pending in state.pending:
                if before.get(pending.seq, pending) != pending:
                    raise ValueError(f"buffered seq {pending.seq} changed its message")
        for message in released:
            if message.message_id in self._received:
                raise AlreadyExistsError(f"message {message.message_id} already seen")

        for message in released:
            self._received[message.message_id] = message
        self._streams[(conversation_id, sender)] = state

    def has_received(self, message_id: MessageId) -> bool:
        """True if this message id was already stored (duplicate check)."""
        return message_id in self._received

    def list_received(
        self, conversation_id: ConversationId, limit: int = 100
    ) -> list[ReceivedMessage]:
        """Return the last ``limit`` messages of a conversation, oldest first."""
        if limit < 1:
            raise ValueError("limit must be at least 1")
        messages = [
            m for m in self._received.values() if m.conversation_id == conversation_id
        ]
        return messages[-limit:]

    # outbox

    def _seq_taken(self, message: OutgoingMessage) -> bool:
        """Whether ANOTHER queued message already uses this (conversation, seq)."""
        return any(
            other.message_id != message.message_id
            and other.conversation_id == message.conversation_id
            and other.seq == message.seq
            for other in self._outgoing.values()
        )

    def enqueue_outgoing(self, message: OutgoingMessage) -> None:
        """Persist a message to send.

        Raises:
            AlreadyExistsError: a message with the same ``message_id`` is already
                queued, OR the local user already used this ``seq`` in this
                conversation.
        """
        if message.message_id in self._outgoing:
            raise AlreadyExistsError("message_id is already queued")
        if self._seq_taken(message):
            raise AlreadyExistsError("seq already used in this conversation")
        self._outgoing[message.message_id] = message

    def update_outgoing(self, message: OutgoingMessage) -> None:
        """Replace the delivery state (status, attempts, retry time) of a queued message.

        Raises:
            NotFoundError: no such message is queued.
            ValueError: an immutable field differs from the stored message.
        """
        current = self._outgoing.get(message.message_id)
        if current is None:
            raise NotFoundError("no such outgoing message")
        if not _same_apart_from_delivery_state(current, message):
            raise ValueError("only status, attempts and next_attempt_at_ms may change")
        self._outgoing[message.message_id] = message

    def get_outgoing(self, message_id: MessageId) -> OutgoingMessage | None:
        """Return one outgoing message, or ``None`` if unknown."""
        return self._outgoing.get(message_id)

    def list_outgoing(
        self,
        conversation_id: ConversationId | None = None,
        statuses: Collection[OutgoingStatus] | None = None,
    ) -> list[OutgoingMessage]:
        """Return outgoing messages, optionally of one conversation and/or in the given statuses.

        Sorted by ``(conversation_id, seq)``. ``None`` means "no filter"; an
        empty collection of statuses matches nothing.
        """
        wanted = None if statuses is None else set(statuses)
        found = [
            m
            for m in self._outgoing.values()
            if (conversation_id is None or m.conversation_id == conversation_id)
            and (wanted is None or m.status in wanted)
        ]
        return sorted(found, key=lambda m: (m.conversation_id, m.seq))

    def list_due_outgoing(self, now_ms: int, limit: int = 100) -> list[OutgoingMessage]:
        """Return up to ``limit`` messages that should be (re)sent now.

        Those with status ``QUEUED`` and ``next_attempt_at_ms`` either ``None``
        or ``<= now_ms``. Sorted by ``sent_at_ms``, then ``conversation_id``,
        then ``seq``.
        """
        if limit < 1:
            raise ValueError("limit must be at least 1")
        due = [
            m
            for m in self._outgoing.values()
            if m.status == OutgoingStatus.QUEUED
            and (m.next_attempt_at_ms is None or m.next_attempt_at_ms <= now_ms)
        ]
        due.sort(key=lambda m: (m.sent_at_ms, m.conversation_id, m.seq))
        return due[:limit]

    def last_outgoing_seq(self, conversation_id: ConversationId) -> int:
        """Highest ``seq`` ever used by the local user in this conversation, or ``0``."""
        return max(
            (
                m.seq
                for m in self._outgoing.values()
                if m.conversation_id == conversation_id
            ),
            default=0,
        )
