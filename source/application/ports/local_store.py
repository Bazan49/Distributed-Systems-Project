"""LocalStorePort: everything the client persists on the user's device."""

from collections.abc import Collection
from typing import Protocol, runtime_checkable

from source.domain.contacts import Contact, ContactState
from source.domain.identifiers import ConversationId, MessageId, UserId
from source.domain.identity import LocalIdentity
from source.domain.messages import OutgoingMessage, ReceivedMessage
from source.domain.outgoing_status import OutgoingStatus


@runtime_checkable
class LocalStorePort(Protocol):
    """Everything the CLIENT persists on the user's device.

    Phase 1 adapter: SQLite. It is the source of truth for the user's chat
    history: no service node keeps history. Every write must be durable when
    the method returns. The store only stores: status transitions are decided
    by the domain (``outgoing_status.advance``) and passed in.

    The history of a conversation has two halves: what the user received
    (``list_received``) and what the user sent (``list_outgoing``). Merging
    them into one timeline is the job of a use case, not of the store.
    """

    # --- identity ---
    def save_identity(self, identity: LocalIdentity) -> None:
        """Persist the local identity.

        Raises:
            AlreadyExistsError: an identity is already saved. A device has
                exactly one identity; use a different device to register another.
        """
        ...

    def load_identity(self) -> LocalIdentity | None:
        """Return the local identity, or None if the user never registered."""
        ...

    # --- contacts ---
    def save_contact(self, contact: Contact) -> None:
        """Insert or update a contact (keyed by ``user_id``)."""
        ...

    def get_contact(self, user_id: UserId) -> Contact | None:
        """Return the contact, or None if unknown."""
        ...

    def list_contacts(self, state: ContactState | None = None) -> list[Contact]:
        """Return all contacts, or only those in ``state``, sorted by ``user_id``."""
        ...

    def delete_contact(self, user_id: UserId) -> bool:
        """Remove a contact. Return ``True`` if it existed, ``False`` otherwise.

        Their messages stay in the history.
        """
        ...

    # --- received messages (history) ---
    def save_incoming_message(self, message: ReceivedMessage) -> bool:
        """Store the message AND mark its id as seen in ONE atomic transaction.

        Returns:
            True if stored; False if the id was already seen (a duplicate;
            nothing is written). A crash can never leave a stored message
            without its seen id, or the other way round.
        """
        ...

    def has_received(self, message_id: MessageId) -> bool:
        """True if this message id was already stored (duplicate check)."""
        ...

    def list_received(
        self, conversation_id: ConversationId, limit: int = 100
    ) -> list[ReceivedMessage]:
        """Return the last ``limit`` messages stored for a conversation.

        The result is in storage order, oldest first. Storage order is
        delivery order: the ordering domain decides WHEN a message is stored,
        so the store never reorders. Defaults to the last 100.

        Raises:
            ValueError: ``limit`` is less than 1.
        """
        ...

    # --- outbox ---
    def enqueue_outgoing(self, message: OutgoingMessage) -> None:
        """Persist a message to send.

        Raises:
            AlreadyExistsError: a message with the same ``message_id`` is already
                queued, OR the local user already used this ``seq`` in this
                conversation. Both are invariants the store protects.
        """
        ...

    def update_outgoing(self, message: OutgoingMessage) -> None:
        """Replace the delivery state of the stored message with the same ``message_id``.

        Only ``status``, ``attempts`` and ``next_attempt_at_ms`` may change.
        Everything else (envelope, conversation, seq, kind, body, sent time) is
        part of the signed message and is immutable once queued. The store
        persists the given state as is; deciding legal status transitions is
        the caller's job (see ``domain.outgoing_status.advance``).

        Raises:
            NotFoundError: no such message is queued.
            ValueError: the message differs from the stored one in an
                immutable field. The store is left unchanged.
        """
        ...

    def get_outgoing(self, message_id: MessageId) -> OutgoingMessage | None:
        """Return one outgoing message, or ``None`` if unknown."""
        ...

    def list_outgoing(
        self,
        conversation_id: ConversationId | None = None,
        statuses: Collection[OutgoingStatus] | None = None,
    ) -> list[OutgoingMessage]:
        """Return outgoing messages, optionally of one conversation and/or in the given statuses.

        Sorted by ``(conversation_id, seq)``. ``None`` means "no filter"; an
        empty collection of statuses matches nothing.
        """
        ...

    def list_due_outgoing(self, now_ms: int, limit: int = 100) -> list[OutgoingMessage]:
        """Return up to ``limit`` messages that should be (re)sent now.

        Those with status ``QUEUED`` and ``next_attempt_at_ms`` either ``None``
        or ``<= now_ms`` (the boundary counts as due). Sorted by ``sent_at_ms``,
        then ``conversation_id``, then ``seq``, so the oldest message goes first.
        ``now_ms`` is passed in so that the store never reads a clock.

        Raises:
            ValueError: ``limit`` is less than 1.
        """
        ...

    def last_outgoing_seq(self, conversation_id: ConversationId) -> int:
        """Return the highest ``seq`` ever used by the local user in this conversation.

        Returns ``0`` if there is none. The next message uses this plus one.
        The value never decreases.
        """
        ...
