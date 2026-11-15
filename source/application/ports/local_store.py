from typing import Protocol, runtime_checkable

from source.domain.identifiers import ConversationId, MessageId, UserId
from source.domain.outgoing_status import OutgoingStatus
from source.domain.records import Contact, OutboxEntry, StoredIdentity, StoredMessage


@runtime_checkable
class LocalStorePort(Protocol):
    """Everything the CLIENT persists on the user's device.

    Phase 1 adapter: SQLite. It is the source of truth for the user's chat
    history: no service node keeps history. Every write must be durable when
    the method returns. The store only stores: status transitions are decided
    by the domain (``outgoing_status.advance``) and passed in.
    """

    # --- identity ---
    def save_identity(self, identity: StoredIdentity) -> None:
        """Persist the local identity (overwrites any previous one)."""
        ...

    def load_identity(self) -> StoredIdentity | None:
        """Return the local identity, or None if the user never registered."""
        ...

    # --- contacts ---
    def save_contact(self, contact: Contact) -> None:
        """Insert or update a contact (keyed by ``user_id``)."""
        ...

    def get_contact(self, user_id: UserId) -> Contact | None:
        """Return the contact, or None if unknown."""
        ...

    def list_contacts(self) -> list[Contact]:
        """All contacts, in any stable order."""
        ...

    # --- history and duplicate detection ---
    def save_incoming_message(self, message: StoredMessage) -> bool:
        """Store the message AND mark its id as seen in ONE atomic transaction.

        Returns:
            True if stored; False if the id was already seen (a duplicate;
            nothing is written). A crash can never leave a stored message
            without its seen id, or the other way round.
        """
        ...

    def save_outgoing_message(self, message: StoredMessage) -> None:
        """Store a message the local user sent. Idempotent by message id."""
        ...

    def has_seen(self, message_id: MessageId) -> bool:
        """True if this message id was already stored (duplicate check)."""
        ...

    def list_history(
        self, conversation_id: ConversationId, limit: int
    ) -> list[StoredMessage]:
        """The newest ``limit`` messages of a conversation, in storage order.

        Storage order is delivery order: the ordering domain decides WHEN a
        message is stored, so the store does not reorder anything.
        """
        ...

    # --- outbox (send queue) ---
    def enqueue_outgoing(self, entry: OutboxEntry) -> None:
        """Add an entry. Idempotent by ``entry.envelope.message_id``."""
        ...

    def get_outgoing(self, message_id: MessageId) -> OutboxEntry | None:
        """Return the entry, or None if unknown."""
        ...

    def due_outgoing(self, now: float) -> list[OutboxEntry]:
        """QUEUED entries whose ``next_attempt_at`` is <= ``now``, oldest first."""
        ...

    def list_outgoing(self, status: OutgoingStatus) -> list[OutboxEntry]:
        """Every entry in the given status (e.g. ACCEPTED, waiting for delivery)."""
        ...

    def update_outgoing(
        self,
        message_id: MessageId,
        status: OutgoingStatus,
        attempts: int,
        next_attempt_at: float,
    ) -> None:
        """Overwrite status, attempt count and next retry time of an entry."""
        ...

    # --- counters ---
    def next_counter(self, key: str) -> int:
        """Atomically increment the named counter and return the NEW value (first call: 1).

        Used for the per-conversation ``seq`` of outgoing messages (starts at 1,
        like ``MessageContent.seq``).
        """
        ...
