from typing import Protocol, runtime_checkable

from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.records import PutOutcome


@runtime_checkable
class MailboxStorePort(Protocol):
    """The mailbox queue stored by ONE service node.

    Phase 1 adapter: SQLite. Persistence only: the policies (how long a TTL
    is, when to delete) belong to the mailbox service in the domain. The node
    only sees envelopes, never the message content.

    Expiry is NODE-LOCAL metadata: the envelope carries no deadline (so the
    network cannot be told how long to keep it). The mailbox service computes
    ``expires_at = clock.now_ms() + TTL`` when it receives the envelope.
    """

    def put(self, envelope: Envelope, expires_at: float) -> PutOutcome:
        """Store ``envelope`` for ``envelope.receiver`` until ``expires_at``.

        Idempotent by ``envelope.message_id``: a second put of the same id
        stores nothing (the first ``expires_at`` is kept) and returns
        ``ALREADY_PRESENT``.
        """
        ...

    def fetch(self, receiver: UserId, limit: int, now: float) -> list[Envelope]:
        """Up to ``limit`` pending envelopes for ``receiver``, oldest first.

        Envelopes whose ``expires_at`` is <= ``now`` are never returned.
        """
        ...

    def delete(self, receiver: UserId, message_ids: list[MessageId]) -> int:
        """Delete the given envelopes and return how many existed."""
        ...

    def purge_expired(self, now: float) -> int:
        """Delete every envelope expired at ``now`` and return how many."""
        ...
