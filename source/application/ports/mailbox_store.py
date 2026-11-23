from typing import Protocol, runtime_checkable

from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.records import PutOutcome


@runtime_checkable
class MailboxStorePort(Protocol):
    """The mailbox queue stored by ONE service node.

    Phase 1 adapter: SQLite. Persistence only: the policies (how long a TTL
    is, when to delete, who may read) belong to the mailbox service. The node
    only sees envelopes, never the message content.

    Every time is an ``int`` in milliseconds of the node's wall clock
    (``ClockPort.now_ms``). Expiry is NODE-LOCAL metadata: the envelope carries
    no deadline (so the network cannot be told how long to keep it). The
    mailbox service computes ``expires_at_ms = clock.now_ms() + ttl_ms`` when
    it receives the envelope.
    """

    def put(self, envelope: Envelope, expires_at_ms: int) -> PutOutcome:
        """Store ``envelope`` for ``envelope.receiver`` until ``expires_at_ms``.

        Idempotent by ``envelope.message_id``: a second put of the same id
        stores nothing (the first ``expires_at_ms`` is kept) and returns
        ``ALREADY_PRESENT``.
        """
        ...

    def fetch(self, receiver: UserId, limit: int, now_ms: int) -> list[Envelope]:
        """Up to ``limit`` pending envelopes for ``receiver``, oldest first.

        Envelopes whose ``expires_at_ms`` is <= ``now_ms`` are never returned.
        """
        ...

    def delete(self, receiver: UserId, message_ids: list[MessageId]) -> int:
        """Delete the given envelopes of ``receiver`` and return how many existed."""
        ...

    def purge_expired(self, now_ms: int) -> int:
        """Delete every envelope with ``expires_at_ms <= now_ms`` and return how many."""
        ...
