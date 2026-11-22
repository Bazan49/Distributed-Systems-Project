"""Mailbox service: the rules of one service node's mailbox
The mailbox is where encrypted envelopes wait for a receiver who is offline.
``MailboxStorePort`` only stores and retrieves; THIS service decides the rules:

* deposit is idempotent: the same message deposited twice is stored once, and
  repeating a deposit does NOT extend its life (a replay cannot keep a message
  alive forever);
* every envelope expires ``ttl_ms`` after it was first deposited, by the
  node's own clock (the envelope carries no deadline);
* an expired envelope is never handed out;
* an envelope is deleted when the receiver acknowledges it;
* a receiver never gets more than ``max_fetch`` envelopes per request.

It depends only on ports (store and clock) and the domain: no Ray, no SQLite.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from source.application.ports.clock import ClockPort
from source.application.ports.mailbox_store import MailboxStorePort
from source.domain.canonical import is_int
from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.records import PutOutcome

DAY_MS = 24 * 3600 * 1000
DEFAULT_TTL_MS = 14 * DAY_MS  # how long a message waits for an offline receiver
DEFAULT_MAX_FETCH = 100  # most envelopes handed out per collect


@dataclass(frozen=True)
class MailboxPolicy:
    """Tunable limits of a mailbox. Immutable, validated on creation."""

    ttl_ms: int = DEFAULT_TTL_MS
    max_fetch: int = DEFAULT_MAX_FETCH

    def __post_init__(self) -> None:
        if not is_int(self.ttl_ms) or self.ttl_ms < 1:
            raise ValueError("ttl_ms must be a positive integer")
        if not is_int(self.max_fetch) or self.max_fetch < 1:
            raise ValueError("max_fetch must be a positive integer")


class MailboxService:
    def __init__(
        self,
        store: MailboxStorePort,
        clock: ClockPort,
        policy: MailboxPolicy | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._policy = policy if policy is not None else MailboxPolicy()

    def deposit(self, envelope: Envelope) -> PutOutcome:
        """Keep ``envelope`` for its receiver until ``now + ttl``.

        Returns ``STORED`` the first time and ``ALREADY_PRESENT`` on a repeat
        (both mean the envelope is safely here). A repeat keeps the ORIGINAL
        expiry: the store never overwrites it.
        """
        expires_at = self._clock.now_ms() + self._policy.ttl_ms
        return self._store.put(envelope, expires_at)

    def collect(self, receiver: UserId, limit: int | None = None) -> list[Envelope]:
        """Pending, non-expired envelopes of ``receiver``, oldest first.

        Collecting does not delete anything: the receiver must acknowledge.
        ``limit`` is capped at the policy's ``max_fetch``.

        Raises:
            ValueError: ``limit`` is given but is not a positive integer.
        """
        if limit is None:
            limit = self._policy.max_fetch
        if not is_int(limit) or limit < 1:
            raise ValueError("limit must be a positive integer")
        capped = min(limit, self._policy.max_fetch)
        return self._store.fetch(receiver, capped, self._clock.now_ms())

    def acknowledge(self, receiver: UserId, message_ids: Sequence[MessageId]) -> int:
        """Delete the confirmed envelopes of ``receiver``; return how many existed.

        Unknown ids are ignored, repeated ids count once, and one receiver can
        never delete another receiver's envelopes.
        """
        unique = list(dict.fromkeys(message_ids))
        if not unique:
            return 0
        return self._store.delete(receiver, unique)

    def purge_expired(self) -> int:
        """Physically remove every expired envelope; return how many."""
        return self._store.purge_expired(self._clock.now_ms())
