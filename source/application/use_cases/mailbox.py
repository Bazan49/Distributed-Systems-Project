"""Mailbox service: the rules of one service node's mailbox
The mailbox is where encrypted envelopes wait for a receiver who is offline.
``MailboxStorePort`` only stores and retrieves; THIS service decides the rules:

* deposit is idempotent: the same message deposited twice is stored once, and
  repeating a deposit does NOT extend its life (a replay cannot keep a message
  alive forever);
* every envelope expires ``ttl_ms`` after it was first deposited, by the
  node's own clock (the envelope carries no deadline);
* an expired envelope is never handed out;
* ONLY the receiver can collect or acknowledge: every such request is a
  ``MailboxRequest`` signed by the receiver's key, fresh (``max_skew_ms``) and
  never seen before (nonce). Anything else is refused with
  ``MailboxAccessDenied`` before the store is touched;
* an envelope is deleted when the receiver acknowledges it;
* a receiver never gets more than ``max_fetch`` envelopes per request.

Depositing needs no signature from the depositor: the envelope itself is signed
by its sender and the receiver verifies it. Spam control is out of Phase 1.

It depends only on ports (store, crypto, clock) and the domain: no Ray, no SQLite.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.mailbox_store import MailboxStorePort
from source.domain.canonical import is_int
from source.domain.envelope import Envelope
from source.domain.mailbox_access import (
    DEFAULT_MAX_SKEW_MS,
    MailboxAction,
    MailboxRequest,
    ReplayGuard,
)
from source.domain.records import PutOutcome

DAY_MS = 24 * 3600 * 1000
DEFAULT_TTL_MS = 14 * DAY_MS  # how long a message waits for an offline receiver
DEFAULT_MAX_FETCH = 100  # most envelopes handed out per collect


@dataclass(frozen=True)
class MailboxPolicy:
    """Tunable limits of a mailbox. Immutable, validated on creation."""

    ttl_ms: int = DEFAULT_TTL_MS
    max_fetch: int = DEFAULT_MAX_FETCH
    max_skew_ms: int = DEFAULT_MAX_SKEW_MS  # tolerated client/node clock difference

    def __post_init__(self) -> None:
        if not is_int(self.ttl_ms) or self.ttl_ms < 1:
            raise ValueError("ttl_ms must be a positive integer")
        if not is_int(self.max_fetch) or self.max_fetch < 1:
            raise ValueError("max_fetch must be a positive integer")
        if not is_int(self.max_skew_ms) or self.max_skew_ms < 1:
            raise ValueError("max_skew_ms must be a positive integer")


class AccessDenial(Enum):
    """Why a collect/acknowledge request was refused."""

    WRONG_ACTION = "wrong_action"  # e.g. a collect request sent to acknowledge
    BAD_IDENTITY = "bad_identity"  # receiver is not the hash of the carried key
    INVALID_SIGNATURE = "invalid_signature"
    STALE = "stale"  # issued_at_ms too far from the node's clock
    REPLAYED = "replayed"  # nonce already used


class MailboxAccessDenied(Exception):
    """The request is not authentic, not fresh, or not for this operation."""

    def __init__(self, reason: AccessDenial) -> None:
        super().__init__(f"mailbox access denied: {reason.value}")
        self.reason = reason


class MailboxService:
    def __init__(
        self,
        store: MailboxStorePort,
        crypto: CryptoPort,
        clock: ClockPort,
        policy: MailboxPolicy | None = None,
    ) -> None:
        self._store = store
        self._crypto = crypto
        self._clock = clock
        self._policy = policy if policy is not None else MailboxPolicy()
        self._replays = ReplayGuard(self._policy.max_skew_ms)

    def deposit(self, envelope: Envelope) -> PutOutcome:
        """Keep ``envelope`` for its receiver until ``now + ttl``.

        Returns ``STORED`` the first time and ``ALREADY_PRESENT`` on a repeat
        (both mean the envelope is safely here). A repeat keeps the ORIGINAL
        expiry: the store never overwrites it.
        """
        expires_at_ms = self._clock.now_ms() + self._policy.ttl_ms
        return self._store.put(envelope, expires_at_ms)

    def collect(self, request: MailboxRequest) -> list[Envelope]:
        """Pending, non-expired envelopes of ``request.receiver``, oldest first.

        Collecting does not delete anything: the receiver must acknowledge.
        ``request.limit`` is capped at the policy's ``max_fetch``.

        Raises:
            MailboxAccessDenied: the request is not a valid, fresh COLLECT
                signed by the receiver.
        """
        self._authorize(request, MailboxAction.COLLECT)
        capped = min(request.limit, self._policy.max_fetch)
        return self._store.fetch(request.receiver, capped, self._clock.now_ms())

    def acknowledge(self, request: MailboxRequest) -> int:
        """Delete the confirmed envelopes of ``request.receiver``; return how many existed.

        Unknown ids are ignored and one receiver can never delete another
        receiver's envelopes.

        Raises:
            MailboxAccessDenied: the request is not a valid, fresh ACKNOWLEDGE
                signed by the receiver.
        """
        self._authorize(request, MailboxAction.ACKNOWLEDGE)
        return self._store.delete(request.receiver, list(request.message_ids))

    def purge_expired(self) -> int:
        """Physically remove every expired envelope; return how many."""
        return self._store.purge_expired(self._clock.now_ms())

    def _authorize(self, request: MailboxRequest, action: MailboxAction) -> None:
        """Cheap checks first; the nonce is only consumed by an authentic request."""
        if request.action is not action:
            raise MailboxAccessDenied(AccessDenial.WRONG_ACTION)
        now = self._clock.now_ms()
        if not self._replays.is_fresh(request.issued_at_ms, now):
            raise MailboxAccessDenied(AccessDenial.STALE)
        if not request.identity_matches():
            raise MailboxAccessDenied(AccessDenial.BAD_IDENTITY)
        if not self._crypto.verify(
            request.public_key, request.signing_bytes(), request.signature
        ):
            raise MailboxAccessDenied(AccessDenial.INVALID_SIGNATURE)
        if not self._replays.check_and_remember(
            request.nonce, request.issued_at_ms, now
        ):
            raise MailboxAccessDenied(AccessDenial.REPLAYED)
