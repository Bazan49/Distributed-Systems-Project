"""Delivery policy: get queued messages out, direct or through the mailboxes, with retries.

``SendMessageService`` only queues a sealed envelope. This use case drains the
outbox. For each message whose turn has come::

    receiver online? (presence)
      yes -> try the direct path with a SHORT deadline
               delivered    -> DELIVERED; it never touches a mailbox
               unreachable  -> fall through to the mailbox
      no / unknown -> mailbox
    deposit in the receiver's mailbox nodes (quorum)
      accepted -> ACCEPTED; the nodes hold it, no more retries
      refused  -> retry later with exponential backoff and jitter,
                  or EXPIRED once the attempt limit is reached

Design decisions:

* **Presence is a hint, never a requirement.** It only decides whether the
  direct path is worth a try. When presence says "offline" or cannot tell,
  the message goes straight to the mailboxes, which always work. Presence is
  looked up once per receiver per round.
* **A failed direct send is not a failed attempt.** It is the signal to use
  the mailbox. Only a refused deposit counts as an attempt.
* **Equal jitter backoff.** The wait after the n-th failed attempt is drawn
  uniformly from ``[c/2, c]`` with ``c = min(cap, base * 2^(n-1))``. The
  random half spreads clients that failed together (so they do not all
  retry at the same instant when a node comes back); the fixed half
  guarantees that the waits keep growing.
* **Per-conversation FIFO at the sender (head of line).** Only the oldest
  QUEUED message of a conversation is sent; if it fails, the later ones wait.
  Otherwise message 2 could arrive while message 1 waits for a retry, and if
  that takes longer than the receiver's gap timeout, message 1 would arrive
  late. Once the head is ACCEPTED, DELIVERED or EXPIRED, the next one goes.
* **Retry times are wall-clock times** (``next_attempt_at_ms``), unlike every
  other timer in the client: they are persisted in the outbox and must keep
  their meaning after a restart, which a monotonic reading would not.
* **At-least-once.** Every outcome is persisted right after its attempt. A
  crash between sending and persisting means the message is sent again,
  and the receiver drops the copy as a duplicate.
* **No private key needed.** Envelopes were signed when queued, so
  delivering them is a matter of moving bytes.

Collaborators. Delivery needs two things from the node side, each declared
below as a small interface so that it depends only on what it uses:

* ``MailboxDepositor``, implemented by ``QuorumClient``: deposit
  in the receiver's 3 nodes, accepted with 2. Delivery calls
  ``QuorumClient.deposit`` and NOT ``deposit_outgoing``: the latter counts
  every try in ``attempts``, while here ``attempts`` counts only FAILED
  deposits, because it drives the backoff and the expiry.
* ``PresenceLookup``, implemented by ``DirectoryClient``: a
  receiver is online while their signed presence record has not expired.

For the quorum to come back after an outage, the client must call
``QuorumClient.refresh_membership`` periodically: a node seen as DEAD is not
tried again until a ping finds it alive.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import Enum
from typing import Protocol

from source.application.ports.clock import ClockPort
from source.application.ports.local_store import LocalStorePort
from source.application.ports.peer_transport import PeerTransportPort
from source.domain.canonical import is_int
from source.domain.envelope import Envelope
from source.domain.identifiers import ConversationId, MessageId, UserId
from source.domain.messages import OutgoingMessage
from source.domain.outgoing_status import OutgoingStatus, advance
from source.domain.records import DirectSendResult

__all__ = [
    "DeliveryOutcome",
    "DeliveryPolicy",
    "DeliveryReport",
    "DeliveryService",
    "DepositResultLike",
    "MailboxDepositor",
    "PresenceLookup",
]


# what this use case needs from the other half


class DepositResultLike(Protocol):
    """The part of a deposit's result that delivery reads.

    ``QuorumClient.deposit`` returns a ``WriteResult``, which satisfies it.
    """

    @property
    def accepted(self) -> bool:
        """True if enough mailbox nodes (2 of 3) stored the envelope."""
        ...


class MailboxDepositor(Protocol):
    """What delivery needs from the quorum client (``QuorumClient``)."""

    def deposit(self, envelope: Envelope) -> DepositResultLike:
        """Store ``envelope`` in the receiver's mailbox nodes.

        Must not raise for node failures (they make ``accepted`` False) and
        must return within its own deadline.
        """
        ...


class PresenceLookup(Protocol):
    """What delivery needs from the directory client (``DirectoryClient``)."""

    def is_online(self, user_id: UserId) -> bool:
        """True only if ``user_id`` has a valid, unexpired presence record.

        False when there is no record or the directory cannot be reached.
        Must not raise.
        """
        ...


# policy


@dataclass(frozen=True)
class DeliveryPolicy:
    """Tunable limits of the delivery policy. Immutable, validated on creation.

    Attributes:
        direct_timeout_ms: deadline of a direct send. Short: a miss only
            means "use the mailbox", and the user must not notice it.
        backoff_base_ms: ceiling of the wait after the first failed attempt.
        backoff_cap_ms: the ceiling never grows beyond this.
        max_attempts: failed deposits after which a message is EXPIRED.
        batch_limit: most messages taken from the outbox per round.

    With the defaults, the ceilings are 1 s, 2 s, 4 s ... capped at 5 min,
    and a message expires after 12 failed attempts (roughly 15 to 30
    minutes of trying, depending on the jitter).
    """

    direct_timeout_ms: int = 1_500
    backoff_base_ms: int = 1_000
    backoff_cap_ms: int = 5 * 60 * 1_000
    max_attempts: int = 12
    batch_limit: int = 100

    def __post_init__(self) -> None:
        for name in (
            "direct_timeout_ms",
            "backoff_base_ms",
            "backoff_cap_ms",
            "max_attempts",
            "batch_limit",
        ):
            value = getattr(self, name)
            if not is_int(value) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.backoff_cap_ms < self.backoff_base_ms:
            raise ValueError("backoff_cap_ms must be >= backoff_base_ms")

    def backoff_ceiling_ms(self, failed_attempts: int) -> int:
        """The longest wait after ``failed_attempts`` (>= 1) failed attempts."""
        if not is_int(failed_attempts) or failed_attempts < 1:
            raise ValueError("failed_attempts must be an integer >= 1")
        # Cap the exponent first: shifting by a huge n builds a huge integer.
        doublings = min(failed_attempts - 1, self.backoff_cap_ms.bit_length())
        return min(self.backoff_cap_ms, self.backoff_base_ms << doublings)


# results


class DeliveryOutcome(str, Enum):
    """What one round did with one message."""

    DELIVERED_DIRECT = "delivered_direct"
    """The receiver got it directly; it never went through a mailbox."""
    ACCEPTED_BY_MAILBOX = "accepted_by_mailbox"
    """Enough mailbox nodes stored it; the receiver will collect it."""
    RETRY_SCHEDULED = "retry_scheduled"
    """Every path failed; it will be tried again after a backoff."""
    EXPIRED = "expired"
    """Every path failed and the attempt limit was reached; given up."""
    WAITING_FOR_EARLIER = "waiting_for_earlier"
    """Held back: an earlier message of the same conversation is still pending."""


@dataclass(frozen=True)
class DeliveryReport:
    """Outcome of one ``deliver_due`` round, in the order messages were considered."""

    outcomes: tuple[tuple[MessageId, DeliveryOutcome], ...] = ()

    def of(self, message_id: MessageId) -> DeliveryOutcome | None:
        """The outcome of ``message_id`` in this round, or None if not considered."""
        return dict(self.outcomes).get(message_id)

    def count(self, outcome: DeliveryOutcome) -> int:
        return sum(1 for _, o in self.outcomes if o is outcome)


# use case


class DeliveryService:
    """Drain the outbox: direct when possible, mailbox otherwise, retry with backoff.

    Call ``deliver_due`` when a message is queued, when the timer for
    ``next_attempt_at_ms`` fires, and when the client reconnects. Not
    thread-safe: one round at a time.
    """

    def __init__(
        self,
        store: LocalStorePort,
        peers: PeerTransportPort,
        mailbox: MailboxDepositor,
        presence: PresenceLookup,
        clock: ClockPort,
        policy: DeliveryPolicy | None = None,
        rng: random.Random | None = None,
    ) -> None:
        """Args:
        rng: source of the jitter. By default ``random.SystemRandom``, which
            draws from the operating system (``os.urandom``): it needs no
            seed, and two clients started at the same moment never share a
            sequence, so their retries really are spread out. Tests inject a
            seeded ``random.Random`` to be deterministic.
        """
        self._store = store
        self._peers = peers
        self._mailbox = mailbox
        self._presence = presence
        self._clock = clock
        self._policy = policy if policy is not None else DeliveryPolicy()
        self._rng = rng if rng is not None else random.SystemRandom()

    @property
    def policy(self) -> DeliveryPolicy:
        return self._policy

    def deliver_due(self) -> DeliveryReport:
        """Try every QUEUED message whose retry time has come, oldest first.

        Raises:
            StoreError: the local store failed. Outcomes persisted before
                the failure are kept; the rest are simply tried again.
        """
        now_ms = self._clock.now_ms()
        due = self._store.list_due_outgoing(now_ms, self._policy.batch_limit)
        heads = self._heads(m.conversation_id for m in due)
        online: dict[UserId, bool] = {}
        outcomes: list[tuple[MessageId, DeliveryOutcome]] = []

        for message in due:
            conversation = message.conversation_id
            if heads.get(conversation) != message.seq:
                outcomes.append(
                    (message.message_id, DeliveryOutcome.WAITING_FOR_EARLIER)
                )
                continue
            outcome = self._attempt(message, online)
            outcomes.append((message.message_id, outcome))
            if outcome is DeliveryOutcome.RETRY_SCHEDULED:
                heads[conversation] = None  # the head stays: the rest wait
            else:
                heads[conversation] = self._next_queued_seq(conversation, message.seq)

        return DeliveryReport(tuple(outcomes))

    def next_attempt_ms(self) -> int | None:
        """Wall-clock time of the earliest pending retry, or None if nothing is queued.

        A message that is due now reports the current time. The caller sets
        a timer for it and calls ``deliver_due``.

        Only the head of each conversation counts: the messages behind it
        cannot go before it, so reporting them as due would make the caller
        wake up for nothing, over and over.
        """
        now_ms = self._clock.now_ms()
        heads: dict[ConversationId, OutgoingMessage] = {}
        # Sorted by (conversation, seq): the first one seen is the head.
        for message in self._store.list_outgoing(statuses=(OutgoingStatus.QUEUED,)):
            heads.setdefault(message.conversation_id, message)
        times = [
            now_ms
            if m.next_attempt_at_ms is None
            else max(now_ms, m.next_attempt_at_ms)
            for m in heads.values()
        ]
        return min(times, default=None)

    # internals

    def _attempt(
        self, message: OutgoingMessage, online: dict[UserId, bool]
    ) -> DeliveryOutcome:
        """Try the paths for ONE message and persist the result."""
        receiver = message.receiver
        if receiver not in online:
            online[receiver] = self._presence.is_online(receiver)

        if online[receiver]:
            sent = self._peers.send_direct(
                receiver, message.envelope, self._policy.direct_timeout_ms
            )
            if sent is DirectSendResult.DELIVERED:
                self._save(message, OutgoingStatus.DELIVERED)
                return DeliveryOutcome.DELIVERED_DIRECT
            # A peer that just failed is not worth another direct try this round.
            online[receiver] = False

        if self._mailbox.deposit(message.envelope).accepted:
            self._save(message, OutgoingStatus.ACCEPTED)
            return DeliveryOutcome.ACCEPTED_BY_MAILBOX

        return self._fail(message)

    def _fail(self, message: OutgoingMessage) -> DeliveryOutcome:
        """Count a failed attempt: schedule a retry, or give up."""
        attempts = message.attempts + 1
        if attempts >= self._policy.max_attempts:
            self._save(
                message, OutgoingStatus.EXPIRED, attempts=attempts, retry_at=None
            )
            return DeliveryOutcome.EXPIRED
        ceiling = self._policy.backoff_ceiling_ms(attempts)
        wait = self._rng.randint(ceiling // 2, ceiling)
        retry_at = self._clock.now_ms() + wait
        self._save(message, OutgoingStatus.QUEUED, attempts=attempts, retry_at=retry_at)
        return DeliveryOutcome.RETRY_SCHEDULED

    def _save(
        self,
        message: OutgoingMessage,
        status: OutgoingStatus,
        *,
        attempts: int | None = None,
        retry_at: int | None = None,
    ) -> None:
        """Persist the new delivery state. Transitions go through the domain rule."""
        self._store.update_outgoing(
            replace(
                message,
                status=advance(message.status, status),
                attempts=message.attempts if attempts is None else attempts,
                next_attempt_at_ms=retry_at,
            )
        )

    def _heads(
        self, conversations: Iterable[ConversationId]
    ) -> dict[ConversationId, int | None]:
        """The seq of the oldest QUEUED message of each conversation (its head)."""
        return {
            conversation: self._next_queued_seq(conversation, 0)
            for conversation in set(conversations)
        }

    def _next_queued_seq(self, conversation: ConversationId, after: int) -> int | None:
        """The lowest seq > ``after`` still QUEUED in ``conversation``, or None."""
        queued = self._store.list_outgoing(
            conversation_id=conversation, statuses=(OutgoingStatus.QUEUED,)
        )
        return next((m.seq for m in queued if m.seq > after), None)
