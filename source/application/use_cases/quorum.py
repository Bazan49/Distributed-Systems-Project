"""Quorum client: how the client talks to the replicated mailboxes and directory.

* ``deposit``: put an envelope in the receiver's 3 nodes; accepted with 2.
* ``deposit_outgoing``: the same for a message of the send queue. It is marked
  ACCEPTED only if the quorum was reached; otherwise it stays QUEUED.
* ``collect`` / ``acknowledge``: ask every live node that may hold the
  receiver's mailbox, merge what they return, and later ask them all to delete.
* ``publish_record`` / ``resolve``: the same quorum for directory records; on
  read, the highest valid version wins.

Which nodes and how answers combine is decided by ``source.domain.quorum``.
This service adds the I/O: it calls the ``NodeGatewayPort`` and, with every
answer, updates the ``MembershipService`` (a node that refused a request is
alive; only a node that did not answer is reported as failing).

Logs (see ``docs/logging.md``): every write (deposit or publish) emits ONE
``quorum.write`` event at DEBUG, with the nodes asked, the nodes that
confirmed and whether the quorum was reached. DEBUG because it happens on
every send and every presence renewal: it is detail for debugging, and the
callers log what the write MEANS (for example ``delivery.mailbox_accepted``).

Depends only on ports, other use cases and the domain: no Ray, no SQLite.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from enum import Enum

from source.application.observability import get_logger
from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.node_gateway import NodeGatewayPort
from source.application.use_cases.directory import (
    RecordCheck,
    check_record_authenticity,
)
from source.application.use_cases.membership import MembershipService
from source.domain.canonical import is_int
from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.mailbox_access import MailboxAction, MailboxRequest
from source.domain.messages import OutgoingMessage
from source.domain.node_id import NodeId
from source.domain.outgoing_status import OutgoingStatus, advance
from source.domain.placement import REPLICAS
from source.domain.quorum import (
    DEFAULT_WRITE_QUORUM,
    confirmations,
    latest_record,
    merge_envelopes,
    quorum_reached,
    read_targets,
    write_targets,
)
from source.domain.records import DirectoryRecord, NodeReply

DEFAULT_TIMEOUT_MS = 2_000

log = get_logger(__name__)


class WriteKind(Enum):
    """What a quorum write stores: an envelope or a directory record."""

    DEPOSIT = "deposit"
    PUBLISH = "publish"


class WriteOutcome(Enum):
    """Whether a quorum write reached ``write_quorum`` confirmations."""

    ACCEPTED = "accepted"
    NO_QUORUM = "no_quorum"


@dataclass(frozen=True)
class QuorumPolicy:
    replicas: int = REPLICAS  # copies of each user's data
    write_quorum: int = DEFAULT_WRITE_QUORUM  # confirmations to accept a write
    timeout_ms: int = DEFAULT_TIMEOUT_MS  # per gateway call

    def __post_init__(self) -> None:
        for name in ("replicas", "write_quorum", "timeout_ms"):
            value = getattr(self, name)
            if not is_int(value) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.write_quorum > self.replicas:
            raise ValueError("write_quorum cannot be larger than replicas")


@dataclass(frozen=True)
class WriteResult:
    """Outcome of a quorum write (deposit or publish)."""

    accepted: bool  # at least ``write_quorum`` nodes confirmed
    confirmed_by: tuple[NodeId, ...]
    asked: tuple[NodeId, ...]

    @property
    def outcome(self) -> WriteOutcome:
        return WriteOutcome.ACCEPTED if self.accepted else WriteOutcome.NO_QUORUM


@dataclass(frozen=True)
class CollectResult:
    """Envelopes merged from every node that answered."""

    envelopes: tuple[Envelope, ...]
    answered_by: tuple[NodeId, ...]  # nodes that returned their mailbox
    asked: tuple[NodeId, ...]


class QuorumClient:
    def __init__(
        self,
        gateway: NodeGatewayPort,
        membership: MembershipService,
        crypto: CryptoPort,
        clock: ClockPort,
        policy: QuorumPolicy | None = None,
    ) -> None:
        self._gateway = gateway
        self._membership = membership
        self._crypto = crypto
        self._clock = clock
        self._policy = policy if policy is not None else QuorumPolicy()

    # ---- membership ----
    def refresh_membership(self) -> list[NodeId]:
        """Ping EVERY known node, dead ones included; return those that answered.

        The quorum skips DEAD nodes, so without this a dead node that came back
        would never be tried again. Call it periodically.
        """
        replies = self._gateway.ping(
            self._gateway.list_nodes(), self._policy.timeout_ms
        )
        self._observe(replies)
        return confirmations(replies)

    # ---- mailbox ----
    def deposit(self, envelope: Envelope) -> WriteResult:
        """Put ``envelope`` in the receiver's nodes; accepted with ``write_quorum``."""
        targets = self._write_targets(envelope.receiver)
        replies = (
            self._gateway.deposit(
                targets, envelope, self._policy.write_quorum, self._policy.timeout_ms
            )
            if targets
            else []
        )
        return self._write_result(
            WriteKind.DEPOSIT, envelope.message_id, targets, replies
        )

    def deposit_outgoing(self, message: OutgoingMessage) -> OutgoingMessage:
        """Try to deposit a queued message; return its new delivery state.

        Only a QUEUED message is sent (any other one is returned unchanged).
        Every try counts in ``attempts``. With the quorum the message becomes
        ACCEPTED; without it, it stays QUEUED for a later retry, which resends
        exactly the same envelope. Persisting the result
        (``LocalStorePort.update_outgoing``) and scheduling the retry
        (``next_attempt_at_ms``) are the caller's job.
        """
        if message.status is not OutgoingStatus.QUEUED:
            return message
        result = self.deposit(message.envelope)
        status = (
            advance(message.status, OutgoingStatus.ACCEPTED)
            if result.accepted
            else message.status
        )
        return replace(message, status=status, attempts=message.attempts + 1)

    def collect(self, request: MailboxRequest) -> CollectResult:
        """Fetch the receiver's pending envelopes from every live node and merge them.

        ``request`` is a COLLECT signed by the receiver; the same one goes to
        every node. The result may contain envelopes the receiver already has
        (a node that was down kept a copy): the receiver drops them by id.
        """
        if request.action is not MailboxAction.COLLECT:
            raise ValueError("collect needs a COLLECT request")
        targets = self._read_targets(request.receiver)
        if not targets:
            return CollectResult(envelopes=(), answered_by=(), asked=())
        replies = self._gateway.collect(targets, request, self._policy.timeout_ms)
        self._observe(replies)
        return CollectResult(
            envelopes=tuple(merge_envelopes(replies)),
            answered_by=tuple(confirmations(replies)),
            asked=tuple(targets),
        )

    def acknowledge(self, request: MailboxRequest) -> tuple[NodeId, ...]:
        """Ask every live node of the receiver to delete the listed envelopes.

        Returns the nodes that confirmed. A node that missed it (down) may
        return those envelopes again later; the receiver drops them as
        duplicates and acknowledges again.
        """
        if request.action is not MailboxAction.ACKNOWLEDGE:
            raise ValueError("acknowledge needs an ACKNOWLEDGE request")
        targets = self._read_targets(request.receiver)
        if not targets:
            return ()
        replies = self._gateway.acknowledge(targets, request, self._policy.timeout_ms)
        self._observe(replies)
        return tuple(confirmations(replies))

    # ---- directory ----
    def publish_record(self, record: DirectoryRecord) -> WriteResult:
        """Store the user's signed record in their nodes; accepted with ``write_quorum``."""
        targets = self._write_targets(record.user_id)
        replies = (
            self._gateway.publish_record(
                targets, record, self._policy.write_quorum, self._policy.timeout_ms
            )
            if targets
            else []
        )
        return self._write_result(WriteKind.PUBLISH, record.user_id, targets, replies)

    def resolve(self, user_id: UserId) -> DirectoryRecord | None:
        """The newest valid record of ``user_id``, or None.

        Nodes are not trusted: every record is checked again here (it belongs
        to ``user_id``, the key matches the id, the signature is valid, it has
        not expired). A forged record is ignored, so a lying node can at most
        hide a record, never replace it. Of the valid ones, the highest
        version wins: one node with an old copy does not matter.
        """
        targets = self._read_targets(user_id)
        if not targets:
            return None
        replies = self._gateway.resolve_record(
            targets, user_id, self._policy.timeout_ms
        )
        self._observe(replies)
        now = self._clock.now_ms()
        valid = [
            r.value
            for r in replies
            if r.ok
            and r.value is not None
            and r.value.user_id == user_id
            and r.value.expires_at_ms > now
            and check_record_authenticity(r.value, self._crypto) is RecordCheck.VALID
        ]
        return latest_record(valid)

    # ---- helpers ----
    def _nodes(self) -> tuple[list[NodeId], list[NodeId]]:
        known = self._gateway.list_nodes()
        return known, self._membership.get_active_nodes(known)

    def _write_targets(self, user: UserId) -> list[NodeId]:
        known, active = self._nodes()
        return write_targets(user, known, active, self._policy.replicas)

    def _read_targets(self, user: UserId) -> list[NodeId]:
        known, active = self._nodes()
        return read_targets(user, known, active, self._policy.replicas)

    def _write_result(
        self,
        kind: WriteKind,
        subject: MessageId | UserId,
        targets: Sequence[NodeId],
        replies: Sequence[NodeReply[object]],
    ) -> WriteResult:
        """Count the confirmations, update membership and log the write (once)."""
        self._observe(replies)
        result = WriteResult(
            accepted=quorum_reached(replies, self._policy.write_quorum),
            confirmed_by=tuple(confirmations(replies)),
            asked=tuple(targets),
        )
        # A deposit is identified by its message, a publish by its user: the
        # standard field names "msg" and "user" (the formatter shortens "user").
        subject_field = "msg" if kind is WriteKind.DEPOSIT else "user"
        log.debug(
            "quorum.write",
            kind=kind,
            **{subject_field: subject},
            asked=result.asked,
            confirmed_by=result.confirmed_by,
            outcome=result.outcome,
        )
        return result

    def _observe(self, replies: Sequence[NodeReply[object]]) -> None:
        """Feed membership: answered (even refusing) = alive; no answer = failure."""
        for reply in replies:
            if reply.answered:
                self._membership.report_success(reply.node)
            else:
                self._membership.report_failure(reply.node)
