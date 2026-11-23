from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.mailbox_access import MailboxRequest
from source.domain.node_id import NodeId
from source.domain.records import DirectoryRecord, NodeReply, PutOutcome


@runtime_checkable
class NodeGatewayPort(Protocol):
    """How a client talks to the service nodes. THE port that hides Ray.

    Phase 1 adapter: Ray actors (``ray.wait`` for "k of n"). Phase 2: our own
    network protocol. Rules for every method:

    * Every call has a ``timeout_ms`` (integer milliseconds). Nothing may
      block forever.
    * One node failing (crash, timeout, actor error) is NOT an exception: it is
      a ``NodeReply`` with ``ok=False``. The quorum logic counts replies; it
      never sees Ray exceptions.
    * A node that REFUSES a request (bad signature, stale or replayed
      request, rejected record) also answers ``ok=False``, with the reason in
      ``error``. The caller must not retry the same signed request.
    * The result has at most one reply per node asked.
    * Envelopes, mailbox requests and records cross the boundary as domain
      objects. Adapters that cross a process boundary should send
      ``to_bytes()`` and rebuild with ``from_bytes()``, which strictly
      validates untrusted input.
    """

    def list_nodes(self) -> list[NodeId]:
        """Service nodes currently known to the cluster (discovery)."""
        ...

    def ping(self, nodes: Sequence[NodeId], timeout_ms: int) -> list[NodeReply[None]]:
        """Check liveness. Feeds the membership state (alive / suspect / dead)."""
        ...

    def deposit(
        self,
        nodes: Sequence[NodeId],
        envelope: Envelope,
        required: int,
        timeout_ms: int,
    ) -> list[NodeReply[PutOutcome]]:
        """Put ``envelope`` in the mailbox of each node.

        Returns as soon as ``required`` nodes answered OK, or when all answered
        or the timeout expired, whichever happens first. Each node sets its own
        expiry (see ``MailboxStorePort``).
        """
        ...

    def collect(
        self, nodes: Sequence[NodeId], request: MailboxRequest, timeout_ms: int
    ) -> list[NodeReply[list[Envelope]]]:
        """Fetch pending envelopes of ``request.receiver`` from every node (wait for all).

        ``request`` must be a COLLECT signed by the receiver (see
        ``source.domain.mailbox_access``); the same request goes to every node.
        """
        ...

    def acknowledge(
        self, nodes: Sequence[NodeId], request: MailboxRequest, timeout_ms: int
    ) -> list[NodeReply[int]]:
        """Ask every node to delete the envelopes listed in ``request``.

        ``request`` must be an ACKNOWLEDGE signed by the receiver. Each OK
        reply carries how many of those envelopes the node still had.
        """
        ...

    def publish_record(
        self,
        nodes: Sequence[NodeId],
        record: DirectoryRecord,
        required: int,
        timeout_ms: int,
    ) -> list[NodeReply[bool]]:
        """Store a signed record in each node. Same early-return rule as ``deposit``.

        ``ok=True`` means the node accepted the record (stored, or already had
        it). A node that rejects it (forged, stale, expired) answers ``ok=False``.
        """
        ...

    def resolve_record(
        self, nodes: Sequence[NodeId], user_id: UserId, timeout_ms: int
    ) -> list[NodeReply[DirectoryRecord | None]]:
        """Read the record of ``user_id`` from every node (the domain keeps the highest version)."""
        ...
