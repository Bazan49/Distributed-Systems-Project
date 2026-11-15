from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.node_id import NodeId
from source.domain.records import DirectoryRecord, NodeReply, PutOutcome


@runtime_checkable
class NodeGatewayPort(Protocol):
    """How a client talks to the service nodes. THE port that hides Ray.

    Phase 1 adapter: Ray actors (``ray.wait`` for "k of n"). Phase 2: our own
    network protocol. Rules for every method:

    * Every call has a ``timeout`` in seconds. Nothing may block forever.
    * One node failing (crash, timeout, actor error) is NOT an exception: it is
      a ``NodeReply`` with ``ok=False``. The quorum logic counts replies; it
      never sees Ray exceptions.
    * The result has at most one reply per node asked.
    * Envelopes cross the boundary as ``Envelope`` objects. Adapters that cross
      a process boundary should send ``envelope.to_bytes()`` and rebuild with
      ``Envelope.from_bytes()``, which strictly validates untrusted input.
    """

    def list_nodes(self) -> list[NodeId]:
        """Service nodes currently known to the cluster (discovery)."""
        ...

    def ping(self, nodes: Sequence[NodeId], timeout: float) -> list[NodeReply[None]]:
        """Check liveness. Feeds the membership state (alive / suspect / dead)."""
        ...

    def deposit(
        self, nodes: Sequence[NodeId], envelope: Envelope, required: int, timeout: float
    ) -> list[NodeReply[PutOutcome]]:
        """Put ``envelope`` in the mailbox of each node.

        Returns as soon as ``required`` nodes answered OK, or when all answered
        or the timeout expired, whichever happens first. Each node sets its own
        expiry (see ``MailboxStorePort``).
        """
        ...

    def collect(
        self, nodes: Sequence[NodeId], receiver: UserId, limit: int, timeout: float
    ) -> list[NodeReply[list[Envelope]]]:
        """Fetch pending envelopes of ``receiver`` from every node (wait for all)."""
        ...

    def acknowledge(
        self,
        nodes: Sequence[NodeId],
        receiver: UserId,
        message_ids: list[MessageId],
        timeout: float,
    ) -> list[NodeReply[int]]:
        """Ask every node to delete the confirmed envelopes."""
        ...

    def publish_record(
        self,
        nodes: Sequence[NodeId],
        record: DirectoryRecord,
        required: int,
        timeout: float,
    ) -> list[NodeReply[bool]]:
        """Store a signed record in each node. Same early-return rule as ``deposit``."""
        ...

    def resolve_record(
        self, nodes: Sequence[NodeId], user_id: UserId, timeout: float
    ) -> list[NodeReply[DirectoryRecord | None]]:
        """Read the record of ``user_id`` from every node (the domain keeps the highest version)."""
        ...
