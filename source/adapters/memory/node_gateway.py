"""In-memory fake of the whole service cluster, behind NodeGatewayPort.

Each node runs the REAL node services (``MailboxService`` and
``DirectoryService``) over in-memory stores, exactly what the Ray node actor
(task 2.2) wraps. So a forged mailbox request or directory record is refused
here just as it would be in the cluster. Nodes can be "killed" and "revived"
by the test (``set_down`` / ``set_up``), which is what lets us test quorum
logic without Ray. The real adapter (task 2.3) replaces this with Ray.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar

from source.adapters.memory.directory_store import InMemoryDirectoryStore
from source.adapters.memory.mailbox_store import InMemoryMailboxStore
from source.application.ports import ClockPort, CryptoPort
from source.application.use_cases.directory import DirectoryPolicy, DirectoryService
from source.application.use_cases.mailbox import (
    MailboxAccessDenied,
    MailboxPolicy,
    MailboxService,
)
from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.mailbox_access import MailboxRequest
from source.domain.node_id import NodeId
from source.domain.records import DirectoryRecord, NodeReply, PutOutcome

T = TypeVar("T")


@dataclass
class _Node:
    mailbox: MailboxService
    directory: DirectoryService


class _Refused(Exception):
    """The node answered, but refused the request."""


class InMemoryNodeGateway:
    def __init__(
        self,
        node_ids: Sequence[NodeId],
        clock: ClockPort,
        crypto: CryptoPort,
        mailbox_policy: MailboxPolicy | None = None,
        directory_policy: DirectoryPolicy | None = None,
    ) -> None:
        self._nodes: dict[NodeId, _Node] = {
            n: _Node(
                mailbox=MailboxService(
                    InMemoryMailboxStore(), crypto, clock, mailbox_policy
                ),
                directory=DirectoryService(
                    InMemoryDirectoryStore(), crypto, clock, directory_policy
                ),
            )
            for n in node_ids
        }
        self._down: set[NodeId] = set()

    # ---- test-only controls (not part of NodeGatewayPort) ----
    def set_down(self, node: NodeId) -> None:
        """Simulate a crashed node: every call to it fails."""
        self._down.add(node)

    def set_up(self, node: NodeId) -> None:
        """Bring the node back (its data is kept, like a restart with SQLite)."""
        self._down.discard(node)

    def mailbox_of(self, node: NodeId) -> MailboxService:
        """Look inside a node (for assertions)."""
        return self._nodes[node].mailbox

    # ---- helpers ----
    def _call(
        self,
        nodes: Sequence[NodeId],
        action: Callable[[_Node], T],
        required: int | None = None,
    ) -> list[NodeReply[T]]:
        """Run ``action`` on each distinct node and turn every outcome into a reply.

        With ``required``, stop as soon as that many nodes answered OK, as
        ``ray.wait(num_returns=required)`` would.
        """
        replies: list[NodeReply[T]] = []
        ok_count = 0
        for node_id in dict.fromkeys(nodes):  # one reply per node asked
            if required is not None and ok_count >= required:
                break
            node = self._nodes.get(node_id)
            if node is None:
                replies.append(NodeReply(node=node_id, ok=False, error="unknown node"))
                continue
            if node_id in self._down:
                replies.append(NodeReply(node=node_id, ok=False, error="node down"))
                continue
            try:
                value = action(node)
            except (MailboxAccessDenied, _Refused) as exc:
                replies.append(
                    NodeReply(node=node_id, ok=False, error=str(exc), refused=True)
                )
                continue
            replies.append(NodeReply(node=node_id, ok=True, value=value))
            ok_count += 1
        return replies

    @staticmethod
    def _publish(node: _Node, record: DirectoryRecord) -> bool:
        outcome = node.directory.publish(record)
        if not outcome.accepted:
            raise _Refused(f"record refused: {outcome.value}")
        return True

    # ---- NodeGatewayPort ----
    def list_nodes(self) -> list[NodeId]:
        return sorted(self._nodes)

    def ping(self, nodes: Sequence[NodeId], timeout_ms: int) -> list[NodeReply[None]]:
        return self._call(nodes, lambda _node: None)

    def deposit(
        self,
        nodes: Sequence[NodeId],
        envelope: Envelope,
        required: int,
        timeout_ms: int,
    ) -> list[NodeReply[PutOutcome]]:
        return self._call(nodes, lambda n: n.mailbox.deposit(envelope), required)

    def collect(
        self, nodes: Sequence[NodeId], request: MailboxRequest, timeout_ms: int
    ) -> list[NodeReply[list[Envelope]]]:
        return self._call(nodes, lambda n: n.mailbox.collect(request))

    def acknowledge(
        self, nodes: Sequence[NodeId], request: MailboxRequest, timeout_ms: int
    ) -> list[NodeReply[int]]:
        return self._call(nodes, lambda n: n.mailbox.acknowledge(request))

    def publish_record(
        self,
        nodes: Sequence[NodeId],
        record: DirectoryRecord,
        required: int,
        timeout_ms: int,
    ) -> list[NodeReply[bool]]:
        return self._call(nodes, lambda n: self._publish(n, record), required)

    def resolve_record(
        self, nodes: Sequence[NodeId], user_id: UserId, timeout_ms: int
    ) -> list[NodeReply[DirectoryRecord | None]]:
        return self._call(nodes, lambda n: n.directory.resolve(user_id))
