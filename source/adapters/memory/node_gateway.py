"""In-memory fake of the whole service cluster, behind NodeGatewayPort.

Each node owns an in-memory mailbox and directory. Nodes can be "killed" and
"revived" by the test (``set_down`` / ``set_up``), which is what lets us test
quorum logic without Ray. The real adapter (task 2.3) replaces this with Ray.
"""

from collections.abc import Sequence

from source.adapters.memory.directory_store import InMemoryDirectoryStore
from source.adapters.memory.mailbox_store import InMemoryMailboxStore
from source.application.ports import ClockPort, DirectoryStorePort, MailboxStorePort
from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.node_id import NodeId
from source.domain.records import DirectoryRecord, NodeReply, PutOutcome

DEFAULT_MAILBOX_TTL_MS = 7 * 24 * 3600 * 1000  # 7 days, in milliseconds


class InMemoryNodeGateway:
    def __init__(
        self,
        node_ids: Sequence[NodeId],
        clock: ClockPort,
        mailbox_ttl_ms: int = DEFAULT_MAILBOX_TTL_MS,
    ) -> None:
        self._clock = clock
        self._ttl_ms = mailbox_ttl_ms
        self._mailboxes: dict[NodeId, MailboxStorePort] = {
            n: InMemoryMailboxStore() for n in node_ids
        }
        self._directories: dict[NodeId, DirectoryStorePort] = {
            n: InMemoryDirectoryStore() for n in node_ids
        }
        self._down: set[NodeId] = set()

    # ---- test-only controls (not part of NodeGatewayPort) ----
    def set_down(self, node: NodeId) -> None:
        """Simulate a crashed node: every call to it fails."""
        self._down.add(node)

    def set_up(self, node: NodeId) -> None:
        """Bring the node back (its data is kept, like a restart with SQLite)."""
        self._down.discard(node)

    def mailbox_of(self, node: NodeId) -> MailboxStorePort:
        """Look inside a node (for assertions)."""
        return self._mailboxes[node]

    # ---- helpers ----
    def _unavailable(self, node: NodeId) -> str | None:
        if node not in self._mailboxes:
            return "unknown node"
        if node in self._down:
            return "node down"
        return None

    @staticmethod
    def _unique(nodes: Sequence[NodeId]) -> list[NodeId]:
        return list(dict.fromkeys(nodes))  # one reply per node asked

    # ---- NodeGatewayPort ----
    def list_nodes(self) -> list[NodeId]:
        return sorted(self._mailboxes)

    def ping(self, nodes: Sequence[NodeId], timeout: float) -> list[NodeReply[None]]:
        replies: list[NodeReply[None]] = []
        for node in self._unique(nodes):
            error = self._unavailable(node)
            replies.append(NodeReply(node=node, ok=error is None, error=error))
        return replies

    def deposit(
        self, nodes: Sequence[NodeId], envelope: Envelope, required: int, timeout: float
    ) -> list[NodeReply[PutOutcome]]:
        replies: list[NodeReply[PutOutcome]] = []
        ok_count = 0
        for node in self._unique(nodes):
            if ok_count >= required:
                break  # early return, as ray.wait(num_returns=required) would
            error = self._unavailable(node)
            if error is not None:
                replies.append(NodeReply(node=node, ok=False, error=error))
                continue
            outcome = self._mailboxes[node].put(
                envelope, self._clock.now_ms() + self._ttl_ms
            )
            replies.append(NodeReply(node=node, ok=True, value=outcome))
            ok_count += 1
        return replies

    def collect(
        self, nodes: Sequence[NodeId], receiver: UserId, limit: int, timeout: float
    ) -> list[NodeReply[list[Envelope]]]:
        replies: list[NodeReply[list[Envelope]]] = []
        for node in self._unique(nodes):
            error = self._unavailable(node)
            if error is not None:
                replies.append(NodeReply(node=node, ok=False, error=error))
                continue
            found = self._mailboxes[node].fetch(receiver, limit, self._clock.now_ms())
            replies.append(NodeReply(node=node, ok=True, value=found))
        return replies

    def acknowledge(
        self,
        nodes: Sequence[NodeId],
        receiver: UserId,
        message_ids: list[MessageId],
        timeout: float,
    ) -> list[NodeReply[int]]:
        replies: list[NodeReply[int]] = []
        for node in self._unique(nodes):
            error = self._unavailable(node)
            if error is not None:
                replies.append(NodeReply(node=node, ok=False, error=error))
                continue
            deleted = self._mailboxes[node].delete(receiver, message_ids)
            replies.append(NodeReply(node=node, ok=True, value=deleted))
        return replies

    def publish_record(
        self,
        nodes: Sequence[NodeId],
        record: DirectoryRecord,
        required: int,
        timeout: float,
    ) -> list[NodeReply[bool]]:
        replies: list[NodeReply[bool]] = []
        ok_count = 0
        for node in self._unique(nodes):
            if ok_count >= required:
                break
            error = self._unavailable(node)
            if error is not None:
                replies.append(NodeReply(node=node, ok=False, error=error))
                continue
            self._directories[node].put(record)
            replies.append(NodeReply(node=node, ok=True, value=True))
            ok_count += 1
        return replies

    def resolve_record(
        self, nodes: Sequence[NodeId], user_id: UserId, timeout: float
    ) -> list[NodeReply[DirectoryRecord | None]]:
        replies: list[NodeReply[DirectoryRecord | None]] = []
        for node in self._unique(nodes):
            error = self._unavailable(node)
            if error is not None:
                replies.append(NodeReply(node=node, ok=False, error=error))
                continue
            replies.append(
                NodeReply(
                    node=node, ok=True, value=self._directories[node].get(user_id)
                )
            )
        return replies
