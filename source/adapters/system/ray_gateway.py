"""RayNodeGateway: the real ``NodeGatewayPort``, talking to the node actors (task 2.3).

The client's quorum logic (``QuorumClient``) only knows the port. This adapter
turns each port call into calls to the service-node actors of task 2.2
(``ServiceNode`` in ``ray_node.py``) and turns whatever happens into
``NodeReply`` values. The quorum logic never sees a Ray exception.

How a call works
================

1. Every node asked gets its call at the same time (Ray actor calls do not
   block: ``handle.method.remote(...)`` returns an ``ObjectRef`` at once).
2. ``ray.wait`` waits for the answers, one by one, until:

   * ``required`` nodes answered OK (writes: "2 of 3"), or
   * every node answered, or
   * the deadline (``timeout_ms`` for the WHOLE call) has passed.

3. Every node asked gets exactly one reply, except the nodes still pending
   when the quorum was reached: they are left out. Their calls are NOT
   cancelled: they go on in the background, so the third node normally also
   stores the envelope, it is just not waited for.

What each outcome becomes
=========================

======================================  ==========================================
What happened                           ``NodeReply``
======================================  ==========================================
The node did it                         ``ok=True``, ``value`` converted back
The node refused (bad signature...)     ``ok=False, refused=True``, its reason
The actor is dead / restarting          ``ok=False``, ``error="node down: ..."``
The method crashed (e.g. disk error)    ``ok=False``, ``error="node error: ..."``
No answer before the deadline           ``ok=False``, ``error="timeout"``
Node not in the configured list         ``ok=False``, ``error="unknown node"``
The answer is malformed (lying node)    ``ok=False``, ``error="invalid answer: ..."``
======================================  ==========================================

Only a refusal has ``refused=True``: every other failure means "did not
answer", which is what membership needs to know.

Wire format: envelopes and mailbox requests travel as bytes and are rebuilt
with ``from_bytes`` (strict validation), in both directions. The node is not
trusted: every answer is checked before it is handed to the quorum logic.

Logs (see ``docs/logging.md``): ``actor.call_timeout`` (WARNING) for each
node that did not answer before the deadline, with the ``method`` and the
``timeout_ms`` of the call. The other failures become replies that the
quorum logic and membership already log (``quorum.write_failed``,
``node.state_changed``).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

import ray
from ray.exceptions import RayActorError, RayError

from source.adapters.system.ray_node import actor_name
from source.application.observability import get_logger
from source.domain.canonical import FormatError, is_int
from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.mailbox_access import MailboxRequest
from source.domain.node_id import NodeId
from source.domain.records import DirectoryRecord, NodeReply, PutOutcome

T = TypeVar("T")

log = get_logger(__name__)

# Turns the ``value`` of an OK reply into the port's type; raises if it is wrong.
Parser = Callable[[Any], T]


DEFAULT_DISCOVERY_TIMEOUT_MS = 5_000


class DiscoveryError(Exception):
    """No initial node gave the list of nodes (all down, unknown or lying)."""


class _InvalidAnswer(Exception):
    """The node's answer does not have the expected shape."""


def _check_timeout(timeout_ms: int) -> None:
    if not is_int(timeout_ms) or timeout_ms < 0:
        raise ValueError("timeout_ms must be a non-negative integer (milliseconds)")


# ---- parsers of the values each method returns ----
def _none(value: Any) -> None:
    if value is not None:
        raise _InvalidAnswer("expected no value")


def _put_outcome(value: Any) -> PutOutcome:
    if not isinstance(value, PutOutcome):
        raise _InvalidAnswer("expected a PutOutcome")
    return value


def _envelopes(value: Any) -> list[Envelope]:
    if not isinstance(value, list) or not all(isinstance(v, bytes) for v in value):
        raise _InvalidAnswer("expected a list of envelopes as bytes")
    return [Envelope.from_bytes(raw) for raw in value]


def _count(value: Any) -> int:
    if not is_int(value) or value < 0:
        raise _InvalidAnswer("expected a count")
    return int(value)


def _true(value: Any) -> bool:
    if value is not True:
        raise _InvalidAnswer("expected True")
    return True


def _node_list(value: Any) -> list[NodeId]:
    if not isinstance(value, list) or not value:
        raise _InvalidAnswer("expected a non-empty list of node ids")
    return [NodeId(v) for v in value]  # ValueError if one is not a valid id


def _record_or_none(value: Any) -> DirectoryRecord | None:
    if value is not None and not isinstance(value, DirectoryRecord):
        raise _InvalidAnswer("expected a DirectoryRecord or None")
    return value


class RayNodeGateway:
    """``NodeGatewayPort`` over the service-node actors of a Ray cluster.

    Args:
        node_ids: the service nodes of the cluster (the initial list from the
            configuration). Each one is found by its actor name,
            ``actor_name(node_id)``.
        namespace: Ray namespace where the node actors live. ``None`` = the
            namespace of the current job (enough when the same driver started
            them; separate client processes must all use the same namespace).

    Ray must already be initialised (``ray.init``) when calls are made.
    """

    def __init__(
        self, node_ids: Sequence[NodeId], namespace: str | None = None
    ) -> None:
        self._nodes = sorted(set(node_ids))
        self._seeds: list[NodeId] = []  # initial nodes, asked first on a refresh
        self._namespace = namespace
        self._handles: dict[NodeId, Any] = {}  # cache of actor handles

    # ---- discovery (task 2.4) ----
    @classmethod
    def from_seeds(
        cls,
        seeds: Sequence[NodeId],
        namespace: str | None = None,
        timeout_ms: int = DEFAULT_DISCOVERY_TIMEOUT_MS,
    ) -> RayNodeGateway:
        """A gateway that learns the cluster from its initial nodes ("seeds").

        A new client only needs the names of a few initial nodes (from the
        configuration). It asks ALL of them at once for the list of nodes and
        keeps the FIRST valid answer: any single live seed is enough.

        Raises:
            DiscoveryError: no seed answered with a valid list in time.
        """
        gateway = cls(seeds, namespace)
        gateway._seeds = list(dict.fromkeys(seeds))
        replies = gateway._ask_for_nodes(timeout_ms)
        if not any(r.ok for r in replies):
            reasons = "; ".join(f"{r.node}: {r.error}" for r in replies)
            raise DiscoveryError(f"no initial node gave the list of nodes ({reasons})")
        return gateway

    def refresh_nodes(self, timeout_ms: int = DEFAULT_DISCOVERY_TIMEOUT_MS) -> bool:
        """Ask any known node for the current list and add the nodes not known yet.

        Returns True if some node answered. Nodes are only ADDED, never removed:
        the list of nodes decides placement (which nodes hold a user's data),
        so it must be stable; a node that is down is skipped by membership
        (DEAD), not by forgetting it. Call it periodically, like
        ``QuorumClient.refresh_membership``.
        """
        return any(r.ok for r in self._ask_for_nodes(timeout_ms))

    def _ask_for_nodes(self, timeout_ms: int) -> list[NodeReply[list[NodeId]]]:
        """Ask the seeds and every known node at once; merge the first valid answer."""
        candidates = list(dict.fromkeys([*self._seeds, *self._nodes]))
        replies = self._call(candidates, "list_nodes", (), _node_list, timeout_ms, 1)
        for reply in replies:
            if reply.ok and reply.value is not None:
                self._nodes = sorted({*self._nodes, *reply.value})
                break
        return replies

    # ---- finding the actors ----
    def _handle(self, node: NodeId) -> Any | None:
        """The actor of ``node``, or None if there is none right now."""
        handle = self._handles.get(node)
        if handle is None:
            try:
                handle = ray.get_actor(
                    actor_name(node.value), namespace=self._namespace
                )
            except ValueError:  # no actor registered under that name
                return None
            self._handles[node] = handle
        return handle

    def _forget(self, node: NodeId) -> None:
        """Drop a cached handle whose actor died: next time, look the name up again.

        If the node is started again (a new actor under the same name), the old
        handle would keep pointing at the dead one.
        """
        self._handles.pop(node, None)

    # ---- the engine ----
    def _call(
        self,
        nodes: Sequence[NodeId],
        method: str,
        args: tuple[Any, ...],
        parse: Parser[T],
        timeout_ms: int,
        required: int | None = None,
    ) -> list[NodeReply[T]]:
        """Call ``method(*args)`` on every distinct node and collect one reply each.

        With ``required``, return as soon as that many nodes answered OK
        (``ray.wait`` one answer at a time), leaving out the nodes still pending.
        """
        _check_timeout(timeout_ms)
        deadline = time.monotonic() + timeout_ms / 1000
        asked = list(dict.fromkeys(nodes))  # one reply per node, in order
        replies: dict[NodeId, NodeReply[T]] = {}
        pending: dict[Any, NodeId] = {}  # ObjectRef -> node

        for node in asked:
            if node not in self._nodes:
                replies[node] = NodeReply(node=node, ok=False, error="unknown node")
                continue
            handle = self._handle(node)
            if handle is None:
                replies[node] = NodeReply(
                    node=node, ok=False, error="node down: no actor"
                )
                continue
            pending[getattr(handle, method).remote(*args)] = node

        ok_count = 0
        while pending and (required is None or ok_count < required):
            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0:
                break
            ready, _ = ray.wait(list(pending), num_returns=1, timeout=remaining_s)
            for ref in ready:
                node = pending.pop(ref)
                reply = self._receive(ref, node, parse)
                replies[node] = reply
                ok_count += reply.ok

        quorum_reached = required is not None and ok_count >= required
        if not quorum_reached:
            for node in pending.values():  # asked, but no answer in time
                replies[node] = NodeReply(node=node, ok=False, error="timeout")
                log.warning(
                    "actor.call_timeout",
                    node=node,
                    method=method,
                    timeout_ms=timeout_ms,
                )
        return [replies[n] for n in asked if n in replies]

    def _receive(self, ref: Any, node: NodeId, parse: Parser[T]) -> NodeReply[T]:
        """Turn one finished call into a reply. Never raises."""
        try:
            answer = ray.get(ref)
        except RayActorError as exc:  # the actor died, or is being restarted
            self._forget(node)
            return NodeReply(
                node=node, ok=False, error=f"node down: {type(exc).__name__}"
            )
        except RayError as exc:  # the method itself raised (e.g. StoreError)
            return NodeReply(
                node=node, ok=False, error=f"node error: {type(exc).__name__}"
            )
        return self._check(answer, node, parse)

    @staticmethod
    def _check(answer: Any, node: NodeId, parse: Parser[T]) -> NodeReply[T]:
        """Validate what the node sent back: the node is not trusted."""
        if not isinstance(answer, NodeReply) or answer.node != node:
            return NodeReply(
                node=node, ok=False, error="invalid answer: not a reply from this node"
            )
        if not answer.ok:
            return NodeReply(
                node=node,
                ok=False,
                error=answer.error or "refused",
                refused=answer.refused,
            )
        try:
            value = parse(answer.value)
        except (_InvalidAnswer, FormatError, ValueError, TypeError) as exc:
            return NodeReply(node=node, ok=False, error=f"invalid answer: {exc}")
        return NodeReply(node=node, ok=True, value=value)

    # ---- NodeGatewayPort ----
    def list_nodes(self) -> list[NodeId]:
        """The nodes known so far (configured, or discovered with ``refresh_nodes``)."""
        return list(self._nodes)

    def ping(self, nodes: Sequence[NodeId], timeout_ms: int) -> list[NodeReply[None]]:
        return self._call(nodes, "ping", (), _none, timeout_ms)

    def deposit(
        self,
        nodes: Sequence[NodeId],
        envelope: Envelope,
        required: int,
        timeout_ms: int,
    ) -> list[NodeReply[PutOutcome]]:
        return self._call(
            nodes, "deposit", (envelope.to_bytes(),), _put_outcome, timeout_ms, required
        )

    def collect(
        self, nodes: Sequence[NodeId], request: MailboxRequest, timeout_ms: int
    ) -> list[NodeReply[list[Envelope]]]:
        return self._call(
            nodes, "collect", (request.to_bytes(),), _envelopes, timeout_ms
        )

    def acknowledge(
        self, nodes: Sequence[NodeId], request: MailboxRequest, timeout_ms: int
    ) -> list[NodeReply[int]]:
        return self._call(
            nodes, "acknowledge", (request.to_bytes(),), _count, timeout_ms
        )

    def publish_record(
        self,
        nodes: Sequence[NodeId],
        record: DirectoryRecord,
        required: int,
        timeout_ms: int,
    ) -> list[NodeReply[bool]]:
        return self._call(
            nodes, "publish_record", (record,), _true, timeout_ms, required
        )

    def resolve_record(
        self, nodes: Sequence[NodeId], user_id: UserId, timeout_ms: int
    ) -> list[NodeReply[DirectoryRecord | None]]:
        return self._call(
            nodes, "resolve_record", (user_id.value,), _record_or_none, timeout_ms
        )
