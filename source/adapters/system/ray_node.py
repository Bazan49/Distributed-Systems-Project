"""The service node as a Ray actor.

One service node = one actor. It wraps the three node-side services:

* ``MailboxService`` over a ``SQLiteMailboxStore``   (pending envelopes);
* ``DirectoryService`` over a ``SQLiteDirectoryStore`` (users' records);
* ``MembershipService`` (this node's view of its peers; soft state in memory).

Restart: the actor is created with ``max_restarts``. If its process dies
(crash, ``ray.kill(actor, no_restart=False)``), Ray starts a NEW process and
runs ``__init__`` again with the same arguments. Everything in memory is lost,
but the node reopens the same SQLite files, so every accepted envelope and
record is still there. That is why the stores must be durable.

Two layers, on purpose:

* ``ServiceNode`` is a plain Python class: all the logic, testable in-process
  without Ray (and measured by coverage).
* ``ServiceNodeActor`` is the same class turned into a Ray actor, and
  ``start_service_node`` creates it with the right options.

Wire format (what crosses the process boundary):

* Envelopes and mailbox requests travel as BYTES (``to_bytes``) and are
  rebuilt here with ``from_bytes``, which strictly validates untrusted input.
  Envelopes go back to the client as bytes too: the client rebuilds and
  validates them, because the node is not trusted either.
* Directory records travel as ``DirectoryRecord`` objects (they have no byte
  format yet); ``DirectoryService`` validates their shape anyway.
* Every operation answers a ``NodeReply``. A REFUSAL (malformed input, bad
  signature, replayed request, stale record) is ``ok=False, refused=True``:
  the node is alive. A storage failure is NOT a refusal: it raises, and the
  caller sees the call fail, exactly like a crash.

The actor runs one call at a time (Ray's default ``max_concurrency=1``). The
services keep unsynchronised state in memory (the replay guard, the
membership view), so do not raise the concurrency without adding locks.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ray
from ray.util import list_named_actors

from source.adapters.system.clock import SystemClock
from source.adapters.system.crypto import NaClCrypto
from source.adapters.system.directory_store import SQLiteDirectoryStore
from source.adapters.system.mailbox_store import SQLiteMailboxStore
from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.use_cases.directory import DirectoryPolicy, DirectoryService
from source.application.use_cases.mailbox import (
    MailboxAccessDenied,
    MailboxPolicy,
    MailboxService,
)
from source.application.use_cases.membership import MembershipService
from source.domain.canonical import FormatError
from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.mailbox_access import MailboxRequest
from source.domain.membership import (
    DEFAULT_DEAD_TIMEOUT_MS,
    DEFAULT_SUSPECT_TIMEOUT_MS,
)
from source.domain.node_id import NodeId
from source.domain.records import DirectoryRecord, NodeReply, PutOutcome

# -1 = restart forever. A service node is meant to live as long as the cluster;
# a finite number would leave it dead after that many crashes over its life.
DEFAULT_MAX_RESTARTS = -1
MAILBOX_DB = "mailbox.db"
DIRECTORY_DB = "directory.db"
ACTOR_PREFIX = "service-node-"


def actor_name(node_id: str) -> str:
    """Name under which the actor of ``node_id`` is registered in Ray.

    Lets anyone find it with ``ray.get_actor(actor_name(...))`` (the gateway
    of task 2.3), and the name survives restarts.
    """
    return f"{ACTOR_PREFIX}{NodeId(node_id).value}"


def node_of_actor(name: str) -> NodeId | None:
    """The node whose actor is called ``name``, or None if it is not a node actor."""
    if not name.startswith(ACTOR_PREFIX):
        return None
    try:
        return NodeId(name[len(ACTOR_PREFIX) :])
    except ValueError:
        return None


def registered_nodes(namespace: str | None = None) -> list[NodeId]:
    """Every service node whose actor is registered in Ray right now (discovery, task 2.4).

    Ray keeps the name of an actor while it lives or is being restarted, and
    drops it when the actor dies for good. So this is the list of nodes that
    exist now, not the list of nodes that ever existed.

    ``namespace``: where the node actors live. ``None`` = the namespace of the
    current job. Careful: INSIDE an actor that is the namespace of the job that
    created it, not the actor's own, which is why a node is told its namespace.
    """
    wanted = namespace if namespace is not None else ray.get_runtime_context().namespace
    found = {
        node_of_actor(entry["name"])
        for entry in list_named_actors(all_namespaces=True)
        if entry["namespace"] == wanted
    }
    return sorted(n for n in found if n is not None)


@dataclass(frozen=True)
class NodeInfo:
    """Who is answering: lets a caller see that the process changed after a restart."""

    node_id: NodeId
    pid: int  # operating-system process id of the current incarnation
    data_dir: str  # folder holding this node's SQLite files


class ServiceNode:
    """All the behaviour of one service node, without Ray."""

    def __init__(
        self,
        node_id: str,
        data_dir: str,
        *,
        crypto: CryptoPort | None = None,
        clock: ClockPort | None = None,
        mailbox_policy: MailboxPolicy | None = None,
        directory_policy: DirectoryPolicy | None = None,
        suspect_timeout_ms: int = DEFAULT_SUSPECT_TIMEOUT_MS,
        dead_timeout_ms: int = DEFAULT_DEAD_TIMEOUT_MS,
        namespace: str | None = None,
        node_lister: Callable[[], list[NodeId]] | None = None,
    ) -> None:
        """Open (or create) this node's databases under ``data_dir/node_id/``.

        ``crypto`` and ``clock`` default to the real ones (``NaClCrypto``,
        ``SystemClock``); tests may pass fakes. All arguments must be
        picklable: Ray stores them to run ``__init__`` again on a restart.

        ``namespace`` is the Ray namespace of the node actors (for discovery).
        ``node_lister`` replaces the Ray lookup of the registered nodes; it
        exists for tests without Ray.

        Raises:
            ValueError: ``node_id`` is not a valid NodeId.
            StoreError: a database cannot be opened.
        """
        self._node = NodeId(node_id)
        folder = Path(data_dir) / self._node.value
        self._data_dir = str(folder)
        clock = clock if clock is not None else SystemClock()
        crypto = crypto if crypto is not None else NaClCrypto()
        self._mailbox_store = SQLiteMailboxStore(folder / MAILBOX_DB)
        self._directory_store = SQLiteDirectoryStore(folder / DIRECTORY_DB)
        self._mailbox = MailboxService(
            self._mailbox_store, crypto, clock, mailbox_policy
        )
        self._directory = DirectoryService(
            self._directory_store, crypto, clock, directory_policy
        )
        self._membership = MembershipService(clock, suspect_timeout_ms, dead_timeout_ms)
        self._list_registered: Callable[[], list[NodeId]] = (
            node_lister
            if node_lister is not None
            else lambda: registered_nodes(namespace)
        )

    # ---- replies ----
    def _ok(self, value: Any = None) -> NodeReply[Any]:
        return NodeReply(node=self._node, ok=True, value=value)

    def _refuse(self, reason: str) -> NodeReply[Any]:
        return NodeReply(node=self._node, ok=False, error=reason, refused=True)

    # ---- liveness ----
    def info(self) -> NodeInfo:
        return NodeInfo(node_id=self._node, pid=os.getpid(), data_dir=self._data_dir)

    def ping(self) -> NodeReply[None]:
        """Answer "alive". Clients and peers use it for membership."""
        return self._ok()

    # ---- mailbox ----
    def deposit(self, envelope: bytes) -> NodeReply[PutOutcome]:
        """Keep the envelope (``Envelope.to_bytes()``) for its receiver.

        ``STORED`` or ``ALREADY_PRESENT``: both mean it is on disk here.
        """
        try:
            parsed = Envelope.from_bytes(envelope)
        except (FormatError, ValueError, TypeError) as exc:
            return self._refuse(f"malformed envelope: {exc}")
        return self._ok(self._mailbox.deposit(parsed))

    def collect(self, request: bytes) -> NodeReply[list[bytes]]:
        """Pending envelopes of the receiver, as bytes, oldest first.

        ``request`` is a signed COLLECT (``MailboxRequest.to_bytes()``).
        """
        try:
            parsed = MailboxRequest.from_bytes(request)
            envelopes = self._mailbox.collect(parsed)
        except (FormatError, ValueError, TypeError) as exc:
            return self._refuse(f"malformed request: {exc}")
        except MailboxAccessDenied as exc:
            return self._refuse(exc.reason.value)
        return self._ok([e.to_bytes() for e in envelopes])

    def acknowledge(self, request: bytes) -> NodeReply[int]:
        """Delete the listed envelopes; the value is how many were still here."""
        try:
            parsed = MailboxRequest.from_bytes(request)
            removed = self._mailbox.acknowledge(parsed)
        except (FormatError, ValueError, TypeError) as exc:
            return self._refuse(f"malformed request: {exc}")
        except MailboxAccessDenied as exc:
            return self._refuse(exc.reason.value)
        return self._ok(removed)

    # ---- directory ----
    def publish_record(self, record: DirectoryRecord) -> NodeReply[bool]:
        """Store a signed record if ``DirectoryService`` accepts it."""
        if not isinstance(record, DirectoryRecord):
            return self._refuse("malformed record")
        outcome = self._directory.publish(record)
        if not outcome.accepted:
            return self._refuse(outcome.value)
        return self._ok(True)

    def resolve_record(self, user_id: str) -> NodeReply[DirectoryRecord | None]:
        """The current record of ``user_id`` (its hex value), or None if unknown/expired."""
        try:
            user = UserId(user_id)
        except (ValueError, TypeError) as exc:
            return self._refuse(f"malformed user id: {exc}")
        return self._ok(self._directory.resolve(user))

    # ---- maintenance ----
    def purge_expired(self) -> tuple[int, int]:
        """Physically delete expired envelopes and records: ``(envelopes, records)``.

        Meant to be called periodically. Until then expired data is never
        returned, it only takes disk space.
        """
        return self._mailbox.purge_expired(), self._directory.purge_expired()

    # ---- membership (this node's view of its peers) ----
    def report_peer(self, peer: str, answered: bool) -> None:
        """Record the outcome of talking to another node.

        ``answered`` is True if the peer answered at all (even refusing).
        """
        node = NodeId(peer)
        if answered:
            self._membership.report_success(node)
        else:
            self._membership.report_failure(node)

    def peer_state(self, peer: str) -> str:
        """``"alive"``, ``"suspect"`` or ``"dead"``. A peer never heard of is suspect."""
        return self._membership.get_state(NodeId(peer)).value

    def active_peers(self, candidates: list[str]) -> list[str]:
        """``candidates`` that this node does not consider dead, in the same order."""
        nodes = [NodeId(c) for c in candidates]
        return [n.value for n in self._membership.get_active_nodes(nodes)]

    # ---- discovery ----
    def list_nodes(self) -> NodeReply[list[str]]:
        """The service nodes available now, as node ids (task 2.4).

        That is: every node actor registered in Ray, plus this node, minus the
        peers this node's membership considers DEAD. Sorted. A new client asks
        this to any initial node to learn the whole cluster.
        """
        others = [n for n in self._list_registered() if n != self._node]
        available = self._membership.get_active_nodes(others)
        return self._ok(sorted(n.value for n in {self._node, *available}))

    # ---- lifecycle ----
    def close(self) -> None:
        """Close both databases (a clean shutdown; a crash loses nothing either)."""
        self._mailbox_store.close()
        self._directory_store.close()


# The Ray actor: the same class, restarted by Ray if its process dies.
# max_task_retries=0: a call in flight when the process dies FAILS instead of
# being run again on the new process. The client's quorum logic already treats
# a failed call as "no answer" and retries with a NEW signed request, which is
# what the mailbox protocol requires (a nonce is never reused).
ServiceNodeActor = ray.remote(max_restarts=DEFAULT_MAX_RESTARTS, max_task_retries=0)(
    ServiceNode
)


def start_service_node(
    node_id: str,
    data_dir: str | Path,
    *,
    max_restarts: int = DEFAULT_MAX_RESTARTS,
    namespace: str | None = None,
    **node_options: Any,
) -> Any:
    """Create (or find, if it already exists) the actor of ``node_id``.

    The actor is registered under ``actor_name(node_id)`` in ``namespace``
    (``None`` = the current job's namespace). If an actor with that name
    already exists, it is returned as is and ``node_options`` are ignored.
    ``node_options`` are the keyword arguments of ``ServiceNode``.

    Returns:
        The Ray actor handle (``ray.actor.ActorHandle``).
    """
    if namespace is None:
        namespace = ray.get_runtime_context().namespace
    return ServiceNodeActor.options(
        name=actor_name(node_id),
        namespace=namespace,
        max_restarts=max_restarts,
        get_if_exists=True,
    ).remote(node_id, str(data_dir), namespace=namespace, **node_options)
