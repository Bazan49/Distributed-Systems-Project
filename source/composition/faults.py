"""Fault tools for the integration tests.

Functions that break and repair a cluster started by the launcher :

    cluster = launch(config)

    kill_node(cluster, "node-2")         # the node dies and stays dead
    restart_node(cluster, "node-2")      # it comes back, with its data

    crash_node(cluster, "node-3")        # it dies and Ray restarts it on its own

    disconnect_client(cluster, "bob")    # Bob goes offline
    reconnect_client(cluster, "bob")     # Bob comes back (and collects his mailbox)

    with node_down(cluster, "node-1"):   # down inside the block, back after it
        ...
    with client_offline(cluster, "bob"):
        ...

Every function WAITS until the change has really happened before returning:
``ray.kill`` is asynchronous, so without waiting a test could still talk to
the old process. If it does not happen within ``timeout_ms``: ``FaultError``.

Data is never deleted: a node or client that comes back reopens the same
files (the node's SQLite stores, the client's local store), as after a real
crash. Calling a function in a state where it makes no sense (killing a node
that is already down, reconnecting a client that is online) raises
``FaultError``: in a test that is always a mistake worth seeing.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator
from typing import Any

import ray
from ray.exceptions import RayActorError

from source.adapters.system.ray.ray_node import actor_name
from source.composition.launcher import (
    RunningCluster,
    client_actor_name,
    start_client,
    start_node,
    stop_actor,
    wait_ready,
)
from source.domain.node_id import NodeId

DEFAULT_FAULT_TIMEOUT_MS = 60_000
_POLL_S = 0.05


class FaultError(RuntimeError):
    """The fault cannot be applied, or did not take effect in time."""


# ---- helpers ----
def _node(cluster: RunningCluster, node: NodeId | str) -> NodeId:
    node_id = node if isinstance(node, NodeId) else NodeId(node)
    if node_id not in cluster.config.nodes:
        raise FaultError(f"{node_id} is not a node of this cluster")
    return node_id


def _client_spec(cluster: RunningCluster, client_id: str) -> Any:
    for spec in cluster.config.clients:
        if spec.client_id == client_id:
            return spec
    raise FaultError(f"{client_id} is not a client of this cluster")


def _wait_until_gone(name: str, namespace: str, timeout_ms: int) -> None:
    """Wait until no actor is registered under ``name``.

    Ray releases the name when the actor is dead for good, so after this the
    actor does not answer any more and the name can be used again.
    """
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        try:
            ray.get_actor(name, namespace=namespace)
        except ValueError:
            return
        time.sleep(_POLL_S)
    raise FaultError(f"{name} was still alive after {timeout_ms} ms")


def _kill(handle: Any, graceful: bool, timeout_ms: int) -> None:
    if graceful:
        stop_actor(handle, timeout_ms)  # close its databases first
    else:
        ray.kill(handle, no_restart=True)  # abrupt, like a power cut


# ---- nodes ----
def kill_node(
    cluster: RunningCluster,
    node: NodeId | str,
    *,
    graceful: bool = False,
    timeout_ms: int = DEFAULT_FAULT_TIMEOUT_MS,
) -> None:
    """Kill a node for good: it stops answering until ``restart_node``.

    By default it is abrupt (no chance to close anything), like a crash that
    Ray does not repair. ``graceful=True`` lets it close its databases first.
    Its files stay on disk.

    Raises:
        FaultError: not a node of the cluster, already down, or still alive
            after ``timeout_ms``.
    """
    node_id = _node(cluster, node)
    handle = cluster.nodes.pop(node_id, None)
    if handle is None:
        raise FaultError(f"{node_id} is already down")
    _kill(handle, graceful, timeout_ms)
    _wait_until_gone(actor_name(node_id.value), cluster.config.namespace, timeout_ms)


def restart_node(
    cluster: RunningCluster,
    node: NodeId | str,
    *,
    timeout_ms: int = DEFAULT_FAULT_TIMEOUT_MS,
) -> None:
    """Start a node that was killed again, over the same files: it keeps its data.

    Returns when the node is ready to answer.

    Raises:
        FaultError: not a node of the cluster, already running, or not ready
            in time.
    """
    node_id = _node(cluster, node)
    if node_id in cluster.nodes:
        raise FaultError(f"{node_id} is already running")
    handle = start_node(cluster.config, node_id)
    try:
        wait_ready({node_id.value: handle}, timeout_ms)
    except Exception as exc:
        with contextlib.suppress(Exception):
            ray.kill(handle, no_restart=True)
        raise FaultError(f"{node_id} did not come back: {exc}") from exc
    cluster.nodes[node_id] = handle


def crash_node(
    cluster: RunningCluster,
    node: NodeId | str,
    *,
    timeout_ms: int = DEFAULT_FAULT_TIMEOUT_MS,
) -> None:
    """Make the node's process die and let RAY restart it (``max_restarts``).

    Unlike ``kill_node`` + ``restart_node``, nobody restarts it by hand: this
    checks the automatic recovery of task 2.2. Returns when a NEW process
    answers. The handle stays valid.

    Raises:
        FaultError: not a node of the cluster, down, or not back in time.
    """
    node_id = _node(cluster, node)
    handle = cluster.nodes.get(node_id)
    if handle is None:
        raise FaultError(f"{node_id} is down: restart it with restart_node")
    old_pid = ray.get(handle.info.remote(), timeout=timeout_ms / 1000).pid
    ray.kill(handle, no_restart=False)
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        remaining = max(deadline - time.monotonic(), 0.001)
        with contextlib.suppress(RayActorError, ray.exceptions.GetTimeoutError):
            if ray.get(handle.info.remote(), timeout=remaining).pid != old_pid:
                return
        time.sleep(_POLL_S)
    raise FaultError(f"{node_id} was not restarted by Ray in {timeout_ms} ms")


@contextlib.contextmanager
def node_down(
    cluster: RunningCluster,
    node: NodeId | str,
    *,
    timeout_ms: int = DEFAULT_FAULT_TIMEOUT_MS,
) -> Iterator[None]:
    """Inside the ``with`` block the node is down; it is restarted on exit (even on error)."""
    kill_node(cluster, node, timeout_ms=timeout_ms)
    try:
        yield
    finally:
        restart_node(cluster, node, timeout_ms=timeout_ms)


# ---- clients ----
def disconnect_client(
    cluster: RunningCluster,
    client_id: str,
    *,
    graceful: bool = False,
    timeout_ms: int = DEFAULT_FAULT_TIMEOUT_MS,
) -> None:
    """Take a client offline: its actor stops, so nobody can deliver to it directly.

    Senders then fall back to the mailboxes. By default it is abrupt, like a
    device losing its connection or the app being closed; ``graceful=True``
    lets it close its local store first. Its local data stays on disk.

    Raises:
        FaultError: not a client of the cluster, already offline, or still
            alive after ``timeout_ms``.
    """
    _client_spec(cluster, client_id)
    handle = cluster.clients.pop(client_id, None)
    if handle is None:
        raise FaultError(f"{client_id} is already offline")
    _kill(handle, graceful, timeout_ms)
    _wait_until_gone(client_actor_name(client_id), cluster.config.namespace, timeout_ms)


def reconnect_client(
    cluster: RunningCluster,
    client_id: str,
    *,
    timeout_ms: int = DEFAULT_FAULT_TIMEOUT_MS,
) -> None:
    """Bring an offline client back, with the same arguments and the same local data.

    The real client actor comes back LOGGED OUT (its password is not kept):
    the test calls ``login`` on it, and its first tick after logging in
    collects the mailbox, so the messages sent while it was offline arrive
    then. Returns when the actor is ready.

    Raises:
        FaultError: not a client of the cluster, already online, or not ready
            in time.
    """
    spec = _client_spec(cluster, client_id)
    if client_id in cluster.clients:
        raise FaultError(f"{client_id} is already online")
    handle = start_client(cluster.config, spec, cluster.client_factory)
    try:
        wait_ready({client_id: handle}, timeout_ms)
    except Exception as exc:
        with contextlib.suppress(Exception):
            ray.kill(handle, no_restart=True)
        raise FaultError(f"{client_id} did not come back: {exc}") from exc
    cluster.clients[client_id] = handle


@contextlib.contextmanager
def client_offline(
    cluster: RunningCluster,
    client_id: str,
    *,
    timeout_ms: int = DEFAULT_FAULT_TIMEOUT_MS,
) -> Iterator[None]:
    """Inside the ``with`` block the client is offline; it reconnects on exit."""
    disconnect_client(cluster, client_id, timeout_ms=timeout_ms)
    try:
        yield
    finally:
        reconnect_client(cluster, client_id, timeout_ms=timeout_ms)
