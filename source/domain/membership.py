"""Membership rules: derive the liveness of a service node from elapsed time.

Pure domain logic: it never reads a clock. Every operation that depends on
time receives ``now_ms`` as an argument, so the rules are deterministic and
the domain does not depend on any port. ``MembershipService`` (application
layer) supplies ``now_ms`` from the ``ClockPort``.

``now_ms`` must come from a MONOTONIC clock: it is used to measure elapsed time.
"""

from __future__ import annotations

from collections.abc import Iterable
from enum import Enum

from source.domain.node_id import NodeId

DEFAULT_SUSPECT_TIMEOUT_MS = 5_000
DEFAULT_DEAD_TIMEOUT_MS = 15_000


class NodeState(str, Enum):
    """The liveness state of a service node."""

    ALIVE = "alive"  # Responded recently
    SUSPECT = "suspect"  # Missed the last check, but not long enough to be dead
    DEAD = "dead"  # Has been silent for too long


class MembershipView:
    """Last time each node was seen, and the state derived from it.

    - ALIVE: seen less than ``suspect_timeout_ms`` ago.
    - SUSPECT: seen between ``suspect_timeout_ms`` and ``dead_timeout_ms`` ago.
    - DEAD: seen ``dead_timeout_ms`` or more ago, or never seen.
    """

    def __init__(
        self,
        suspect_timeout_ms: int = DEFAULT_SUSPECT_TIMEOUT_MS,
        dead_timeout_ms: int = DEFAULT_DEAD_TIMEOUT_MS,
    ) -> None:
        if suspect_timeout_ms <= 0 or dead_timeout_ms <= 0:
            raise ValueError("Timeouts must be positive integers")
        if suspect_timeout_ms >= dead_timeout_ms:
            raise ValueError("suspect_timeout_ms must be less than dead_timeout_ms")

        self._suspect_timeout = suspect_timeout_ms
        self._dead_timeout = dead_timeout_ms
        self._last_seen: dict[NodeId, int] = {}

    def record_success(self, node: NodeId, now_ms: int) -> None:
        """Record that ``node`` responded at ``now_ms``."""
        self._last_seen[node] = now_ms

    def state_of(self, node: NodeId, now_ms: int) -> NodeState:
        """State of ``node`` at ``now_ms``."""
        last_seen = self._last_seen.get(node)
        if last_seen is None:
            return NodeState.DEAD

        elapsed = now_ms - last_seen
        if elapsed >= self._dead_timeout:
            return NodeState.DEAD
        if elapsed >= self._suspect_timeout:
            return NodeState.SUSPECT
        return NodeState.ALIVE

    def active_nodes(self, candidates: Iterable[NodeId], now_ms: int) -> list[NodeId]:
        """``candidates`` that are ALIVE or SUSPECT at ``now_ms``, in order."""
        return [n for n in candidates if self.state_of(n, now_ms) != NodeState.DEAD]

    def prune_dead(self, now_ms: int) -> int:
        """Forget nodes that are DEAD at ``now_ms``; return how many were removed."""
        to_remove = [
            node
            for node, last_seen in self._last_seen.items()
            if (now_ms - last_seen) >= self._dead_timeout
        ]
        for node in to_remove:
            del self._last_seen[node]
        return len(to_remove)
