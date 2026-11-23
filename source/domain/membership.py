"""Membership rules: derive the liveness of a service node from replies and time.

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

    ALIVE = "alive"  # Responded recently, and has not failed since
    SUSPECT = "suspect"  # Failed or went quiet, or never contacted: worth trying
    DEAD = "dead"  # Silent or failing for too long: not worth trying


class MembershipView:
    """What is known about each node, and the state derived from it.

    Rules, with ``ref`` = time of the last success, or of the first failure if
    the node never answered:

    - never contacted (no success, no failure): SUSPECT. A client that has not
      pinged anyone yet must still try its nodes.
    - ``now - ref >= dead_timeout_ms``: DEAD.
    - a failure after the last success, or ``now - ref >= suspect_timeout_ms``:
      SUSPECT.
    - otherwise: ALIVE.

    A success always brings a node back to ALIVE. DEAD nodes are left out of
    ``active_nodes``, so someone (the quorum logic) must keep pinging every
    known node for a dead one to be noticed when it comes back.
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
        self._last_success: dict[NodeId, int] = {}
        self._failing_since: dict[NodeId, int] = {}  # first failure after last success

    def record_success(self, node: NodeId, now_ms: int) -> None:
        """Record that ``node`` answered at ``now_ms``: it is ALIVE again."""
        self._last_success[node] = now_ms
        self._failing_since.pop(node, None)

    def record_failure(self, node: NodeId, now_ms: int) -> None:
        """Record that a call to ``node`` failed (timeout, crash, error) at ``now_ms``.

        The node becomes SUSPECT at once. Repeated failures do not move the
        reference time: DEAD still comes ``dead_timeout_ms`` after the last
        success (or after the first failure, if it never answered).
        """
        self._failing_since.setdefault(node, now_ms)

    def state_of(self, node: NodeId, now_ms: int) -> NodeState:
        """State of ``node`` at ``now_ms``."""
        last_success = self._last_success.get(node)
        failing_since = self._failing_since.get(node)
        ref = last_success if last_success is not None else failing_since
        if ref is None:
            return NodeState.SUSPECT  # never contacted

        elapsed = now_ms - ref
        if elapsed >= self._dead_timeout:
            return NodeState.DEAD
        if failing_since is not None or elapsed >= self._suspect_timeout:
            return NodeState.SUSPECT
        return NodeState.ALIVE

    def active_nodes(self, candidates: Iterable[NodeId], now_ms: int) -> list[NodeId]:
        """``candidates`` that are not DEAD at ``now_ms`` (ALIVE, SUSPECT or never contacted), in order."""
        return [n for n in candidates if self.state_of(n, now_ms) != NodeState.DEAD]

    def prune_dead(self, now_ms: int) -> int:
        """Forget nodes that are DEAD at ``now_ms``; return how many were removed.

        Prevents leaks when nodes leave for good. A forgotten node counts as
        never contacted, so if it is still a candidate it gets tried again.
        """
        known = set(self._last_success) | set(self._failing_since)
        to_remove = [n for n in known if self.state_of(n, now_ms) == NodeState.DEAD]
        for node in to_remove:
            self._last_success.pop(node, None)
            self._failing_since.pop(node, None)
        return len(to_remove)
