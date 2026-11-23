"""Membership service: tracks the liveness of service nodes.

The rules (ALIVE / SUSPECT / DEAD) live in ``source.domain.membership``; this
service only supplies the current time from the ``ClockPort``. It uses the
monotonic clock, because it measures elapsed time.

Depends only on ports and the domain: no Ray, no SQLite.
"""

from __future__ import annotations

from collections.abc import Iterable

from source.application.ports.clock import ClockPort
from source.domain.membership import (
    DEFAULT_DEAD_TIMEOUT_MS,
    DEFAULT_SUSPECT_TIMEOUT_MS,
    MembershipView,
    NodeState,
)
from source.domain.node_id import NodeId


class MembershipService:
    """A ``MembershipView`` driven by a clock."""

    def __init__(
        self,
        clock: ClockPort,
        suspect_timeout_ms: int = DEFAULT_SUSPECT_TIMEOUT_MS,
        dead_timeout_ms: int = DEFAULT_DEAD_TIMEOUT_MS,
    ) -> None:
        self._clock = clock
        self._view = MembershipView(suspect_timeout_ms, dead_timeout_ms)

    def report_success(self, node: NodeId) -> None:
        """Record that ``node`` just successfully responded."""
        self._view.record_success(node, self._clock.monotonic_ms())

    def report_failure(self, node: NodeId) -> None:
        """Record that ``node`` did not answer (timeout, crash, actor error).

        Only for nodes that did NOT answer. A ``NodeReply`` with ``ok=False``
        can also mean the node answered and refused the request (bad
        signature, stale request): that node is alive and must not be
        reported here.
        """
        self._view.record_failure(node, self._clock.monotonic_ms())

    def get_state(self, node: NodeId) -> NodeState:
        """Current state of ``node``."""
        return self._view.state_of(node, self._clock.monotonic_ms())

    def get_active_nodes(self, candidates: Iterable[NodeId]) -> list[NodeId]:
        """``candidates`` that are not DEAD (the quorum logic skips dead nodes).

        A node never contacted is included: a new client must try its nodes.
        """
        return self._view.active_nodes(candidates, self._clock.monotonic_ms())

    def prune_dead(self) -> int:
        """Forget DEAD nodes to prevent leaks; return how many were removed."""
        return self._view.prune_dead(self._clock.monotonic_ms())
