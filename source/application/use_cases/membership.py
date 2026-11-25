"""Membership service: tracks the liveness of service nodes.

The rules (ALIVE / SUSPECT / DEAD) live in ``source.domain.membership``; this
service only supplies the current time from the ``ClockPort``. It uses the
monotonic clock, because it measures elapsed time.

Logs (see ``docs/logging.md``): ``node.state_changed`` each time the state
SEEN for a node changes, with ``from_state`` and ``to_state``; INFO, or
WARNING when the node becomes DEAD. The state is derived from time, so it is
not stored anywhere: this service remembers the last state it saw of each
node and logs a change when it notices it (on a report or a query). A node
never contacted counts as SUSPECT, like in the domain.

Depends only on ports and the domain: no Ray, no SQLite.
"""

from __future__ import annotations

from collections.abc import Iterable

from source.application.observability import get_logger
from source.application.ports.clock import ClockPort
from source.domain.membership import (
    DEFAULT_DEAD_TIMEOUT_MS,
    DEFAULT_SUSPECT_TIMEOUT_MS,
    MembershipView,
    NodeState,
)
from source.domain.node_id import NodeId

log = get_logger(__name__)


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
        self._seen: dict[NodeId, NodeState] = {}  # last state seen (and logged)

    def _see(self, node: NodeId, now_ms: int) -> NodeState:
        """The state of ``node`` now; log it if it is not the one seen last time."""
        state = self._view.state_of(node, now_ms)
        before = self._seen.get(node, NodeState.SUSPECT)  # never contacted
        if state is not before:
            level = log.warning if state is NodeState.DEAD else log.info
            level("node.state_changed", node=node, from_state=before, to_state=state)
        self._seen[node] = state
        return state

    def report_success(self, node: NodeId) -> None:
        """Record that ``node`` just successfully responded."""
        now = self._clock.monotonic_ms()
        self._view.record_success(node, now)
        self._see(node, now)

    def report_failure(self, node: NodeId) -> None:
        """Record that ``node`` did not answer (timeout, crash, actor error).

        Only for nodes that did NOT answer. A ``NodeReply`` with ``ok=False``
        can also mean the node answered and refused the request (bad
        signature, stale request): that node is alive and must not be
        reported here.
        """
        now = self._clock.monotonic_ms()
        self._view.record_failure(node, now)
        self._see(node, now)

    def get_state(self, node: NodeId) -> NodeState:
        """Current state of ``node``."""
        return self._see(node, self._clock.monotonic_ms())

    def get_active_nodes(self, candidates: Iterable[NodeId]) -> list[NodeId]:
        """``candidates`` that are not DEAD (the quorum logic skips dead nodes).

        A node never contacted is included: a new client must try its nodes.
        """
        now = self._clock.monotonic_ms()
        return [n for n in candidates if self._see(n, now) is not NodeState.DEAD]

    def prune_dead(self) -> int:
        """Forget DEAD nodes to prevent leaks; return how many were removed.

        A forgotten node counts as never contacted again (SUSPECT); that is
        not a change of the node, so it is not logged.
        """
        now = self._clock.monotonic_ms()
        for node in [n for n, s in self._seen.items() if s is NodeState.DEAD]:
            if self._view.state_of(node, now) is NodeState.DEAD:
                self._seen[node] = NodeState.SUSPECT
        return self._view.prune_dead(now)
