"""RayNodeGateway against real node actors (task 2.3).

Acceptance criterion: it passes the contract of the port (the same
``NodeGatewayContract`` the in-memory gateway passes).

Plus what the contract cannot see: calls with a deadline, returning with 2 of
3 while the third call goes on, and an actor error treated as a node down.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import ray
from ray.exceptions import RayActorError

from source.adapters.memory.crypto import FakeCrypto
from source.adapters.system.clock import SystemClock
from source.adapters.system.ray_gateway import RayNodeGateway
from source.adapters.system.ray_node import actor_name, start_service_node
from source.application.ports import ClockPort, NodeGatewayPort
from source.application.use_cases.membership import MembershipService
from source.application.use_cases.quorum import QuorumClient
from source.domain.node_id import NodeId
from source.domain.records import NodeReply
from tests.contracts.mailbox_store_contract import make_envelope
from tests.contracts.node_gateway_contract import GatewayHarness, NodeGatewayContract
from tests.support.builders import BOB, collect_request, make_outgoing

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_cluster")]

WAIT_S = 60
TIMEOUT_MS = 5_000


def _wait_until(condition: Any, what: str) -> None:
    deadline = time.monotonic() + WAIT_S
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting until {what}")


def _answers(handle: Any) -> bool:
    try:
        return bool(ray.get(handle.ping.remote(), timeout=5).ok)
    except RayActorError:
        return False


def _is_dead(handle: Any) -> bool:
    try:
        ray.get(handle.ping.remote(), timeout=5)
    except RayActorError:
        return True
    return False


class RayHarness:
    """Three real node actors, plus the ability to kill and revive them."""

    def __init__(self, data_dir: Path) -> None:
        prefix = secrets.token_hex(3)  # actor names are global: unique per test
        self.nodes = [NodeId(f"n{prefix}-{i}") for i in (1, 2, 3)]
        self.clock: ClockPort = SystemClock()
        self._data_dir = data_dir
        self._handles = {node: self._start(node) for node in self.nodes}
        self.gateway: NodeGatewayPort = RayNodeGateway(self.nodes)

    def _start(self, node: NodeId) -> Any:
        """Start the actor and wait until it answers (the name of a just-killed
        actor may take a moment to be released)."""
        handle = start_service_node(node.value, self._data_dir, crypto=FakeCrypto())
        if not _answers(handle):
            _wait_until(
                lambda: _answers(
                    start_service_node(node.value, self._data_dir, crypto=FakeCrypto())
                ),
                f"{node} starts",
            )
            handle = ray.get_actor(actor_name(node.value))
        return handle

    def handle(self, node: NodeId) -> Any:
        return self._handles[node]

    def set_down(self, node: NodeId) -> None:
        """Kill the actor for good (``ray.kill`` is asynchronous: wait until it is dead)."""
        handle = self._handles[node]
        ray.kill(handle, no_restart=True)
        _wait_until(lambda: _is_dead(handle), f"{node} is dead")

    def set_up(self, node: NodeId) -> None:
        """Start a NEW actor for the node over the same files: its data is kept."""
        self._handles[node] = self._start(node)

    def close(self) -> None:
        for handle in self._handles.values():
            ray.kill(handle, no_restart=True)


@pytest.fixture
def ray_harness(tmp_path: Path) -> Iterator[RayHarness]:
    harness = RayHarness(tmp_path)
    yield harness
    harness.close()


# ================= acceptance criterion: the contract =================
class TestRayNodeGateway(NodeGatewayContract):
    @pytest.fixture
    def harness(self, ray_harness: RayHarness) -> GatewayHarness:
        return ray_harness


# ================= beyond the contract =================
def test_with_two_of_three_it_returns_early_but_the_third_still_stores(
    ray_harness: RayHarness,
) -> None:
    gateway = ray_harness.gateway
    envelope = make_envelope()

    replies = gateway.deposit(ray_harness.nodes, envelope, 2, TIMEOUT_MS)

    assert sum(r.ok for r in replies) >= 2

    # The call to the third node was not cancelled: it ends up holding a copy.
    def three_copies() -> bool:
        request = collect_request(BOB, ray_harness.clock.now_ms())
        held = gateway.collect(ray_harness.nodes, request, TIMEOUT_MS)
        return sum(r.ok and r.value == [envelope] for r in held) == 3

    _wait_until(three_copies, "the third node stores the envelope")


def test_a_restarted_actor_answers_again_through_the_same_gateway(
    ray_harness: RayHarness,
) -> None:
    node = ray_harness.nodes[0]
    ray.kill(ray_harness.handle(node), no_restart=False)  # crash: Ray restarts it
    _wait_until(
        lambda: ray_harness.gateway.ping([node], TIMEOUT_MS)[0].ok,
        "the restarted node answers",
    )


def test_a_node_started_again_under_the_same_name_is_found_again(
    ray_harness: RayHarness,
) -> None:
    node = ray_harness.nodes[0]
    gateway = ray_harness.gateway
    assert gateway.ping([node], TIMEOUT_MS)[0].ok  # the handle is now cached
    ray_harness.set_down(node)
    [down] = gateway.ping([node], TIMEOUT_MS)
    assert not down.ok and not down.refused and down.error
    ray_harness.set_up(node)  # a NEW actor: the cached handle is stale
    assert gateway.ping([node], TIMEOUT_MS)[0].ok


def test_negative_timeouts_are_rejected(ray_harness: RayHarness) -> None:
    with pytest.raises(ValueError):
        ray_harness.gateway.ping(ray_harness.nodes, -1)


# ---- misbehaving nodes, played by fake actors registered under a node's name ----
@ray.remote
class _MisbehavingNode:
    """Stands in for a node actor; ``mode`` says how it misbehaves."""

    def __init__(self, node: NodeId, mode: str) -> None:
        self._node = node
        self._mode = mode

    def ping(self) -> Any:
        if self._mode == "slow":
            time.sleep(10)
        if self._mode == "crash":
            raise RuntimeError("disk on fire")
        if self._mode == "lie":
            return NodeReply(node=NodeId("someone-else"), ok=True)
        return NodeReply(node=self._node, ok=True)

    def collect(self, request: bytes) -> Any:
        return NodeReply(node=self._node, ok=True, value=[b"not an envelope"])

    def acknowledge(self, request: bytes) -> Any:
        return NodeReply(node=self._node, ok=True, value=-3)


@pytest.fixture
def fake_node() -> Iterator[Any]:
    created: list[Any] = []

    def make(mode: str) -> tuple[NodeId, RayNodeGateway]:
        node = NodeId(f"fake-{secrets.token_hex(3)}")
        created.append(
            _MisbehavingNode.options(name=actor_name(node.value)).remote(node, mode)  # type: ignore[attr-defined]
        )
        return node, RayNodeGateway([node])

    yield make
    for handle in created:
        ray.kill(handle, no_restart=True)


def test_a_slow_node_becomes_a_timeout_within_the_deadline(fake_node: Any) -> None:
    node, gateway = fake_node("slow")
    started = time.monotonic()
    [reply] = gateway.ping([node], 300)
    assert time.monotonic() - started < 5  # did not wait the 10 s of the node
    assert not reply.ok and not reply.refused and reply.error == "timeout"


def test_an_error_inside_the_actor_is_a_failed_node_not_an_exception(
    fake_node: Any,
) -> None:
    node, gateway = fake_node("crash")
    [reply] = gateway.ping([node], TIMEOUT_MS)
    assert not reply.ok and not reply.refused
    assert reply.error is not None and reply.error.startswith("node error")


def test_an_answer_from_the_wrong_node_is_rejected(fake_node: Any) -> None:
    node, gateway = fake_node("lie")
    [reply] = gateway.ping([node], TIMEOUT_MS)
    assert not reply.ok and reply.error is not None
    assert reply.error.startswith("invalid answer")


def test_malformed_values_from_a_node_are_rejected(fake_node: Any) -> None:
    node, gateway = fake_node("honest")
    request = collect_request(BOB, SystemClock().now_ms())
    [collected] = gateway.collect([node], request, TIMEOUT_MS)
    [acked] = gateway.acknowledge([node], request, TIMEOUT_MS)
    assert not collected.ok and collected.error.startswith("invalid answer")
    assert not acked.ok and acked.error.startswith("invalid answer")


def test_a_node_without_an_actor_is_down_not_unknown() -> None:
    node = NodeId(f"ghost-{secrets.token_hex(3)}")
    [reply] = RayNodeGateway([node]).ping([node], TIMEOUT_MS)
    assert not reply.ok and reply.error == "node down: no actor"


# ================= the quorum logic (1B.7) on real Ray =================
def test_the_quorum_client_accepts_with_one_node_down_and_not_with_two(
    ray_harness: RayHarness,
) -> None:
    clock = SystemClock()
    client = QuorumClient(
        ray_harness.gateway, MembershipService(clock), FakeCrypto(), clock
    )
    ray_harness.set_down(ray_harness.nodes[0])
    assert client.deposit(make_outgoing().envelope).accepted
    ray_harness.set_down(ray_harness.nodes[1])
    assert not client.deposit(make_outgoing().envelope).accepted
