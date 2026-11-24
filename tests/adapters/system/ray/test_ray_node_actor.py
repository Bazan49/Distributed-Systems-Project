"""The service node as a REAL Ray actor.

Acceptance criterion: after ``ray.kill`` the actor comes back and keeps its data.

The node uses ``FakeCrypto`` (signatures made in this process verify in the
actor's process: they only depend on the keys, which travel inside the data)
and the real clock, because a ``ManualClock`` would be a copy in the actor.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import ray
from ray.exceptions import GetTimeoutError, RayActorError

from source.adapters.memory.crypto import FakeCrypto
from source.adapters.system.clock import SystemClock
from source.adapters.system.ray_node import actor_name, start_service_node
from source.domain.envelope import Envelope
from source.domain.records import PutOutcome
from tests.contracts.mailbox_store_contract import make_envelope
from tests.support.builders import ALICE, BOB, collect_request, signed_record

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("ray_cluster")]

CALL_TIMEOUT_S = 30
RESTART_TIMEOUT_S = 60


def now_ms() -> int:
    return SystemClock().now_ms()


@pytest.fixture
def node_id() -> str:
    """A fresh node id per test: actor names are global in the Ray cluster."""
    return f"node-{secrets.token_hex(4)}"


@pytest.fixture
def node(node_id: str, tmp_path: Path) -> Iterator[Any]:
    handle = start_service_node(node_id, tmp_path, crypto=FakeCrypto())
    yield handle
    ray.kill(handle, no_restart=True)  # clean up: no restart after the test


def call(handle: Any, method: str, *args: Any) -> Any:
    return ray.get(getattr(handle, method).remote(*args), timeout=CALL_TIMEOUT_S)


def wait_for_new_process(handle: Any, old_pid: int) -> int:
    """Wait until a DIFFERENT process answers (``ray.kill`` is asynchronous).

    While Ray restarts the actor, calls fail with ``RayActorError`` (or its
    subclass ``ActorUnavailableError``): that is expected, keep trying.
    """
    deadline = time.monotonic() + RESTART_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            pid: int = call(handle, "info").pid
            if pid != old_pid:
                return pid
        except (RayActorError, GetTimeoutError):
            pass
        time.sleep(0.1)
    raise AssertionError("the actor did not come back")


def wait_until_dead(handle: Any) -> None:
    """Wait until calls to the actor fail (right after ``ray.kill`` a call may
    still reach the old process)."""
    deadline = time.monotonic() + RESTART_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            call(handle, "ping")
        except RayActorError:
            return
        time.sleep(0.05)
    raise AssertionError("the actor did not die")


def bob_collects(handle: Any) -> list[Envelope]:
    reply = call(handle, "collect", collect_request(BOB, now_ms()).to_bytes())
    assert reply.ok
    return [Envelope.from_bytes(raw) for raw in reply.value]


# ================= acceptance criterion =================
def test_after_ray_kill_the_actor_comes_back_and_keeps_its_data(node: Any) -> None:
    envelope = make_envelope()
    record = signed_record(ALICE, now_ms(), version=5)
    assert call(node, "deposit", envelope.to_bytes()).value is PutOutcome.STORED
    assert call(node, "publish_record", record).ok
    old_pid = call(node, "info").pid

    ray.kill(node, no_restart=False)  # like a crash: Ray restarts it

    new_pid = wait_for_new_process(node, old_pid)
    assert new_pid != old_pid  # really a new process: memory was lost
    assert bob_collects(node) == [envelope]
    assert call(node, "resolve_record", ALICE.user_id.value).value == record
    older = signed_record(ALICE, now_ms(), version=4)
    assert call(node, "publish_record", older).error == "rejected_stale"


def test_it_survives_several_crashes_in_a_row(node: Any) -> None:
    envelope = make_envelope()
    call(node, "deposit", envelope.to_bytes())
    pid = call(node, "info").pid
    for _ in range(3):
        ray.kill(node, no_restart=False)
        pid = wait_for_new_process(node, pid)
    assert bob_collects(node) == [envelope]


def test_after_a_restart_it_is_still_found_by_name(node_id: str, node: Any) -> None:
    old_pid = call(node, "info").pid
    ray.kill(node, no_restart=False)
    wait_for_new_process(node, old_pid)

    found = ray.get_actor(actor_name(node_id))
    assert call(found, "ping").ok
    assert call(found, "info").pid != old_pid


# ================= without restart =================
def test_killed_without_restart_it_stays_dead_but_its_data_does_not(
    node_id: str, tmp_path: Path
) -> None:
    handle = start_service_node(node_id, tmp_path, crypto=FakeCrypto())
    envelope = make_envelope()
    call(handle, "deposit", envelope.to_bytes())

    ray.kill(handle, no_restart=True)  # a deliberate shutdown
    wait_until_dead(handle)  # ray.kill is asynchronous
    with pytest.raises(RayActorError):  # and it stays dead: no restart
        call(handle, "ping")

    again = start_service_node(node_id, tmp_path, crypto=FakeCrypto())
    try:
        assert bob_collects(again) == [envelope]  # the files were still there
    finally:
        ray.kill(again, no_restart=True)


# ================= the wire format through Ray =================
def test_a_refusal_crosses_ray_as_a_reply_not_an_exception(node: Any) -> None:
    raw = collect_request(BOB, now_ms()).to_bytes()
    assert call(node, "collect", raw).ok
    replay = call(node, "collect", raw)
    assert not replay.ok and replay.refused and replay.error == "replayed"


def test_start_service_node_returns_the_existing_actor(
    node_id: str, node: Any, tmp_path: Path
) -> None:
    same = start_service_node(node_id, tmp_path, crypto=FakeCrypto())
    assert call(same, "info").pid == call(node, "info").pid
