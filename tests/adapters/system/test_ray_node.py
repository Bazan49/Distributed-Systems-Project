"""Tests of ServiceNode WITHOUT Ray: the logic of the node, in-process.

The restart itself is tested with real Ray in ``ray/test_ray_node_actor.py``.
Here a "restart" is what Ray does: build a NEW ServiceNode with the same
arguments over the same folder.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.system.ray.ray_node import (
    DIRECTORY_DB,
    MAILBOX_DB,
    ServiceNode,
    actor_name,
    node_of_actor,
)
from source.application.ports.errors import StoreError
from source.application.use_cases.directory import DEFAULT_MAX_LEASE_MS
from source.application.use_cases.mailbox import DEFAULT_TTL_MS
from source.domain.envelope import Envelope
from source.domain.node_id import NodeId
from source.domain.records import PutOutcome
from tests.contracts.mailbox_store_contract import make_envelope
from tests.support.builders import (
    ALICE,
    BOB,
    CARLA,
    ack_request,
    collect_request,
    signed_record,
)
from tests.support.logs import event_names, has_event


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


def make_node(
    tmp_path: Path, clock: ManualClock, node_id: str = "node-1"
) -> ServiceNode:
    return ServiceNode(
        node_id,
        str(tmp_path),
        crypto=FakeCrypto(),
        clock=clock,
        purge_interval_ms=None,  # no background thread: the tests purge by hand
    )


@pytest.fixture
def node(tmp_path: Path, clock: ManualClock) -> ServiceNode:
    return make_node(tmp_path, clock)


def bob_collects(node: ServiceNode, clock: ManualClock) -> list[Envelope]:
    reply = node.collect(collect_request(BOB, clock.now_ms()).to_bytes())
    assert reply.ok and reply.value is not None
    return [Envelope.from_bytes(raw) for raw in reply.value]


# ================= basics =================
def test_the_node_keeps_its_databases_in_its_own_folder(tmp_path: Path) -> None:
    node = make_node(tmp_path, ManualClock(), "node-7")
    assert (tmp_path / "node-7" / MAILBOX_DB).exists()
    assert (tmp_path / "node-7" / DIRECTORY_DB).exists()
    info = node.info()
    assert info.node_id.value == "node-7"
    assert info.data_dir == str(tmp_path / "node-7")
    assert info.pid > 0


def test_an_invalid_node_id_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        ServiceNode("Not A Node", str(tmp_path))
    with pytest.raises(ValueError):
        actor_name("../etc")


def test_the_actor_name_is_derived_from_the_node_id() -> None:
    assert actor_name("node-1") == "service-node-node-1"


def test_ping_answers_ok_with_the_node_id(node: ServiceNode) -> None:
    reply = node.ping()
    assert reply.ok and reply.node.value == "node-1"


def test_by_default_it_uses_the_real_clock_and_crypto(tmp_path: Path) -> None:
    node = ServiceNode("node-1", str(tmp_path), purge_interval_ms=None)
    assert node.ping().ok
    node.close()


# ================= mailbox =================
def test_a_deposited_envelope_can_be_collected(
    node: ServiceNode, clock: ManualClock
) -> None:
    envelope = make_envelope()
    reply = node.deposit(envelope.to_bytes())
    assert reply.ok and reply.value is PutOutcome.STORED
    assert bob_collects(node, clock) == [envelope]


def test_deposit_is_idempotent(node: ServiceNode) -> None:
    raw = make_envelope().to_bytes()
    node.deposit(raw)
    assert node.deposit(raw).value is PutOutcome.ALREADY_PRESENT


def test_a_malformed_envelope_is_refused(node: ServiceNode) -> None:
    reply = node.deposit(b"not an envelope")
    assert not reply.ok and reply.refused
    assert reply.error is not None and "malformed envelope" in reply.error


def test_acknowledge_deletes_the_envelopes(
    node: ServiceNode, clock: ManualClock
) -> None:
    envelope = make_envelope()
    node.deposit(envelope.to_bytes())
    request = ack_request(BOB, clock.now_ms(), [envelope.message_id])
    reply = node.acknowledge(request.to_bytes())
    assert reply.ok and reply.value == 1
    assert bob_collects(node, clock) == []


@pytest.mark.parametrize("operation", ["collect", "acknowledge"])
def test_malformed_mailbox_requests_are_refused(
    node: ServiceNode, operation: str
) -> None:
    reply = getattr(node, operation)(b"{}")
    assert not reply.ok and reply.refused
    assert "malformed request" in reply.error


def test_someone_else_cannot_collect_bobs_mailbox(
    node: ServiceNode, clock: ManualClock
) -> None:
    node.deposit(make_envelope().to_bytes())
    honest = collect_request(CARLA, clock.now_ms())
    # Carla signs a request but claims to be Bob: the id no longer matches the key.
    forged = honest.__class__(
        action=honest.action,
        receiver=BOB.user_id,
        public_key=honest.public_key,
        limit=honest.limit,
        message_ids=honest.message_ids,
        issued_at_ms=honest.issued_at_ms,
        nonce=honest.nonce,
        signature=honest.signature,
    )
    reply = node.collect(forged.to_bytes())
    assert not reply.ok and reply.refused and reply.error == "bad_identity"


def test_a_replayed_request_is_refused(node: ServiceNode, clock: ManualClock) -> None:
    raw = collect_request(BOB, clock.now_ms()).to_bytes()
    assert node.collect(raw).ok
    replay = node.collect(raw)
    assert replay.refused and replay.error == "replayed"
    ack = ack_request(BOB, clock.now_ms(), [make_envelope().message_id]).to_bytes()
    assert node.acknowledge(ack).ok
    assert node.acknowledge(ack).error == "replayed"


# ================= directory =================
def test_a_published_record_can_be_resolved(
    node: ServiceNode, clock: ManualClock
) -> None:
    record = signed_record(ALICE, clock.now_ms())
    assert node.publish_record(record).value is True
    reply = node.resolve_record(ALICE.user_id.value)
    assert reply.ok and reply.value == record


def test_an_unknown_user_resolves_to_no_record(node: ServiceNode) -> None:
    reply = node.resolve_record(BOB.user_id.value)
    assert reply.ok and reply.value is None


def test_a_stale_record_is_refused_with_the_reason(
    node: ServiceNode, clock: ManualClock
) -> None:
    node.publish_record(signed_record(ALICE, clock.now_ms(), version=2))
    reply = node.publish_record(signed_record(ALICE, clock.now_ms(), version=1))
    assert reply.refused and reply.error == "rejected_stale"


def test_something_that_is_not_a_record_is_refused(node: ServiceNode) -> None:
    reply = node.publish_record("not a record")
    assert reply.refused and reply.error == "malformed record"


def test_a_malformed_user_id_is_refused(node: ServiceNode) -> None:
    reply = node.resolve_record("bob")
    assert reply.refused and reply.error is not None
    assert "malformed user id" in reply.error


# ================= restart: the data survives =================
def test_a_new_incarnation_over_the_same_folder_keeps_every_accepted_item(
    tmp_path: Path, clock: ManualClock
) -> None:
    first = make_node(tmp_path, clock)
    envelope = make_envelope()
    record = signed_record(ALICE, clock.now_ms(), version=3)
    first.deposit(envelope.to_bytes())
    first.publish_record(record)
    # No close(): Ray does not call it when the process dies.

    second = make_node(tmp_path, clock)  # what Ray does on a restart

    assert bob_collects(second, clock) == [envelope]
    assert second.resolve_record(ALICE.user_id.value).value == record
    stale = second.publish_record(signed_record(ALICE, clock.now_ms(), version=2))
    assert stale.error == "rejected_stale"  # the highest version survived too


def test_nodes_with_the_same_data_dir_do_not_share_data(
    tmp_path: Path, clock: ManualClock
) -> None:
    one = make_node(tmp_path, clock, "node-1")
    two = make_node(tmp_path, clock, "node-2")
    one.deposit(make_envelope().to_bytes())
    assert bob_collects(two, clock) == []


# ================= maintenance =================
def test_purge_removes_expired_envelopes_and_records(
    node: ServiceNode, clock: ManualClock
) -> None:
    node.deposit(make_envelope().to_bytes())
    node.publish_record(signed_record(ALICE, clock.now_ms()))
    assert node.purge_expired() == (0, 0)
    clock.advance(max(DEFAULT_TTL_MS, DEFAULT_MAX_LEASE_MS))
    assert node.purge_expired() == (1, 1)


def test_a_storage_failure_is_an_error_not_a_refusal(node: ServiceNode) -> None:
    node.close()
    with pytest.raises(StoreError):
        node.deposit(make_envelope().to_bytes())


# ================= membership of peers =================
def test_the_node_tracks_its_peers(node: ServiceNode, clock: ManualClock) -> None:
    assert node.peer_state("node-2") == "suspect"  # never heard of
    node.report_peer("node-2", answered=True)
    assert node.peer_state("node-2") == "alive"
    node.report_peer("node-3", answered=False)
    clock.advance(15_000)
    assert node.peer_state("node-3") == "dead"
    assert node.active_peers(["node-3", "node-4"]) == ["node-4"]


# ================= discovery (task 2.4) =================
def test_node_of_actor_reads_back_the_node_id() -> None:
    assert node_of_actor(actor_name("node-3")) == NodeId("node-3")
    assert node_of_actor("echo") is None  # some other actor
    assert node_of_actor("service-node-Not Valid") is None


def test_list_nodes_answers_the_registered_nodes_and_itself(
    tmp_path: Path, clock: ManualClock
) -> None:
    registered = [NodeId("node-3"), NodeId("node-2")]  # this node not among them
    node = ServiceNode(
        "node-1",
        str(tmp_path),
        crypto=FakeCrypto(),
        clock=clock,
        node_lister=lambda: registered,
        purge_interval_ms=None,
    )
    reply = node.list_nodes()
    assert reply.ok and reply.value == ["node-1", "node-2", "node-3"]


def test_list_nodes_leaves_out_peers_this_node_knows_are_dead(
    tmp_path: Path, clock: ManualClock
) -> None:
    node = ServiceNode(
        "node-1",
        str(tmp_path),
        crypto=FakeCrypto(),
        clock=clock,
        node_lister=lambda: [NodeId("node-1"), NodeId("node-2"), NodeId("node-3")],
        purge_interval_ms=None,
    )
    node.report_peer("node-3", answered=False)
    clock.advance(15_000)
    assert node.list_nodes().value == ["node-1", "node-2"]


# ---------------- logs: actor.started (docs/logging.md) ----------------
def test_the_log_setup_runs_first_and_the_start_is_logged(
    tmp_path: Path, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    seen_before_setup: list[list[str]] = []

    def setup() -> None:  # records what had been logged when it ran
        seen_before_setup.append(event_names(caplog))

    ServiceNode(
        "node-1",
        str(tmp_path),
        crypto=FakeCrypto(),
        clock=clock,
        log_setup=setup,
        purge_interval_ms=None,
    )

    assert seen_before_setup == [[]]  # it ran once, before any event
    assert has_event(caplog, "actor.started", node=NodeId("node-1"))
    assert "actor.restarted" not in event_names(caplog)  # not a Ray restart


def test_the_node_events_reach_the_log(
    node: ServiceNode, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    envelope = make_envelope()
    node.deposit(envelope.to_bytes())
    assert has_event(
        caplog, "mailbox.deposited", msg=envelope.message_id, outcome=PutOutcome.STORED
    )


# ---------------- the node purges by itself ----------------
EXPIRED_MS = max(DEFAULT_TTL_MS, DEFAULT_MAX_LEASE_MS)  # everything has expired


def fill(node: ServiceNode, clock: ManualClock) -> None:
    """One envelope and one record, then let them expire."""
    node.deposit(make_envelope().to_bytes())
    node.publish_record(signed_record(ALICE, clock.now_ms()))
    clock.advance(EXPIRED_MS)


def wait_until(condition: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + 5
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting until {what}")
        time.sleep(0.01)


def test_a_purge_that_deletes_something_is_logged_and_one_that_does_not_is_not(
    node: ServiceNode, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    assert node.purge_expired() == (0, 0)
    assert "node.purged" not in event_names(caplog)  # nothing to say
    fill(node, clock)
    assert node.purge_expired() == (1, 1)
    assert has_event(caplog, "node.purged", envelopes=1, records=1)
    [record] = [r for r in caplog.records if r.getMessage() == "node.purged"]
    assert record.levelno == logging.DEBUG


def test_a_node_purges_what_expired_while_it_was_down(
    tmp_path: Path, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    before = make_node(tmp_path, clock)
    fill(before, clock)
    before.close()  # down while everything expires

    caplog.set_level(logging.DEBUG, logger="p08")
    again = make_node(tmp_path, clock)  # restart over the same files

    assert has_event(caplog, "node.purged", envelopes=1, records=1)
    assert again.purge_expired() == (0, 0)  # already gone from disk


def test_the_background_thread_purges_on_its_own_and_stops_on_close(
    tmp_path: Path, clock: ManualClock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    node = ServiceNode(
        "node-1", str(tmp_path), crypto=FakeCrypto(), clock=clock, purge_interval_ms=20
    )
    fill(node, clock)
    wait_until(lambda: has_event(caplog, "node.purged"), "the thread purges")

    node.close()
    assert not any(t.name == "purge-node-1" for t in threading.enumerate())


def test_a_failed_purge_is_logged_and_the_node_keeps_purging(
    tmp_path: Path,
    clock: ManualClock,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    node = ServiceNode(
        "node-1", str(tmp_path), crypto=FakeCrypto(), clock=clock, purge_interval_ms=20
    )
    real = node._mailbox.purge_expired
    failures = iter([StoreError("disk full")])

    def flaky() -> int:
        error = next(failures, None)
        if error is not None:
            raise error
        return real()

    monkeypatch.setattr(node._mailbox, "purge_expired", flaky)
    fill(node, clock)

    wait_until(lambda: has_event(caplog, "node.purged"), "a later purge works")
    assert has_event(caplog, "node.purge_failed", node=NodeId("node-1"))
    [failed] = [r for r in caplog.records if r.getMessage() == "node.purge_failed"]
    assert failed.levelno == logging.ERROR and failed.exc_info is not None
    assert node.ping().ok  # still serving
    node.close()


def test_a_failure_at_start_does_not_stop_the_node(
    tmp_path: Path,
    clock: ManualClock,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(self: object) -> int:
        raise StoreError("disk full")

    monkeypatch.setattr(
        "source.application.use_cases.mailbox.MailboxService.purge_expired", broken
    )
    caplog.set_level(logging.DEBUG, logger="p08")
    node = make_node(tmp_path, clock)
    assert has_event(caplog, "node.purge_failed")
    assert node.deposit(make_envelope().to_bytes()).ok


def test_the_purge_waits_for_a_call_in_progress(node: ServiceNode) -> None:
    done = threading.Event()

    def purge() -> None:
        node.purge_expired()
        done.set()

    with node._lock:  # a call to the node is running
        purger = threading.Thread(target=purge)
        purger.start()
        assert not done.wait(0.2)  # the purge cannot run beside it
    assert done.wait(5)  # it runs as soon as the call ends
    purger.join()


@pytest.mark.parametrize("interval", [0, -1, 1.5, True])
def test_the_purge_interval_must_be_a_positive_integer(
    tmp_path: Path, interval: object
) -> None:
    with pytest.raises(ValueError, match="purge_interval_ms"):
        ServiceNode("node-1", str(tmp_path), purge_interval_ms=interval)
