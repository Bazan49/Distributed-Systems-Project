"""Tests for the client's directory use case (task 1B.8), with a fake clock.

Acceptance criterion: the presence is renewed with a fake clock.

Alice and Bob are two clients (each with its own membership view) talking to
the same in-memory cluster of 3 nodes, all driven by one ``ManualClock``.
"""

from __future__ import annotations

import logging

import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.application.use_cases.client_directory import (
    DirectoryClient,
    PresencePolicy,
)
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.membership import MembershipService
from source.application.use_cases.quorum import QuorumClient
from source.domain.directory import public_key_of
from source.domain.node_id import NodeId
from source.domain.placement import rank_nodes
from tests.support.builders import ALICE, BOB, CARLA, Person, make_identity
from tests.support.logs import event_names, has_event

NODES = [NodeId("node-1"), NodeId("node-2"), NodeId("node-3")]
LEASE_MS = 30_000
MARGIN_MS = 20_000  # renew when 20 s are left, i.e. 10 s after publishing
APP_TICK_MS = 1_000


class World:
    def __init__(self) -> None:
        self.clock = ManualClock()
        self.gateway = InMemoryNodeGateway(NODES, self.clock, FakeCrypto())
        self.quorums: list[QuorumClient] = []
        self.alice = self.client_for(ALICE)
        self.bob = self.client_for(BOB)

    def client_for(self, person: Person) -> DirectoryClient:
        quorum = QuorumClient(
            self.gateway, MembershipService(self.clock), FakeCrypto(), self.clock
        )
        self.quorums.append(quorum)
        me = UnlockedIdentity(
            identity=make_identity(person), private_key=person.keys.private_key
        )
        return DirectoryClient(
            me, quorum, FakeCrypto(), self.clock, PresencePolicy(LEASE_MS, MARGIN_MS)
        )

    def run_app(self, duration_ms: int, *, alice_renews: bool = True) -> int:
        """Let time pass as the app's main loop would see it, one tick at a time.

        Each tick every client refreshes its membership view and Alice renews
        her presence if it is due. Returns how many records Alice published.
        """
        published = 0
        for _ in range(duration_ms // APP_TICK_MS):
            self.clock.advance(APP_TICK_MS)
            for quorum in self.quorums:
                quorum.refresh_membership()
            if alice_renews and self.alice.renew_if_due() is not None:
                published += 1
        return published


@pytest.fixture
def world() -> World:
    return World()


# ================= publish my own record =================
def test_after_publishing_a_contact_can_resolve_me(world: World) -> None:
    now = world.clock.now_ms()
    attempt = world.alice.publish()

    assert attempt.accepted
    found = world.bob.resolve_contact(ALICE.user_id)
    assert found == attempt.record
    assert found.user_id == ALICE.user_id
    assert public_key_of(found) == ALICE.keys.public_key
    assert found.expires_at_ms == now + LEASE_MS
    assert world.alice.online_until_ms == now + LEASE_MS
    assert world.bob.is_online(ALICE.user_id)


def test_before_publishing_i_look_offline(world: World) -> None:
    assert world.alice.online_until_ms is None
    assert not world.bob.is_online(ALICE.user_id)


def test_each_publish_has_a_higher_version(world: World) -> None:
    first = world.alice.publish()
    second = world.alice.publish()  # same millisecond
    world.clock.advance(5)
    third = world.alice.publish()
    assert first.record.version < second.record.version < third.record.version
    assert all(a.accepted for a in (first, second, third))
    assert world.bob.resolve_contact(ALICE.user_id) == third.record


# ================= renew the presence (acceptance criterion) =================
def test_the_presence_is_renewed_and_never_lapses(world: World) -> None:
    start = world.clock.now_ms()
    assert world.alice.renew_if_due() is not None  # first presence

    seen = []
    for _ in range(6):  # one minute, checked every 10 s
        world.run_app(10_000)
        record = world.bob.resolve_contact(ALICE.user_id)
        assert record is not None  # Alice never looked offline
        assert record.expires_at_ms > world.clock.now_ms()
        seen.append(record)

    assert seen[-1].expires_at_ms > start + 60_000  # leases were extended
    assert world.alice.online_until_ms == seen[-1].expires_at_ms
    versions = [r.version for r in seen]
    assert versions == sorted(set(versions))  # a new, higher version each time


def test_renewal_happens_about_every_ten_seconds(world: World) -> None:
    world.alice.publish()
    assert world.run_app(60_000) == 6


def test_nothing_is_published_before_the_renewal_point(world: World) -> None:
    attempt = world.alice.publish()
    assert world.alice.renew_if_due() is None
    world.clock.advance(LEASE_MS - MARGIN_MS - 1)
    assert world.alice.renew_if_due() is None
    world.clock.advance(1)
    renewal = world.alice.renew_if_due()
    assert renewal is not None and renewal.accepted
    assert renewal.record.version > attempt.record.version
    assert renewal.record.expires_at_ms == world.clock.now_ms() + LEASE_MS


def test_without_renewals_the_presence_expires_on_its_own(world: World) -> None:
    world.alice.publish()
    world.run_app(LEASE_MS - APP_TICK_MS, alice_renews=False)
    assert world.bob.is_online(ALICE.user_id)
    world.run_app(APP_TICK_MS, alice_renews=False)  # the lease ends now
    assert not world.bob.is_online(ALICE.user_id)
    assert world.alice.online_until_ms is None


def test_a_failed_renewal_is_retried_until_the_nodes_come_back(
    world: World,
) -> None:
    first = world.alice.publish()
    for node in rank_nodes(ALICE.user_id, NODES)[:2]:
        world.gateway.set_down(node)

    world.run_app(LEASE_MS - MARGIN_MS)  # renewal due, but no quorum
    failed = world.alice.renew_if_due()
    assert failed is not None and not failed.accepted
    assert world.alice.online_until_ms == first.record.expires_at_ms  # unchanged

    for node in NODES:
        world.gateway.set_up(node)
    world.run_app(APP_TICK_MS)  # the next tick retries and succeeds
    renewed = world.bob.resolve_contact(ALICE.user_id)
    assert renewed is not None
    assert renewed.version > failed.record.version
    assert world.alice.online_until_ms == renewed.expires_at_ms


def test_a_failed_publish_leaves_me_offline(world: World) -> None:
    for node in NODES[:2]:
        world.gateway.set_down(node)
    attempt = world.alice.publish()
    assert not attempt.accepted
    assert world.alice.online_until_ms is None
    assert world.alice.renew_if_due() is not None  # still due: retried


# ================= versions across clock changes and restarts =================
def test_renewal_still_works_if_the_wall_clock_goes_back(world: World) -> None:
    first = world.alice.publish()
    world.clock.set_wall_clock(world.clock.now_ms() - 60_000)  # NTP correction

    second = world.alice.publish()

    assert second.record.version == first.record.version + 1
    assert second.accepted


def test_after_a_restart_the_new_session_is_not_rejected_as_old(
    world: World,
) -> None:
    world.alice.publish()
    world.clock.advance(1)
    restarted = world.client_for(ALICE)  # forgot every previous version

    attempt = restarted.publish()

    assert attempt.accepted
    assert world.bob.resolve_contact(ALICE.user_id) == attempt.record


# ================= resolve a contact =================
def test_an_unknown_contact_resolves_to_none(world: World) -> None:
    assert world.alice.resolve_contact(CARLA.user_id) is None


def test_each_user_sees_the_other(world: World) -> None:
    world.alice.publish()
    world.bob.publish()
    assert world.alice.is_online(BOB.user_id)
    assert world.bob.is_online(ALICE.user_id)


# ================= policy =================
@pytest.mark.parametrize(
    ("lease", "margin"),
    [
        (0, 1),
        (30_000, 0),
        (30_000, 30_000),
        (30_000, 40_000),
        (10 * 60 * 1000 + 1, 1_000),  # nodes refuse leases over 10 minutes
        (1.5, 1),
    ],
)
def test_invalid_policies_are_rejected(lease: object, margin: object) -> None:
    with pytest.raises(ValueError):
        PresencePolicy(lease, margin)


def test_the_default_policy_renews_well_before_the_lease_ends() -> None:
    policy = PresencePolicy()
    assert policy.renew_margin_ms > policy.lease_ms // 2


# ====== logs: presence.published / presence.renew_failed (docs/logging.md) ======
def presence_events(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [n for n in event_names(caplog) if n.startswith("presence.")]


def test_a_publication_is_logged_with_its_version_and_lease(
    world: World, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    attempt = world.alice.publish()
    assert has_event(
        caplog,
        "presence.published",
        version=attempt.record.version,
        expires_in_ms=LEASE_MS,
    )
    [record] = [r for r in caplog.records if r.getMessage() == "presence.published"]
    assert record.levelno == logging.DEBUG  # every ~10 s: detail


def test_a_failed_renewal_says_how_long_the_old_presence_lasts(
    world: World, caplog: pytest.LogCaptureFixture
) -> None:
    world.alice.publish()
    for node in rank_nodes(ALICE.user_id, NODES)[:2]:
        world.gateway.set_down(node)
    world.clock.advance(LEASE_MS - MARGIN_MS)
    caplog.set_level(logging.DEBUG, logger="p08")

    failed = world.alice.renew_if_due()

    assert failed is not None
    assert presence_events(caplog) == ["presence.renew_failed"]  # only that one
    assert has_event(
        caplog,
        "presence.renew_failed",
        version=failed.record.version,
        expires_in_ms=MARGIN_MS,  # what is left of the previous lease
    )
    [record] = [r for r in caplog.records if r.getMessage() == "presence.renew_failed"]
    assert record.levelno == logging.WARNING


def test_a_failed_first_publish_has_nothing_left(
    world: World, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    for node in NODES[:2]:
        world.gateway.set_down(node)
    world.alice.publish()
    assert has_event(caplog, "presence.renew_failed", expires_in_ms=0)


def test_renew_if_due_logs_nothing_when_nothing_is_due(
    world: World, caplog: pytest.LogCaptureFixture
) -> None:
    world.alice.publish()
    caplog.set_level(logging.DEBUG, logger="p08")
    assert world.alice.renew_if_due() is None
    assert presence_events(caplog) == []
