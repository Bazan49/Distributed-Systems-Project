"""Tests for the mailbox service, on in-memory adapters, a fake crypto and a manual clock."""

import logging
from dataclasses import replace

import pytest
from hypothesis import given
from hypothesis import strategies as st

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.mailbox_store import InMemoryMailboxStore
from source.application.ports import MailboxStorePort
from source.application.use_cases.mailbox import (
    DAY_MS,
    DEFAULT_MAX_FETCH,
    DEFAULT_TTL_MS,
    AccessDenial,
    MailboxAccessDenied,
    MailboxPolicy,
    MailboxService,
)
from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId
from source.domain.mailbox_access import DEFAULT_MAX_SKEW_MS
from source.domain.records import PutOutcome
from tests.contracts.mailbox_store_contract import make_envelope
from tests.support.builders import (
    BOB,
    CARLA,
    Person,
    ack_request,
    collect_request,
    signed_by,
)
from tests.support.logs import events, has_event


class Mailbox:
    """The service plus its clock: each call is a freshly signed request at ``now``."""

    def __init__(
        self,
        clock: ManualClock,
        policy: MailboxPolicy | None = None,
        store: MailboxStorePort | None = None,
    ) -> None:
        self.clock = clock
        self.service = MailboxService(
            store if store is not None else InMemoryMailboxStore(),
            FakeCrypto(),
            clock,
            policy,
        )

    def deposit(self, envelope: Envelope) -> PutOutcome:
        return self.service.deposit(envelope)

    def collect(self, person: Person = BOB, limit: int = 1_000) -> list[Envelope]:
        return self.service.collect(
            collect_request(person, self.clock.now_ms(), limit=limit)
        )

    def acknowledge(self, person: Person, ids: list[MessageId]) -> int:
        return self.service.acknowledge(ack_request(person, self.clock.now_ms(), ids))


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def box(clock: ManualClock) -> Mailbox:
    return Mailbox(clock)


def denied(reason: AccessDenial) -> pytest.RaisesExc[MailboxAccessDenied]:
    return pytest.raises(MailboxAccessDenied, match=reason.value)


# ---------------- deposit and collect ----------------
def test_deposit_then_collect(box: Mailbox) -> None:
    env = make_envelope()
    assert box.deposit(env) is PutOutcome.STORED
    assert box.collect() == [env]


def test_depositing_twice_stores_only_one(box: Mailbox) -> None:
    env = make_envelope()
    assert box.deposit(env) is PutOutcome.STORED
    assert box.deposit(env) is PutOutcome.ALREADY_PRESENT
    assert box.collect() == [env]


def test_collecting_does_not_delete(box: Mailbox) -> None:
    env = make_envelope()
    box.deposit(env)
    box.collect()
    assert box.collect() == [env]


def test_collect_returns_oldest_first(box: Mailbox) -> None:
    envs = [make_envelope() for _ in range(3)]
    for env in envs:
        box.deposit(env)
    assert box.collect() == envs


def test_receivers_only_see_their_own_envelopes(box: Mailbox) -> None:
    box.deposit(make_envelope(BOB.user_id))
    assert box.collect(CARLA) == []


# ---------------- acknowledge ----------------
def test_acknowledging_deletes_and_counts(box: Mailbox) -> None:
    keep, drop = make_envelope(), make_envelope()
    box.deposit(keep)
    box.deposit(drop)
    assert box.acknowledge(BOB, [drop.message_id]) == 1
    assert box.collect() == [keep]


def test_acknowledging_an_unknown_id_deletes_nothing(box: Mailbox) -> None:
    box.deposit(make_envelope())
    assert box.acknowledge(BOB, [MessageId.new()]) == 0
    assert len(box.collect()) == 1


def test_a_repeated_id_in_one_acknowledge_counts_once(box: Mailbox) -> None:
    env = make_envelope()
    box.deposit(env)
    assert box.acknowledge(BOB, [env.message_id, env.message_id]) == 1


def test_one_receiver_cannot_delete_anothers_envelopes(box: Mailbox) -> None:
    env = make_envelope(BOB.user_id)
    box.deposit(env)
    assert box.acknowledge(CARLA, [env.message_id]) == 0  # her own (empty) queue
    assert box.collect() == [env]


def test_acknowledging_twice_is_safe(box: Mailbox) -> None:
    env = make_envelope()
    box.deposit(env)
    assert box.acknowledge(BOB, [env.message_id]) == 1
    assert box.acknowledge(BOB, [env.message_id]) == 0


# ---------------- access control ----------------
def test_nobody_else_can_collect_bobs_mailbox(box: Mailbox, clock: ManualClock) -> None:
    """Carla signs with her key but claims to be Bob: the key does not match the id."""
    box.deposit(make_envelope())
    honest = collect_request(CARLA, clock.now_ms())
    forged = replace(honest, receiver=BOB.user_id)
    with denied(AccessDenial.BAD_IDENTITY):
        box.service.collect(forged)


def test_bobs_key_with_someone_elses_signature_is_refused(
    box: Mailbox, clock: ManualClock
) -> None:
    """Carla copies Bob's public key into the request but cannot sign as Bob."""
    env = make_envelope()
    box.deposit(env)
    request = ack_request(BOB, clock.now_ms(), [env.message_id])
    forged = replace(request, signature=signed_by(CARLA)(request.signing_bytes()))
    with denied(AccessDenial.INVALID_SIGNATURE):
        box.service.acknowledge(forged)
    assert box.collect() == [env]  # nothing was deleted


def test_a_tampered_request_is_refused(box: Mailbox, clock: ManualClock) -> None:
    """Changing a signed field (here the ids to delete) breaks the signature."""
    env = make_envelope()
    box.deposit(env)
    request = ack_request(BOB, clock.now_ms(), [MessageId.new()])
    tampered = replace(request, message_ids=(env.message_id,))
    with denied(AccessDenial.INVALID_SIGNATURE):
        box.service.acknowledge(tampered)
    assert box.collect() == [env]


def test_a_collect_request_cannot_be_used_to_acknowledge(
    box: Mailbox, clock: ManualClock
) -> None:
    with denied(AccessDenial.WRONG_ACTION):
        box.service.acknowledge(collect_request(BOB, clock.now_ms()))
    with denied(AccessDenial.WRONG_ACTION):
        box.service.collect(ack_request(BOB, clock.now_ms(), [MessageId.new()]))


def test_a_replayed_request_is_refused(box: Mailbox, clock: ManualClock) -> None:
    request = collect_request(BOB, clock.now_ms())
    box.service.collect(request)
    with denied(AccessDenial.REPLAYED):
        box.service.collect(request)


@pytest.mark.parametrize("offset", [DEFAULT_MAX_SKEW_MS + 1, -DEFAULT_MAX_SKEW_MS - 1])
def test_a_request_outside_the_clock_skew_window_is_stale(
    box: Mailbox, clock: ManualClock, offset: int
) -> None:
    with denied(AccessDenial.STALE):
        box.service.collect(collect_request(BOB, clock.now_ms() + offset))


def test_a_request_at_the_edge_of_the_window_is_accepted(
    box: Mailbox, clock: ManualClock
) -> None:
    request = collect_request(BOB, clock.now_ms())
    clock.advance(DEFAULT_MAX_SKEW_MS)
    assert box.service.collect(request) == []


def test_a_refused_request_does_not_burn_its_nonce(
    box: Mailbox, clock: ManualClock
) -> None:
    """A forged copy sent first must not block the genuine request with the same nonce."""
    genuine = collect_request(BOB, clock.now_ms())
    with denied(AccessDenial.INVALID_SIGNATURE):
        box.service.collect(replace(genuine, signature=b"forged"))
    assert box.service.collect(genuine) == []


def test_policy_skew_is_used(clock: ManualClock) -> None:
    box = Mailbox(clock, MailboxPolicy(max_skew_ms=10))
    with denied(AccessDenial.STALE):
        box.service.collect(collect_request(BOB, clock.now_ms() - 11))


# ---------------- expiry ----------------
def test_a_message_is_available_until_the_ttl_and_gone_exactly_at_it(
    box: Mailbox, clock: ManualClock
) -> None:
    box.deposit(make_envelope())
    clock.advance(DEFAULT_TTL_MS - 1)
    assert len(box.collect()) == 1  # one millisecond before the deadline
    clock.advance(1)
    assert box.collect() == []  # exactly at the deadline


def test_a_message_deposited_14_days_ago_is_gone(
    box: Mailbox, clock: ManualClock
) -> None:
    box.deposit(make_envelope())
    clock.advance(14 * DAY_MS)
    assert box.collect() == []


def test_repeating_a_deposit_does_not_extend_its_life(
    box: Mailbox, clock: ManualClock
) -> None:
    env = make_envelope()
    box.deposit(env)
    clock.advance(10 * DAY_MS)
    assert box.deposit(env) is PutOutcome.ALREADY_PRESENT  # a replay
    clock.advance(5 * DAY_MS)  # 15 days after the FIRST deposit
    assert box.collect() == []


def test_each_message_expires_on_its_own_schedule(
    box: Mailbox, clock: ManualClock
) -> None:
    old, new = make_envelope(), make_envelope()
    box.deposit(old)
    clock.advance(10 * DAY_MS)
    box.deposit(new)
    clock.advance(5 * DAY_MS)  # old is 15 days, new is 5 days
    assert box.collect() == [new]


def test_a_custom_ttl_is_respected(clock: ManualClock) -> None:
    box = Mailbox(clock, MailboxPolicy(ttl_ms=1000))
    box.deposit(make_envelope())
    clock.advance(999)
    assert len(box.collect()) == 1
    clock.advance(1)
    assert box.collect() == []


def test_purge_removes_only_expired_envelopes_and_counts_them(
    box: Mailbox, clock: ManualClock
) -> None:
    box.deposit(make_envelope())
    clock.advance(10 * DAY_MS)
    fresh = make_envelope()
    box.deposit(fresh)
    clock.advance(5 * DAY_MS)  # first one is 15 days old: expired
    assert box.service.purge_expired() == 1
    assert box.service.purge_expired() == 0
    assert box.collect() == [fresh]


def test_setting_the_wall_clock_back_makes_an_expired_message_visible_again(
    box: Mailbox, clock: ManualClock
) -> None:
    """Characterization test: a deadline is a DATE, so the mailbox uses the wall clock
    and an NTP correction backwards can bring an expired message back. Known limitation."""
    box.deposit(make_envelope())
    clock.advance(DEFAULT_TTL_MS)
    assert box.collect() == []
    clock.set_wall_clock(clock.now_ms() - DAY_MS)  # NTP correction: a day back
    assert len(box.collect()) == 1  # visible again: known, documented limitation


# ---------------- limits ----------------
def test_collect_respects_the_limit(box: Mailbox) -> None:
    for _ in range(5):
        box.deposit(make_envelope())
    assert len(box.collect(limit=2)) == 2


def test_collect_is_capped_by_the_policy(clock: ManualClock) -> None:
    box = Mailbox(clock, MailboxPolicy(max_fetch=2))
    for _ in range(5):
        box.deposit(make_envelope())
    assert len(box.collect(limit=10)) == 2


def test_policy_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="ttl_ms"):
        MailboxPolicy(ttl_ms=0)
    with pytest.raises(ValueError, match="max_fetch"):
        MailboxPolicy(max_fetch=0)
    with pytest.raises(ValueError, match="max_skew_ms"):
        MailboxPolicy(max_skew_ms=0)


def test_default_policy_matches_the_documented_constants() -> None:
    policy = MailboxPolicy()
    assert policy.ttl_ms == 14 * DAY_MS and policy.max_fetch == DEFAULT_MAX_FETCH == 100
    assert policy.max_skew_ms == DEFAULT_MAX_SKEW_MS


# ---------------- property ----------------
@given(count=st.integers(min_value=1, max_value=30))
def test_collecting_and_acknowledging_everything_empties_the_mailbox(
    count: int,
) -> None:
    box = Mailbox(ManualClock())
    for _ in range(count):
        box.deposit(make_envelope())
    pending = box.collect()
    assert len(pending) == count
    assert box.acknowledge(BOB, [e.message_id for e in pending]) == count
    assert box.collect() == []


# ------- logs: mailbox.* (docs/logging.md) -------
def test_deposits_are_logged_with_their_outcome(
    box: Mailbox, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    env = make_envelope()
    box.deposit(env)
    box.deposit(env)  # the copy from another replica path
    assert events(caplog) == [
        ("mailbox.deposited", {"msg": env.message_id, "outcome": PutOutcome.STORED}),
        (
            "mailbox.deposited",
            {"msg": env.message_id, "outcome": PutOutcome.ALREADY_PRESENT},
        ),
    ]
    assert {r.levelno for r in caplog.records} == {logging.DEBUG}


def test_collect_and_acknowledge_are_logged_with_a_count(
    box: Mailbox, caplog: pytest.LogCaptureFixture
) -> None:
    envs = [make_envelope(), make_envelope()]
    for env in envs:
        box.deposit(env)
    caplog.set_level(logging.DEBUG, logger="p08")
    box.collect(BOB)
    box.acknowledge(BOB, [envs[0].message_id])
    assert has_event(caplog, "mailbox.collected", user=BOB.user_id, count=2)
    assert has_event(caplog, "mailbox.acknowledged", user=BOB.user_id, count=1)
