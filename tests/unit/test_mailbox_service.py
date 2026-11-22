"""Tests for the mailbox service, on in-memory adapters and a manual clock."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.mailbox_store import InMemoryMailboxStore
from source.application.use_cases.mailbox import (
    DAY_MS,
    DEFAULT_MAX_FETCH,
    DEFAULT_TTL_MS,
    MailboxPolicy,
    MailboxService,
)
from source.domain.identifiers import MessageId, UserId
from source.domain.records import PutOutcome
from tests.contracts.mailbox_store_contract import BOB, CAROL, make_envelope


class DoubleCountingStore(InMemoryMailboxStore):
    """A store that wrongly counts a repeated id once per occurrence.

    The service must give the same answer whatever store is plugged in.
    """

    def delete(self, receiver: UserId, message_ids: list[MessageId]) -> int:
        present = {e.message_id for e in self.fetch(receiver, 10**6, 0)}
        existed = sum(1 for m in message_ids if m in present)
        super().delete(receiver, message_ids)
        return existed


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def service(clock: ManualClock) -> MailboxService:
    return MailboxService(InMemoryMailboxStore(), clock)


# ---------------- deposit and collect ----------------
def test_deposit_then_collect(service: MailboxService) -> None:
    env = make_envelope()
    assert service.deposit(env) is PutOutcome.STORED
    assert service.collect(BOB) == [env]


def test_depositing_twice_stores_only_one(service: MailboxService) -> None:
    env = make_envelope()
    assert service.deposit(env) is PutOutcome.STORED
    assert service.deposit(env) is PutOutcome.ALREADY_PRESENT
    assert service.collect(BOB) == [env]


def test_collecting_does_not_delete(service: MailboxService) -> None:
    env = make_envelope()
    service.deposit(env)
    service.collect(BOB)
    assert service.collect(BOB) == [env]


def test_collect_returns_oldest_first(service: MailboxService) -> None:
    envs = [make_envelope() for _ in range(3)]
    for env in envs:
        service.deposit(env)
    assert service.collect(BOB) == envs


def test_receivers_only_see_their_own_envelopes(service: MailboxService) -> None:
    service.deposit(make_envelope(BOB))
    assert service.collect(CAROL) == []


# ---------------- acknowledge ----------------
def test_acknowledging_deletes_and_counts(service: MailboxService) -> None:
    keep, drop = make_envelope(), make_envelope()
    service.deposit(keep)
    service.deposit(drop)
    assert service.acknowledge(BOB, [drop.message_id]) == 1
    assert service.collect(BOB) == [keep]


def test_acknowledging_an_unknown_id_deletes_nothing(service: MailboxService) -> None:
    service.deposit(make_envelope())
    assert service.acknowledge(BOB, [MessageId.new()]) == 0
    assert len(service.collect(BOB)) == 1


def test_acknowledging_with_no_ids_is_a_no_op(service: MailboxService) -> None:
    assert service.acknowledge(BOB, []) == 0


def test_a_repeated_id_in_one_acknowledge_counts_once(clock: ManualClock) -> None:
    # with a store that would count it twice, the service still answers 1
    service = MailboxService(DoubleCountingStore(), clock)
    env = make_envelope()
    service.deposit(env)
    assert service.acknowledge(BOB, [env.message_id, env.message_id]) == 1


def test_one_receiver_cannot_delete_anothers_envelopes(service: MailboxService) -> None:
    env = make_envelope(BOB)
    service.deposit(env)
    assert service.acknowledge(CAROL, [env.message_id]) == 0
    assert service.collect(BOB) == [env]


def test_acknowledging_twice_is_safe(service: MailboxService) -> None:
    env = make_envelope()
    service.deposit(env)
    assert service.acknowledge(BOB, [env.message_id]) == 1
    assert service.acknowledge(BOB, [env.message_id]) == 0


# ---------------- expiry ----------------
def test_a_message_is_available_until_the_ttl_and_gone_exactly_at_it(
    service: MailboxService, clock: ManualClock
) -> None:
    service.deposit(make_envelope())
    clock.advance(DEFAULT_TTL_MS - 1)
    assert len(service.collect(BOB)) == 1  # one millisecond before the deadline
    clock.advance(1)
    assert service.collect(BOB) == []  # exactly at the deadline


def test_a_message_deposited_14_days_ago_is_gone(
    service: MailboxService, clock: ManualClock
) -> None:
    service.deposit(make_envelope())
    clock.advance(14 * DAY_MS)
    assert service.collect(BOB) == []


def test_repeating_a_deposit_does_not_extend_its_life(
    service: MailboxService, clock: ManualClock
) -> None:
    env = make_envelope()
    service.deposit(env)
    clock.advance(10 * DAY_MS)
    assert service.deposit(env) is PutOutcome.ALREADY_PRESENT  # a replay
    clock.advance(5 * DAY_MS)  # 15 days after the FIRST deposit
    assert service.collect(BOB) == []


def test_each_message_expires_on_its_own_schedule(
    service: MailboxService, clock: ManualClock
) -> None:
    old, new = make_envelope(), make_envelope()
    service.deposit(old)
    clock.advance(10 * DAY_MS)
    service.deposit(new)
    clock.advance(5 * DAY_MS)  # old is 15 days, new is 5 days
    assert service.collect(BOB) == [new]


def test_a_custom_ttl_is_respected(clock: ManualClock) -> None:
    service = MailboxService(InMemoryMailboxStore(), clock, MailboxPolicy(ttl_ms=1000))
    service.deposit(make_envelope())
    clock.advance(999)
    assert len(service.collect(BOB)) == 1
    clock.advance(1)
    assert service.collect(BOB) == []


def test_purge_removes_only_expired_envelopes_and_counts_them(
    service: MailboxService, clock: ManualClock
) -> None:
    service.deposit(make_envelope())
    clock.advance(10 * DAY_MS)
    fresh = make_envelope()
    service.deposit(fresh)
    clock.advance(5 * DAY_MS)  # first one is 15 days old: expired
    assert service.purge_expired() == 1
    assert service.purge_expired() == 0
    assert service.collect(BOB) == [fresh]


def test_setting_the_wall_clock_back_makes_an_expired_message_visible_again(
    service: MailboxService, clock: ManualClock
) -> None:
    """Characterization test: a deadline is a DATE, so the mailbox uses the wall clock
    and an NTP correction backwards can bring an expired message back. Known limitation."""
    service.deposit(make_envelope())
    clock.advance(DEFAULT_TTL_MS)
    assert service.collect(BOB) == []
    clock.set_wall_clock(clock.now_ms() - DAY_MS)  # NTP correction: a day back
    assert len(service.collect(BOB)) == 1  # visible again: known, documented limitation


# ---------------- limits ----------------
def test_collect_respects_the_limit(service: MailboxService) -> None:
    for _ in range(5):
        service.deposit(make_envelope())
    assert len(service.collect(BOB, limit=2)) == 2


def test_collect_is_capped_by_the_policy(clock: ManualClock) -> None:
    service = MailboxService(InMemoryMailboxStore(), clock, MailboxPolicy(max_fetch=2))
    for _ in range(5):
        service.deposit(make_envelope())
    assert len(service.collect(BOB, limit=10)) == 2
    assert len(service.collect(BOB)) == 2  # no limit given: the cap applies


@pytest.mark.parametrize("bad", [0, -1])
def test_collect_rejects_a_non_positive_limit(
    service: MailboxService, bad: int
) -> None:
    with pytest.raises(ValueError, match="positive"):
        service.collect(BOB, limit=bad)


def test_policy_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="ttl_ms"):
        MailboxPolicy(ttl_ms=0)
    with pytest.raises(ValueError, match="max_fetch"):
        MailboxPolicy(max_fetch=0)


def test_default_policy_matches_the_documented_constants() -> None:
    policy = MailboxPolicy()
    assert policy.ttl_ms == 14 * DAY_MS and policy.max_fetch == DEFAULT_MAX_FETCH == 100


# ---------------- property ----------------
@given(count=st.integers(min_value=1, max_value=30))
def test_collecting_and_acknowledging_everything_empties_the_mailbox(
    count: int,
) -> None:
    service = MailboxService(InMemoryMailboxStore(), ManualClock())
    for _ in range(count):
        service.deposit(make_envelope())
    pending = service.collect(BOB)
    assert len(pending) == count
    assert service.acknowledge(BOB, [e.message_id for e in pending]) == count
    assert service.collect(BOB) == []
