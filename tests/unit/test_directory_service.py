"""Tests for the directory service (task 1B.5), on in-memory adapters, a fake crypto
and a manual clock."""

from dataclasses import replace
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.directory_store import InMemoryDirectoryStore
from source.application.use_cases.directory import (
    DEFAULT_MAX_LEASE_MS,
    DirectoryPolicy,
    DirectoryService,
    PublishOutcome,
    RecordCheck,
    check_record_authenticity,
)
from source.domain.directory import build_record
from source.domain.identifiers import UserId
from source.domain.records import DirectoryRecord, KeyPair

MINUTE = 60_000
crypto = FakeCrypto()
ALICE_KEYS = crypto.generate_key_pair()
ALICE = UserId.from_public_key(ALICE_KEYS.public_key)
MALLORY_KEYS = crypto.generate_key_pair()  # an attacker


def tweak(record: DirectoryRecord, **changes: Any) -> DirectoryRecord:
    """A copy of ``record`` with some fields changed (to build tampered records)."""
    return replace(record, **changes)


def record_for(
    keys: KeyPair, clock: ManualClock, version: int = 1, lease_ms: int = MINUTE
) -> DirectoryRecord:
    return build_record(
        public_key=keys.public_key,
        version=version,
        expires_at_ms=clock.now_ms() + lease_ms,
        sign=lambda data: crypto.sign(keys.private_key, data),
    )


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def store() -> InMemoryDirectoryStore:
    return InMemoryDirectoryStore()


@pytest.fixture
def service(store: InMemoryDirectoryStore, clock: ManualClock) -> DirectoryService:
    return DirectoryService(store, crypto, clock)


# ---------------- publish and resolve ----------------
def test_publishing_a_valid_record_stores_it(
    service: DirectoryService, clock: ManualClock
) -> None:
    record = record_for(ALICE_KEYS, clock)
    assert service.publish(record) is PublishOutcome.STORED
    assert service.resolve(ALICE) == record


def test_resolving_an_unknown_user_gives_nothing(service: DirectoryService) -> None:
    assert service.resolve(ALICE) is None


def test_publishing_the_identical_record_again_is_harmless(
    service: DirectoryService, clock: ManualClock
) -> None:
    record = record_for(ALICE_KEYS, clock)
    service.publish(record)
    outcome = service.publish(record)
    assert outcome is PublishOutcome.UNCHANGED and outcome.accepted
    assert service.resolve(ALICE) == record


def test_the_accepted_property() -> None:
    assert PublishOutcome.STORED.accepted and PublishOutcome.UNCHANGED.accepted
    assert not PublishOutcome.REJECTED_STALE.accepted
    assert not PublishOutcome.REJECTED_INVALID_SIGNATURE.accepted


# ---------------- versions: the higher one wins ----------------
def test_a_higher_version_replaces_the_stored_one(
    service: DirectoryService, clock: ManualClock
) -> None:
    service.publish(record_for(ALICE_KEYS, clock, version=1))
    newer = record_for(ALICE_KEYS, clock, version=2)
    assert service.publish(newer) is PublishOutcome.STORED
    assert service.resolve(ALICE) == newer


def test_a_lower_version_is_rejected_and_changes_nothing(
    service: DirectoryService, clock: ManualClock
) -> None:
    current = record_for(ALICE_KEYS, clock, version=5)
    service.publish(current)
    older = record_for(ALICE_KEYS, clock, version=4)
    assert service.publish(older) is PublishOutcome.REJECTED_STALE
    assert service.resolve(ALICE) == current


def test_the_same_version_with_different_content_is_rejected(
    service: DirectoryService, clock: ManualClock
) -> None:
    first = record_for(ALICE_KEYS, clock, version=3, lease_ms=MINUTE)
    service.publish(first)
    conflicting = record_for(ALICE_KEYS, clock, version=3, lease_ms=2 * MINUTE)
    assert service.publish(conflicting) is PublishOutcome.REJECTED_STALE
    assert service.resolve(ALICE) == first  # the first one wins


def test_an_old_valid_record_cannot_replace_a_newer_one(
    service: DirectoryService, clock: ManualClock
) -> None:
    """A replay of something the user really signed, but earlier."""
    old = record_for(ALICE_KEYS, clock, version=1, lease_ms=5 * MINUTE)
    clock.advance(MINUTE)
    new = record_for(ALICE_KEYS, clock, version=2)
    service.publish(new)
    assert service.publish(old) is PublishOutcome.REJECTED_STALE
    assert service.resolve(ALICE) == new


# ---------------- forgeries ----------------
def test_a_record_signed_by_someone_else_is_rejected(
    service: DirectoryService, clock: ManualClock
) -> None:
    """Mallory claims Alice's key but signs with her own."""
    forged = build_record(
        public_key=ALICE_KEYS.public_key,
        version=1,
        expires_at_ms=clock.now_ms() + MINUTE,
        sign=lambda data: crypto.sign(MALLORY_KEYS.private_key, data),
    )
    assert service.publish(forged) is PublishOutcome.REJECTED_INVALID_SIGNATURE
    assert service.resolve(ALICE) is None


def test_a_record_claiming_someone_elses_id_is_rejected(
    service: DirectoryService, clock: ManualClock
) -> None:
    """Mallory publishes her own key but under Alice's id."""
    own = record_for(MALLORY_KEYS, clock)
    claiming_alice = tweak(own, user_id=ALICE)
    assert service.publish(claiming_alice) is PublishOutcome.REJECTED_BAD_IDENTITY
    assert service.resolve(ALICE) is None


@pytest.mark.parametrize(
    "field,value", [("version", 99), ("payload", b"another-key-bytes")]
)
def test_altering_a_signed_record_invalidates_it(
    service: DirectoryService, clock: ManualClock, field: str, value: object
) -> None:
    record = record_for(ALICE_KEYS, clock)
    tampered = tweak(record, **{field: value})
    assert service.publish(tampered) in (
        PublishOutcome.REJECTED_INVALID_SIGNATURE,
        PublishOutcome.REJECTED_BAD_IDENTITY,
    )
    assert service.resolve(ALICE) is None


def test_a_captured_record_cannot_be_given_a_longer_presence(
    service: DirectoryService, clock: ManualClock
) -> None:
    """The expiry is covered by the signature, so it cannot be stretched."""
    record = record_for(ALICE_KEYS, clock, lease_ms=MINUTE)
    stretched = tweak(record, expires_at_ms=record.expires_at_ms + 5 * MINUTE)
    assert service.publish(stretched) is PublishOutcome.REJECTED_INVALID_SIGNATURE


def test_a_forgery_never_overwrites_a_valid_record(
    service: DirectoryService, clock: ManualClock
) -> None:
    genuine = record_for(ALICE_KEYS, clock, version=1)
    service.publish(genuine)
    forged = build_record(
        public_key=ALICE_KEYS.public_key,
        version=50,
        expires_at_ms=clock.now_ms() + MINUTE,
        sign=lambda data: crypto.sign(MALLORY_KEYS.private_key, data),
    )
    assert not service.publish(forged).accepted
    assert service.resolve(ALICE) == genuine


def test_a_malformed_record_is_rejected(
    service: DirectoryService, clock: ManualClock
) -> None:
    record = tweak(record_for(ALICE_KEYS, clock), version=0)
    assert service.publish(record) is PublishOutcome.REJECTED_MALFORMED


# ---------------- presence lease ----------------
def test_an_already_expired_record_is_rejected(
    service: DirectoryService, clock: ManualClock
) -> None:
    record = record_for(ALICE_KEYS, clock, lease_ms=MINUTE)
    clock.advance(MINUTE + 1)
    assert service.publish(record) is PublishOutcome.REJECTED_EXPIRED


def test_a_record_expiring_exactly_now_is_rejected(
    service: DirectoryService, clock: ManualClock
) -> None:
    record = record_for(ALICE_KEYS, clock, lease_ms=MINUTE)
    clock.advance(MINUTE)
    assert service.publish(record) is PublishOutcome.REJECTED_EXPIRED


def test_a_lease_longer_than_the_policy_is_rejected(
    service: DirectoryService, clock: ManualClock
) -> None:
    record = record_for(ALICE_KEYS, clock, lease_ms=DEFAULT_MAX_LEASE_MS + 1)
    assert service.publish(record) is PublishOutcome.REJECTED_LEASE_TOO_LONG


def test_a_lease_of_exactly_the_maximum_is_accepted(
    service: DirectoryService, clock: ManualClock
) -> None:
    record = record_for(ALICE_KEYS, clock, lease_ms=DEFAULT_MAX_LEASE_MS)
    assert service.publish(record) is PublishOutcome.STORED


def test_presence_is_visible_until_the_lease_ends_and_gone_exactly_at_it(
    service: DirectoryService, clock: ManualClock
) -> None:
    service.publish(record_for(ALICE_KEYS, clock, lease_ms=MINUTE))
    clock.advance(MINUTE - 1)
    assert service.resolve(ALICE) is not None  # 1 ms before the deadline
    clock.advance(1)
    assert service.resolve(ALICE) is None  # the user now looks offline


def test_renewing_before_expiry_keeps_the_user_present(
    service: DirectoryService, clock: ManualClock
) -> None:
    service.publish(record_for(ALICE_KEYS, clock, version=1, lease_ms=MINUTE))
    clock.advance(40_000)
    renewal = record_for(ALICE_KEYS, clock, version=2, lease_ms=MINUTE)
    assert service.publish(renewal) is PublishOutcome.STORED
    clock.advance(30_000)  # 70 s after the first publish: the old lease is over
    assert service.resolve(ALICE) == renewal


def test_an_old_record_replayed_after_it_expired_is_rejected(
    service: DirectoryService, clock: ManualClock
) -> None:
    old = record_for(ALICE_KEYS, clock, version=1, lease_ms=MINUTE)
    service.publish(old)
    clock.advance(10 * MINUTE)
    assert service.publish(old) is PublishOutcome.REJECTED_EXPIRED


def test_an_expired_record_is_hidden_even_while_still_stored(
    service: DirectoryService, store: InMemoryDirectoryStore, clock: ManualClock
) -> None:
    record = record_for(ALICE_KEYS, clock, lease_ms=MINUTE)
    service.publish(record)
    clock.advance(MINUTE)
    assert store.get(ALICE) == record  # still physically there
    assert service.resolve(ALICE) is None  # but never handed out


def test_an_older_record_cannot_come_back_after_the_newer_one_was_purged(
    service: DirectoryService, clock: ManualClock
) -> None:
    """Alice publishes v3 (10 min) and then v5 (1 min) and goes offline. An
    attacker who captured v3 replays it after the node purged the expired v5."""
    old = record_for(ALICE_KEYS, clock, version=3, lease_ms=10 * MINUTE)
    service.publish(old)
    service.publish(record_for(ALICE_KEYS, clock, version=5, lease_ms=MINUTE))
    clock.advance(2 * MINUTE)
    assert service.purge_expired() == 1
    assert service.publish(old) is PublishOutcome.REJECTED_STALE
    assert service.resolve(ALICE) is None  # Alice stays offline


def test_after_a_purge_a_newer_version_is_still_accepted(
    service: DirectoryService, clock: ManualClock
) -> None:
    service.publish(record_for(ALICE_KEYS, clock, version=5, lease_ms=MINUTE))
    clock.advance(2 * MINUTE)
    service.purge_expired()
    renewed = record_for(ALICE_KEYS, clock, version=6)
    assert service.publish(renewed) is PublishOutcome.STORED
    assert service.resolve(ALICE) == renewed


def test_purge_removes_only_expired_records_and_counts_them(
    service: DirectoryService, clock: ManualClock
) -> None:
    bob_keys = crypto.generate_key_pair()
    service.publish(record_for(ALICE_KEYS, clock, lease_ms=MINUTE))
    service.publish(record_for(bob_keys, clock, lease_ms=5 * MINUTE))
    clock.advance(2 * MINUTE)
    assert service.purge_expired() == 1
    assert service.purge_expired() == 0
    assert service.resolve(UserId.from_public_key(bob_keys.public_key)) is not None


def test_a_custom_maximum_lease_is_respected(
    store: InMemoryDirectoryStore, clock: ManualClock
) -> None:
    service = DirectoryService(
        store, crypto, clock, DirectoryPolicy(max_lease_ms=MINUTE)
    )
    assert (
        service.publish(record_for(ALICE_KEYS, clock, lease_ms=MINUTE))
        is PublishOutcome.STORED
    )
    bob = crypto.generate_key_pair()
    assert (
        service.publish(record_for(bob, clock, lease_ms=MINUTE + 1))
        is PublishOutcome.REJECTED_LEASE_TOO_LONG
    )


def test_policy_rejects_an_invalid_maximum_lease() -> None:
    with pytest.raises(ValueError, match="max_lease_ms"):
        DirectoryPolicy(max_lease_ms=0)


# ---------------- the check the client can reuse ----------------
def test_authenticity_check_results(clock: ManualClock) -> None:
    good = record_for(ALICE_KEYS, clock)
    assert check_record_authenticity(good, crypto) is RecordCheck.VALID
    assert (
        check_record_authenticity(tweak(good, version=0), crypto)
        is RecordCheck.MALFORMED
    )
    assert check_record_authenticity(
        tweak(good, user_id=UserId.from_public_key(b"x")), crypto
    ) is (RecordCheck.BAD_IDENTITY)
    assert (
        check_record_authenticity(tweak(good, version=9), crypto)
        is RecordCheck.INVALID_SIGNATURE
    )


# ---------------- property ----------------
@given(
    versions=st.lists(st.integers(min_value=1, max_value=50), min_size=1, max_size=15)
)
def test_whatever_the_arrival_order_the_highest_version_wins(
    versions: list[int],
) -> None:
    clock = ManualClock()
    service = DirectoryService(InMemoryDirectoryStore(), crypto, clock)
    for version in versions:
        service.publish(record_for(ALICE_KEYS, clock, version=version))
    resolved = service.resolve(ALICE)
    assert resolved is not None and resolved.version == max(versions)
