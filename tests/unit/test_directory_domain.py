"""Tests for source.domain.directory (task 1B.5, domain part)."""

from dataclasses import replace
from typing import Any

import pytest

from source.adapters.memory.crypto import FakeCrypto
from source.domain.directory import (
    MAX_PUBLIC_KEY_BYTES,
    DirectoryRecordError,
    build_record,
    identity_matches,
    public_key_of,
    signing_bytes,
    validate_shape,
)
from source.domain.envelope import MAX_SIGNATURE_BYTES, Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.records import DirectoryRecord

NOW = 1_700_000_000_000
crypto = FakeCrypto()
KEYS = crypto.generate_key_pair()


def tweak(record: DirectoryRecord, **changes: Any) -> DirectoryRecord:
    """A copy of ``record`` with some fields changed (to build tampered records)."""
    return replace(record, **changes)


def make(**changes: Any) -> DirectoryRecord:
    record = build_record(
        public_key=KEYS.public_key,
        version=5,
        expires_at_ms=NOW + 60_000,
        sign=lambda data: crypto.sign(KEYS.private_key, data),
    )
    return tweak(record, **changes)


def test_build_record_derives_the_user_id_from_the_key() -> None:
    record = make()
    assert record.user_id == UserId.from_public_key(KEYS.public_key)
    assert identity_matches(record)
    assert public_key_of(record) == KEYS.public_key


def test_a_built_record_has_a_valid_signature() -> None:
    record = make()
    assert crypto.verify(KEYS.public_key, signing_bytes(record), record.signature)


def test_signing_bytes_are_deterministic() -> None:
    assert signing_bytes(make()) == signing_bytes(make())


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", 6),
        ("expires_at", NOW + 120_000),
        ("payload", b"another-key"),
        ("user_id", UserId.from_public_key(b"someone else")),
    ],
)
def test_every_signed_field_changes_the_signing_bytes(
    field: str, value: object
) -> None:
    assert signing_bytes(tweak(make(), **{field: value})) != signing_bytes(make())


def test_the_signature_itself_is_not_part_of_the_signed_bytes() -> None:
    assert signing_bytes(tweak(make(), signature=b"different")) == signing_bytes(make())


def test_directory_and_envelope_signatures_cannot_be_confused() -> None:
    """Domain separation: the two signing contexts start with different prefixes."""
    alice, bob = UserId.from_public_key(b"alice"), UserId.from_public_key(b"bob")
    envelope = Envelope.build(
        message_id=MessageId.new(),
        sender=alice,
        receiver=bob,
        ciphertext=b"x",
        sign=lambda _d: b"sig",
    )
    assert signing_bytes(make()).startswith(b"mensajeria/directory/v1\0")
    assert envelope.signing_bytes().startswith(b"mensajeria/envelope/v1\0")
    assert not signing_bytes(make()).startswith(b"mensajeria/envelope/")


def test_identity_does_not_match_when_the_id_is_someone_elses() -> None:
    assert not identity_matches(make(user_id=UserId.from_public_key(b"someone else")))


# ---------------- shape validation ----------------
def test_a_good_record_is_well_formed() -> None:
    validate_shape(make())


@pytest.mark.parametrize(
    "changes",
    [
        {"version": 0},
        {"version": -1},
        {"version": True},  # bool is not a real integer here
        {"version": 1.5},
        {"expires_at": -1},
        {"expires_at": 1.5},
        {"expires_at": True},
        {"payload": b""},
        {"payload": b"k" * (MAX_PUBLIC_KEY_BYTES + 1)},
        {"payload": "not bytes"},
        {"signature": b""},
        {"signature": b"s" * (MAX_SIGNATURE_BYTES + 1)},
        {"signature": "not bytes"},
        {"user_id": "a-plain-string"},
    ],
)
def test_malformed_records_are_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(DirectoryRecordError):
        validate_shape(make(**changes))


def test_signing_bytes_refuse_a_malformed_record() -> None:
    with pytest.raises(DirectoryRecordError):
        signing_bytes(make(version=0))


def test_the_largest_allowed_values_are_accepted() -> None:
    validate_shape(
        make(payload=b"k" * MAX_PUBLIC_KEY_BYTES, signature=b"s" * MAX_SIGNATURE_BYTES)
    )
