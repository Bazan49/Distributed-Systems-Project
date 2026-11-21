"""Unit tests for the domain identifiers."""

import pytest

from source.domain.identifiers import ConversationId, MessageId, UserId

PK_A = b"clave-publica-de-a"
PK_B = b"clave-publica-de-b"


def test_user_id_is_deterministic_hash_of_public_key() -> None:
    assert UserId.from_public_key(PK_A) == UserId.from_public_key(PK_A)
    assert UserId.from_public_key(PK_A) != UserId.from_public_key(PK_B)
    assert len(UserId.from_public_key(PK_A).value) == 64


@pytest.mark.parametrize(
    "bad",
    ["", "abc", "G" * 64, "A" * 64, "a" * 63, "a" * 65, 123, None],
)
def test_user_id_rejects_invalid_formats(bad: object) -> None:
    with pytest.raises(ValueError):
        UserId(bad)


@pytest.mark.parametrize("bad", [b"", "texto", None])
def test_user_id_rejects_invalid_public_key(bad: object) -> None:
    with pytest.raises(ValueError):
        UserId.from_public_key(bad)


def test_message_id_new_has_format_and_does_not_repeat() -> None:
    ids = {MessageId.new().value for _ in range(500)}
    assert len(ids) == 500
    assert all(len(i) == 32 for i in ids)


@pytest.mark.parametrize(
    "bad",
    ["", "a" * 31, "a" * 33, "Z" * 32, "A" * 32, 5, None],
)
def test_message_id_rejects_invalid_formats(bad: object) -> None:
    with pytest.raises(ValueError):
        MessageId(bad)


def test_conversation_id_is_symmetric() -> None:
    a = UserId.from_public_key(PK_A)
    b = UserId.from_public_key(PK_B)
    assert ConversationId.between(a, b) == ConversationId.between(b, a)


def test_conversation_id_distinguishes_different_pairs() -> None:
    a = UserId.from_public_key(PK_A)
    b = UserId.from_public_key(PK_B)
    c = UserId.from_public_key(b"clave-publica-de-c")
    assert ConversationId.between(a, b) != ConversationId.between(a, c)


def test_conversation_id_requires_distinct_users() -> None:
    a = UserId.from_public_key(PK_A)
    with pytest.raises(ValueError):
        ConversationId.between(a, a)


def test_identifiers_are_immutable_and_hashable() -> None:
    a = UserId.from_public_key(PK_A)
    with pytest.raises(AttributeError):
        a.value = "x" * 64  
    assert {a: 1}[UserId.from_public_key(PK_A)] == 1
