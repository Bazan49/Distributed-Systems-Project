import dataclasses
from typing import Any

import pytest

from source.domain.canonical import FormatError
from source.domain.identifiers import Alias, UserId
from source.domain.identity import LocalIdentity

PUBLIC_KEY = b"alice-public-key"


def make_identity(**over: Any) -> LocalIdentity:
    args: dict[str, Any] = {
        "user_id": UserId.from_public_key(PUBLIC_KEY),
        "public_key": PUBLIC_KEY,
        "alias": Alias("Alice"),
        "protected_private_key": b"protected-blob",
        "created_at_ms": 1_700_000_000_000,
    }
    args.update(over)
    return LocalIdentity(**args)


def test_a_valid_identity_is_created() -> None:
    identity = make_identity()
    assert identity.alias == Alias("Alice")
    assert identity.user_id == UserId.from_public_key(PUBLIC_KEY)


def test_the_identity_is_immutable() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        make_identity().created_at_ms = 5  # type: ignore[misc]


def test_the_protected_key_is_hidden_from_repr() -> None:
    assert "protected-blob" not in repr(make_identity())


def test_the_user_id_must_be_the_hash_of_the_public_key() -> None:
    with pytest.raises(FormatError):
        make_identity(user_id=UserId.from_public_key(b"someone-else"))


@pytest.mark.parametrize("user_id", ["a" * 64, None, 5])
def test_rejects_a_user_id_of_the_wrong_type(user_id: Any) -> None:
    with pytest.raises(FormatError):
        make_identity(user_id=user_id)


@pytest.mark.parametrize("alias", ["Alice", None, 5])
def test_rejects_an_alias_of_the_wrong_type(alias: Any) -> None:
    with pytest.raises(FormatError):
        make_identity(alias=alias)


@pytest.mark.parametrize("key", [b"", "text", None])
def test_rejects_an_invalid_public_key(key: Any) -> None:
    with pytest.raises(FormatError):
        make_identity(public_key=key)


@pytest.mark.parametrize("blob", [b"", "text", None])
def test_rejects_an_invalid_protected_private_key(blob: Any) -> None:
    with pytest.raises(FormatError):
        make_identity(protected_private_key=blob)


@pytest.mark.parametrize("created", [-1, True, 1.5, "0", None])
def test_rejects_an_invalid_creation_time(created: Any) -> None:
    with pytest.raises(FormatError):
        make_identity(created_at_ms=created)
