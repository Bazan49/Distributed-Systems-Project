"""Tests for the identity use cases: register and log in.

Most tests run on in-memory adapters (FakeCrypto, MemoryLocalStore,
ManualClock). The last section uses the real adapters (NaClCrypto with the
minimum Argon2id cost, SqliteLocalStore on a temporary file) to check:
the right password recovers the same identity after a restart, and the wrong one fails.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from nacl import pwhash

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.system.crypto import NaClCrypto
from source.adapters.system.sqlite.sqlite_local_store import SqliteLocalStore
from source.application.ports.errors import AlreadyExistsError, CorruptedStoreError
from source.application.use_cases.identity import (
    MAX_PASSWORD_LENGTH,
    MIN_PASSWORD_LENGTH,
    AlreadyRegisteredError,
    IdentityService,
    NotRegisteredError,
    UnlockedIdentity,
    WeakPasswordError,
    WrongPasswordError,
    check_password_policy,
)
from source.domain.identifiers import Alias, UserId
from source.domain.identity import LocalIdentity
from source.domain.records import KeyPair
from tests.support.builders import ALICE, BOB, make_identity

PASSWORD = "correct horse battery staple"


class CountingCrypto(FakeCrypto):
    """FakeCrypto that counts the expensive calls, to check they are avoided."""

    def __init__(self) -> None:
        super().__init__()
        self.generated = 0
        self.protected = 0

    def generate_key_pair(self) -> KeyPair:
        self.generated += 1
        return super().generate_key_pair()

    def protect_private_key(self, private_key: bytes, password: str) -> bytes:
        self.protected += 1
        return super().protect_private_key(private_key, password)


class RacingStore(MemoryLocalStore):
    """A store where another process registers between our check and our write."""

    def load_identity(self) -> LocalIdentity | None:
        return None

    def save_identity(self, identity: LocalIdentity) -> None:
        raise AlreadyExistsError("an identity is already stored")


@pytest.fixture
def crypto() -> CountingCrypto:
    return CountingCrypto()


@pytest.fixture
def store() -> MemoryLocalStore:
    return MemoryLocalStore()


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def service(
    crypto: CountingCrypto, store: MemoryLocalStore, clock: ManualClock
) -> IdentityService:
    return IdentityService(crypto, store, clock)


# ---------------- password policy ----------------


@pytest.mark.parametrize(
    "password",
    ["a" * MIN_PASSWORD_LENGTH, "a" * MAX_PASSWORD_LENGTH, "  spaces  ", "ñandú€😀xyz"],
)
def test_policy_accepts(password: str) -> None:
    check_password_policy(password)


@pytest.mark.parametrize(
    "password",
    [
        "",
        "a" * (MIN_PASSWORD_LENGTH - 1),
        "a" * (MAX_PASSWORD_LENGTH + 1),
        None,
        b"bytes-password",
    ],
)
def test_policy_rejects(password: object) -> None:
    with pytest.raises(WeakPasswordError):
        check_password_policy(password)


def test_weak_password_error_is_a_value_error() -> None:
    assert issubclass(WeakPasswordError, ValueError)


# ---------------- register ----------------


def test_register_returns_an_unlocked_session(
    service: IdentityService, clock: ManualClock
) -> None:
    session = service.register("Alice", PASSWORD)

    assert session.alias == Alias("Alice")
    assert session.user_id == UserId.from_public_key(session.public_key)
    assert session.identity.created_at_ms == clock.now_ms()


def test_register_persists_the_identity(
    service: IdentityService, store: MemoryLocalStore
) -> None:
    session = service.register("Alice", PASSWORD)

    assert store.load_identity() == session.identity
    assert service.is_registered()


def test_register_never_stores_the_private_key_in_the_clear(
    service: IdentityService, store: MemoryLocalStore
) -> None:
    session = service.register("Alice", PASSWORD)

    stored = store.load_identity()
    assert stored is not None
    assert session.private_key not in stored.protected_private_key


def test_registered_key_can_sign_and_decrypt(
    service: IdentityService, crypto: CountingCrypto
) -> None:
    session = service.register("Alice", PASSWORD)

    signature = crypto.sign(session.private_key, b"hi")
    assert crypto.verify(session.public_key, b"hi", signature)
    sealed = crypto.encrypt_for(session.public_key, b"secret")
    assert crypto.decrypt(session.private_key, sealed) == b"secret"


def test_two_registrations_give_different_identities(clock: ManualClock) -> None:
    a = IdentityService(FakeCrypto(), MemoryLocalStore(), clock).register(
        "Same", PASSWORD
    )
    b = IdentityService(FakeCrypto(), MemoryLocalStore(), clock).register(
        "Same", PASSWORD
    )

    # The alias is just a display name: identity comes from the key.
    assert a.user_id != b.user_id


def test_register_twice_fails_and_keeps_the_first_identity(
    service: IdentityService, store: MemoryLocalStore
) -> None:
    first = service.register("Alice", PASSWORD)

    with pytest.raises(AlreadyRegisteredError):
        service.register("Mallory", "another password")
    assert store.load_identity() == first.identity


def test_register_twice_does_not_derive_any_key(
    service: IdentityService, crypto: CountingCrypto
) -> None:
    service.register("Alice", PASSWORD)

    with pytest.raises(AlreadyRegisteredError):
        service.register("Alice", PASSWORD)
    assert crypto.generated == 1
    assert crypto.protected == 1


def test_register_race_is_reported_as_already_registered(
    crypto: CountingCrypto, clock: ManualClock
) -> None:
    service = IdentityService(crypto, RacingStore(), clock)

    with pytest.raises(AlreadyRegisteredError):
        service.register("Alice", PASSWORD)


@pytest.mark.parametrize("alias", ["", " Alice", "Alice ", "a" * 65, "tab\there"])
def test_register_rejects_invalid_alias_before_any_crypto(
    service: IdentityService, crypto: CountingCrypto, alias: str
) -> None:
    with pytest.raises(ValueError):
        service.register(alias, PASSWORD)
    assert crypto.generated == 0
    assert not service.is_registered()


def test_register_rejects_weak_password_before_any_crypto(
    service: IdentityService, crypto: CountingCrypto
) -> None:
    with pytest.raises(WeakPasswordError):
        service.register("Alice", "short")
    assert crypto.generated == 0
    assert not service.is_registered()


# ---------------- unlock (log in) ----------------


def test_is_registered_is_false_on_a_new_device(service: IdentityService) -> None:
    assert not service.is_registered()


def test_unlock_without_identity_fails(service: IdentityService) -> None:
    with pytest.raises(NotRegisteredError):
        service.unlock(PASSWORD)


def test_unlock_with_the_right_password_recovers_the_same_identity(
    service: IdentityService,
) -> None:
    registered = service.register("Alice", PASSWORD)

    unlocked = service.unlock(PASSWORD)

    assert unlocked == registered
    assert unlocked.private_key == registered.private_key


def test_unlock_with_a_wrong_password_fails(service: IdentityService) -> None:
    service.register("Alice", PASSWORD)

    with pytest.raises(WrongPasswordError):
        service.unlock(PASSWORD + "!")


def test_unlock_with_a_non_text_password_fails(service: IdentityService) -> None:
    service.register("Alice", PASSWORD)

    with pytest.raises(WrongPasswordError):
        service.unlock(None)


def test_unlock_does_not_apply_the_password_policy(
    store: MemoryLocalStore, clock: ManualClock
) -> None:
    # An identity protected with a password that today's policy would refuse
    # (e.g. created under an older, laxer policy) must stay reachable.
    crypto = FakeCrypto()
    store.save_identity(
        LocalIdentity(
            user_id=ALICE.user_id,
            public_key=ALICE.keys.public_key,
            alias=Alias("Alice"),
            protected_private_key=crypto.protect_private_key(
                ALICE.keys.private_key, "abc"
            ),
            created_at_ms=0,
        )
    )
    service = IdentityService(crypto, store, clock)

    assert service.unlock("abc").private_key == ALICE.keys.private_key


def test_unlock_detects_a_key_that_belongs_to_someone_else(
    store: MemoryLocalStore, clock: ManualClock
) -> None:
    # Alice's public data with Bob's private key behind it: the password is
    # right, but the stored identity is inconsistent.
    crypto = FakeCrypto()
    alice = make_identity(ALICE)
    store.save_identity(
        LocalIdentity(
            user_id=alice.user_id,
            public_key=alice.public_key,
            alias=alice.alias,
            protected_private_key=crypto.protect_private_key(
                BOB.keys.private_key, PASSWORD
            ),
            created_at_ms=alice.created_at_ms,
        )
    )
    service = IdentityService(crypto, store, clock)

    with pytest.raises(CorruptedStoreError):
        service.unlock(PASSWORD)


def test_unlock_detects_a_malformed_private_key(
    store: MemoryLocalStore, clock: ManualClock
) -> None:
    crypto = FakeCrypto()
    alice = make_identity(ALICE)
    store.save_identity(
        LocalIdentity(
            user_id=alice.user_id,
            public_key=alice.public_key,
            alias=alice.alias,
            protected_private_key=crypto.protect_private_key(b"not a key", PASSWORD),
            created_at_ms=alice.created_at_ms,
        )
    )
    service = IdentityService(crypto, store, clock)

    with pytest.raises(CorruptedStoreError):
        service.unlock(PASSWORD)


# ---------------- the session object ----------------


def test_session_repr_never_shows_the_private_key(service: IdentityService) -> None:
    session = service.register("Alice", PASSWORD)

    assert session.private_key.hex() not in repr(session)
    assert repr(session.private_key) not in repr(session)


def test_session_requires_a_local_identity() -> None:
    with pytest.raises(TypeError):
        UnlockedIdentity(identity="alice", private_key=b"k")


@pytest.mark.parametrize("private_key", [b"", "text", None])
def test_session_requires_a_private_key(private_key: object) -> None:
    with pytest.raises(ValueError):
        UnlockedIdentity(identity=make_identity(), private_key=private_key)


# ---------------- real adapters: survives a restart ----------------


def _real_service(db_path: Path) -> tuple[IdentityService, SqliteLocalStore]:
    crypto = NaClCrypto(
        opslimit=pwhash.argon2id.OPSLIMIT_MIN, memlimit=pwhash.argon2id.MEMLIMIT_MIN
    )
    store = SqliteLocalStore(db_path)
    return IdentityService(crypto, store, ManualClock()), store


def test_identity_survives_a_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "client.db"
    service, store = _real_service(db_path)
    registered = service.register("Alice", PASSWORD)
    store.close()

    # "Restart": brand-new objects over the same file.
    service, store = _real_service(db_path)
    with store:
        assert service.is_registered()
        with pytest.raises(WrongPasswordError):
            service.unlock("not the password")
        unlocked = service.unlock(PASSWORD)

    assert unlocked.user_id == registered.user_id
    assert unlocked.private_key == registered.private_key


def test_private_key_is_not_in_the_database_file(tmp_path: Path) -> None:
    db_path = tmp_path / "client.db"
    service, store = _real_service(db_path)
    session = service.register("Alice", PASSWORD)
    store.close()

    on_disk = b"".join(p.read_bytes() for p in tmp_path.iterdir() if p.is_file())
    assert session.private_key not in on_disk
    # Neither half of the composite key may leak on its own.
    half = len(session.private_key) // 2
    assert session.private_key[:half] not in on_disk
    assert session.private_key[half:] not in on_disk
