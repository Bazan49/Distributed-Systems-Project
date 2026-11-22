"""Identity use cases: register a new identity and log in by unlocking it.

A device holds exactly one identity (see ``LocalStorePort.save_identity``).
The private key is never persisted in the clear: it is stored protected under
a key derived from the user's password (``CryptoPort.protect_private_key``),
so "logging in" means "unlocking the private key with the password". There is
no server-side account: nothing in this module talks to the network.

The unlocked private key exists only in memory, inside an ``UnlockedIdentity``
that the caller keeps for the session and hands to the other use cases (send,
receive, contacts) that need to sign or decrypt.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.errors import (
    AlreadyExistsError,
    CorruptedStoreError,
    DecryptionError,
    WrongPasswordError,
)
from source.application.ports.local_store import LocalStorePort
from source.domain.identifiers import Alias, UserId
from source.domain.identity import LocalIdentity

__all__ = [
    "MAX_PASSWORD_LENGTH",
    "MIN_PASSWORD_LENGTH",
    "AlreadyRegisteredError",
    "IdentityError",
    "IdentityService",
    "NotRegisteredError",
    "UnlockedIdentity",
    "WeakPasswordError",
    "WrongPasswordError",
    "check_password_policy",
]

MIN_PASSWORD_LENGTH = 8
# Upper bound only to refuse absurd input (e.g. a pasted file); Argon2id itself
# accepts any length.
MAX_PASSWORD_LENGTH = 1024

# Fixed bytes used to prove that a private key matches a public key. They are
# never sent anywhere: the proof is computed and checked locally.
_KEY_CHECK_CHALLENGE = b"mensajeria/identity/key-check/v1"


# errors


class IdentityError(Exception):
    """Base class of the errors raised by the identity use cases."""


class AlreadyRegisteredError(IdentityError):
    """This device already holds an identity; a device has exactly one."""


class NotRegisteredError(IdentityError):
    """There is no identity on this device yet: register first."""


class WeakPasswordError(IdentityError, ValueError):
    """The password does not satisfy the password policy."""


# ``WrongPasswordError`` (from the crypto port) is re-exported: it is what
# ``unlock`` raises for a wrong password, so callers can import every error of
# this use case from this module.


# session


@dataclass(frozen=True)
class UnlockedIdentity:
    """The local identity together with its private key, in memory only.

    This is the "logged in" state. It is never persisted: dropping it is
    logging out. ``private_key`` is excluded from ``repr`` so that logging, a
    traceback or a failed assertion never prints it.
    """

    identity: LocalIdentity
    private_key: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.identity, LocalIdentity):
            raise TypeError("identity must be a LocalIdentity")
        if not isinstance(self.private_key, bytes) or not self.private_key:
            raise ValueError("private_key must be non-empty bytes")

    @property
    def user_id(self) -> UserId:
        return self.identity.user_id

    @property
    def alias(self) -> Alias:
        return self.identity.alias

    @property
    def public_key(self) -> bytes:
        return self.identity.public_key


# use cases


def check_password_policy(password: str) -> None:
    """Fail unless ``password`` is acceptable for protecting a private key.

    The policy is deliberately simple: a length range, counted in characters.
    The password is never altered (no stripping, no case folding): what the
    user typed is exactly what protects the key.

    Raises:
        WeakPasswordError: not a string, too short or too long.
    """
    if not isinstance(password, str):
        raise WeakPasswordError("the password must be text")
    if len(password) < MIN_PASSWORD_LENGTH:
        raise WeakPasswordError(
            f"the password must have at least {MIN_PASSWORD_LENGTH} characters"
        )
    if len(password) > MAX_PASSWORD_LENGTH:
        raise WeakPasswordError(
            f"the password must have at most {MAX_PASSWORD_LENGTH} characters"
        )


class IdentityService:
    """Create the device's identity and unlock it ("register" and "log in")."""

    def __init__(
        self, crypto: CryptoPort, store: LocalStorePort, clock: ClockPort
    ) -> None:
        self._crypto = crypto
        self._store = store
        self._clock = clock

    def is_registered(self) -> bool:
        """True if this device already holds an identity."""
        return self._store.load_identity() is not None

    def register(self, alias: str, password: str) -> UnlockedIdentity:
        """Create a new identity, persist it protected by ``password`` and log in.

        The key pair is generated here, on the device; the ``UserId`` is the
        hash of the public key, so no one has to assign or approve it. The
        returned session is already unlocked: there is no need to call
        ``unlock`` right after registering.

        Raises:
            ValueError: ``alias`` is not a valid alias.
            WeakPasswordError: ``password`` does not satisfy the policy.
            AlreadyRegisteredError: this device already holds an identity.
        """
        valid_alias = Alias(alias)
        check_password_policy(password)
        if self.is_registered():
            raise AlreadyRegisteredError("this device already holds an identity")

        key_pair = self._crypto.generate_key_pair()
        identity = LocalIdentity(
            user_id=UserId.from_public_key(key_pair.public_key),
            public_key=key_pair.public_key,
            alias=valid_alias,
            protected_private_key=self._crypto.protect_private_key(
                key_pair.private_key, password
            ),
            created_at_ms=self._clock.now_ms(),
        )
        try:
            self._store.save_identity(identity)
        except AlreadyExistsError as exc:
            # Another process registered between our check and our write.
            raise AlreadyRegisteredError(
                "this device already holds an identity"
            ) from exc
        return UnlockedIdentity(identity=identity, private_key=key_pair.private_key)

    def unlock(self, password: str) -> UnlockedIdentity:
        """Log in: unlock the stored private key with ``password``.

        Raises:
            NotRegisteredError: this device holds no identity.
            WrongPasswordError: the password is wrong (or the protected key
                is damaged: the two cannot be told apart).
            CorruptedStoreError: the key unlocked but does not belong to the
                stored public key, so the stored identity is inconsistent.
        """
        identity = self._store.load_identity()
        if identity is None:
            raise NotRegisteredError("there is no identity on this device")
        if not isinstance(password, str):
            raise WrongPasswordError("wrong password")

        private_key = self._crypto.unlock_private_key(
            identity.protected_private_key, password
        )
        if not self._key_pair_matches(private_key, identity.public_key):
            raise CorruptedStoreError(
                "the stored private key does not match the stored public key"
            )
        return UnlockedIdentity(identity=identity, private_key=private_key)

    def _key_pair_matches(self, private_key: bytes, public_key: bytes) -> bool:
        """True if ``private_key`` is the private half of ``public_key``.

        Checked through the port, because only the crypto adapter knows the
        key layout.
        """
        try:
            signature = self._crypto.sign(private_key, _KEY_CHECK_CHALLENGE)
            if not self._crypto.verify(public_key, _KEY_CHECK_CHALLENGE, signature):
                return False
            sealed = self._crypto.encrypt_for(public_key, _KEY_CHECK_CHALLENGE)
            return self._crypto.decrypt(private_key, sealed) == _KEY_CHECK_CHALLENGE
        except (ValueError, DecryptionError):
            return False
