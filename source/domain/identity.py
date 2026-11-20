"""The local user's own identity, as persisted on the device."""

from __future__ import annotations

from dataclasses import dataclass, field

from source.domain.canonical import FormatError, is_int
from source.domain.identifiers import Alias, UserId


@dataclass(frozen=True)
class LocalIdentity:
    """Identity of the user of this device.

    The private key is never stored in the clear: ``protected_private_key`` is
    the private key encrypted under a key derived from the user's password.
    The user must provide their password to unlock the private key.
    Unlocking it is what "logging in" means.
    """

    user_id: UserId
    public_key: bytes
    alias: Alias
    protected_private_key: bytes = field(repr=False)
    created_at_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.user_id, UserId) or not isinstance(self.alias, Alias):
            raise FormatError("invalid user_id or alias")
        if not isinstance(self.public_key, bytes) or not self.public_key:
            raise FormatError("public_key must be non-empty bytes")
        if UserId.from_public_key(self.public_key) != self.user_id:
            raise FormatError("user_id is not the hash of public_key")
        if (
            not isinstance(self.protected_private_key, bytes)
            or not self.protected_private_key
        ):
            raise FormatError("protected_private_key must be non-empty bytes")
        if not is_int(self.created_at_ms) or self.created_at_ms < 0:
            raise FormatError("created_at_ms must be an integer >= 0")
