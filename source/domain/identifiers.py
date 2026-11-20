"""Domain identifiers: UserId, MessageId, ConversationId and Alias.

Immutable value objects that validate their format on creation, so no other
layer ever handles "any string" as an identifier. Standard library only.
"""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass

_HEX_64 = re.compile(r"[0-9a-f]{64}")
_HEX_32 = re.compile(r"[0-9a-f]{32}")

MAX_ALIAS_LENGTH = 64


@dataclass(frozen=True, order=True)
class UserId:
    """A user identity: SHA-256 of the user's public key (64 hex characters).

    There is no central account registry: anyone holding the public key can
    recompute the identifier and check that it matches.
    """

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not _HEX_64.fullmatch(self.value):
            raise ValueError("UserId must be 64 lowercase hexadecimal characters")

    @classmethod
    def from_public_key(cls, public_key: bytes) -> UserId:
        if not isinstance(public_key, bytes) or not public_key:
            raise ValueError("the public key must be non-empty bytes")
        return cls(hashlib.sha256(public_key).hexdigest())

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, order=True)
class MessageId:
    """Unique message identifier: 128 random bits (32 hex characters).

    It is the key of duplicate suppression: a message keeps the same MessageId
    across every retry and over every delivery path. It is random on purpose so
    it reveals nothing about content, order or sender.
    """

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not _HEX_32.fullmatch(self.value):
            raise ValueError("MessageId must be 32 lowercase hexadecimal characters")

    @classmethod
    def new(cls) -> MessageId:
        return cls(secrets.token_hex(16))

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, order=True)
class ConversationId:
    """Identifier of the conversation between two users (64 hex characters).

    Derived from both UserIds in canonical order, so A->B and B->A produce the
    same value and nobody needs to coordinate to obtain it.
    """

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not _HEX_64.fullmatch(self.value):
            raise ValueError(
                "ConversationId must be 64 lowercase hexadecimal characters"
            )

    @classmethod
    def between(cls, a: UserId, b: UserId) -> ConversationId:
        if a == b:
            raise ValueError("a conversation needs two distinct users")
        first, second = sorted((a.value, b.value))
        digest = hashlib.sha256(f"conversation/v1\0{first}\0{second}".encode("ascii"))
        return cls(digest.hexdigest())

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class Alias:
    """A user-chosen display name. It is NOT unique and NOT an identity.

    1 to 64 printable characters, with no leading or trailing whitespace.
    """

    value: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.value, str)
            or not (1 <= len(self.value) <= MAX_ALIAS_LENGTH)
            or self.value != self.value.strip()
            or not self.value.isprintable()
        ):
            raise ValueError(
                "Alias must be 1-64 printable characters without surrounding spaces"
            )

    def __str__(self) -> str:
        return self.value
