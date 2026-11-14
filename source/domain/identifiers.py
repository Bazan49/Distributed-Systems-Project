"""Domain identifiers: UserId, MessageId, and ConversationId.

These are immutable value objects that validate their format on creation,
so no other layer handles "arbitrary strings" as identifiers.
"""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass

_HEX_64 = re.compile(r"[0-9a-f]{64}")
_HEX_32 = re.compile(r"[0-9a-f]{32}")


@dataclass(frozen=True, order=True)
class UserId:
    """A user's identity: SHA-256 of their public key (64 hex chars).

    There is no central account registry: anyone with the public key can
    recompute the identifier and verify that it matches.
    """

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not _HEX_64.fullmatch(self.value):
            raise ValueError("UserId must be 64 lowercase hexadecimal characters")

    @classmethod
    def from_public_key(cls, public_key: bytes) -> UserId:
        if not isinstance(public_key, bytes) or not public_key:
            raise ValueError("public key must be a non-empty bytes object")
        return cls(hashlib.sha256(public_key).hexdigest())

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, order=True)
class MessageId:
    """Unique message identifier: 128 random bits (32 hex chars).

    It is the key for duplicate suppression: the same message keeps the same
    MessageId across all retries and over any delivery path. It is random on
    purpose: it reveals no content, order, or sender.
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
    """Identifier for the conversation between two users (64 hex chars).

    It is derived from the two UserIds in canonical order, so A->B and B->A
    yield the same value and no coordination is needed to compute it.
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
            raise ValueError("a conversation requires two distinct users")
        first, second = sorted((a.value, b.value))
        digest = hashlib.sha256(f"conversation/v1\0{first}\0{second}".encode("ascii"))
        return cls(digest.hexdigest())

    def __str__(self) -> str:
        return self.value
