"""Contacts of the local user."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from source.domain.canonical import FormatError, is_int
from source.domain.identifiers import Alias, UserId


class ContactState(str, Enum):
    OUTGOING_REQUEST = "outgoing_request"  # we asked them; waiting for their acceptance
    INCOMING_REQUEST = "incoming_request"  # they asked us; sits in the requests inbox
    ACTIVE = "active"  # both sides accepted; we can chat


@dataclass(frozen=True)
class Contact:
    """Another user known to the local user.

    The identifier must be the hash of the public key: that is what makes a
    contact link self-verifying without any central authority.
    """

    user_id: UserId
    public_key: bytes
    alias: Alias
    state: ContactState
    created_at_ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.user_id, UserId) or not isinstance(self.alias, Alias):
            raise FormatError("invalid user_id or alias")
        if not isinstance(self.state, ContactState):
            raise FormatError("invalid state")
        if not isinstance(self.public_key, bytes) or not self.public_key:
            raise FormatError("public_key must be non-empty bytes")
        if UserId.from_public_key(self.public_key) != self.user_id:
            raise FormatError("user_id is not the hash of public_key")
        if not is_int(self.created_at_ms) or self.created_at_ms < 0:
            raise FormatError("created_at_ms must be an integer >= 0")
