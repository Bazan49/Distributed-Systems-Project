"""Plain data types that cross the port boundaries.

Immutable and without behaviour. Business rules (ordering, signature checks,
version comparison, status transitions) live in other domain modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar

from source.domain.identifiers import UserId
from source.domain.node_id import NodeId

T = TypeVar("T")


@dataclass(frozen=True)
class KeyPair:
    """An identity key pair. Raw bytes; the format is an adapter detail.

    Only the crypto adapter knows what is inside each key (sizes, composition);
    every other layer treats them as opaque. ``private_key`` is excluded from
    ``repr`` so that logging, a traceback or a failed assertion never prints it.
    """

    public_key: bytes
    private_key: bytes = field(repr=False)


@dataclass(frozen=True)
class DirectoryRecord:
    """A signed record published by a user (presence, current location)."""

    user_id: UserId
    version: int  # the higher version wins
    payload: bytes
    signature: bytes
    expires_at_ms: int  # end of the presence lease (wall clock, ms)


class PutOutcome(Enum):
    STORED = "stored"
    ALREADY_PRESENT = "already_present"  # idempotent repeat, still a success


class DirectSendResult(Enum):
    DELIVERED = "delivered"
    UNREACHABLE = "unreachable"  # timeout, peer offline, or any transport failure


@dataclass(frozen=True)
class NodeReply(Generic[T]):
    """The answer (or failure) of ONE service node to ONE call.

    A failed call is data, not an exception, so quorum logic can count replies
    without try/except around every node.
    """

    node: NodeId
    ok: bool
    value: T | None = None
    error: str | None = None
