"""
Outgoing message status.

These are LOCAL state of the sender (they live in its send queue), not
traveling in the envelope. The receiver needs no state machine: a received
message is stored exactly once in its history.

    QUEUED ──► ACCEPTED ──► DELIVERED
       │           │
       └───────────┴──────► EXPIRED ──► DELIVERED   
"""

from __future__ import annotations
from enum import Enum

class OutgoingStatus(str, Enum):
    QUEUED = "queued"        # stored in local queue; no delivery guarantee yet
    ACCEPTED = "accepted"    # durable: 2 of 3 mailboxes persisted it to disk
    DELIVERED = "delivered"  # receiver stored it in history and acknowledged
    EXPIRED = "expired"      # stopped trying (deadline or retries exhausted)


_ALLOWED: dict[OutgoingStatus, frozenset[OutgoingStatus]] = {
    OutgoingStatus.QUEUED: frozenset(
        {OutgoingStatus.ACCEPTED, OutgoingStatus.DELIVERED, OutgoingStatus.EXPIRED}
    ),
    OutgoingStatus.ACCEPTED: frozenset(
        {OutgoingStatus.DELIVERED, OutgoingStatus.EXPIRED}
    ),
    # If the sender gave up but the receiver confirms later, the message did
    # arrive: the real confirmation prevails over our "expired".
    OutgoingStatus.EXPIRED: frozenset({OutgoingStatus.DELIVERED}),
    OutgoingStatus.DELIVERED: frozenset(),
}


def can_transition(current: OutgoingStatus, target: OutgoingStatus) -> bool:
    """Is the transition legal?"""
    return target in _ALLOWED[current]


def advance(current: OutgoingStatus, observed: OutgoingStatus) -> OutgoingStatus:
    """Apply an observed event tolerantly.

    Network events arrive out of order or repeated: a late "accepted" when
    already delivered, or the same event twice, must not fail or roll back
    the state. If the transition is legal, advance; if not, keep current.
    """
    return observed if can_transition(current, observed) else current
