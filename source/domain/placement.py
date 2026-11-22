"""Placement by affinity: which service nodes hold a user's data.

Every user's mailbox and directory record live on ``REPLICAS`` nodes (3 by
default). This module answers, for a user and the list of known nodes: which
nodes are those, in order of preference (the first one is the "primary").

Algorithm: rendezvous hashing (also called highest-random-weight). Each
(user, node) pair gets a pseudo-random score from SHA-256. The nodes with the
highest scores win. Properties we rely on:

* Deterministic: the same inputs always give the same answer, on any machine.
* Independent of the order of the input list.
* Minimal disruption: if a node disappears, only the users that had it among
  their preferred nodes change, and each of them just replaces that node with
  the next best one. Nobody else moves.

Pure standard library; no I/O, no Ray.
"""

from __future__ import annotations

import hashlib
from collections.abc import Collection

from source.domain.identifiers import UserId
from source.domain.node_id import NodeId

REPLICAS = 3

# Domain separation: a placement score can never be confused with another hash
# of the same two values made for a different purpose. The trailing NUL byte
# separates the fields unambiguously.
_PLACEMENT_DOMAIN = b"placement/v1\0"


def _score(user: UserId, node: NodeId) -> bytes:
    payload = (
        _PLACEMENT_DOMAIN
        + user.value.encode("ascii")
        + b"\0"
        + node.value.encode("ascii")
    )
    return hashlib.sha256(payload).digest()


def rank_nodes(user: UserId, nodes: Collection[NodeId]) -> list[NodeId]:
    """All ``nodes`` ordered from most to least preferred for ``user``.

    Duplicates in ``nodes`` are ignored. Ties (practically impossible with a
    256-bit hash) are broken by node id so the result is always deterministic.
    """
    unique = set(nodes)
    return sorted(
        unique, key=lambda node: (_score(user, node), node.value), reverse=True
    )


def preferred_nodes(
    user: UserId, nodes: Collection[NodeId], count: int = REPLICAS
) -> list[NodeId]:
    """The ``count`` nodes that should hold ``user``'s data, best first.

    If there are fewer than ``count`` distinct nodes, all of them are returned.

    Raises:
        ValueError: ``count`` is smaller than 1.
    """
    if count < 1:
        raise ValueError("count must be at least 1")
    return rank_nodes(user, nodes)[:count]
