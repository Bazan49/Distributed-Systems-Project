"""Quorum rules of the client: which nodes to ask and how to combine answers.

Pure standard library: no clock, no I/O, no Ray. The use case
(``source.application.use_cases.quorum``) calls the nodes and feeds the
answers here.

The rules (from the design):

* WRITE (deposit an envelope, publish a record): send to the user's
  ``REPLICAS`` (3) nodes and call it accepted when ``write_quorum`` (2) of
  them confirm. A node known to be DEAD is skipped and the next node of the
  user's ranking takes its place, so a dead node does not cost a copy.
* READ (collect, resolve): ask every live node that may hold the data and
  merge. Two clients can see a different set of dead nodes, so a writer may
  have used a replacement node; the reader asks both the user's normal nodes
  and the replacements it would use itself.
* Envelopes from several nodes are merged without duplicates.
* Of several directory records, the highest version wins.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Sequence

from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.node_id import NodeId
from source.domain.placement import REPLICAS, preferred_nodes, rank_nodes
from source.domain.records import DirectoryRecord, NodeReply

DEFAULT_WRITE_QUORUM = 2


def write_targets(
    user: UserId,
    known: Collection[NodeId],
    active: Collection[NodeId],
    replicas: int = REPLICAS,
) -> list[NodeId]:
    """The ``replicas`` best nodes for ``user`` among the ``active`` ones.

    ``known`` is every node of the cluster; ``active`` the ones not DEAD. A
    dead node is replaced by the next node in the user's ranking. Returns
    fewer nodes (maybe none) if there are not enough active nodes.
    """
    usable = set(active)
    return preferred_nodes(user, [n for n in known if n in usable], replicas)


def read_targets(
    user: UserId,
    known: Collection[NodeId],
    active: Collection[NodeId],
    replicas: int = REPLICAS,
) -> list[NodeId]:
    """Every active node that may hold ``user``'s data, best first.

    That is the user's normal nodes (as if nobody were dead) plus the
    replacements this client would write to. A writer that thought a node was
    dead used a replacement; a writer that did not used the normal node.
    Either way the copy is in this list, unless the node is DEAD now.
    """
    usable = set(active)
    wanted = set(preferred_nodes(user, known, replicas)) | set(
        write_targets(user, known, active, replicas)
    )
    return [n for n in rank_nodes(user, known) if n in wanted and n in usable]


def confirmations(replies: Iterable[NodeReply[object]]) -> list[NodeId]:
    """The nodes that answered OK, in reply order, without repetitions."""
    return list(dict.fromkeys(r.node for r in replies if r.ok))


def quorum_reached(replies: Iterable[NodeReply[object]], required: int) -> bool:
    """Did at least ``required`` distinct nodes answer OK?"""
    return len(confirmations(replies)) >= required


def merge_envelopes(
    replies: Sequence[NodeReply[list[Envelope]]],
) -> list[Envelope]:
    """Join what every node returned, dropping exact duplicates.

    Keeps the order in which envelopes were first seen. Two DIFFERENT envelopes
    with the same ``message_id`` are both kept: one of them is forged (a node
    may lie), and only the receiver can tell which one by checking signatures.
    Dropping by id here could let a lying node hide the real envelope.
    """
    merged: dict[Envelope, None] = {}
    for reply in replies:
        if reply.ok and reply.value is not None:
            for envelope in reply.value:
                merged.setdefault(envelope, None)
    return list(merged)


def latest_record(records: Iterable[DirectoryRecord]) -> DirectoryRecord | None:
    """The record with the highest version, or None if there is none.

    Ties (two different records with the same version, which only their owner
    can produce) are broken by the signature bytes, so every client picks the
    same one.
    """
    return max(records, key=lambda r: (r.version, r.signature), default=None)
