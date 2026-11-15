"""Identifier of a service node (mailbox / directory node)."""

from __future__ import annotations

import re
from dataclasses import dataclass

_NODE_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")


@dataclass(frozen=True, order=True)
class NodeId:
    """Name of a service node, e.g. ``node-1``. Lowercase letters, digits and dashes."""

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not _NODE_ID.fullmatch(self.value):
            raise ValueError("NodeId must match [a-z0-9][a-z0-9-]{0,62}")

    def __str__(self) -> str:
        return self.value
