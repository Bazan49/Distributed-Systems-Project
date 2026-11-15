from typing import Protocol, runtime_checkable

from source.domain.identifiers import UserId
from source.domain.records import DirectoryRecord


@runtime_checkable
class DirectoryStorePort(Protocol):
    """The directory records stored by ONE service node.

    Phase 1 adapter: SQLite. Persistence only: signature validation and the
    "higher version wins" rule are domain logic, applied BEFORE calling ``put``.
    """

    def get(self, user_id: UserId) -> DirectoryRecord | None:
        """The stored record of ``user_id`` (even if expired), or None."""
        ...

    def put(self, record: DirectoryRecord) -> None:
        """Insert or overwrite the record of ``record.user_id``."""
        ...

    def delete_expired(self, now: float) -> int:
        """Delete records expired at ``now`` and return how many."""
        ...
