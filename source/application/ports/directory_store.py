from typing import Protocol, runtime_checkable

from source.domain.identifiers import UserId
from source.domain.records import DirectoryRecord


@runtime_checkable
class DirectoryStorePort(Protocol):
    """The directory records stored by ONE service node.

    Phase 1 adapter: SQLite. Persistence only: signature validation and the
    "higher version wins" rule are domain logic, applied BEFORE calling ``put``.

    The store remembers the highest version ever stored for each user, even
    after ``delete_expired`` removed the record. Without that memory, a node
    that purged Alice's v5 would accept a replay of her older v3 whose lease
    has not ended yet.
    """

    def get(self, user_id: UserId) -> DirectoryRecord | None:
        """The stored record of ``user_id`` (even if expired), or None."""
        ...

    def put(self, record: DirectoryRecord) -> None:
        """Insert or overwrite the record of ``record.user_id``."""
        ...

    def highest_version(self, user_id: UserId) -> int:
        """Highest ``version`` ever stored for ``user_id``, or 0 if none.

        Never decreases: not when a record is deleted by ``delete_expired``,
        and not when ``put`` stores a lower version.
        """
        ...

    def delete_expired(self, now_ms: int) -> int:
        """Delete records with ``expires_at_ms <= now_ms`` and return how many.

        The highest version of each user is kept (see ``highest_version``).
        """
        ...
