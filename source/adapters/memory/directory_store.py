from source.domain.identifiers import UserId
from source.domain.records import DirectoryRecord


class InMemoryDirectoryStore:
    """Dict-based DirectoryStorePort for tests. Persistence only, no validation."""

    def __init__(self) -> None:
        self._records: dict[UserId, DirectoryRecord] = {}

    def get(self, user_id: UserId) -> DirectoryRecord | None:
        return self._records.get(user_id)

    def put(self, record: DirectoryRecord) -> None:
        self._records[record.user_id] = record

    def delete_expired(self, now_ms: int) -> int:
        expired = [
            uid for uid, rec in self._records.items() if rec.expires_at_ms <= now_ms
        ]
        for uid in expired:
            del self._records[uid]
        return len(expired)
