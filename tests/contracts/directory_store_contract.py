"""Contract every DirectoryStorePort implementation must satisfy."""

from source.application.ports import DirectoryStorePort
from source.domain.identifiers import UserId
from source.domain.records import DirectoryRecord

ALICE = UserId.from_public_key(b"alice")
BOB = UserId.from_public_key(b"bob")
NOW = 1_000.0


def make_record(
    user: UserId = ALICE, version: int = 1, expires_at: float = NOW + 100
) -> DirectoryRecord:
    return DirectoryRecord(
        user_id=user,
        version=version,
        payload=b"presence",
        signature=b"sig",
        expires_at=expires_at,
    )


class DirectoryStoreContract:
    """Subclass it and override the ``store`` fixture."""

    def test_get_unknown_user_returns_none(self, store: DirectoryStorePort) -> None:
        assert store.get(ALICE) is None

    def test_put_then_get(self, store: DirectoryStorePort) -> None:
        record = make_record()
        store.put(record)
        assert store.get(ALICE) == record

    def test_put_overwrites_the_previous_record(
        self, store: DirectoryStorePort
    ) -> None:
        store.put(make_record(version=1))
        newer = make_record(version=2)
        store.put(newer)
        assert store.get(ALICE) == newer

    def test_records_are_per_user(self, store: DirectoryStorePort) -> None:
        store.put(make_record(user=ALICE))
        assert store.get(BOB) is None

    def test_get_returns_expired_records_until_deleted(
        self, store: DirectoryStorePort
    ) -> None:
        # the store does NOT filter by time: expiry policy is domain logic
        expired = make_record(expires_at=NOW - 1)
        store.put(expired)
        assert store.get(ALICE) == expired

    def test_delete_expired_counts_and_removes_only_expired(
        self, store: DirectoryStorePort
    ) -> None:
        store.put(make_record(user=ALICE, expires_at=NOW - 1))
        store.put(make_record(user=BOB, expires_at=NOW + 100))
        assert store.delete_expired(NOW) == 1
        assert store.get(ALICE) is None
        assert store.get(BOB) is not None
        assert store.delete_expired(NOW) == 0

    def test_a_record_expiring_exactly_now_is_expired(
        self, store: DirectoryStorePort
    ) -> None:
        store.put(make_record(expires_at=NOW))
        assert store.delete_expired(NOW) == 1
