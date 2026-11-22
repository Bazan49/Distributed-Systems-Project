"""Tests of SqliteLocalStore: the shared contract plus what is specific to SQLite."""

import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from source.adapters.system.sqlite.schema import APPLICATION_ID, SCHEMA_VERSION
from source.adapters.system.sqlite.sqlite_local_store import SqliteLocalStore
from source.application.ports.errors import (
    AlreadyExistsError,
    CorruptedStoreError,
    StoreError,
)
from source.application.ports.local_store import LocalStorePort
from tests.contracts.local_store_contract import LocalStoreContract
from tests.support.builders import (
    BOB,
    make_contact,
    make_outgoing,
    make_received,
)


class TestSqliteLocalStoreContract(LocalStoreContract):
    _opened: list[SqliteLocalStore]

    @pytest.fixture
    def store(self, tmp_path: Path, request: pytest.FixtureRequest) -> LocalStorePort:
        self._opened = []
        request.addfinalizer(self._close_all)
        return self._open(tmp_path / "user.db")

    def reopen(self, store: LocalStorePort) -> LocalStorePort:
        """Simulate a process restart: close the connection, open the same file."""
        assert isinstance(store, SqliteLocalStore)
        store.close()
        return self._open(store.path)

    def _open(self, path: Path) -> SqliteLocalStore:
        opened = SqliteLocalStore(path)
        self._opened.append(opened)
        return opened

    def _close_all(self) -> None:
        for opened in self._opened:
            opened.close()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "user.db"


def _raw(path: Path) -> closing[sqlite3.Connection]:
    """A second, independent connection to the same file (autocommit)."""
    return closing(sqlite3.connect(path, isolation_level=None))


# ------------------------------------------------------------------- basics


def test_it_satisfies_the_port(db_path: Path) -> None:
    """``SqliteLocalStore`` implements every method of ``LocalStorePort``."""
    with SqliteLocalStore(db_path) as store:
        assert isinstance(store, LocalStorePort)


def test_the_folder_of_the_database_is_created_if_missing(tmp_path: Path) -> None:
    """The composition root can pass a path in a folder that does not exist yet."""
    path = tmp_path / "does" / "not" / "exist" / "user.db"
    with SqliteLocalStore(path):
        assert path.exists()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes")
def test_the_database_file_is_private_to_the_owner(db_path: Path) -> None:
    """The file is created with mode 0o600: no access for group or others."""
    with SqliteLocalStore(db_path):
        assert db_path.stat().st_mode & 0o077 == 0


def test_a_closed_store_refuses_every_call_and_close_is_idempotent(
    db_path: Path,
) -> None:
    """After ``close`` every operation raises ``StoreError``; closing twice is fine."""
    store = SqliteLocalStore(db_path)
    store.close()
    store.close()
    with pytest.raises(StoreError):
        store.load_identity()
    with pytest.raises(StoreError):
        store.save_contact(make_contact(BOB))


# ------------------------------------------------- durability configuration


def test_the_database_is_configured_for_durability_and_integrity(db_path: Path) -> None:
    """WAL journal, synchronous=FULL and foreign keys ON are in force."""
    with SqliteLocalStore(db_path) as store:
        conn = store._connection()
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


# ------------------------------------------------ identification, versioning


def test_a_new_database_is_stamped_with_application_id_and_version(
    db_path: Path,
) -> None:
    """The file identifies itself so other programs and versions can recognise it."""
    SqliteLocalStore(db_path).close()
    with _raw(db_path) as raw:
        assert raw.execute("PRAGMA application_id").fetchone()[0] == APPLICATION_ID
        assert raw.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_a_sqlite_file_that_is_not_a_message_store_is_refused_untouched(
    tmp_path: Path,
) -> None:
    """A foreign SQLite file is rejected, and not even switched to WAL."""
    path = tmp_path / "other.db"
    with _raw(path) as raw:
        raw.execute("CREATE TABLE notes (text TEXT)")
    with pytest.raises(StoreError):
        SqliteLocalStore(path)
    with _raw(path) as raw:
        assert raw.execute("PRAGMA journal_mode").fetchone()[0] != "wal"


def test_a_database_from_a_newer_version_is_refused(db_path: Path) -> None:
    """Downgrades are not supported: a newer schema version is rejected."""
    SqliteLocalStore(db_path).close()
    with _raw(db_path) as raw:
        raw.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    with pytest.raises(StoreError):
        SqliteLocalStore(db_path)


# ------------------------------------------------------ design decisions


def test_a_seen_id_outlives_its_history_row(db_path: Path) -> None:
    """Pruning the history must not allow a replay: the id stays seen and the
    same message is still rejected as a duplicate."""
    message = make_received()
    with SqliteLocalStore(db_path) as store:
        assert store.save_incoming_message(message) is True
        with _raw(db_path) as raw:
            raw.execute("DELETE FROM received_messages")
        assert store.list_received(message.conversation_id) == []
        assert store.has_received(message.message_id) is True
        assert store.save_incoming_message(message) is False


def test_the_seq_counter_never_decreases_even_if_the_outbox_is_pruned(
    db_path: Path,
) -> None:
    """``last_outgoing_seq`` is independent of the queued rows."""
    with SqliteLocalStore(db_path) as store:
        message = make_outgoing(seq=5)
        store.enqueue_outgoing(message)
        with _raw(db_path) as raw:
            raw.execute("DELETE FROM outgoing_messages")
        assert store.last_outgoing_seq(message.conversation_id) == 5
        store.enqueue_outgoing(make_outgoing(seq=6))
        assert store.last_outgoing_seq(message.conversation_id) == 6


def test_a_rejected_enqueue_does_not_move_the_seq_counter(db_path: Path) -> None:
    """A conflicting enqueue rolls back completely, including the counter."""
    with SqliteLocalStore(db_path) as store:
        first = make_outgoing(seq=1)
        store.enqueue_outgoing(first)
        clash = make_outgoing(seq=9, message_id=first.message_id)
        with pytest.raises(AlreadyExistsError):
            store.enqueue_outgoing(clash)
        assert store.last_outgoing_seq(first.conversation_id) == 1


def test_a_corrupted_row_is_reported_and_never_returned(db_path: Path) -> None:
    """A row that violates a domain invariant raises ``CorruptedStoreError``."""
    with SqliteLocalStore(db_path) as store:
        store.save_contact(make_contact(BOB))
        with _raw(db_path) as raw:
            raw.execute("UPDATE contacts SET state = 'not-a-state'")
        with pytest.raises(CorruptedStoreError):
            store.get_contact(BOB.user_id)
        with pytest.raises(CorruptedStoreError):
            store.list_contacts()


def test_a_database_locked_by_another_writer_raises_a_store_error(
    db_path: Path,
) -> None:
    """If another process holds the write lock, the write fails with ``StoreError``
    after the busy timeout and nothing is written; it works once the lock is gone."""
    with (
        SqliteLocalStore(db_path, busy_timeout_ms=50) as store,
        _raw(db_path) as blocker,
    ):
        blocker.execute("BEGIN IMMEDIATE")
        with pytest.raises(StoreError):
            store.save_contact(make_contact(BOB))
        blocker.execute("ROLLBACK")
        assert store.list_contacts() == []
        store.save_contact(make_contact(BOB))
        assert len(store.list_contacts()) == 1


# ---------------------------------------------------------------- atomicity


def test_a_failure_between_the_two_writes_of_an_incoming_message_leaves_neither(
    db_path: Path,
) -> None:
    """The seen id is written first and the message second. A failure injected
    by a trigger on the second insert must roll the first one back too."""
    message = make_received()
    with SqliteLocalStore(db_path) as store, _raw(db_path) as raw:
        raw.execute(
            """
            CREATE TRIGGER inject_failure BEFORE INSERT ON received_messages
            BEGIN SELECT RAISE(ABORT, 'injected failure'); END
            """
        )
        with pytest.raises(StoreError):
            store.save_incoming_message(message)
        raw.execute("DROP TRIGGER inject_failure")

        assert store.has_received(message.message_id) is False
        assert store.list_received(message.conversation_id) == []
        assert store.save_incoming_message(message) is True
