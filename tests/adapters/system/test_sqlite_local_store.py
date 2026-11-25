"""Tests of SqliteLocalStore: the shared contract plus what is specific to SQLite."""

import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from source.adapters.system.sqlite.schema import (
    _MIGRATIONS,
    APPLICATION_ID,
    SCHEMA_VERSION,
)
from source.adapters.system.sqlite.sqlite_local_store import SqliteLocalStore
from source.application.ports.errors import (
    AlreadyExistsError,
    CorruptedStoreError,
    StoreError,
)
from source.application.ports.local_store import LocalStorePort
from source.domain.messages import InboundState, ReceivedMessage
from source.domain.ordering import Pending, StreamSnapshot
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


def _inbound_state(
    next_seq: int, pending: list[ReceivedMessage] | None = None
) -> InboundState:
    return StreamSnapshot(
        next_seq,
        tuple(Pending(m.seq, m.message_id, m) for m in pending or []),
        frozenset(),
    )


@pytest.mark.parametrize("failing_table", ["received_messages", "inbound_streams"])
def test_a_failure_inside_commit_inbound_leaves_nothing(
    db_path: Path, failing_table: str
) -> None:
    """``commit_inbound`` writes the history first and the stream state last. A
    failure at either end must roll back everything: no seen id, no history,
    the previous state intact."""
    buffered = make_received(sender=BOB, seq=2)
    first = make_received(sender=BOB, seq=1)
    conversation = first.conversation_id
    before = _inbound_state(1, [buffered])
    with SqliteLocalStore(db_path) as store, _raw(db_path) as raw:
        store.commit_inbound(conversation, BOB.user_id, [], before)
        raw.execute(
            f"""
            CREATE TRIGGER inject_failure BEFORE UPDATE ON {failing_table}
            BEGIN SELECT RAISE(ABORT, 'injected failure'); END
            """
        )
        raw.execute(
            f"""
            CREATE TRIGGER inject_failure_insert BEFORE INSERT ON {failing_table}
            BEGIN SELECT RAISE(ABORT, 'injected failure'); END
            """
        )
        with pytest.raises(StoreError):
            store.commit_inbound(
                conversation, BOB.user_id, [first, buffered], _inbound_state(3)
            )
        raw.execute("DROP TRIGGER inject_failure")
        raw.execute("DROP TRIGGER inject_failure_insert")

        assert store.has_received(first.message_id) is False
        assert store.list_received(conversation) == []
        assert store.load_inbound_stream(conversation, BOB.user_id) == before


@pytest.mark.parametrize(
    "corruption",
    [
        "UPDATE inbound_streams SET skipped = 'not json'",
        "UPDATE inbound_streams SET skipped = '{\"a\": 1}'",
        "UPDATE inbound_streams SET skipped = '[5]'",  # not behind next_seq
        "UPDATE inbound_pending SET kind = 'not-a-kind'",
    ],
)
def test_a_corrupted_inbound_state_is_reported(db_path: Path, corruption: str) -> None:
    """A stored stream state that is not a valid snapshot raises
    ``CorruptedStoreError``, on load and on the next commit."""
    buffered = make_received(sender=BOB, seq=2)
    conversation = buffered.conversation_id
    with SqliteLocalStore(db_path) as store:
        store.commit_inbound(
            conversation, BOB.user_id, [], _inbound_state(1, [buffered])
        )
        with _raw(db_path) as raw:
            raw.execute(corruption)
        with pytest.raises(CorruptedStoreError):
            store.load_inbound_stream(conversation, BOB.user_id)
        if "inbound_pending" in corruption:
            with pytest.raises(CorruptedStoreError):
                store.commit_inbound(
                    conversation, BOB.user_id, [], _inbound_state(1, [buffered])
                )


def test_deleting_a_stream_row_deletes_its_buffer(db_path: Path) -> None:
    """The buffer cannot outlive its stream (foreign key with cascade)."""
    buffered = make_received(sender=BOB, seq=2)
    with SqliteLocalStore(db_path) as store:
        store.commit_inbound(
            buffered.conversation_id, BOB.user_id, [], _inbound_state(1, [buffered])
        )
    with _raw(db_path) as raw:
        raw.execute("PRAGMA foreign_keys = ON")
        raw.execute("DELETE FROM inbound_streams")
        assert raw.execute("SELECT count(*) FROM inbound_pending").fetchone()[0] == 0


# ---------------------------------------------------------------- migration


def test_a_version_1_database_is_upgraded_in_place(db_path: Path) -> None:
    """A store written by the previous version (no inbound tables) is upgraded
    when opened: its data is kept and the new tables work."""
    message = make_received()
    with _raw(db_path) as raw:
        for statement in _MIGRATIONS[0]:
            raw.execute(statement)
        raw.execute(f"PRAGMA application_id = {APPLICATION_ID}")
        raw.execute("PRAGMA user_version = 1")
        raw.execute(
            "INSERT INTO seen_message_ids (message_id) VALUES (?)",
            (message.message_id.value,),
        )
        raw.execute(
            """
            INSERT INTO received_messages
                (message_id, conversation_id, sender, seq, kind, body,
                 sent_at_ms, received_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message.message_id.value,
                message.conversation_id.value,
                message.sender.value,
                message.seq,
                message.kind.value,
                message.body,
                message.sent_at_ms,
                message.received_at_ms,
            ),
        )

    with SqliteLocalStore(db_path) as store:
        assert store.list_received(message.conversation_id) == [message]
        assert store.load_inbound_stream(message.conversation_id, BOB.user_id) is None
        store.commit_inbound(
            message.conversation_id, BOB.user_id, [], _inbound_state(2)
        )
        assert store.load_inbound_stream(
            message.conversation_id, BOB.user_id
        ) == _inbound_state(2)

    with _raw(db_path) as raw:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_a_version_2_database_gets_lamport_timestamps(db_path: Path) -> None:
    """Messages stored before Lamport timestamps existed get ``lamport = seq``
    (valid, and growing with each sender's messages), in the history, the
    outbox and the ordering buffer; and ``max_lamport`` uses its indexes."""
    stored = make_received(seq=3)
    buffered = make_received(seq=5)
    sent = make_outgoing(seq=2)
    conversation = stored.conversation_id
    with _raw(db_path) as raw:
        for version in (0, 1):
            for statement in _MIGRATIONS[version]:
                raw.execute(statement)
        raw.execute(f"PRAGMA application_id = {APPLICATION_ID}")
        raw.execute("PRAGMA user_version = 2")
        raw.execute(
            "INSERT INTO seen_message_ids (message_id) VALUES (?)",
            (stored.message_id.value,),
        )
        old_received = (
            "conversation_id, sender, seq, message_id, kind, body, "
            "sent_at_ms, received_at_ms"
        )
        for table, message in (
            ("received_messages", stored),
            ("inbound_pending", buffered),
        ):
            if table == "inbound_pending":
                raw.execute(
                    "INSERT INTO inbound_streams VALUES (?, ?, 4, '[]')",
                    (conversation.value, BOB.user_id.value),
                )
            raw.execute(
                f"INSERT INTO {table} ({old_received}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",  # noqa: S608 - constant table names
                (
                    conversation.value,
                    message.sender.value,
                    message.seq,
                    message.message_id.value,
                    message.kind.value,
                    message.body,
                    message.sent_at_ms,
                    message.received_at_ms,
                ),
            )
        raw.execute(
            """
            INSERT INTO outgoing_messages
                (message_id, conversation_id, seq, kind, body, sent_at_ms,
                 envelope, status, attempts, next_attempt_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sent.message_id.value,
                conversation.value,
                sent.seq,
                sent.kind.value,
                sent.body,
                sent.sent_at_ms,
                sent.envelope.to_bytes(),
                sent.status.value,
                sent.attempts,
                sent.next_attempt_at_ms,
            ),
        )

    with SqliteLocalStore(db_path) as store:
        assert [m.lamport for m in store.list_received(conversation)] == [3]
        outgoing = store.get_outgoing(sent.message_id)
        assert outgoing is not None and outgoing.lamport == 2
        state = store.load_inbound_stream(conversation, BOB.user_id)
        assert state is not None and state.pending[0].item.lamport == 5
        assert store.max_lamport(conversation) == 3  # buffered messages do not count

    with _raw(db_path) as raw:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        plan = " ".join(
            str(row)
            for row in raw.execute(
                "EXPLAIN QUERY PLAN SELECT max(lamport) FROM received_messages "
                "WHERE conversation_id = ?",
                (conversation.value,),
            )
        )
        assert "idx_received_lamport" in plan


def test_a_database_error_while_loading_a_stream_is_a_store_error(
    db_path: Path,
) -> None:
    """SQLite errors inside the read transaction surface as ``StoreError``."""
    with SqliteLocalStore(db_path) as store:
        store.commit_inbound(
            make_received().conversation_id, BOB.user_id, [], _inbound_state(1)
        )
        with _raw(db_path) as raw:
            raw.execute("DROP TABLE inbound_pending")
        with pytest.raises(StoreError):
            store.load_inbound_stream(make_received().conversation_id, BOB.user_id)
