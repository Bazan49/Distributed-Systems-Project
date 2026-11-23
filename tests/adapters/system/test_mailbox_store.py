import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

import pytest

from source.adapters.system.mailbox_store import SQLiteMailboxStore
from source.application.ports import MailboxStorePort
from source.application.ports.errors import CorruptedStoreError, StoreError
from source.domain.records import PutOutcome
from tests.contracts.mailbox_store_contract import (
    BOB,
    NOW,
    MailboxStoreContract,
    make_envelope,
)


class TestSqliteMailboxStore(MailboxStoreContract):
    """Runs the exact same contract tests, but against the real SQLite adapter."""

    @pytest.fixture
    def store(self, tmp_path: Path) -> MailboxStorePort:
        db_file = tmp_path / "mailbox_test.db"
        return SQLiteMailboxStore(db_file)


def test_envelopes_survive_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "node.db"
    first = SQLiteMailboxStore(path)
    env = make_envelope()
    first.put(env, NOW + 100)
    first.close()
    second = SQLiteMailboxStore(path)
    assert second.fetch(BOB, 10, NOW) == [env]
    assert second.put(env, NOW + 999) is PutOutcome.ALREADY_PRESENT
    second.close()


def test_the_database_uses_wal_and_integer_milliseconds(tmp_path: Path) -> None:
    path = tmp_path / "node.db"
    store = SQLiteMailboxStore(path)
    store.put(make_envelope(), NOW + 100)
    with closing(sqlite3.connect(path)) as raw:
        assert raw.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert (
            raw.execute("SELECT typeof(expires_at_ms) FROM mailbox_v2").fetchone()[0]
            == "integer"
        )
    store.close()


def test_the_store_can_be_used_from_several_threads(tmp_path: Path) -> None:
    """A Ray actor may call the store from different threads."""
    store = SQLiteMailboxStore(tmp_path / "node.db")
    envelopes = [make_envelope() for _ in range(40)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(
            pool.map(lambda e: store.put(e, NOW + 100), envelopes + envelopes)
        )
    assert outcomes.count(PutOutcome.STORED) == 40
    assert len(store.fetch(BOB, 1_000, NOW)) == 40
    store.close()


def test_a_corrupted_envelope_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "node.db"
    store = SQLiteMailboxStore(path)
    store.put(make_envelope(), NOW + 100)
    with closing(sqlite3.connect(path)) as raw, raw:
        raw.execute("UPDATE mailbox_v2 SET envelope = x'00'")
    with pytest.raises(CorruptedStoreError):
        store.fetch(BOB, 10, NOW)
    store.close()


def test_an_unopenable_path_is_a_store_error(tmp_path: Path) -> None:
    (tmp_path / "dir.db").mkdir()
    with pytest.raises(StoreError):
        SQLiteMailboxStore(tmp_path / "dir.db")
