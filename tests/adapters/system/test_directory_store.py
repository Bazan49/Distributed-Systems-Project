import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

import pytest

from source.adapters.system.directory_store import SQLiteDirectoryStore
from source.application.ports import DirectoryStorePort
from source.application.ports.errors import CorruptedStoreError, StoreError
from source.domain.identifiers import UserId
from tests.contracts.directory_store_contract import (
    ALICE,
    NOW,
    DirectoryStoreContract,
    make_record,
)


class TestSqliteDirectoryStore(DirectoryStoreContract):
    """Runs the exact same contract tests, but against the real SQLite adapter."""

    @pytest.fixture
    def store(self, tmp_path: Path) -> DirectoryStorePort:
        return SQLiteDirectoryStore(tmp_path / "directory.db")


def test_records_and_versions_survive_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "node.db"
    first = SQLiteDirectoryStore(path)
    first.put(make_record(version=4))
    first.put(make_record(user=UserId.from_public_key(b"x"), expires_at_ms=NOW - 1))
    first.delete_expired(NOW)
    first.close()
    second = SQLiteDirectoryStore(path)
    assert second.get(ALICE) == make_record(version=4)
    assert second.highest_version(UserId.from_public_key(b"x")) == 1  # purged, kept
    second.close()


def test_a_killed_process_loses_no_accepted_record(tmp_path: Path) -> None:
    """The writer dies with os._exit (no close, no cleanup); nothing is lost."""
    path = tmp_path / "node.db"
    script = (
        "import os, sys\n"
        "from source.adapters.system.directory_store import SQLiteDirectoryStore\n"
        "from tests.contracts.directory_store_contract import make_record\n"
        "store = SQLiteDirectoryStore(sys.argv[1])\n"
        "store.put(make_record(version=9))\n"
        "os._exit(9)\n"
    )
    result = subprocess.run(  # noqa: S603 - our own interpreter and script
        [sys.executable, "-c", script, str(path)], check=False
    )
    assert result.returncode == 9
    store = SQLiteDirectoryStore(path)
    assert store.get(ALICE) == make_record(version=9)
    assert store.highest_version(ALICE) == 9
    store.close()


def test_put_writes_both_tables_or_neither(tmp_path: Path) -> None:
    """If the version cannot be written, the record is not stored either."""
    path = tmp_path / "node.db"
    store = SQLiteDirectoryStore(path)
    store.put(make_record(version=1))
    with closing(sqlite3.connect(path)) as raw, raw:
        raw.execute(
            "CREATE TRIGGER fail BEFORE UPDATE ON directory_versions"
            " BEGIN SELECT RAISE(ABORT, 'simulated failure'); END"
        )
    with pytest.raises(StoreError):
        store.put(make_record(version=2))
    assert store.get(ALICE) == make_record(version=1)
    assert store.highest_version(ALICE) == 1
    store.close()


def test_the_database_uses_wal_and_integer_milliseconds(tmp_path: Path) -> None:
    path = tmp_path / "node.db"
    store = SQLiteDirectoryStore(path)
    store.put(make_record())
    with closing(sqlite3.connect(path)) as raw:
        assert raw.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        kind = raw.execute(
            "SELECT typeof(expires_at_ms) FROM directory_records"
        ).fetchone()[0]
        assert kind == "integer"
    store.close()


def test_the_store_can_be_used_from_several_threads(tmp_path: Path) -> None:
    """A Ray actor may call the store from different threads."""
    store = SQLiteDirectoryStore(tmp_path / "node.db")
    versions = list(range(1, 81))
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda v: store.put(make_record(version=v)), versions))
    assert store.highest_version(ALICE) == 80
    store.close()


@pytest.mark.parametrize(
    "damage",
    [
        "version = 'abc'",  # SQLite accepts text in an INTEGER column
        "signature = 'not bytes'",
        "payload = x''",
    ],
)
def test_a_damaged_record_is_reported_not_returned(tmp_path: Path, damage: str) -> None:
    path = tmp_path / "node.db"
    store = SQLiteDirectoryStore(path)
    store.put(make_record())
    with closing(sqlite3.connect(path)) as raw, raw:
        raw.execute(f"UPDATE directory_records SET {damage}")  # noqa: S608 - test literal
    with pytest.raises(CorruptedStoreError):
        store.get(ALICE)
    store.close()


def test_an_unopenable_path_is_a_store_error(tmp_path: Path) -> None:
    (tmp_path / "dir.db").mkdir()
    with pytest.raises(StoreError):
        SQLiteDirectoryStore(tmp_path / "dir.db")


def test_a_closed_store_raises_a_store_error(tmp_path: Path) -> None:
    store = SQLiteDirectoryStore(tmp_path / "node.db")
    store.close()
    with pytest.raises(StoreError):
        store.get(ALICE)
