"""SQLite adapter for the directory store of one service node.

Survives process restarts. Implements the same contract as the in-memory version.

* Two tables. ``directory_records`` holds the current record of each user and
  is what ``delete_expired`` cleans. ``directory_versions`` holds the highest
  version ever stored per user and is NEVER cleaned: it is what stops an older
  record from coming back after a purge (see ``DirectoryStorePort``).
* ``put`` writes both tables in ONE transaction, so a crash can never leave a
  record stored without its version (or the other way round).
* Times are ``INTEGER`` milliseconds (``expires_at_ms``), like the rest of the
  system.
* Write-ahead logging and ``synchronous=FULL``: an accepted record is on disk.
* A Ray actor may run calls on more than one thread, so the single connection
  is opened with ``check_same_thread=False`` and every use is serialised by a
  lock. SQLite errors are translated into ``StoreError``.
* No policy here: signatures and the "higher version wins" rule belong to
  ``DirectoryService``. What is READ back is checked for shape, because SQLite
  does not enforce column types: a damaged row raises ``CorruptedStoreError``.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from source.application.ports.errors import CorruptedStoreError, StoreError
from source.domain.directory import validate_shape
from source.domain.identifiers import UserId
from source.domain.records import DirectoryRecord

_SCHEMA = """
CREATE TABLE IF NOT EXISTS directory_records (
    user_id        TEXT    PRIMARY KEY,
    version        INTEGER NOT NULL CHECK (version >= 0),
    payload        BLOB    NOT NULL,
    signature      BLOB    NOT NULL,
    expires_at_ms  INTEGER NOT NULL CHECK (expires_at_ms >= 0)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_directory_records_expires
    ON directory_records (expires_at_ms);
CREATE TABLE IF NOT EXISTS directory_versions (
    user_id          TEXT    PRIMARY KEY,
    highest_version  INTEGER NOT NULL CHECK (highest_version >= 0)
) WITHOUT ROWID;
"""


class SQLiteDirectoryStore:
    def __init__(self, db_path: str | Path, busy_timeout_ms: int = 5_000) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        try:
            self._conn = sqlite3.connect(
                self.db_path,
                timeout=busy_timeout_ms / 1000,
                isolation_level=None,  # explicit transactions only
                check_same_thread=False,
            )
            self._conn.execute("PRAGMA synchronous = FULL")
            mode = self._conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise StoreError(f"could not enable WAL (journal mode is {mode!r})")
            self._conn.executescript(_SCHEMA)
        except sqlite3.Error as exc:
            raise StoreError(f"cannot open the directory database: {exc}") from exc

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def get(self, user_id: UserId) -> DirectoryRecord | None:
        with self._lock:
            try:
                row = self._conn.execute(
                    "SELECT user_id, version, payload, signature, expires_at_ms"
                    " FROM directory_records WHERE user_id = ?",
                    (user_id.value,),
                ).fetchone()
            except sqlite3.Error as exc:
                raise StoreError(f"cannot read the directory: {exc}") from exc
        if row is None:
            return None
        try:
            record = DirectoryRecord(
                user_id=UserId(row[0]),
                version=row[1],
                payload=row[2],
                signature=row[3],
                expires_at_ms=row[4],
            )
            # SQLite does not enforce column types: never hand out a half-valid record.
            validate_shape(record)
        except ValueError as exc:
            raise CorruptedStoreError(f"stored record is invalid: {exc}") from exc
        return record

    def put(self, record: DirectoryRecord) -> None:
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    self._conn.execute(
                        "INSERT INTO directory_records"
                        " (user_id, version, payload, signature, expires_at_ms)"
                        " VALUES (?, ?, ?, ?, ?)"
                        " ON CONFLICT (user_id) DO UPDATE SET"
                        " version = excluded.version,"
                        " payload = excluded.payload,"
                        " signature = excluded.signature,"
                        " expires_at_ms = excluded.expires_at_ms",
                        (
                            record.user_id.value,
                            record.version,
                            record.payload,
                            record.signature,
                            record.expires_at_ms,
                        ),
                    )
                    self._conn.execute(
                        "INSERT INTO directory_versions (user_id, highest_version)"
                        " VALUES (?, ?)"
                        " ON CONFLICT (user_id) DO UPDATE SET"
                        " highest_version = max(highest_version, excluded.highest_version)",
                        (record.user_id.value, record.version),
                    )
                except BaseException:
                    self._conn.execute("ROLLBACK")
                    raise
                self._conn.execute("COMMIT")
            except sqlite3.Error as exc:
                raise StoreError(f"cannot store the record: {exc}") from exc

    def highest_version(self, user_id: UserId) -> int:
        with self._lock:
            try:
                row = self._conn.execute(
                    "SELECT highest_version FROM directory_versions WHERE user_id = ?",
                    (user_id.value,),
                ).fetchone()
            except sqlite3.Error as exc:
                raise StoreError(f"cannot read the directory: {exc}") from exc
        return 0 if row is None else int(row[0])

    def delete_expired(self, now_ms: int) -> int:
        with self._lock:
            try:
                cursor = self._conn.execute(
                    "DELETE FROM directory_records WHERE expires_at_ms <= ?",
                    (int(now_ms),),
                )
            except sqlite3.Error as exc:
                raise StoreError(f"cannot purge the directory: {exc}") from exc
        return cursor.rowcount
