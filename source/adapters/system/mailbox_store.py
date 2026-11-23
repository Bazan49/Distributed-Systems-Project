"""SQLite adapter for the mailbox store of one service node.

Survives process restarts. Implements the same contract as the in-memory version.

* Times are ``INTEGER`` milliseconds (``expires_at_ms``), like the rest of the
  system: never REAL, never seconds.
* Write-ahead logging and ``synchronous=FULL``: a deposit acknowledged to the
  client is on disk, and readers do not block the writer.
* A Ray actor may run calls on more than one thread, so the single connection
  is opened with ``check_same_thread=False`` and every use is serialised by a
  lock. SQLite errors are translated into ``StoreError``.
* "Oldest first" means order of arrival (``arrival`` autoincrement), not order
  of expiry.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from source.application.ports.errors import CorruptedStoreError, StoreError
from source.domain.canonical import FormatError
from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.records import PutOutcome

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mailbox_v2 (
    arrival        INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id     TEXT    NOT NULL UNIQUE,
    receiver_id    TEXT    NOT NULL,
    envelope       BLOB    NOT NULL,
    expires_at_ms  INTEGER NOT NULL CHECK (expires_at_ms >= 0)
);
CREATE INDEX IF NOT EXISTS idx_mailbox_v2_receiver
    ON mailbox_v2 (receiver_id, arrival);
CREATE INDEX IF NOT EXISTS idx_mailbox_v2_expires
    ON mailbox_v2 (expires_at_ms);
"""


class SQLiteMailboxStore:
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
            raise StoreError(f"cannot open the mailbox database: {exc}") from exc

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def put(self, envelope: Envelope, expires_at_ms: int) -> PutOutcome:
        with self._lock:
            try:
                cursor = self._conn.execute(
                    "INSERT OR IGNORE INTO mailbox_v2"
                    " (message_id, receiver_id, envelope, expires_at_ms)"
                    " VALUES (?, ?, ?, ?)",
                    (
                        envelope.message_id.value,
                        envelope.receiver.value,
                        envelope.to_bytes(),
                        int(expires_at_ms),
                    ),
                )
            except sqlite3.Error as exc:
                raise StoreError(f"cannot store the envelope: {exc}") from exc
        if cursor.rowcount > 0:
            return PutOutcome.STORED
        return PutOutcome.ALREADY_PRESENT

    def fetch(self, receiver: UserId, limit: int, now_ms: int) -> list[Envelope]:
        with self._lock:
            try:
                rows = self._conn.execute(
                    "SELECT envelope FROM mailbox_v2"
                    " WHERE receiver_id = ? AND expires_at_ms > ?"
                    " ORDER BY arrival LIMIT ?",
                    (receiver.value, int(now_ms), limit),
                ).fetchall()
            except sqlite3.Error as exc:
                raise StoreError(f"cannot read the mailbox: {exc}") from exc
        try:
            return [Envelope.from_bytes(row[0]) for row in rows]
        except FormatError as exc:
            raise CorruptedStoreError(f"stored envelope is invalid: {exc}") from exc

    def delete(self, receiver: UserId, message_ids: list[MessageId]) -> int:
        if not message_ids:
            return 0
        unique = list(dict.fromkeys(m.value for m in message_ids))
        # Only "?" placeholders are interpolated; every value is a bound parameter.
        placeholders = ",".join("?" * len(unique))
        query = (
            "DELETE FROM mailbox_v2 WHERE receiver_id = ?"  # noqa: S608
            f" AND message_id IN ({placeholders})"
        )
        with self._lock:
            try:
                cursor = self._conn.execute(query, (receiver.value, *unique))
            except sqlite3.Error as exc:
                raise StoreError(f"mailbox delete failed: {exc}") from exc
        return cursor.rowcount

    def purge_expired(self, now_ms: int) -> int:
        with self._lock:
            try:
                cursor = self._conn.execute(
                    "DELETE FROM mailbox_v2 WHERE expires_at_ms <= ?", (int(now_ms),)
                )
            except sqlite3.Error as exc:
                raise StoreError(f"cannot purge the mailbox: {exc}") from exc
        return cursor.rowcount
