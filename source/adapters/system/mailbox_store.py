"""SQLite adapter for the mailbox store.

Survives process restarts. Implements the same contract as the in-memory version.
"""

import sqlite3
from pathlib import Path

from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.records import PutOutcome


class SQLiteMailboxStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._create_schema()

    def _create_schema(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mailbox (
                message_id TEXT PRIMARY KEY,
                recipient_id TEXT NOT NULL,
                envelope_data BLOB NOT NULL,
                expires_at_ms REAL NOT NULL
            )
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_mailbox_recipient ON mailbox(recipient_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_mailbox_expires ON mailbox(expires_at_ms)"
        )
        self._conn.commit()

    def put(self, envelope: Envelope, expires_at_ms: float) -> PutOutcome:
        cursor = self._conn.cursor()
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO mailbox (message_id, recipient_id, envelope_data, expires_at_ms)
                VALUES (?, ?, ?, ?)
            """,
                (
                    envelope.message_id.value,
                    envelope.receiver.value,
                    envelope.to_bytes(),
                    expires_at_ms,
                ),
            )
            self._conn.commit()

            if cursor.rowcount > 0:
                return PutOutcome.STORED
            return PutOutcome.ALREADY_PRESENT
        except sqlite3.Error:
            self._conn.rollback()
            raise

    def fetch(self, recipient: UserId, limit: int, now_ms: float) -> list[Envelope]:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT envelope_data FROM mailbox
            WHERE recipient_id = ? AND expires_at_ms > ?
            ORDER BY expires_at_ms ASC
            LIMIT ?
        """,
            (recipient.value, now_ms, limit),
        )

        return [Envelope.from_bytes(row["envelope_data"]) for row in cursor.fetchall()]

    def delete(self, recipient: UserId, message_ids: list[MessageId]) -> int:
        if not message_ids:
            return 0

        cursor = self._conn.cursor()

        query = "DELETE FROM mailbox WHERE recipient_id = ? AND message_id = ?"

        params = [(recipient.value, m.value) for m in message_ids]

        cursor.executemany(query, params)
        self._conn.commit()

        return cursor.rowcount

    def purge_expired(self, now_ms: float) -> int:
        cursor = self._conn.cursor()
        cursor.execute("DELETE FROM mailbox WHERE expires_at_ms <= ?", (now_ms,))
        self._conn.commit()
        return cursor.rowcount
