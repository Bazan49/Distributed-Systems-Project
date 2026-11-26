"""Schema, versioning and identification of the local store database file.

Versioning
    The schema version lives in ``PRAGMA user_version``. A fresh database is
    version 0 and is brought to ``SCHEMA_VERSION`` by running, in order, the
    migrations registered in ``_MIGRATIONS`` (``_MIGRATIONS[n]`` upgrades
    version ``n`` to ``n + 1``). The first migration is the creation of the
    whole schema. Every upgrade runs inside ONE transaction together with the
    version bump, so a crash can never leave a half-migrated database.

Identification
    ``PRAGMA application_id`` is stamped with ``APPLICATION_ID`` so that this
    adapter refuses to open (and therefore to modify) a SQLite file that
    belongs to something else, and refuses files written by a NEWER version
    of the program (downgrades are not supported).

Portability
    Only features of SQLite 3.24 or later are used (``ON CONFLICT DO NOTHING``
    and upserts). ``STRICT`` tables are deliberately NOT used because they
    need 3.37; the domain objects already validate every type before it
    reaches the database, and the CHECK constraints below are a second line
    of defence for numeric ranges.
"""

from __future__ import annotations

import contextlib
import sqlite3

from source.application.ports.errors import StoreError

SCHEMA_VERSION = 3
APPLICATION_ID = 0x4D53474C  # "MSGL"

_V1: tuple[str, ...] = (
    # One row at most: the device has exactly one identity.
    """
    CREATE TABLE identity (
        id                    INTEGER PRIMARY KEY CHECK (id = 1),
        user_id               TEXT    NOT NULL,
        public_key            BLOB    NOT NULL,
        alias                 TEXT    NOT NULL,
        protected_private_key BLOB    NOT NULL,
        created_at_ms         INTEGER NOT NULL CHECK (created_at_ms >= 0)
    )
    """,
    """
    CREATE TABLE contacts (
        user_id       TEXT    PRIMARY KEY,
        public_key    BLOB    NOT NULL,
        alias         TEXT    NOT NULL,
        state         TEXT    NOT NULL,
        created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0)
    )
    """,
    # Every message id ever accepted. Deliberately separate from the history so
    # that deleting history can never re-enable a replay.
    """
    CREATE TABLE seen_message_ids (
        message_id TEXT PRIMARY KEY
    ) WITHOUT ROWID
    """,
    # ``position`` is the storage order of the history. AUTOINCREMENT
    # guarantees it is never reused, even after deletions.
    """
    CREATE TABLE received_messages (
        position        INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id      TEXT    NOT NULL UNIQUE
                                REFERENCES seen_message_ids (message_id),
        conversation_id TEXT    NOT NULL,
        sender          TEXT    NOT NULL,
        seq             INTEGER NOT NULL CHECK (seq >= 1),
        kind            TEXT    NOT NULL,
        body            TEXT    NOT NULL,
        sent_at_ms      INTEGER NOT NULL CHECK (sent_at_ms >= 0),
        received_at_ms  INTEGER NOT NULL CHECK (received_at_ms >= 0)
    )
    """,
    """
    CREATE INDEX idx_received_by_conversation
        ON received_messages (conversation_id, position)
    """,
    # ``envelope`` holds the exact signed bytes; the other columns repeat
    # fields of the message only so they can be queried and sorted.
    """
    CREATE TABLE outgoing_messages (
        message_id         TEXT    PRIMARY KEY,
        conversation_id    TEXT    NOT NULL,
        seq                INTEGER NOT NULL CHECK (seq >= 1),
        kind               TEXT    NOT NULL,
        body               TEXT    NOT NULL,
        sent_at_ms         INTEGER NOT NULL CHECK (sent_at_ms >= 0),
        envelope           BLOB    NOT NULL,
        status             TEXT    NOT NULL,
        attempts           INTEGER NOT NULL CHECK (attempts >= 0),
        next_attempt_at_ms INTEGER CHECK (
            next_attempt_at_ms IS NULL OR next_attempt_at_ms >= 0
        ),
        UNIQUE (conversation_id, seq)
    )
    """,
    # Serves ``list_due_outgoing``: filter by status, then already in the
    # order the query asks for.
    """
    CREATE INDEX idx_outgoing_due
        ON outgoing_messages (status, sent_at_ms, conversation_id, seq)
    """,
    # High-water mark of the seq numbers used per conversation. Kept apart from
    # the outbox so it can never decrease, even if old messages are pruned.
    """
    CREATE TABLE outgoing_counters (
        conversation_id TEXT    PRIMARY KEY,
        last_seq        INTEGER NOT NULL CHECK (last_seq >= 0)
    ) WITHOUT ROWID
    """,
)

# Version 2: persistent ordering state of incoming streams (one per
# conversation and sender), committed together with the history it releases.
_V2: tuple[str, ...] = (
    # ``skipped`` is a JSON array of the seqs given up on that may still
    # arrive late. It is bounded by the ordering policy's late window, so
    # rewriting it whole on every commit is cheap.
    """
    CREATE TABLE inbound_streams (
        conversation_id TEXT    NOT NULL,
        sender          TEXT    NOT NULL,
        next_seq        INTEGER NOT NULL CHECK (next_seq >= 1),
        skipped         TEXT    NOT NULL,
        PRIMARY KEY (conversation_id, sender)
    ) WITHOUT ROWID
    """,
    # Messages waiting behind a gap, already decrypted (the history keeps
    # plaintext too). Same columns as ``received_messages`` so both are read
    # the same way. Deleting a stream deletes its buffer.
    """
    CREATE TABLE inbound_pending (
        conversation_id TEXT    NOT NULL,
        sender          TEXT    NOT NULL,
        seq             INTEGER NOT NULL CHECK (seq >= 1),
        message_id      TEXT    NOT NULL,
        kind            TEXT    NOT NULL,
        body            TEXT    NOT NULL,
        sent_at_ms      INTEGER NOT NULL CHECK (sent_at_ms >= 0),
        received_at_ms  INTEGER NOT NULL CHECK (received_at_ms >= 0),
        PRIMARY KEY (conversation_id, sender, seq),
        UNIQUE (conversation_id, sender, message_id),
        FOREIGN KEY (conversation_id, sender)
            REFERENCES inbound_streams (conversation_id, sender)
            ON DELETE CASCADE
    ) WITHOUT ROWID
    """,
)

# Version 3: the Lamport timestamp of every message, and the indexes that make
# ``max_lamport`` read one index entry per table instead of scanning. Messages
# stored before have none; ``seq`` takes its place, which is a valid timestamp
# (>= 1) and grows with each sender's messages.
_V3: tuple[str, ...] = (
    "ALTER TABLE received_messages ADD COLUMN lamport INTEGER NOT NULL DEFAULT 0",
    "UPDATE received_messages SET lamport = seq",
    "ALTER TABLE outgoing_messages ADD COLUMN lamport INTEGER NOT NULL DEFAULT 0",
    "UPDATE outgoing_messages SET lamport = seq",
    "ALTER TABLE inbound_pending ADD COLUMN lamport INTEGER NOT NULL DEFAULT 0",
    "UPDATE inbound_pending SET lamport = seq",
    """
    CREATE INDEX idx_received_lamport
        ON received_messages (conversation_id, lamport)
    """,
    """
    CREATE INDEX idx_outgoing_lamport
        ON outgoing_messages (conversation_id, lamport)
    """,
)

# _MIGRATIONS[n] upgrades a database from version n to n + 1.
_MIGRATIONS: dict[int, tuple[str, ...]] = {
    0: _V1,
    1: _V2,
    2: _V3,
}


def _pragma_int(conn: sqlite3.Connection, name: str) -> int:
    value = conn.execute(f"PRAGMA {name}").fetchone()[0]
    return int(value)


def _has_tables(conn: sqlite3.Connection) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone() is not None


def _check_ownership(
    version: int, application_id: int, conn: sqlite3.Connection
) -> None:
    """Refuse files that are not ours or come from a newer program version."""
    if version == 0:
        if application_id != 0 or _has_tables(conn):
            raise StoreError("the file is a SQLite database but not a message store")
    elif application_id != APPLICATION_ID:
        raise StoreError("the file is a SQLite database but not a message store")
    if version > SCHEMA_VERSION:
        raise StoreError(
            f"the store has schema version {version}, newer than the supported "
            f"{SCHEMA_VERSION}: upgrade the program"
        )


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create or upgrade the schema so the connection can be used.

    The connection must be in autocommit mode (``isolation_level=None``).

    Raises:
        StoreError: the file is not a message store or is from a newer version.
    """
    version = _pragma_int(conn, "user_version")
    application_id = _pragma_int(conn, "application_id")
    _check_ownership(version, application_id, conn)
    if version == SCHEMA_VERSION:
        return  # fast path: nothing to do, no write lock taken

    # Slow path. Take the write lock and look again: another process may have
    # created or upgraded the database since the check above.
    conn.execute("BEGIN IMMEDIATE")
    try:
        version = _pragma_int(conn, "user_version")
        application_id = _pragma_int(conn, "application_id")
        _check_ownership(version, application_id, conn)
        for current in range(version, SCHEMA_VERSION):
            for statement in _MIGRATIONS[current]:
                conn.execute(statement)
        if version != SCHEMA_VERSION:
            conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
        raise
