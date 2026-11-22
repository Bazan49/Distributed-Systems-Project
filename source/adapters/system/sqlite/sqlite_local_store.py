"""SqliteLocalStore: the durable ``LocalStorePort`` adapter.

It keeps the whole client state of ONE user (identity, contacts, history, seen
message ids, outbox and sequence counters) in ONE SQLite file. It is the
source of truth for the user's chat history: no service node keeps history.
"""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
from collections.abc import Callable, Collection, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Any, TypeVar

from source.adapters.system.sqlite.schema import ensure_schema
from source.application.ports.errors import (
    AlreadyExistsError,
    CorruptedStoreError,
    NotFoundError,
    StoreError,
)
from source.application.ports.local_store import LocalStorePort
from source.domain.contacts import Contact, ContactState
from source.domain.envelope import Envelope, MessageKind
from source.domain.identifiers import Alias, ConversationId, MessageId, UserId
from source.domain.identity import LocalIdentity
from source.domain.messages import OutgoingMessage, ReceivedMessage
from source.domain.outgoing_status import OutgoingStatus

DEFAULT_BUSY_TIMEOUT_MS = 5_000
_MIN_SQLITE_VERSION = (3, 24, 0)
_FILE_MODE = 0o600

T = TypeVar("T")


# opening the database


def _create_private_file(path: Path) -> None:
    """Create the file (and its folder) owner-only if it does not exist yet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    os.close(os.open(path, os.O_CREAT | os.O_RDWR, _FILE_MODE))


def _configure(conn: sqlite3.Connection) -> None:
    """Per-connection settings that the rest of the adapter relies on."""
    if sqlite3.sqlite_version_info < _MIN_SQLITE_VERSION:
        raise StoreError(
            f"SQLite {sqlite3.sqlite_version} is too old: 3.24 or later is required"
        )
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = FULL")
    # Fails with OperationalError if the JSON functions are not compiled in.
    conn.execute("SELECT 1 FROM json_each('[]')")


def _enable_wal(conn: sqlite3.Connection) -> None:
    mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
    if str(mode).lower() != "wal":
        raise StoreError(
            f"could not enable write-ahead logging (journal mode is {mode!r})"
        )


def _connect(path: Path, busy_timeout_ms: int) -> sqlite3.Connection:
    _create_private_file(path)
    try:
        conn = sqlite3.connect(
            path, timeout=busy_timeout_ms / 1000, isolation_level=None
        )
    except sqlite3.Error as exc:
        raise StoreError(f"cannot open the database: {exc}") from exc
    try:
        conn.row_factory = sqlite3.Row
        _configure(conn)
        ensure_schema(conn)  # refuses foreign files BEFORE the journal mode changes
        _enable_wal(conn)
    except sqlite3.Error as exc:
        conn.close()
        raise StoreError(f"cannot initialise the database: {exc}") from exc
    except BaseException:
        conn.close()
        raise
    return conn


# row -> domain object


def _identity_from_row(row: sqlite3.Row) -> LocalIdentity:
    return LocalIdentity(
        user_id=UserId(row["user_id"]),
        public_key=row["public_key"],
        alias=Alias(row["alias"]),
        protected_private_key=row["protected_private_key"],
        created_at_ms=row["created_at_ms"],
    )


def _contact_from_row(row: sqlite3.Row) -> Contact:
    return Contact(
        user_id=UserId(row["user_id"]),
        public_key=row["public_key"],
        alias=Alias(row["alias"]),
        state=ContactState(row["state"]),
        created_at_ms=row["created_at_ms"],
    )


def _received_from_row(row: sqlite3.Row) -> ReceivedMessage:
    return ReceivedMessage(
        message_id=MessageId(row["message_id"]),
        conversation_id=ConversationId(row["conversation_id"]),
        sender=UserId(row["sender"]),
        seq=row["seq"],
        kind=MessageKind(row["kind"]),
        body=row["body"],
        sent_at_ms=row["sent_at_ms"],
        received_at_ms=row["received_at_ms"],
    )


def _outgoing_from_row(row: sqlite3.Row) -> OutgoingMessage:
    return OutgoingMessage(
        envelope=Envelope.from_bytes(row["envelope"]),
        conversation_id=ConversationId(row["conversation_id"]),
        seq=row["seq"],
        kind=MessageKind(row["kind"]),
        body=row["body"],
        sent_at_ms=row["sent_at_ms"],
        status=OutgoingStatus(row["status"]),
        attempts=row["attempts"],
        next_attempt_at_ms=row["next_attempt_at_ms"],
    )


def _decode(build: Callable[[sqlite3.Row], T], row: sqlite3.Row, what: str) -> T:
    """Rebuild a domain object, turning any invariant violation into corruption."""
    try:
        return build(row)
    except (ValueError, TypeError) as exc:  # FormatError is a ValueError
        raise CorruptedStoreError(f"a stored {what} is not valid: {exc}") from exc


# the adapter


class SqliteLocalStore(LocalStorePort):
    """``LocalStorePort`` backed by one SQLite file."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    ) -> None:
        """Open (creating it if needed) the store at ``path``.

        Args:
            path: the database file; its folder is created if missing.
            busy_timeout_ms: how long a write waits for another process that
                holds the write lock before failing with ``StoreError``.

        Raises:
            StoreError: the file is not a message store, is from a newer
                version, or SQLite cannot open it.
        """
        self._path = Path(path)
        self._conn: sqlite3.Connection | None = _connect(self._path, busy_timeout_ms)

    # lifecycle

    @property
    def path(self) -> Path:
        """The database file (used to reopen the store after a restart)."""
        return self._path

    def close(self) -> None:
        """Close the connection. Idempotent; later calls raise ``StoreError``."""
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            conn.close()
        except sqlite3.Error as exc:
            raise StoreError(f"error while closing the database: {exc}") from exc

    def __enter__(self) -> SqliteLocalStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    # plumbing

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise StoreError("the store is closed")
        return self._conn

    def _fetch_all(self, sql: str, params: Any = ()) -> list[sqlite3.Row]:
        try:
            return self._connection().execute(sql, params).fetchall()
        except sqlite3.Error as exc:
            raise StoreError(f"database error: {exc}") from exc

    def _fetch_one(self, sql: str, params: Any = ()) -> sqlite3.Row | None:
        try:
            row: sqlite3.Row | None = self._connection().execute(sql, params).fetchone()
        except sqlite3.Error as exc:
            raise StoreError(f"database error: {exc}") from exc
        return row

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """One atomic, durable write transaction.

        Commits when the block ends normally; rolls back on ANY exception and
        re-raises it. ``sqlite3.Error`` becomes ``StoreError``.
        """
        conn = self._connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    with contextlib.suppress(sqlite3.Error):
                        conn.execute("ROLLBACK")
                raise
        except sqlite3.Error as exc:
            raise StoreError(f"database error: {exc}") from exc

    # identity

    def save_identity(self, identity: LocalIdentity) -> None:
        """Persist the local identity.

        Raises:
            AlreadyExistsError: an identity is already saved.
        """
        with self._write() as conn:
            cursor = conn.execute(
                """
                INSERT INTO identity
                    (id, user_id, public_key, alias, protected_private_key, created_at_ms)
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (
                    identity.user_id.value,
                    identity.public_key,
                    identity.alias.value,
                    identity.protected_private_key,
                    identity.created_at_ms,
                ),
            )
            if cursor.rowcount == 0:
                raise AlreadyExistsError("an identity is already stored")

    def load_identity(self) -> LocalIdentity | None:
        """Return the local identity, or None if the user never registered."""
        row = self._fetch_one(
            """
            SELECT user_id, public_key, alias, protected_private_key, created_at_ms
            FROM identity WHERE id = 1
            """
        )
        return None if row is None else _decode(_identity_from_row, row, "identity")

    # contacts

    def save_contact(self, contact: Contact) -> None:
        """Insert or update a contact (keyed by ``user_id``)."""
        with self._write() as conn:
            conn.execute(
                """
                INSERT INTO contacts (user_id, public_key, alias, state, created_at_ms)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                    public_key    = excluded.public_key,
                    alias         = excluded.alias,
                    state         = excluded.state,
                    created_at_ms = excluded.created_at_ms
                """,
                (
                    contact.user_id.value,
                    contact.public_key,
                    contact.alias.value,
                    contact.state.value,
                    contact.created_at_ms,
                ),
            )

    def get_contact(self, user_id: UserId) -> Contact | None:
        """Return the contact, or None if unknown."""
        row = self._fetch_one(
            """
            SELECT user_id, public_key, alias, state, created_at_ms
            FROM contacts WHERE user_id = ?
            """,
            (user_id.value,),
        )
        return None if row is None else _decode(_contact_from_row, row, "contact")

    def list_contacts(self, state: ContactState | None = None) -> list[Contact]:
        """Return all contacts, or only those in ``state``, sorted by ``user_id``."""
        rows = self._fetch_all(
            """
            SELECT user_id, public_key, alias, state, created_at_ms
            FROM contacts
            WHERE (:state IS NULL OR state = :state)
            ORDER BY user_id
            """,
            {"state": None if state is None else state.value},
        )
        return [_decode(_contact_from_row, row, "contact") for row in rows]

    def delete_contact(self, user_id: UserId) -> bool:
        """Remove a contact. Return ``True`` if it existed. Their messages stay."""
        with self._write() as conn:
            cursor = conn.execute(
                "DELETE FROM contacts WHERE user_id = ?", (user_id.value,)
            )
            return cursor.rowcount > 0

    # received messages

    def save_incoming_message(self, message: ReceivedMessage) -> bool:
        """Store the message AND mark its id as seen in ONE atomic transaction.

        Returns:
            True if stored; False if the id was already seen (nothing written).
        """
        with self._write() as conn:
            seen = conn.execute(
                "INSERT INTO seen_message_ids (message_id) VALUES (?) ON CONFLICT DO NOTHING",
                (message.message_id.value,),
            )
            if seen.rowcount == 0:
                return False
            conn.execute(
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
            return True

    def has_received(self, message_id: MessageId) -> bool:
        """True if this message id was already accepted (duplicate check)."""
        row = self._fetch_one(
            "SELECT 1 FROM seen_message_ids WHERE message_id = ?",
            (message_id.value,),
        )
        return row is not None

    def list_received(
        self, conversation_id: ConversationId, limit: int = 100
    ) -> list[ReceivedMessage]:
        """Return the last ``limit`` messages of a conversation, oldest first.

        Raises:
            ValueError: ``limit`` is less than 1.
        """
        if limit < 1:
            raise ValueError("limit must be at least 1")
        rows = self._fetch_all(
            """
            SELECT message_id, conversation_id, sender, seq, kind, body,
                   sent_at_ms, received_at_ms
            FROM (
                SELECT position, message_id, conversation_id, sender, seq, kind, body,
                       sent_at_ms, received_at_ms
                FROM received_messages
                WHERE conversation_id = ?
                ORDER BY position DESC
                LIMIT ?
            )
            ORDER BY position ASC
            """,
            (conversation_id.value, limit),
        )
        return [_decode(_received_from_row, row, "received message") for row in rows]

    # outbox

    def enqueue_outgoing(self, message: OutgoingMessage) -> None:
        """Persist a message to send and advance the conversation's seq counter.

        Raises:
            AlreadyExistsError: the ``message_id`` is already queued, or this
                ``seq`` was already used in the conversation.
        """
        with self._write() as conn:
            cursor = conn.execute(
                """
                INSERT INTO outgoing_messages
                    (message_id, conversation_id, seq, kind, body, sent_at_ms,
                     envelope, status, attempts, next_attempt_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (
                    message.message_id.value,
                    message.conversation_id.value,
                    message.seq,
                    message.kind.value,
                    message.body,
                    message.sent_at_ms,
                    message.envelope.to_bytes(),
                    message.status.value,
                    message.attempts,
                    message.next_attempt_at_ms,
                ),
            )
            if cursor.rowcount == 0:
                raise AlreadyExistsError(
                    "message_id or (conversation, seq) is already queued"
                )
            conn.execute(
                """
                INSERT INTO outgoing_counters (conversation_id, last_seq) VALUES (?, ?)
                ON CONFLICT (conversation_id) DO UPDATE
                    SET last_seq = max(last_seq, excluded.last_seq)
                """,
                (message.conversation_id.value, message.seq),
            )

    def update_outgoing(self, message: OutgoingMessage) -> None:
        """Replace the delivery state (status, attempts, retry time) of a queued message.

        Raises:
            NotFoundError: no such message is queued.
            ValueError: an immutable field differs from the stored message.
        """
        with self._write() as conn:
            stored = conn.execute(
                """
                SELECT envelope, conversation_id, seq, kind, body, sent_at_ms
                FROM outgoing_messages WHERE message_id = ?
                """,
                (message.message_id.value,),
            ).fetchone()
            if stored is None:
                raise NotFoundError("no such outgoing message")
            immutable = (
                message.envelope.to_bytes(),
                message.conversation_id.value,
                message.seq,
                message.kind.value,
                message.body,
                message.sent_at_ms,
            )
            if tuple(stored) != immutable:
                raise ValueError(
                    "only status, attempts and next_attempt_at_ms may change"
                )
            conn.execute(
                """
                UPDATE outgoing_messages
                SET status = ?, attempts = ?, next_attempt_at_ms = ?
                WHERE message_id = ?
                """,
                (
                    message.status.value,
                    message.attempts,
                    message.next_attempt_at_ms,
                    message.message_id.value,
                ),
            )

    def get_outgoing(self, message_id: MessageId) -> OutgoingMessage | None:
        """Return one outgoing message, or ``None`` if unknown."""
        row = self._fetch_one(
            """
            SELECT envelope, conversation_id, seq, kind, body, sent_at_ms,
                   status, attempts, next_attempt_at_ms
            FROM outgoing_messages WHERE message_id = ?
            """,
            (message_id.value,),
        )
        return (
            None
            if row is None
            else _decode(_outgoing_from_row, row, "outgoing message")
        )

    def list_outgoing(
        self,
        conversation_id: ConversationId | None = None,
        statuses: Collection[OutgoingStatus] | None = None,
    ) -> list[OutgoingMessage]:
        """Return outgoing messages, optionally of one conversation and/or statuses.

        Sorted by ``(conversation_id, seq)``. ``None`` means "no filter"; an
        empty collection of statuses matches nothing.
        """
        statuses_json = (
            None if statuses is None else json.dumps(sorted(s.value for s in statuses))
        )
        rows = self._fetch_all(
            """
            SELECT envelope, conversation_id, seq, kind, body, sent_at_ms,
                   status, attempts, next_attempt_at_ms
            FROM outgoing_messages
            WHERE (:conversation IS NULL OR conversation_id = :conversation)
              AND (:statuses IS NULL OR status IN (SELECT value FROM json_each(:statuses)))
            ORDER BY conversation_id, seq
            """,
            {
                "conversation": None
                if conversation_id is None
                else conversation_id.value,
                "statuses": statuses_json,
            },
        )
        return [_decode(_outgoing_from_row, row, "outgoing message") for row in rows]

    def list_due_outgoing(self, now_ms: int, limit: int = 100) -> list[OutgoingMessage]:
        """Return up to ``limit`` QUEUED messages whose retry time has come.

        Sorted by ``sent_at_ms``, then ``conversation_id``, then ``seq``.

        Raises:
            ValueError: ``limit`` is less than 1.
        """
        if limit < 1:
            raise ValueError("limit must be at least 1")
        rows = self._fetch_all(
            """
            SELECT envelope, conversation_id, seq, kind, body, sent_at_ms,
                   status, attempts, next_attempt_at_ms
            FROM outgoing_messages
            WHERE status = ?
              AND (next_attempt_at_ms IS NULL OR next_attempt_at_ms <= ?)
            ORDER BY sent_at_ms, conversation_id, seq
            LIMIT ?
            """,
            (OutgoingStatus.QUEUED.value, now_ms, limit),
        )
        return [_decode(_outgoing_from_row, row, "outgoing message") for row in rows]

    def last_outgoing_seq(self, conversation_id: ConversationId) -> int:
        """Highest ``seq`` ever used by the local user in this conversation, or ``0``."""
        row = self._fetch_one(
            "SELECT last_seq FROM outgoing_counters WHERE conversation_id = ?",
            (conversation_id.value,),
        )
        return 0 if row is None else int(row["last_seq"])
