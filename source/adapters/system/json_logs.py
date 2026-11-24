"""JSON Lines formatter: one JSON object per log record, one record per line.

The shape of every line (see ``docs/logging.md``)::

    {"ts": "2026-11-23T22:28:04.512Z", "level": "INFO", "logger": "p08.delivery",
     "event": "delivery.mailbox_accepted", "pid": 4242, "process": "demo",
     "run": "a1b2c3d4", "client": "3fa2c1d0", "msg": "9c1e04b7...", "seq": 4}

Privacy is enforced HERE, whatever the caller passes:

* a field whose name is sensitive (``body``, ``password``, ``link``...) is
  written as ``"[redacted]"``;
* ``bytes`` are never written, only their length (keys, ciphertexts and
  signatures are bytes);
* ``UserId`` and ``ConversationId`` values are shortened to 8 characters;
  ``MessageId`` is kept whole, since it is random and is what lets an event
  be followed from process to process.

Records that do not come from ``EventLogger`` (third-party libraries) are
still valid lines, with ``"event": "log"`` and their text in ``"message"``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum

from source.application.observability import (
    CONTEXT_ATTR,
    EVENT_ATTR,
    FIELDS_ATTR,
    short,
)
from source.domain.identifiers import ConversationId, UserId

__all__ = ["REDACTED", "SENSITIVE_FIELDS", "JsonLinesFormatter"]

REDACTED = "[redacted]"

# Field names whose value must never reach a log.
SENSITIVE_FIELDS = frozenset(
    {
        "body",
        "text",
        "content",
        "plaintext",
        "ciphertext",
        "password",
        "passphrase",
        "secret",
        "private_key",
        "key",
        "signature",
        "sig",
        "link",
        "envelope",
        "request",
        "alias",
        "name",
    }
)

# Keys written by the formatter itself; a field with one of these names is
# renamed so it cannot overwrite them.
_RESERVED = frozenset(
    {"ts", "level", "logger", "event", "pid", "message", "error", "traceback"}
)


class JsonLinesFormatter(logging.Formatter):
    """Formats each record as one line of JSON."""

    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, EVENT_ATTR, None)
        line: dict[str, object] = {
            "ts": _timestamp(record.created),
            "level": record.levelname,
            "logger": record.name,
            "event": event if event is not None else "log",
            "pid": record.process,
        }
        if event is None:
            line["message"] = record.getMessage()
        line.update(_clean(getattr(record, CONTEXT_ATTR, {})))
        line.update(_clean(getattr(record, FIELDS_ATTR, {})))
        if record.exc_info and record.exc_info[1] is not None:
            error = record.exc_info[1]
            line["error"] = f"{type(error).__name__}: {error}"
            line["traceback"] = self.formatException(record.exc_info)
        return json.dumps(line, ensure_ascii=False, separators=(",", ":"), default=str)


def _timestamp(created: float) -> str:
    """UTC, ISO 8601, millisecond precision: ``2026-11-23T22:28:04.512Z``."""
    moment = datetime.fromtimestamp(created, tz=timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _clean(fields: Mapping[str, object]) -> dict[str, object]:
    cleaned: dict[str, object] = {}
    for key, value in fields.items():
        name = f"field_{key}" if key in _RESERVED else key
        cleaned[name] = (
            REDACTED if key.lower() in SENSITIVE_FIELDS else _jsonable(value)
        )
    return cleaned


def _jsonable(value: object) -> object:
    """A JSON-friendly, privacy-safe version of ``value``."""
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, UserId | ConversationId):
        return short(value)
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if isinstance(value, bytes | bytearray | memoryview):
        return f"<{len(value)} bytes>"
    if isinstance(value, Mapping):
        return {
            str(k): REDACTED if str(k).lower() in SENSITIVE_FIELDS else _jsonable(v)
            for k, v in value.items()
        }
    if isinstance(value, list | tuple | set | frozenset):
        items = sorted(value, key=str) if isinstance(value, set | frozenset) else value
        return [_jsonable(item) for item in items]
    return str(value)
