"""Tests for the JSON Lines formatter, including its privacy guarantees."""

from __future__ import annotations

import json
import logging
import re
import sys

import pytest

from source.adapters.system.json_logs import REDACTED, JsonLinesFormatter
from source.application.observability import (
    CONTEXT_ATTR,
    EVENT_ATTR,
    FIELDS_ATTR,
)
from source.domain.identifiers import ConversationId, MessageId, UserId
from source.domain.outgoing_status import OutgoingStatus

USER = UserId("3fa2c1d0" + "e" * 56)
OTHER = UserId("9c1e04b7" + "f" * 56)
MESSAGE = MessageId("ab" * 16)


def record(
    event: str | None = "delivery.mailbox_accepted",
    *,
    fields: dict[str, object] | None = None,
    context: dict[str, object] | None = None,
    level: int = logging.INFO,
    text: str = "",
    exc_info: object = None,
) -> logging.LogRecord:
    made = logging.LogRecord(
        "p08.delivery",
        level,
        __file__,
        1,
        event or text,
        None,
        exc_info,
    )
    if event is not None:
        setattr(made, EVENT_ATTR, event)
        setattr(made, FIELDS_ATTR, fields or {})
        setattr(made, CONTEXT_ATTR, context or {})
    return made


def line(rec: logging.LogRecord) -> dict[str, object]:
    text = JsonLinesFormatter().format(rec)
    assert "\n" not in text  # one record, one line
    parsed = json.loads(text)
    assert isinstance(parsed, dict)
    return parsed


def test_the_standard_keys_come_first() -> None:
    out = line(
        record(fields={"seq": 4}, context={"process": "demo", "client": "3fa2c1d0"})
    )

    assert list(out)[:5] == ["ts", "level", "logger", "event", "pid"]
    assert out["level"] == "INFO"
    assert out["logger"] == "p08.delivery"
    assert out["event"] == "delivery.mailbox_accepted"
    assert (out["process"], out["client"], out["seq"]) == ("demo", "3fa2c1d0", 4)


def test_the_timestamp_is_utc_iso_with_milliseconds() -> None:
    assert re.fullmatch(
        r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", str(line(record())["ts"])
    )


@pytest.mark.parametrize(
    "field",
    [
        "body",
        "password",
        "private_key",
        "link",
        "signature",
        "ciphertext",
        "alias",
        "Body",
    ],
)
def test_sensitive_fields_are_redacted(field: str) -> None:
    secret = "frase secreta 123"
    out = line(record(fields={field: secret}))

    assert out[field] == REDACTED
    assert secret not in json.dumps(out)


def test_sensitive_keys_are_redacted_inside_nested_values() -> None:
    out = line(record(fields={"detail": {"password": "x", "attempt": 2}}))
    assert out["detail"] == {"password": REDACTED, "attempt": 2}


def test_bytes_are_never_written_only_their_size() -> None:
    out = line(record(fields={"payload": b"\x01secret-bytes"}))
    assert out["payload"] == "<13 bytes>"


def test_user_and_conversation_ids_are_shortened_message_ids_are_not() -> None:
    conversation = ConversationId.between(USER, OTHER)
    out = line(record(fields={"peer": USER, "conv": conversation, "msg": MESSAGE}))

    assert out["peer"] == "3fa2c1d0"
    assert out["conv"] == conversation.value[:8]
    assert out["msg"] == MESSAGE.value


def test_enums_lists_and_sets_become_plain_json() -> None:
    out = line(
        record(
            fields={
                "status": OutgoingStatus.ACCEPTED,
                "confirmed_by": ("node-1", "node-3"),
                "down": {"node-3", "node-1"},
                "ok": True,
                "nothing": None,
                "ratio": 0.5,
            }
        )
    )
    assert out["status"] == "accepted"
    assert out["confirmed_by"] == ["node-1", "node-3"]
    assert out["down"] == ["node-1", "node-3"]
    assert (out["ok"], out["nothing"], out["ratio"]) == (True, None, 0.5)


def test_unknown_objects_are_written_as_text() -> None:
    class Thing:
        def __str__(self) -> str:
            return "a thing"

    assert line(record(fields={"thing": Thing()}))["thing"] == "a thing"


def test_a_field_cannot_overwrite_a_standard_key() -> None:
    out = line(record(fields={"event": "fake", "level": "fake"}))
    assert out["event"] == "delivery.mailbox_accepted"
    assert (out["field_event"], out["field_level"]) == ("fake", "fake")


def test_non_ascii_text_stays_readable() -> None:
    assert line(record(fields={"reason": "contraseña"}))["reason"] == "contraseña"


def test_an_exception_adds_error_and_traceback() -> None:
    try:
        raise RuntimeError("disk full")
    except RuntimeError:
        out = line(record("store.failed", level=logging.ERROR, exc_info=sys.exc_info()))

    assert out["error"] == "RuntimeError: disk full"
    assert "Traceback" in str(out["traceback"])


def test_a_third_party_record_is_still_a_valid_line() -> None:
    out = line(record(None, text="ray worker started"))
    assert (out["event"], out["message"]) == ("log", "ray worker started")
