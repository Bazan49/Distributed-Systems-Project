"""How the CLI shows things: plain functions, checked directly."""

from __future__ import annotations

from datetime import timezone

import pytest

from source.adapters.cli.render import (
    ADD_OUTCOME,
    STATE_LABEL,
    STATUS_LABEL,
    describe_error,
    format_time,
    history_lines,
    short_id,
)
from source.application.ports.errors import CorruptedStoreError, WrongPasswordError
from source.application.use_cases.client_app import EntryKind, HistoryEntry
from source.application.use_cases.contacts import (
    AddOutcome,
    InvalidLinkError,
    NoPendingRequestError,
)
from source.application.use_cases.identity import WeakPasswordError
from source.domain.contacts import ContactState
from source.domain.identifiers import UserId
from source.domain.outgoing_status import OutgoingStatus

UTC = timezone.utc
NOON = 12 * 3600 * 1000  # 12:00 UTC on the first day of 1970


def test_every_status_state_and_outcome_has_a_label() -> None:
    assert set(STATUS_LABEL) == set(OutgoingStatus)
    assert set(STATE_LABEL) == set(ContactState)
    assert set(ADD_OUTCOME) == set(AddOutcome)


def test_ids_and_times_are_short() -> None:
    assert short_id(UserId("ab" * 32)) == "abababab"
    assert format_time(NOON + 5 * 60_000, UTC) == "12:05"


def test_a_conversation_shows_both_sides_statuses_and_gaps() -> None:
    entries = [
        HistoryEntry(EntryKind.RECEIVED, 1, NOON, "hola"),
        HistoryEntry(EntryKind.MISSING, 2, NOON),
        HistoryEntry(EntryKind.RECEIVED, 3, NOON + 60_000, "¿estás?"),
        HistoryEntry(
            EntryKind.SENT, 1, NOON + 120_000, "sí", status=OutgoingStatus.DELIVERED
        ),
        HistoryEntry(
            EntryKind.SENT, 2, NOON + 180_000, "¿y tú?", status=OutgoingStatus.QUEUED
        ),
    ]
    assert history_lines(entries, "Bob", UTC) == [
        "  [12:00] Bob: hola",
        "          … falta el mensaje nº 2 de Bob (llegará más tarde)",
        "  [12:01] Bob: ¿estás?",
        "  [12:02] Tú: sí   ✓✓ entregado",
        "  [12:03] Tú: ¿y tú?   · en cola",
    ]


def test_an_empty_conversation_says_so() -> None:
    assert history_lines([], "Bob") == ["  (todavía no hay mensajes)"]


@pytest.mark.parametrize(
    ("error", "invalid_value", "expected"),
    [
        (WrongPasswordError("x"), None, "Contraseña incorrecta."),
        (WeakPasswordError("x"), None, "al menos 8 caracteres"),
        (InvalidLinkError("this is your own contact link"), None, "tu propio enlace"),
        (InvalidLinkError("invalid contact link: x"), None, "no es válido"),
        (NoPendingRequestError("x"), None, "ya no está pendiente"),
        (CorruptedStoreError("x"), None, "No se pudo leer o guardar"),
        (ValueError("x"), "Alias inválido.", "Alias inválido."),
    ],
)
def test_expected_errors_are_told_in_words(
    error: Exception, invalid_value: str | None, expected: str
) -> None:
    message = describe_error(error, invalid_value)
    assert message is not None and expected in message


def test_other_errors_are_not_described() -> None:
    assert describe_error(RuntimeError("bug")) is None
    assert describe_error(ValueError("x")) is None  # no screen explained it
