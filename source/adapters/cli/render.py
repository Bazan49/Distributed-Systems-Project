"""How every piece of the app is shown: plain functions from data to text.

The texts are in Spanish: they are what the user reads. Nothing here talks
to the client or the terminal, so it is all tested directly.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, tzinfo

from source.application.ports.errors import StoreError, WrongPasswordError
from source.application.use_cases.client_app import (
    EntryKind,
    HistoryEntry,
    NotLoggedInError,
    Profile,
)
from source.application.use_cases.contacts import (
    AddOutcome,
    ContactView,
    InvalidLinkError,
    NoPendingRequestError,
)
from source.application.use_cases.identity import (
    MIN_PASSWORD_LENGTH,
    AlreadyRegisteredError,
    NotRegisteredError,
    WeakPasswordError,
)
from source.application.use_cases.send_message import (
    ContactNotActiveError,
    UnknownContactError,
)
from source.domain.contacts import ContactState
from source.domain.identifiers import UserId
from source.domain.outgoing_status import OutgoingStatus

__all__ = [
    "ADD_OUTCOME",
    "STATE_LABEL",
    "STATUS_LABEL",
    "contact_line",
    "describe_error",
    "format_time",
    "header",
    "history_lines",
    "look_alike_warning",
    "profile_lines",
    "short_id",
]

STATUS_LABEL = {
    OutgoingStatus.QUEUED: "· en cola",
    OutgoingStatus.ACCEPTED: "✓ en los buzones",
    OutgoingStatus.DELIVERED: "✓✓ entregado",
    OutgoingStatus.EXPIRED: "✗ no se pudo entregar",
}

STATE_LABEL = {
    ContactState.ACTIVE: "activo",
    ContactState.OUTGOING_REQUEST: "esperando a que acepte",
    ContactState.INCOMING_REQUEST: "te pidió agregarte",
}

ADD_OUTCOME = {
    AddOutcome.REQUEST_SENT: "Solicitud enviada a {alias}. Podréis hablar cuando acepte.",
    AddOutcome.ALREADY_REQUESTED: "Ya le habías enviado una solicitud a {alias}.",
    AddOutcome.ACCEPTED: "{alias} ya te lo había pedido: ahora sois contactos.",
    AddOutcome.ALREADY_CONTACT: "{alias} ya era tu contacto.",
}


def short_id(user_id: UserId) -> str:
    """The first 8 characters of an id: enough to tell users apart on screen."""
    return user_id.value[:8]


def format_time(ms: int, tz: tzinfo | None = None) -> str:
    """``HH:MM`` of a wall-clock time in milliseconds (local time by default)."""
    return datetime.fromtimestamp(ms / 1000, tz).strftime("%H:%M")


def header(title: str) -> str:
    return f"\n═══ {title} ═══"


def profile_lines(profile: Profile, *, include_identity: bool = True) -> list[str]:
    lines = [f"Alias:  {profile.alias.value}"]
    if include_identity:
        lines.extend(
            [
                f"Id:     {profile.user_id.value}",
                f"Huella: {profile.fingerprint}",
            ]
        )
    lines.extend(
        ["", "Tu enlace de contacto (compártelo para que te agreguen):", profile.link]
    )
    return lines


def contact_line(number: int, view: ContactView) -> str:
    contact = view.contact
    state = STATE_LABEL[contact.state]
    line = (
        f"{number:>3}. {contact.alias.value}  ({short_id(contact.user_id)})"
        f"  [{state}]  huella: {view.fingerprint}"
    )
    return line + ("  ⚠" if view.look_alikes else "")


def look_alike_warning(view: ContactView) -> list[str]:
    """A warning if other contacts have the same alias (possible impersonation)."""
    if not view.look_alikes:
        return []
    others = ", ".join(short_id(c.user_id) for c in view.look_alikes)
    return [
        f"  ⚠ Otros contactos también se llaman «{view.contact.alias.value}» ({others}).",
        "    Puede ser la misma persona con una cuenta nueva, o alguien que se hace",
        "    pasar por ella: compara la huella por otro medio antes de confiar.",
    ]


def history_lines(
    entries: Sequence[HistoryEntry], contact_alias: str, tz: tzinfo | None = None
) -> list[str]:
    """A conversation, one line per message, oldest first."""
    if not entries:
        return ["  (todavía no hay mensajes)"]
    lines = []
    for entry in entries:
        if entry.kind is EntryKind.MISSING:
            lines.append(
                f"          … falta el mensaje nº {entry.seq} de {contact_alias}"
                " (llegará más tarde)"
            )
            continue
        when = format_time(entry.sent_at_ms, tz)
        if entry.kind is EntryKind.SENT:
            status = STATUS_LABEL[entry.status] if entry.status is not None else ""
            lines.append(f"  [{when}] Tú: {entry.body}   {status}".rstrip())
        else:
            lines.append(f"  [{when}] {contact_alias}: {entry.body}")
    return lines


def describe_error(exc: Exception, invalid_value: str | None = None) -> str | None:
    """What to tell the user about ``exc``; None if it is not an expected error.

    ``invalid_value`` explains a plain ``ValueError`` (an invalid alias, an
    empty message...), which only the screen that asked for it can name.
    """
    known: list[tuple[type[Exception], str]] = [
        (WrongPasswordError, "Contraseña incorrecta."),
        (
            WeakPasswordError,
            f"La contraseña debe tener al menos {MIN_PASSWORD_LENGTH} caracteres.",
        ),
        (
            AlreadyRegisteredError,
            "Este dispositivo ya tiene una cuenta: inicia sesión.",
        ),
        (NotRegisteredError, "Este dispositivo todavía no tiene cuenta."),
        (NoPendingRequestError, "Esa solicitud ya no está pendiente."),
        (UnknownContactError, "Esa persona no es tu contacto."),
        (
            ContactNotActiveError,
            "Ese contacto todavía no ha aceptado: aún no podéis hablar.",
        ),
        (NotLoggedInError, "La sesión se cerró: vuelve a iniciar sesión."),
        (StoreError, "No se pudo leer o guardar en este dispositivo."),
    ]
    if isinstance(exc, InvalidLinkError):
        if "own" in str(exc):
            return "Ese es tu propio enlace."
        return "El enlace no es válido: ¿lo copiaste completo?"
    for kind, text in known:
        if isinstance(exc, kind):
            return text
    if isinstance(exc, ValueError) and invalid_value is not None:
        return invalid_value
    return None
