"""Structured, event-based logging for application and adapter code.

Every log record is an EVENT: a stable dotted name plus named fields::

    from source.application.observability import get_logger

    log = get_logger(__name__)
    log.info("delivery.mailbox_accepted", msg=message.message_id, seq=message.seq)

Never a free-form sentence: event names are searched, counted and compared
across runs, so they must not change. See ``docs/logging.md`` for the
naming rules, the standard fields, the levels and the catalogue of events.

WHO emits an event is not passed by hand. It comes from two layers of
context, merged into every record:

* the **process context** (``set_process_context``), set once at start-up:
  ``process``, ``run`` and, in a node or a Ray actor, ``node``;
* the **call context** (``log_context``), set around a piece of work, for
  example ``client=...`` when several clients share one process (as in
  tests).

This module only builds records; where they go and how they look (JSON
Lines in a file) is configured once per process by the composition root.
The domain never logs: it returns its decisions, and the caller logs them.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType

__all__ = [
    "CONTEXT_ATTR",
    "EVENT_ATTR",
    "FIELDS_ATTR",
    "EventLogger",
    "current_context",
    "get_logger",
    "log_context",
    "set_process_context",
    "short",
]

# Names of the attributes the logger adds to each ``logging.LogRecord``.
# Fields travel in ONE attribute instead of loose ``extra`` keys, because
# ``LogRecord`` reserves names such as ``msg`` that we want to use as fields.
EVENT_ATTR = "p08_event"
FIELDS_ATTR = "p08_fields"
CONTEXT_ATTR = "p08_context"

SHORT_ID_LENGTH = 8

_process: dict[str, object] = {}
_EMPTY: Mapping[str, object] = MappingProxyType({})
_call: ContextVar[Mapping[str, object]] = ContextVar("p08_log_context", default=_EMPTY)


def set_process_context(**fields: object) -> None:
    """Fields added to every event of this process (``None`` removes one).

    Call it once at start-up: the composition root sets ``process`` and
    ``run``; a node or a Ray actor also sets ``node``.
    """
    for key, value in fields.items():
        if value is None:
            _process.pop(key, None)
        else:
            _process[key] = value


@contextmanager
def log_context(**fields: object) -> Iterator[None]:
    """Add ``fields`` to every event logged inside the ``with`` block.

    Blocks nest, and the previous context comes back when one ends, so a
    client handling a direct envelope inside another client's call logs
    under its own identity.
    """
    token = _call.set(MappingProxyType({**_call.get(), **fields}))
    try:
        yield
    finally:
        _call.reset(token)


def current_context() -> dict[str, object]:
    """Process context overlaid with the current call context."""
    return {**_process, **_call.get()}


def short(value: object) -> str:
    """The first characters of an id: enough to tell users apart in a log,
    without writing whole identities everywhere."""
    return str(value)[:SHORT_ID_LENGTH]


class EventLogger:
    """A thin wrapper over ``logging.Logger`` that logs events with fields."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    @property
    def name(self) -> str:
        return self._logger.name

    def is_enabled_for(self, level: int) -> bool:
        return self._logger.isEnabledFor(level)

    def debug(self, event: str, **fields: object) -> None:
        self._log(logging.DEBUG, event, fields)

    def info(self, event: str, **fields: object) -> None:
        self._log(logging.INFO, event, fields)

    def warning(self, event: str, **fields: object) -> None:
        self._log(logging.WARNING, event, fields)

    def error(self, event: str, **fields: object) -> None:
        self._log(logging.ERROR, event, fields)

    def exception(self, event: str, **fields: object) -> None:
        """An ERROR with the exception being handled (call it inside ``except``)."""
        self._log(logging.ERROR, event, fields, exc_info=True)

    def _log(
        self, level: int, event: str, fields: dict[str, object], exc_info: bool = False
    ) -> None:
        if not self._logger.isEnabledFor(level):
            return  # nothing is built for a disabled level
        self._logger.log(
            level,
            event,
            exc_info=exc_info,
            stacklevel=3,  # report the caller of debug()/info()/..., not this file
            extra={
                EVENT_ATTR: event,
                FIELDS_ATTR: fields,
                CONTEXT_ATTR: current_context(),
            },
        )


def get_logger(module_name: str) -> EventLogger:
    """The event logger of a module: pass ``__name__``.

    Loggers are named ``p08.<last part of the module>`` (for example
    ``p08.delivery``), so every logger of the project shares the ``p08``
    parent and the names stay short in the log.
    """
    return EventLogger(logging.getLogger(f"p08.{module_name.rsplit('.', 1)[-1]}"))
