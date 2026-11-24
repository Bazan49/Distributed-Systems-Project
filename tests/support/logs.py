"""Helpers to assert on logged events in tests.

    def test_a_forged_envelope_is_logged(caplog):
        caplog.set_level(logging.DEBUG, logger="p08")
        ...
        assert ("receive.rejected", {"reason": "bad_signature"}) in events(caplog)

``events`` returns, for every event record, its name and its fields
(without the context), so a test can check exactly what a use case logged.
"""

from __future__ import annotations

import pytest

from source.application.observability import CONTEXT_ATTR, EVENT_ATTR, FIELDS_ATTR


def events(caplog: pytest.LogCaptureFixture) -> list[tuple[str, dict[str, object]]]:
    """``(event, fields)`` of every event captured, in order."""
    return [
        (getattr(r, EVENT_ATTR), dict(getattr(r, FIELDS_ATTR)))
        for r in caplog.records
        if hasattr(r, EVENT_ATTR)
    ]


def event_names(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [name for name, _ in events(caplog)]


def contexts(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    """The context (process + call) of every event captured, in order."""
    return [
        dict(getattr(r, CONTEXT_ATTR)) for r in caplog.records if hasattr(r, EVENT_ATTR)
    ]


def has_event(caplog: pytest.LogCaptureFixture, name: str, **expected: object) -> bool:
    """True if some event ``name`` was logged whose fields include ``expected``."""
    return any(
        event == name and all(fields.get(k) == v for k, v in expected.items())
        for event, fields in events(caplog)
    )
