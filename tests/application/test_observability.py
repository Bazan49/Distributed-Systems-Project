"""Tests for the event logging API (source.application.observability)."""

from __future__ import annotations

import logging

import pytest

from source.application.observability import (
    current_context,
    get_logger,
    log_context,
    set_process_context,
    short,
)
from tests.support.logs import contexts, events, has_event


@pytest.fixture
def capture(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.DEBUG, logger="p08")
    return caplog


def test_loggers_are_named_after_the_module_under_p08() -> None:
    assert get_logger("source.application.use_cases.delivery").name == "p08.delivery"
    assert get_logger("screens").name == "p08.screens"


def test_an_event_carries_its_name_and_fields(
    capture: pytest.LogCaptureFixture,
) -> None:
    get_logger("observability_test").info(
        "delivery.retry_scheduled", attempt=2, retry_in_ms=1500
    )

    assert events(capture) == [
        ("delivery.retry_scheduled", {"attempt": 2, "retry_in_ms": 1500})
    ]
    assert capture.records[0].getMessage() == "delivery.retry_scheduled"


@pytest.mark.parametrize(
    ("method", "level"),
    [
        ("debug", logging.DEBUG),
        ("info", logging.INFO),
        ("warning", logging.WARNING),
        ("error", logging.ERROR),
    ],
)
def test_each_level(capture: pytest.LogCaptureFixture, method: str, level: int) -> None:
    getattr(get_logger("x"), method)("some.event")
    assert capture.records[0].levelno == level


def test_msg_is_a_valid_field_name(capture: pytest.LogCaptureFixture) -> None:
    """``msg`` is reserved by LogRecord; fields travel apart, so it is usable."""
    get_logger("x").info("message.queued", msg="9c1e", seq=4)
    assert has_event(capture, "message.queued", msg="9c1e", seq=4)


def test_exception_logs_an_error_with_the_exception(
    capture: pytest.LogCaptureFixture,
) -> None:
    try:
        raise RuntimeError("disk full")
    except RuntimeError:
        get_logger("x").exception("store.failed", operation="commit")

    record = capture.records[0]
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None and str(record.exc_info[1]) == "disk full"


def test_disabled_levels_build_nothing(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="p08")
    log = get_logger("x")

    log.debug("noise", value=1)

    assert events(caplog) == []
    assert not log.is_enabled_for(logging.DEBUG)


def test_the_call_context_applies_inside_the_block_only(
    capture: pytest.LogCaptureFixture,
) -> None:
    log = get_logger("x")
    with log_context(client="aaaa1111"):
        log.info("inside")
    log.info("outside")

    inside, outside = contexts(capture)
    assert inside["client"] == "aaaa1111"
    assert "client" not in outside


def test_call_contexts_nest_and_restore(capture: pytest.LogCaptureFixture) -> None:
    """Bob handles a direct envelope inside Alice's call."""
    log = get_logger("x")
    with log_context(client="alice"):
        with log_context(client="bob", extra="y"):
            log.info("bob.event")
        log.info("alice.event")

    bob, alice = contexts(capture)
    assert (bob["client"], bob["extra"]) == ("bob", "y")
    assert alice["client"] == "alice" and "extra" not in alice


def test_the_process_context_applies_everywhere(
    capture: pytest.LogCaptureFixture,
) -> None:
    set_process_context(process="node-2", node="node-2")
    get_logger("x").info("anything")

    assert contexts(capture)[0]["node"] == "node-2"
    set_process_context(node=None)
    assert "node" not in current_context()


def test_the_call_context_overrides_the_process_context() -> None:
    set_process_context(role="process")
    with log_context(role="call"):
        assert current_context()["role"] == "call"
    assert current_context()["role"] == "process"


def test_short_ids() -> None:
    assert short("3fa2c1d0e9b8a7f6") == "3fa2c1d0"
    assert short("abc") == "abc"
