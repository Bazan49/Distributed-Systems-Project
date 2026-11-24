"""Fixtures shared by the whole test suite."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest

from source.application import observability


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Undo any global logging change a test makes (handlers, level, context).

    ``configure_logging`` replaces the root handlers and sets the process
    context; without this, one test that calls it would change what every
    later test (and pytest's own ``caplog``) sees.
    """
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    process = dict(observability._process)
    yield
    for handler in root.handlers[:]:
        if handler not in handlers:
            root.removeHandler(handler)
            handler.close()
    for handler in handlers:
        if handler not in root.handlers:
            root.addHandler(handler)
    root.setLevel(level)
    observability._process.clear()
    observability._process.update(process)
