"""Helpers to drive the real client actors started by the launcher in tests."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import ray

from source.application.use_cases.client_app import EntryKind, HistoryEntry, Profile
from source.domain.contacts import ContactState

PASSWORD = "contraseña larga"
WAIT_S = 60
T = TypeVar("T")


def client_options(log_dir: Path) -> dict[str, Any]:
    """Options of a launched client for tests: cheap password key, fast loop."""
    return {"kdf": "fast", "tick_interval_ms": 200, "log_dir": str(log_dir)}


def call(handle: Any, method: str, *args: Any) -> Any:
    """Call ``method`` on a client actor and wait for its answer."""
    return ray.get(getattr(handle, method).remote(*args), timeout=WAIT_S)


def wait_for(read: Callable[[], T], ok: Callable[[T], bool], what: str) -> T:
    """Call ``read`` until ``ok`` holds for its value (or fail after ``WAIT_S``)."""
    deadline = time.monotonic() + WAIT_S
    while True:
        value = read()
        if ok(value):
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting until {what}: {value!r}")
        time.sleep(0.1)


def befriend(alice: Any, bob: Any) -> tuple[Profile, Profile]:
    """Register both clients and make them active contacts of each other."""
    a: Profile = call(alice, "register", "Alice", PASSWORD)
    b: Profile = call(bob, "register", "Bob", PASSWORD)
    call(alice, "add_contact", b.link)
    wait_for(lambda: call(bob, "pending_requests"), bool, "Bob gets the request")
    call(bob, "accept", a.user_id)
    wait_for(
        lambda: call(alice, "contacts", ContactState.ACTIVE),
        bool,
        "Alice learns that Bob accepted",
    )
    return a, b


def received(history: list[HistoryEntry]) -> list[str]:
    """The bodies of the received messages of a history."""
    return [e.body for e in history if e.kind is EntryKind.RECEIVED]
