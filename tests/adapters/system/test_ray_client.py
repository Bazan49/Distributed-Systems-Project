"""Tests of ClientHost WITHOUT Ray: what the host adds around the client.

The host is a lock, a main loop and a set of calls handed to ``ClientApp``.
What the client does with those calls is tested in
``tests/application/use_cases/test_client_app.py``; here a fake client
records them, so each test checks only the host. The actor itself (a real
process, reached by name) is tested with real Ray in
``ray/test_ray_client_actor.py``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any, cast

import pytest

from source.adapters.system.ray.ray_client import (
    DEVICE_ACTOR_PREFIX,
    ClientHost,
    device_actor_name,
)
from source.application.use_cases.client_app import ClientApp, Profile
from source.domain.contacts import ContactState
from source.domain.identifiers import Alias
from tests.support.builders import ALICE, BOB
from tests.support.logs import has_event

PROFILE = Profile(
    user_id=ALICE.user_id,
    alias=Alias("Alice"),
    fingerprint="fingerprint",
    link="p08:contact:link",
)
REPORT = object()  # what the fake client's tick returns


class FakeApp:
    """Stands in for ``ClientApp``: records every call, answers with markers."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.logged_in = False
        self.ticks = 0
        self.tick_error: Exception | None = None

    def register(self, alias: str, password: str) -> Profile:
        self.calls.append(("register", (alias, password)))
        self.logged_in = True
        return PROFILE

    def login(self, password: str) -> Profile:
        self.calls.append(("login", (password,)))
        self.logged_in = True
        return PROFILE

    def logout(self) -> None:
        self.calls.append(("logout", ()))
        self.logged_in = False

    def add_change_listener(self, listener: Callable[[], None]) -> None:
        self.listener = listener  # the host subscribes once; not a client call

    def tick(self) -> object:
        self.ticks += 1
        if self.tick_error is not None:
            raise self.tick_error
        return REPORT

    def __getattr__(self, name: str) -> Callable[..., object]:
        def method(*args: Any) -> object:
            self.calls.append((name, args))
            return ("result", name)

        return method


class Built:
    """A host over a fake client, plus the lock the factory received."""

    def __init__(self, tick_interval_ms: int | None = None) -> None:
        self.app = FakeApp()
        self.locks: list[threading.RLock] = []

        def make_app(lock: threading.RLock) -> ClientApp:
            self.locks.append(lock)
            return cast(ClientApp, self.app)

        self.host = ClientHost(make_app, tick_interval_ms)


def test_the_device_actor_is_named_after_the_device() -> None:
    assert device_actor_name("alice") == f"{DEVICE_ACTOR_PREFIX}alice"


def test_the_factory_is_called_once_with_the_hosts_lock() -> None:
    built = Built()
    assert len(built.locks) == 1
    assert hasattr(built.locks[0], "acquire")


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("is_registered", ()),
        ("profile", ()),
        ("add_contact", ("p08:contact:link",)),
        ("contacts", (ContactState.ACTIVE,)),
        ("pending_requests", ()),
        ("accept", (BOB.user_id,)),
        ("remove_contact", (BOB.user_id,)),
        ("send", (BOB.user_id, "hola")),
        ("history", (BOB.user_id, 10)),
    ],
)
def test_each_call_reaches_the_client_and_returns_its_answer(
    method: str, args: tuple[Any, ...]
) -> None:
    built = Built()
    assert getattr(built.host, method)(*args) == ("result", method)
    assert built.app.calls == [(method, args)]


@pytest.mark.parametrize(
    ("method", "args"),
    [("reject", (BOB.user_id,)), ("logout", ())],
)
def test_calls_without_an_answer_reach_the_client(
    method: str, args: tuple[Any, ...]
) -> None:
    built = Built()
    assert getattr(built.host, method)(*args) is None
    assert built.app.calls == [(method, args)]


@pytest.mark.parametrize(
    ("method", "args"),
    [("register", ("Alice", "password")), ("login", ("password",))],
)
def test_going_online_returns_the_profile_and_is_logged(
    method: str, args: tuple[Any, ...], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="p08")
    built = Built()
    assert getattr(built.host, method)(*args) == PROFILE
    assert built.app.calls == [(method, args)]
    assert built.host.logged_in()
    assert has_event(caplog, "client.online")


def test_tick_does_nothing_while_logged_out() -> None:
    built = Built()
    assert built.host.tick() is None
    assert built.app.ticks == 0


def test_tick_returns_the_clients_report_while_logged_in() -> None:
    built = Built()
    built.host.login("password")
    assert built.host.tick() is REPORT


def test_a_failing_tick_is_logged_and_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    built = Built()
    built.host.login("password")
    built.app.tick_error = RuntimeError("disk on fire")
    assert built.host.tick() is None
    assert has_event(caplog, "client.tick_failed")


def test_the_loop_ticks_on_its_own_until_closed() -> None:
    built = Built(tick_interval_ms=1)
    built.host.login("password")
    deadline = time.monotonic() + 5
    while built.app.ticks < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert built.app.ticks >= 3
    built.host.close()
    after_close = built.app.ticks
    time.sleep(0.05)
    assert built.app.ticks == after_close
    assert not built.host.logged_in()


def test_close_without_a_loop_logs_out_and_is_idempotent() -> None:
    built = Built()
    built.host.login("password")
    built.host.close()
    built.host.close()
    assert not built.host.logged_in()


def test_the_lock_serialises_the_calls() -> None:
    built = Built()
    done = threading.Event()

    def call() -> None:
        built.host.profile()
        done.set()

    with built.locks[0]:
        threading.Thread(target=call, daemon=True).start()
        time.sleep(0.05)
        assert not done.is_set()  # waits for the lock
    assert done.wait(5)


def test_the_tick_interval_must_be_positive() -> None:
    with pytest.raises(ValueError):
        Built(tick_interval_ms=0)
