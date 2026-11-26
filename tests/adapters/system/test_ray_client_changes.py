"""``ClientHost.wait_for_change``: block until the client reports a change."""

from __future__ import annotations

import threading
import time

from tests.adapters.system.test_ray_client import Built


def test_it_answers_at_once_with_the_current_count() -> None:
    built = Built()
    assert built.host.wait_for_change(-1, 10) == 0
    built.app.listener()
    assert built.host.wait_for_change(-1, 10) == 1


def test_without_changes_it_returns_the_same_count_after_the_timeout() -> None:
    built = Built()
    started = time.monotonic()
    assert built.host.wait_for_change(0, 0.2) == 0
    assert time.monotonic() - started >= 0.2


def test_a_change_wakes_a_waiting_caller_at_once() -> None:
    built = Built()
    got: list[int] = []
    waiter = threading.Thread(
        target=lambda: got.append(built.host.wait_for_change(0, 30))
    )
    waiter.start()
    time.sleep(0.1)
    started = time.monotonic()

    built.app.listener()  # the app reports a change
    waiter.join(5)

    assert got == [1]
    assert time.monotonic() - started < 5  # not after the 30 s timeout


def test_waiting_does_not_hold_the_client_lock() -> None:
    built = Built()
    waiter = threading.Thread(target=built.host.wait_for_change, args=(0, 1.0))
    waiter.start()
    time.sleep(0.1)
    built.host.is_registered()  # goes through while the wait is still blocked
    assert waiter.is_alive()
    assert built.app.calls == [("is_registered", ())]
    waiter.join(5)
