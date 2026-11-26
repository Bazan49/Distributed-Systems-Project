"""The client as a Ray actor.

One device = one actor. It hosts a ``ClientApp`` in its own process and runs
the client's main loop there, like an app on a phone:

* it **ticks on its own**: a background thread calls ``ClientApp.tick()``
  every ``tick_interval_ms`` while someone is logged in. That renews the
  presence, delivers what is due and, right after logging in, collects from
  the mailboxes (``ClientApp`` collects on its first tick);
* it **receives direct envelopes**: on login ``ClientApp`` starts listening
  on its peer transport. With Ray (``RayPeerTransport``) that registers the
  user's inbox as ``client-<user_id>``, so other client actors can reach it
  by name, and its envelopes end up in this client's history;
* it **answers the interface**: register, log in, add contacts, send, read
  the history... each one a remote call, returning plain values.

Names: the actor is ``client-device-<device>``. It cannot be named after the
user, because a Ray name is fixed when the actor is created and the user id
only exists after ``register`` or ``login``. The user's name,
``client-<user_id>``, belongs to the inbox, which lives while the user is
listening.

Two layers:

* ``ClientHost`` is a plain class with all the behaviour, testable without Ray;
* ``ClientActor`` is the same class as a Ray actor, and ``start_client``
  creates it with the right options.

The host does not build the client: it receives a factory, called once inside
the actor's process, that builds it (stores, cryptography, connections to the
nodes) and receives the host's lock to protect the client from the threads of
the transport. Wiring is the composition root's job, not an adapter's.

Changes: ``wait_for_change`` lets an interface wait until a conversation may
look different (a message stored, a sent message changing state), instead of
asking every second. The host counts the app's change notifications; the call
blocks until the count passes the one the caller already saw. It does not
take the client's lock, so it never holds up the other calls.

Concurrency: ``ClientApp`` is not thread-safe. Every entry (the loop, remote
calls and the transport's listening thread) holds the same lock, while the
actor accepts several calls at once (``max_concurrency``) so that a call
waits for the lock instead of queueing behind the whole actor.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import ray

from source.application.observability import get_logger
from source.application.use_cases.client_app import (
    ClientApp,
    HistoryEntry,
    Profile,
    TickReport,
)
from source.application.use_cases.contacts import AddResult, ContactView
from source.domain.contacts import ContactState
from source.domain.identifiers import UserId
from source.domain.messages import OutgoingMessage

__all__ = [
    "DEFAULT_MAX_CONCURRENCY",
    "DEFAULT_TICK_INTERVAL_MS",
    "DEVICE_ACTOR_PREFIX",
    "AppFactory",
    "ClientActor",
    "ClientHost",
    "device_actor_name",
    "start_client",
]

log = get_logger(__name__)

DEVICE_ACTOR_PREFIX = "client-device-"
DEFAULT_TICK_INTERVAL_MS = 500
DEFAULT_MAX_CONCURRENCY = 4

AppFactory = Callable[[threading.RLock], ClientApp]
"""Builds the client inside the actor; receives the lock that serialises it."""


def device_actor_name(device: str) -> str:
    """Name under which the actor of ``device`` is registered in Ray."""
    return f"{DEVICE_ACTOR_PREFIX}{device}"


class ClientHost:
    """One device's client, its lock and its main loop, without Ray.

    Args:
        make_app: builds the ``ClientApp``; called once, here.
        tick_interval_ms: how often the loop ticks. ``None`` = no loop: the
            caller ticks with ``tick()`` (tests do, to stay deterministic).
    """

    def __init__(
        self,
        make_app: AppFactory,
        tick_interval_ms: int | None = DEFAULT_TICK_INTERVAL_MS,
    ) -> None:
        if tick_interval_ms is not None and tick_interval_ms < 1:
            raise ValueError(
                "tick_interval_ms must be a positive number of milliseconds"
            )
        self._lock = threading.RLock()
        self._changes = threading.Condition()  # never held with self._lock
        self._version = 0
        self._app = make_app(self._lock)
        self._app.add_change_listener(self._on_change)
        self._stop = threading.Event()
        self._loop: threading.Thread | None = None
        if tick_interval_ms is not None:
            self._loop = threading.Thread(
                target=self._run, args=(tick_interval_ms / 1000,), daemon=True
            )
            self._loop.start()

    # ---- changes ----
    def _on_change(self) -> None:
        with self._changes:
            self._version += 1
            self._changes.notify_all()

    def wait_for_change(self, after: int, timeout_s: float) -> int:
        """Block until the change count is higher than ``after``; return the count.

        Returns at once if it already is (``after=-1`` gives the current
        count), and after ``timeout_s`` if nothing changed: then the result is
        ``after`` itself. Waiting again with the result never misses a change.
        """
        with self._changes:
            self._changes.wait_for(lambda: self._version > after, timeout=timeout_s)
            return self._version

    # ---- the main loop ----
    def _run(self, interval_s: float) -> None:
        while not self._stop.wait(interval_s):
            self.tick()

    def tick(self) -> TickReport | None:
        """One round of periodic work; None if nobody is logged in.

        Never raises: a failing round is logged and the next one tries again,
        as the loop must survive a store or network error.
        """
        with self._lock:
            if not self._app.logged_in:
                return None
            try:
                return self._app.tick()
            except Exception:
                log.exception("client.tick_failed")
                return None

    def close(self) -> None:
        """Stop the loop and go offline (the inbox disappears). Idempotent."""
        self._stop.set()
        if self._loop is not None and self._loop is not threading.current_thread():
            self._loop.join()
        with self._lock:
            self._app.logout()

    # ---- the client's API, one call at a time ----
    def is_registered(self) -> bool:
        with self._lock:
            return self._app.is_registered()

    def logged_in(self) -> bool:
        with self._lock:
            return self._app.logged_in

    def register(self, alias: str, password: str) -> Profile:
        with self._lock:
            return self._app.register(alias, password)

    def login(self, password: str) -> Profile:
        with self._lock:
            return self._app.login(password)

    def logout(self) -> None:
        with self._lock:
            self._app.logout()

    def profile(self) -> Profile:
        with self._lock:
            return self._app.profile()

    def add_contact(self, link: str) -> AddResult:
        with self._lock:
            return self._app.add_contact(link)

    def contacts(self, state: ContactState | None = None) -> list[ContactView]:
        with self._lock:
            return self._app.contacts(state)

    def pending_requests(self) -> list[ContactView]:
        with self._lock:
            return self._app.pending_requests()

    def accept(self, user_id: UserId) -> ContactView:
        with self._lock:
            return self._app.accept(user_id)

    def reject(self, user_id: UserId) -> None:
        with self._lock:
            self._app.reject(user_id)

    def remove_contact(self, user_id: UserId) -> bool:
        with self._lock:
            return self._app.remove_contact(user_id)

    def send(self, to: UserId, body: str) -> OutgoingMessage:
        with self._lock:
            return self._app.send(to, body)

    def history(self, contact: UserId, limit: int = 50) -> list[HistoryEntry]:
        with self._lock:
            return self._app.history(contact, limit)


# The Ray actor. Not restarted: a device that crashes is a phone that turned
# off. It is started again by whoever runs it, and finds its identity and
# history in its local store.
ClientActor = ray.remote(max_restarts=0, max_task_retries=0)(ClientHost)


def start_client(
    device: str,
    make_app: AppFactory,
    *,
    namespace: str | None = None,
    tick_interval_ms: int | None = DEFAULT_TICK_INTERVAL_MS,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
) -> Any:
    """Create (or find, if it already exists) the actor of ``device``.

    The actor is registered under ``device_actor_name(device)`` in
    ``namespace`` (``None`` = the current job's namespace). If it already
    exists it is returned as is and the other arguments are ignored.

    Returns:
        The Ray actor handle (``ray.actor.ActorHandle``).
    """
    if namespace is None:
        namespace = ray.get_runtime_context().namespace
    return ClientActor.options(
        name=device_actor_name(device),
        namespace=namespace,
        max_concurrency=max_concurrency,
        get_if_exists=True,
    ).remote(make_app, tick_interval_ms)
