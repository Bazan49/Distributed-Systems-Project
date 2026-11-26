"""``RemoteClient``: the client actor of a Ray cluster, as a ``ClientApi``.

How the interface (``source/adapters/cli``) reaches a client that runs in a
Ray cluster. Every method is one remote call to the device's actor
(``ClientHost`` in ``ray_client.py``), waiting for its answer.

* An error raised INSIDE the actor (wrong password, invalid link...) reaches
  the driver wrapped in a ``RayTaskError``; it is unwrapped, so the interface
  sees the same exception as with a local client.
* If the actor is gone, or does not answer in time, the call raises
  ``ClientUnavailableError``.
"""

from __future__ import annotations

from typing import Any

import ray
from ray.exceptions import GetTimeoutError, RayActorError, RayTaskError

from source.adapters.cli.client_api import ClientUnavailableError
from source.application.use_cases.client_app import HistoryEntry, Profile, TickReport
from source.application.use_cases.contacts import AddResult, ContactView
from source.domain.contacts import ContactState
from source.domain.identifiers import UserId
from source.domain.messages import OutgoingMessage

__all__ = ["DEFAULT_CALL_TIMEOUT_S", "RemoteClient"]

# Logging in derives a key from the password (about half a second), and a
# send may wait for the quorum: generous, but not forever.
DEFAULT_CALL_TIMEOUT_S = 60.0


class RemoteClient:
    """The client of a device whose actor runs in a Ray cluster."""

    def __init__(self, handle: Any, timeout_s: float = DEFAULT_CALL_TIMEOUT_S) -> None:
        self._handle = handle
        self._timeout_s = timeout_s

    def _call(self, method: str, *args: Any) -> Any:
        try:
            ref = getattr(self._handle, method).remote(*args)
            return ray.get(ref, timeout=self._timeout_s)
        except RayActorError as exc:  # the actor died, or is being stopped
            raise ClientUnavailableError("the client actor is gone") from exc
        except GetTimeoutError as exc:
            raise ClientUnavailableError("the client actor does not answer") from exc
        except RayTaskError as exc:
            cause = getattr(exc, "cause", None)
            if isinstance(cause, Exception):
                raise cause from None  # the error the client itself raised
            raise

    # account
    def is_registered(self) -> bool:
        result: bool = self._call("is_registered")
        return result

    def logged_in(self) -> bool:
        result: bool = self._call("logged_in")
        return result

    def register(self, alias: str, password: str) -> Profile:
        result: Profile = self._call("register", alias, password)
        return result

    def login(self, password: str) -> Profile:
        result: Profile = self._call("login", password)
        return result

    def logout(self) -> None:
        self._call("logout")

    def profile(self) -> Profile:
        result: Profile = self._call("profile")
        return result

    # contacts
    def add_contact(self, link: str) -> AddResult:
        result: AddResult = self._call("add_contact", link)
        return result

    def contacts(self, state: ContactState | None = None) -> list[ContactView]:
        result: list[ContactView] = self._call("contacts", state)
        return result

    def pending_requests(self) -> list[ContactView]:
        result: list[ContactView] = self._call("pending_requests")
        return result

    def accept(self, user_id: UserId) -> ContactView:
        result: ContactView = self._call("accept", user_id)
        return result

    def reject(self, user_id: UserId) -> None:
        self._call("reject", user_id)

    def remove_contact(self, user_id: UserId) -> bool:
        result: bool = self._call("remove_contact", user_id)
        return result

    # conversations
    def send(self, to: UserId, body: str) -> OutgoingMessage:
        result: OutgoingMessage = self._call("send", to, body)
        return result

    def history(self, contact: UserId, limit: int = 50) -> list[HistoryEntry]:
        result: list[HistoryEntry] = self._call("history", contact, limit)
        return result

    # network
    def tick(self) -> TickReport | None:
        result: TickReport | None = self._call("tick")
        return result

    # changes
    def wait_for_change(self, after: int, timeout_s: float) -> int:
        """Block in the actor until something changes (or ``timeout_s`` passes).

        Ray waits a little longer than the actor, so an idle wait is not
        mistaken for an actor that does not answer.
        """
        try:
            ref = self._handle.wait_for_change.remote(after, timeout_s)
            result: int = ray.get(ref, timeout=timeout_s + self._timeout_s)
        except RayActorError as exc:
            raise ClientUnavailableError("the client actor is gone") from exc
        except GetTimeoutError as exc:
            raise ClientUnavailableError("the client actor does not answer") from exc
        return result
