"""A user played by a script, and real clients on in-memory adapters, for the CLI tests."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass

from source.adapters.cli.console import Wakeup
from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.adapters.memory.peer_transport import InMemoryPeerNetwork
from source.adapters.system.ray.ray_client import ClientHost
from source.application.use_cases.client_app import ClientApp
from source.domain.node_id import NodeId

PASSWORD = "contraseña larga"
NODES = [NodeId(f"node-{i}") for i in (1, 2, 3)]

REDRAW_WAIT_S = 5.0  # how long a test waits for a redraw that must come
NO_REDRAW_WAIT_S = 0.5  # how long it waits to be sure that none comes


@dataclass(frozen=True)
class Arrives:
    """In a conversation: ``action`` happens, and the screen MUST redraw itself."""

    action: Callable[[], object]


@dataclass(frozen=True)
class NothingNew:
    """In a conversation: ``action`` happens, and the screen must NOT redraw."""

    action: Callable[[], object]


Answer = str | None | Callable[[], str | None] | Arrives | NothingNew


class ScriptedConsole:
    """Answers every question with the next item of the script.

    An item may be a function: it runs when the question is asked (to make
    another user act at that moment) and its result is the answer. When the
    script ends, the next question raises ``EOFError``, like Ctrl+D.

    In a conversation, ``Arrives`` and ``NothingNew`` play what happens while
    the user is looking at the screen without typing: they are not answers,
    the next item is. ``redraws`` counts the automatic redraws.
    """

    def __init__(self, *answers: Answer) -> None:
        self._answers = list(answers)
        self.lines: list[str] = []
        self.prompts: list[str] = []
        self.redraws = 0

    def _next(self, prompt: str) -> str | None:
        self.prompts.append(prompt)
        if not self._answers:
            raise EOFError
        answer = self._answers.pop(0)
        if isinstance(answer, Arrives | NothingNew):
            raise AssertionError("Arrives/NothingNew only while in a conversation")
        return answer() if callable(answer) else answer

    def ask(self, prompt: str) -> str:
        answer = self._next(prompt)
        if answer is None:
            raise AssertionError("Esc can only be used in a conversation")
        return answer

    def ask_or_escape(
        self,
        prompt: str,
        *,
        wakeup: Wakeup | None = None,
        on_wakeup: Callable[[], None] | None = None,
    ) -> str | None:
        while self._answers and isinstance(self._answers[0], Arrives | NothingNew):
            event = self._answers.pop(0)
            assert isinstance(event, Arrives | NothingNew)
            assert wakeup is not None and on_wakeup is not None
            event.action()
            expected = isinstance(event, Arrives)
            woken = wakeup.wait(REDRAW_WAIT_S if expected else NO_REDRAW_WAIT_S)
            assert woken == expected, (
                "the screen did not redraw itself"
                if expected
                else "it redrew for nothing"
            )
            if woken:
                wakeup.clear()
                self.redraws += 1
                on_wakeup()
        return self._next(prompt)

    def secret(self, prompt: str) -> str:
        answer = self._next(prompt)
        if answer is None:
            raise AssertionError("Esc cannot be used for a secret")
        return answer

    def show(self, text: str = "") -> None:
        self.lines.append(text)

    @property
    def output(self) -> str:
        return "\n".join(self.lines)

    @property
    def unused(self) -> int:
        """Answers never asked for: a script that does not match the screens."""
        return len(self._answers)


class World:
    """Real clients (``ClientHost`` over ``ClientApp``) sharing an in-memory cluster."""

    def __init__(self) -> None:
        self.clock = ManualClock()
        self.crypto = FakeCrypto()
        self.gateway = InMemoryNodeGateway(NODES, self.clock, self.crypto)
        self.network = InMemoryPeerNetwork()

    def device(self) -> ClientHost:
        """A new device without an account. No loop: the CLI ticks it."""
        store = MemoryLocalStore()
        return ClientHost(
            lambda _lock: ClientApp(
                crypto=self.crypto,
                store=store,
                clock=self.clock,
                gateway=self.gateway,
                peers_for=self.network.transport_for,
                rng=random.Random(0),
            ),
            tick_interval_ms=None,
        )

    def user(self, alias: str) -> ClientHost:
        """A device with an account, logged in."""
        host = self.device()
        host.register(alias, PASSWORD)
        return host


def befriend(a: ClientHost, b: ClientHost) -> None:
    """``a`` asks, ``b`` accepts: both are active contacts."""
    a.add_contact(b.profile().link)
    b.accept(a.profile().user_id)
