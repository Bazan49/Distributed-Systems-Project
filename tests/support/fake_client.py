"""A stand-in for the client actor of task 2.5, used to test the launcher (2.7).

It follows the agreed signature of the client factory::

    start_fake_client(client_id, data_dir, *, seeds, namespace, **options)
        -> handle of an actor registered as "cliente-<client_id>"

When the real client actor exists, the launcher tests can use it instead.

To let tests see connections, it appends to ``<data_dir>/events.log`` one line
``connect`` each time it starts and ``close`` each time it is closed cleanly.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import ray

from source.composition.launcher import client_actor_name

EVENTS_LOG = "events.log"


def events_of(data_dir: str | Path) -> list[str]:
    """What the fake client recorded in ``data_dir`` (connections and closes)."""
    log = Path(data_dir) / EVENTS_LOG
    return log.read_text(encoding="utf-8").split() if log.exists() else []


@ray.remote
class FakeClientActor:
    def __init__(
        self, client_id: str, data_dir: str, seeds: list[str], options: dict[str, Any]
    ) -> None:
        Path(data_dir).mkdir(parents=True, exist_ok=True)  # like a real local store
        self._log = Path(data_dir) / EVENTS_LOG
        self._record("connect")
        self._description = {
            "client_id": client_id,
            "data_dir": data_dir,
            "seeds": seeds,
            "options": options,
        }
        self.closed = False

    def describe(self) -> dict[str, Any]:
        return self._description

    def close(self) -> None:
        self.closed = True
        self._record("close")

    def _record(self, event: str) -> None:
        with self._log.open("a", encoding="utf-8") as log:
            log.write(event + "\n")


def start_fake_client(
    client_id: str,
    data_dir: str,
    *,
    seeds: list[str],
    namespace: str,
    **options: Any,
) -> Any:
    return FakeClientActor.options(  # type: ignore[attr-defined]
        name=client_actor_name(client_id), namespace=namespace
    ).remote(client_id, data_dir, seeds, options)


def broken_factory(client_id: str, data_dir: str, **_: Any) -> Any:
    """A factory that fails, to test that the launcher rolls back."""
    raise RuntimeError(f"cannot start {client_id}")
