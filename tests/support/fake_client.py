"""A stand-in for the client actor of task 2.5, used to test the launcher (2.7).

It follows the agreed signature of the client factory::

    start_fake_client(client_id, data_dir, *, seeds, namespace, **options)
        -> handle of an actor registered as "cliente-<client_id>"

When the real client actor exists, the launcher tests can use it instead.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import ray

from source.composition.launcher import client_actor_name


@ray.remote
class FakeClientActor:
    def __init__(
        self, client_id: str, data_dir: str, seeds: list[str], options: dict[str, Any]
    ) -> None:
        Path(data_dir).mkdir(parents=True, exist_ok=True)  # like a real local store
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
