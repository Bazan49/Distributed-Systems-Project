"""Launcher: start N service nodes and M clients from a configuration file.

Usage::

    python -m source.composition.launcher cluster.json

It starts every node, then every client, prints ``READY`` and waits. Ctrl+C
(SIGINT) or SIGTERM stops everything: clients first, then nodes.

Configuration file (JSON)
=========================

.. code-block:: json

    {
      "namespace": "mensajeria",
      "data_dir": "data/cluster",
      "nodes": ["node-1", "node-2", "node-3"],
      "seeds": ["node-1"],
      "clients": [{"id": "alice"}, {"id": "bob", "options": {"kdf": "fast"}}],
      "client_factory": "source.composition.launcher:start_ray_device",
      "ray_address": null,
      "pin_nodes": false
    }

=================  ========  ===================================================
Key                Required  Meaning
=================  ========  ===================================================
``nodes``          yes       Ids of the service nodes to start (at least one).
``namespace``      no        Ray namespace of every actor (default ``mensajeria``).
``data_dir``       no        Folder of the SQLite files, relative to the config
                             file (default ``data``). Node ``x`` uses
                             ``<data_dir>/nodes/x/``, client ``y`` uses
                             ``<data_dir>/clients/y/``.
``seeds``          no        Initial nodes the clients are told about (default:
                             every node). Must be nodes of the list.
``clients``        no        Clients to start: ``id`` (1 to 32 of ``a-z``,
                             ``0-9``, ``-``) and optional ``options`` (passed
                             as is to the client factory).
``client_factory`` no        ``module:function`` that starts ONE client actor
                             (see below). Default: ``start_ray_device``, the
                             real client actor.
``ray_address``    no        Address of an existing Ray cluster; ``null`` starts
                             a local one, ``"auto"`` joins the one this
                             machine belongs to (Docker).
``pin_nodes``      no        ``true``: node ``x`` only runs on the Ray machine
                             that offers the resource ``service-node-x``, the
                             one that holds its files (Docker). Default false.
=================  ========  ===================================================

Clients
=======

The launcher calls the client factory once per client::

    factory(client_id, data_dir, *, seeds=[...], namespace="...", **options)
        -> Ray actor handle, registered as client_actor_name(client_id)

The default, ``start_ray_device``, starts the real client actor
(``ClientHost`` over ``ClientApp``): a device called ``client_id`` whose
SQLite store is ``<data_dir>/<client_id>.db``. Its options are ``kdf``
(``"secure"``, the default, or ``"fast"``, only for tests),
``tick_interval_ms`` (how often its loop runs) and ``log_dir`` (folder of its
log; default ``P08_LOG_DIR`` or ``logs/``). The device starts logged out:
whoever drives it calls ``register`` (first time) or ``login``.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import json
import re
import signal
import sys
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import ray
from ray.exceptions import GetTimeoutError, RayError

from source.adapters.system.ray.ray_client import (
    DEFAULT_TICK_INTERVAL_MS,
    DEVICE_ACTOR_PREFIX,
    device_actor_name,
)
from source.adapters.system.ray_node import node_resource, start_service_node
from source.composition.client import ClientSettings, KdfStrength
from source.composition.ray_network import start_ray_client
from source.domain.node_id import NodeId

DEFAULT_NAMESPACE = "mensajeria"
DEFAULT_DATA_DIR = "data"
DEFAULT_CLIENT_FACTORY = "source.composition.launcher:start_ray_device"
CLIENT_ACTOR_PREFIX = DEVICE_ACTOR_PREFIX
DEFAULT_READY_TIMEOUT_MS = 60_000
DEFAULT_STOP_TIMEOUT_MS = 5_000

_CLIENT_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,31}")  # also a valid device name
_KEYS = {
    "namespace",
    "data_dir",
    "nodes",
    "seeds",
    "clients",
    "client_factory",
    "ray_address",
    "pin_nodes",
}

ClientFactory = Callable[..., Any]


class ConfigError(ValueError):
    """The configuration file is missing, is not valid JSON or breaks a rule."""


class LaunchError(RuntimeError):
    """Something could not be started. Whatever had started was stopped again."""


def client_actor_name(client_id: str) -> str:
    """Name of the actor of a client: the client actor's device name."""
    return device_actor_name(client_id)


# ============================ configuration ============================
@dataclass(frozen=True)
class ClientSpec:
    client_id: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LaunchConfig:
    nodes: tuple[NodeId, ...]
    seeds: tuple[NodeId, ...]
    clients: tuple[ClientSpec, ...]
    namespace: str
    data_dir: Path
    client_factory: str
    ray_address: str | None
    pin_nodes: bool = False

    def node_dir(self) -> Path:
        return self.data_dir / "nodes"

    def client_dir(self, client_id: str) -> Path:
        return self.data_dir / "clients" / client_id


def _str(value: Any, what: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{what} must be a non-empty string")
    return value


def _node_ids(value: Any, what: str) -> tuple[NodeId, ...]:
    if not isinstance(value, list):
        raise ConfigError(f"{what} must be a list of node ids")
    try:
        nodes = tuple(NodeId(v) for v in value)
    except ValueError as exc:
        raise ConfigError(f"{what}: {exc}") from exc
    if len(set(nodes)) != len(nodes):
        raise ConfigError(f"{what} has repeated node ids")
    return nodes


def _clients(value: Any) -> tuple[ClientSpec, ...]:
    if not isinstance(value, list):
        raise ConfigError("clients must be a list")
    specs = []
    for entry in value:
        if not isinstance(entry, dict) or not set(entry) <= {"id", "options"}:
            raise ConfigError('each client must be {"id": ..., "options": {...}}')
        client_id = entry.get("id")
        if not isinstance(client_id, str) or not _CLIENT_ID.fullmatch(client_id):
            raise ConfigError(
                f"client id {client_id!r} must match [a-z0-9][a-z0-9-]{{0,31}}"
            )
        options = entry.get("options", {})
        if not isinstance(options, dict):
            raise ConfigError(f"options of client {client_id} must be an object")
        specs.append(ClientSpec(client_id, dict(options)))
    if len({s.client_id for s in specs}) != len(specs):
        raise ConfigError("clients has repeated ids")
    return tuple(specs)


def parse_config(raw: Any, base_dir: Path) -> LaunchConfig:
    """Validate a decoded configuration. Relative ``data_dir`` is taken from ``base_dir``.

    Raises:
        ConfigError: with a message that says what is wrong.
    """
    if not isinstance(raw, dict):
        raise ConfigError("the configuration must be a JSON object")
    unknown = set(raw) - _KEYS
    if unknown:
        raise ConfigError(f"unknown keys: {sorted(unknown)}")
    if "nodes" not in raw:
        raise ConfigError("nodes is required")
    nodes = _node_ids(raw["nodes"], "nodes")
    if not nodes:
        raise ConfigError("nodes must have at least one node")
    seeds = _node_ids(raw["seeds"], "seeds") if "seeds" in raw else nodes
    if not seeds or not set(seeds) <= set(nodes):
        raise ConfigError("seeds must be a non-empty subset of nodes")
    data_dir = Path(_str(raw.get("data_dir", DEFAULT_DATA_DIR), "data_dir"))
    factory = _str(raw.get("client_factory", DEFAULT_CLIENT_FACTORY), "client_factory")
    if factory.count(":") != 1:
        raise ConfigError('client_factory must look like "package.module:function"')
    address = raw.get("ray_address")
    if address is not None:
        address = _str(address, "ray_address")
    pin_nodes = raw.get("pin_nodes", False)
    if not isinstance(pin_nodes, bool):
        raise ConfigError("pin_nodes must be true or false")
    return LaunchConfig(
        nodes=nodes,
        seeds=seeds,
        clients=_clients(raw.get("clients", [])),
        namespace=_str(raw.get("namespace", DEFAULT_NAMESPACE), "namespace"),
        data_dir=data_dir if data_dir.is_absolute() else base_dir / data_dir,
        client_factory=factory,
        ray_address=address,
        pin_nodes=pin_nodes,
    )


def load_config(path: str | Path) -> LaunchConfig:
    """Read and validate a configuration file.

    Raises:
        ConfigError: the file cannot be read, is not JSON, or is not valid.
    """
    path = Path(path)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{path} is not valid JSON: {exc}") from exc
    return parse_config(raw, path.resolve().parent)


def resolve_factory(spec: str) -> ClientFactory:
    """Import ``module:function``.

    Raises:
        LaunchError: the module or the function does not exist.
    """
    module_name, _, function_name = spec.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise LaunchError(f"cannot import the client factory {spec!r} ({exc})") from exc
    factory = getattr(module, function_name, None)
    if not callable(factory):
        raise LaunchError(f"{spec!r} is not a function")
    return factory  # type: ignore[no-any-return]


# ============================ the running cluster ============================
def wait_ready(handles: dict[str, Any], timeout_ms: int) -> None:
    """Wait until every actor has run its ``__init__`` (``__ray_ready__``)."""
    refs = {handle.__ray_ready__.remote(): name for name, handle in handles.items()}
    try:
        ray.get(list(refs), timeout=timeout_ms / 1000)
    except GetTimeoutError as exc:
        raise LaunchError(
            f"not ready after {timeout_ms} ms: {sorted(handles)}"
        ) from exc
    except RayError as exc:  # an __init__ raised (bad data dir, broken database...)
        raise LaunchError(f"an actor failed to start: {exc}") from exc


def start_node(config: LaunchConfig, node: NodeId) -> Any:
    """Start (or find) the actor of ``node`` with the configuration's settings.

    Always over the same folder, so a node started again finds its data.
    """
    return start_service_node(
        node.value,
        config.node_dir(),
        namespace=config.namespace,
        resources={node_resource(node.value): 1} if config.pin_nodes else None,
    )


def start_client(config: LaunchConfig, spec: ClientSpec, factory: ClientFactory) -> Any:
    """Start the actor of one client through the factory (always the same arguments)."""
    return factory(
        spec.client_id,
        str(config.client_dir(spec.client_id)),
        seeds=[s.value for s in config.seeds],
        namespace=config.namespace,
        **spec.options,
    )


def start_ray_device(
    client_id: str,
    data_dir: str,
    *,
    seeds: Sequence[str],
    namespace: str,
    **options: Any,
) -> Any:
    """The default client factory: the real client actor of device ``client_id``.

    Its store is ``<data_dir>/<client_id>.db``. Options: ``kdf`` (``"secure"``
    or ``"fast"``), ``tick_interval_ms`` and ``log_dir``.

    Raises:
        LaunchError: an unknown option or a wrong value.
    """
    unknown = set(options) - {"kdf", "tick_interval_ms", "log_dir"}
    if unknown:
        raise LaunchError(f"unknown options for client {client_id}: {sorted(unknown)}")
    try:
        kdf = KdfStrength(options.get("kdf", KdfStrength.SECURE.value))
    except ValueError as exc:
        raise LaunchError(f"kdf of client {client_id} must be secure or fast") from exc
    tick = options.get("tick_interval_ms", DEFAULT_TICK_INTERVAL_MS)
    if isinstance(tick, bool) or not isinstance(tick, int) or tick < 1:
        raise LaunchError(f"tick_interval_ms of client {client_id} must be >= 1")
    log_dir = options.get("log_dir")
    if log_dir is not None and (not isinstance(log_dir, str) or not log_dir):
        raise LaunchError(f"log_dir of client {client_id} must be a folder name")
    return start_ray_client(
        client_id,
        seeds,
        namespace=namespace,
        settings=ClientSettings(data_dir=Path(data_dir), kdf=kdf),
        log_dir=Path(log_dir) if log_dir is not None else None,
        tick_interval_ms=tick,
    )


def _no_clients(*_args: Any, **_kwargs: Any) -> Any:  # pragma: no cover
    raise LaunchError("no client factory")


def stop_actor(handle: Any, timeout_ms: int) -> None:
    """Let the actor close its databases if it can, then kill it for good."""
    for method in ("close", "stop"):
        if hasattr(handle, method):
            # Dead already, or failed to close: killing it is enough.
            with contextlib.suppress(RayError):
                ray.get(getattr(handle, method).remote(), timeout=timeout_ms / 1000)
            break
    ray.kill(handle, no_restart=True)


@dataclass
class RunningCluster:
    """The actors started by ``launch``. Stop it with ``stop()``."""

    config: LaunchConfig
    client_factory: ClientFactory = _no_clients
    nodes: dict[NodeId, Any] = field(default_factory=dict)  # running nodes only
    clients: dict[str, Any] = field(default_factory=dict)  # running clients only

    def stop(self, timeout_ms: int = DEFAULT_STOP_TIMEOUT_MS) -> None:
        """Stop the clients, then the nodes. Safe to call more than once.

        Clients go first: while they stop they may still talk to the nodes.
        Each actor may close its databases; then it is killed WITHOUT restart.
        Its files stay on disk, so launching again picks the data up.
        """
        for client_id in list(self.clients):
            stop_actor(self.clients.pop(client_id), timeout_ms)
        for node in list(self.nodes):
            stop_actor(self.nodes.pop(node), timeout_ms)

    def summary(self) -> str:
        nodes = ", ".join(n.value for n in self.nodes)
        clients = ", ".join(client_actor_name(c) for c in self.clients)
        return f"namespace={self.config.namespace} nodes=[{nodes}] clients=[{clients}]"


def launch(
    config: LaunchConfig,
    *,
    ready_timeout_ms: int = DEFAULT_READY_TIMEOUT_MS,
) -> RunningCluster:
    """Start every node, wait until they are up, then every client.

    All or nothing: if anything fails, whatever was started is stopped again
    and ``LaunchError`` is raised. Connects to Ray if it is not connected yet.
    """
    factory: ClientFactory = (
        resolve_factory(config.client_factory) if config.clients else _no_clients
    )
    if not ray.is_initialized():
        ray.init(
            address=config.ray_address,
            namespace=config.namespace,
            include_dashboard=False,
            log_to_driver=False,
        )
    cluster = RunningCluster(config, client_factory=factory)
    try:
        for node in config.nodes:
            cluster.nodes[node] = start_node(config, node)
        wait_ready({n.value: h for n, h in cluster.nodes.items()}, ready_timeout_ms)

        for spec in config.clients:
            cluster.clients[spec.client_id] = start_client(config, spec, factory)
        wait_ready(cluster.clients, ready_timeout_ms)
    except BaseException:
        cluster.stop()
        raise
    return cluster


# ============================ command line ============================
def main(argv: Sequence[str] | None = None) -> int:
    """``python -m source.composition.launcher CONFIG``: start, wait, stop on a signal."""
    parser = argparse.ArgumentParser(
        prog="python -m source.composition.launcher",
        description="Start the service nodes and clients of a configuration file.",
    )
    parser.add_argument("config", help="path of the JSON configuration file")
    parser.add_argument(
        "--ready-timeout-ms",
        type=int,
        default=DEFAULT_READY_TIMEOUT_MS,
        help="how long to wait for every actor to start (Docker workers may "
        "join the cluster a bit later)",
    )
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
        cluster = launch(config, ready_timeout_ms=args.ready_timeout_ms)
    except (ConfigError, LaunchError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    stop_requested = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        stop_requested.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    print(f"READY {cluster.summary()}", flush=True)
    print("Press Ctrl+C to stop.", flush=True)
    while not stop_requested.wait(0.5):  # short waits: signals are handled at once
        pass

    print("STOPPING", flush=True)
    cluster.stop()
    ray.shutdown()
    print("STOPPED", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
