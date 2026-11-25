"""Clients on Ray: the deployment a client joins, and how its actor builds it.

``RayNetwork`` is the ``Network`` of a Ray cluster: the service nodes are
reached through ``RayNodeGateway`` and other clients through
``RayPeerTransport``.

``RayClientFactory`` is what a client actor receives (``start_client``) to
build its ``ClientApp`` inside its own process: it configures that process's
log, finds the nodes from the initial ones, and wires the real store,
cryptography and clock with ``build_client``. It holds only plain,
picklable settings, because Ray ships it to the actor's process.

    handle = start_ray_client("alice", seeds=["n1", "n2", "n3"], namespace="p08")
    ray.get(handle.register.remote("Alice", "a long password"))
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from source.adapters.system.clock import SystemClock
from source.adapters.system.ray.ray_client import (
    DEFAULT_TICK_INTERVAL_MS,
    start_client,
)
from source.adapters.system.ray.ray_peer_transport import RayPeerTransport
from source.adapters.system.ray.ray_gateway import RayNodeGateway
from source.application.ports.node_gateway import NodeGatewayPort
from source.application.ports.peer_transport import PeerTransportPort
from source.application.use_cases.client_app import ClientApp
from source.composition.client import (
    ClientSettings,
    build_client,
    make_crypto,
    make_store,
    valid_device_name,
)
from source.composition.logging_setup import configure_logging
from source.domain.identifiers import UserId
from source.domain.node_id import NodeId

__all__ = ["RayClientFactory", "RayNetwork", "start_ray_client"]


class RayNetwork:
    """A Ray cluster, as one client sees it.

    Args:
        gateway: the client's connection to the service nodes.
        namespace: Ray namespace shared by the nodes and every client.
        guard: lock held while a direct envelope is handed to the client.
    """

    def __init__(
        self,
        gateway: NodeGatewayPort,
        namespace: str | None = None,
        guard: threading.RLock | None = None,
    ) -> None:
        self._gateway = gateway
        self._namespace = namespace
        self._guard = guard

    @property
    def gateway(self) -> NodeGatewayPort:
        return self._gateway

    def peers_for(self, user_id: UserId) -> PeerTransportPort:
        return RayPeerTransport(user_id, self._namespace, self._guard)


@dataclass(frozen=True)
class RayClientFactory:
    """Builds the ``ClientApp`` of one device inside its actor.

    Attributes:
        device: the device's name (its store file and its log file).
        seeds: ids of the initial service nodes, from the configuration.
        namespace: Ray namespace shared by the nodes and every client.
        settings: store folder, password-derivation cost and client policy.
        log_dir: folder of the log; ``None`` = ``P08_LOG_DIR`` or ``logs/``.
    """

    device: str
    seeds: tuple[str, ...]
    namespace: str | None = None
    settings: ClientSettings = field(default_factory=ClientSettings)
    log_dir: Path | None = None

    def __post_init__(self) -> None:
        if not valid_device_name(self.device):
            raise ValueError(
                "a device name is 1 to 32 lowercase letters, digits, '-' or '_', "
                "starting with a letter or digit"
            )
        if not self.seeds:
            raise ValueError("at least one initial service node is needed")
        for seed in self.seeds:
            NodeId(seed)  # ValueError if it is not a valid node id

    def __call__(self, guard: threading.RLock) -> ClientApp:
        configure_logging(
            process=f"client-{self.device}", log_dir=self.log_dir, device=self.device
        )
        gateway = RayNodeGateway.from_seeds(
            [NodeId(seed) for seed in self.seeds], self.namespace
        )
        return build_client(
            network=RayNetwork(gateway, self.namespace, guard),
            store=make_store(self.settings, self.device),
            crypto=make_crypto(self.settings.kdf),
            clock=SystemClock(),
            settings=self.settings,
        )


def start_ray_client(
    device: str,
    seeds: Sequence[str],
    *,
    namespace: str | None = None,
    settings: ClientSettings | None = None,
    log_dir: Path | None = None,
    tick_interval_ms: int | None = DEFAULT_TICK_INTERVAL_MS,
) -> Any:
    """Start (or find) the client actor of ``device`` in the Ray cluster.

    Ray must already be initialised (``ray.init``), and the service nodes
    ``seeds`` must be running in ``namespace``.

    Returns:
        The Ray actor handle of the client.
    """
    factory = RayClientFactory(
        device=device,
        seeds=tuple(seeds),
        namespace=namespace,
        settings=settings if settings is not None else ClientSettings(),
        log_dir=log_dir,
    )
    return start_client(
        device, factory, namespace=namespace, tick_interval_ms=tick_interval_ms
    )
