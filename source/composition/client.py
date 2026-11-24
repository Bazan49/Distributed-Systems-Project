"""Build a ``ClientApp`` from settings: the client's composition root.

A client needs two kinds of things:

* **Local to the device**: its store (in memory, or a SQLite file), its
  cryptography and a clock. ``ClientSettings`` chooses them.
* **The network it joins**: the service nodes (a ``NodeGatewayPort``) and the
  way to reach other clients directly (``peers_for``). That is a ``Network``.

``Network`` is the extension point between deployments. Today it is
implemented by ``DemoNetwork`` (every client and node in one process, in
memory). In stage 2 a Ray implementation provides the same two members, and
nothing else changes: not ``ClientApp``, not the CLI. That is what makes
"one console per user" possible: each console is a process that builds its
client over the shared Ray cluster.

This package is the only place allowed to know every layer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Protocol

from nacl import pwhash

from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.system.crypto import NaClCrypto
from source.adapters.system.sqlite.sqlite_local_store import SqliteLocalStore
from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.local_store import LocalStorePort
from source.application.ports.node_gateway import NodeGatewayPort
from source.application.ports.peer_transport import PeerTransportPort
from source.application.use_cases.client_app import ClientApp, ClientPolicy
from source.domain.identifiers import UserId

__all__ = [
    "ClientSettings",
    "KdfStrength",
    "Network",
    "build_client",
    "make_crypto",
    "make_store",
    "valid_device_name",
]

_DEVICE_NAME = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")


class Network(Protocol):
    """What a client needs from the deployment it joins."""

    @property
    def gateway(self) -> NodeGatewayPort:
        """How this client talks to the service nodes."""
        ...

    def peers_for(self, user_id: UserId) -> PeerTransportPort:
        """The direct transport of the client logged in as ``user_id``."""
        ...


class KdfStrength(str, Enum):
    """Cost of deriving the key that protects the private key (Argon2id)."""

    SECURE = "secure"
    """libsodium's moderate profile: about half a second and 256 MiB per login.
    What a real device should use: it makes guessing passwords expensive."""
    FAST = "fast"
    """The minimum cost. Only for tests and quick demos, never for real data."""


@dataclass(frozen=True)
class ClientSettings:
    """Device-local choices for a client.

    Attributes:
        data_dir: folder for the devices' SQLite files (``<name>.db``). None
            keeps everything in memory: it is lost when the process ends.
        kdf: cost of protecting the private key with the password.
        policy: how often the client does its periodic network work.
    """

    data_dir: Path | None = None
    kdf: KdfStrength = KdfStrength.SECURE
    policy: ClientPolicy = field(default_factory=ClientPolicy)


def valid_device_name(name: str) -> bool:
    """A device name is used as a file name: lowercase letters, digits, ``-`` and ``_``."""
    return isinstance(name, str) and _DEVICE_NAME.fullmatch(name) is not None


def make_crypto(kdf: KdfStrength) -> CryptoPort:
    """The real cryptography (PyNaCl), with the chosen password-derivation cost."""
    if kdf is KdfStrength.FAST:
        return NaClCrypto(
            opslimit=pwhash.argon2id.OPSLIMIT_MIN, memlimit=pwhash.argon2id.MEMLIMIT_MIN
        )
    return NaClCrypto()


def make_store(settings: ClientSettings, name: str) -> LocalStorePort:
    """The local store of device ``name``: a SQLite file, or memory.

    Raises:
        ValueError: ``name`` is not a valid device name.
    """
    if not valid_device_name(name):
        raise ValueError(
            "a device name is 1 to 32 lowercase letters, digits, '-' or '_', "
            "starting with a letter or digit"
        )
    if settings.data_dir is None:
        return MemoryLocalStore()
    return SqliteLocalStore(settings.data_dir / f"{name}.db")


def build_client(
    *,
    network: Network,
    store: LocalStorePort,
    crypto: CryptoPort,
    clock: ClockPort,
    settings: ClientSettings,
) -> ClientApp:
    """A ``ClientApp`` for one device, joined to ``network``."""
    return ClientApp(
        crypto=crypto,
        store=store,
        clock=clock,
        gateway=network.gateway,
        peers_for=network.peers_for,
        policy=settings.policy,
    )
