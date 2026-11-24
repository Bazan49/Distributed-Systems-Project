"""RayNetwork and RayClientFactory, without a Ray cluster.

Building the client inside a real actor is tested in
``tests/adapters/system/ray/test_ray_client_actor.py``.
"""

from __future__ import annotations

import pickle
import threading

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.adapters.system.ray.ray_peer_transport import RayPeerTransport
from source.composition.client import ClientSettings, KdfStrength
from source.composition.ray_network import RayClientFactory, RayNetwork
from source.domain.node_id import NodeId
from tests.support.builders import ALICE


def test_the_network_gives_its_gateway_and_a_ray_transport_per_user() -> None:
    clock = ManualClock()
    gateway = InMemoryNodeGateway([NodeId("n1")], clock, FakeCrypto())
    network = RayNetwork(gateway, namespace="p08", guard=threading.RLock())
    assert network.gateway is gateway
    assert isinstance(network.peers_for(ALICE.user_id), RayPeerTransport)


def test_the_factory_can_be_shipped_to_another_process() -> None:
    factory = RayClientFactory(
        device="alice",
        seeds=("n1", "n2"),
        namespace="p08",
        settings=ClientSettings(kdf=KdfStrength.FAST),
    )
    # Our own bytes, just made: not untrusted input.
    assert pickle.loads(pickle.dumps(factory)) == factory  # noqa: S301
