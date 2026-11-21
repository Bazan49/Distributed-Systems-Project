import pytest

from source.adapters.memory.peer_transport import InMemoryPeerNetwork
from tests.contract.peer_transport_contract import (
    PeerTransportContract,
    TransportFactory,
)


class TestInMemoryPeerTransport(PeerTransportContract):
    @pytest.fixture
    def make_transport(self) -> TransportFactory:
        return InMemoryPeerNetwork().transport_for


# Task 2.6 will add, with NO change to the contract:
# class TestRayPeerTransport(PeerTransportContract): ...
