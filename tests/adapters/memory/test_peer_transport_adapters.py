import pytest

from source.adapters.memory.peer_transport import InMemoryPeerNetwork
from tests.contracts.peer_transport_contract import (
    PeerTransportContract,
    TransportFactory,
)


class TestInMemoryPeerTransport(PeerTransportContract):
    @pytest.fixture
    def make_transport(self) -> TransportFactory:
        return InMemoryPeerNetwork().transport_for
