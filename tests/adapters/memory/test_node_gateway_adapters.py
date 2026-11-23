import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.application.ports import ClockPort, NodeGatewayPort
from source.domain.node_id import NodeId
from tests.contracts.node_gateway_contract import GatewayHarness, NodeGatewayContract

NODES = [NodeId("node-1"), NodeId("node-2"), NodeId("node-3")]


class InMemoryHarness:
    def __init__(self) -> None:
        self.clock: ClockPort = ManualClock()
        self._impl = InMemoryNodeGateway(NODES, self.clock, FakeCrypto())
        self.gateway: NodeGatewayPort = self._impl
        self.nodes = list(NODES)

    def set_down(self, node: NodeId) -> None:
        self._impl.set_down(node)

    def set_up(self, node: NodeId) -> None:
        self._impl.set_up(node)


class TestInMemoryNodeGateway(NodeGatewayContract):
    @pytest.fixture
    def harness(self) -> GatewayHarness:
        return InMemoryHarness()
