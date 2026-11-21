import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.application.ports import NodeGatewayPort
from source.domain.node_id import NodeId
from tests.contracts.node_gateway_contract import GatewayHarness, NodeGatewayContract

NODES = [NodeId("node-1"), NodeId("node-2"), NodeId("node-3")]


class InMemoryHarness:
    def __init__(self) -> None:
        self._impl = InMemoryNodeGateway(NODES, ManualClock())
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


# Task 2.3 will add, with NO change to the contract:
# class TestRayNodeGateway(NodeGatewayContract):
#     harness = a fixture that starts 3 Ray node actors and kills one with ray.kill
