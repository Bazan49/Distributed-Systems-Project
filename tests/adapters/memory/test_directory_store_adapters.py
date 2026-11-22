import pytest

from source.adapters.memory.directory_store import InMemoryDirectoryStore
from source.application.ports import DirectoryStorePort
from tests.contracts.directory_store_contract import DirectoryStoreContract


class TestInMemoryDirectoryStore(DirectoryStoreContract):
    @pytest.fixture
    def store(self) -> DirectoryStorePort:
        return InMemoryDirectoryStore()
