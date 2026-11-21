import pytest

from source.adapters.memory.directory_store import InMemoryDirectoryStore
from source.application.ports import DirectoryStorePort
from tests.contracts.directory_store_contract import DirectoryStoreContract


class TestInMemoryDirectoryStore(DirectoryStoreContract):
    @pytest.fixture
    def store(self) -> DirectoryStorePort:
        return InMemoryDirectoryStore()


# Task 1B.6 will add, with NO change to the contract:
# class TestSqliteDirectoryStore(DirectoryStoreContract): ...
