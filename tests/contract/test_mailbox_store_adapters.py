import pytest

from source.adapters.memory.mailbox_store import InMemoryMailboxStore
from source.application.ports import MailboxStorePort
from tests.contract.mailbox_store_contract import MailboxStoreContract


class TestInMemoryMailboxStore(MailboxStoreContract):
    @pytest.fixture
    def store(self) -> MailboxStorePort:
        return InMemoryMailboxStore()


# Task 1B.4 will add, with NO change to the contract:
# class TestSqliteMailboxStore(MailboxStoreContract):
#     @pytest.fixture
#     def store(self, tmp_path): return SqliteMailboxStore(tmp_path / "mailbox.db")
