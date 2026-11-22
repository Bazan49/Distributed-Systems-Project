import pytest

from source.adapters.memory.mailbox_store import InMemoryMailboxStore
from source.application.ports import MailboxStorePort
from tests.contracts.mailbox_store_contract import MailboxStoreContract


class TestInMemoryMailboxStore(MailboxStoreContract):
    @pytest.fixture
    def store(self) -> MailboxStorePort:
        return InMemoryMailboxStore()
