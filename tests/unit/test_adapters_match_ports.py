from source.adapters.memory.directory_store import InMemoryDirectoryStore
from source.adapters.memory.mailbox_store import InMemoryMailboxStore
from source.application.ports import DirectoryStorePort, MailboxStorePort


def test_adapters_have_the_port_methods() -> None:
    # runtime_checkable only checks method NAMES; mypy --strict checks the types.
    assert isinstance(InMemoryDirectoryStore(), DirectoryStorePort)
    assert isinstance(InMemoryMailboxStore(), MailboxStorePort)
