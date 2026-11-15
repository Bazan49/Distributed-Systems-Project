from source.adapters.memory.directory_store import InMemoryDirectoryStore
from source.application.ports import DirectoryStorePort


def test_directory_store_has_the_port_methods() -> None:
    # runtime_checkable only checks method NAMES; mypy --strict checks the types.
    assert isinstance(InMemoryDirectoryStore(), DirectoryStorePort)
