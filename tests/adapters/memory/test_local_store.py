"""Tests of MemoryLocalStore: the shared contract plus what is specific to this adapter."""

import dataclasses

import pytest

from source.adapters.memory.local_store import MemoryLocalStore
from source.application.ports.local_store import LocalStorePort
from tests.contracts.local_store_contract import LocalStoreContract
from tests.support.builders import (
    BOB,
    make_contact,
    make_outgoing,
    make_received,
)


class TestMemoryLocalStoreContract(LocalStoreContract):
    @pytest.fixture
    def store(self) -> LocalStorePort:
        return MemoryLocalStore()

    # reopen() is inherited: it skips the durability tests, which only real adapters can pass.


def test_it_satisfies_the_port() -> None:
    """``MemoryLocalStore`` implements every method of ``LocalStorePort``."""
    assert isinstance(MemoryLocalStore(), LocalStorePort)


def test_two_stores_never_share_data() -> None:
    """Each instance has its own state; nothing leaks through class-level attributes."""
    one, two = MemoryLocalStore(), MemoryLocalStore()
    one.save_contact(make_contact(BOB))
    assert two.list_contacts() == []


def test_returned_lists_are_copies_that_cannot_corrupt_the_store() -> None:
    """Clearing a list returned by the store does not remove anything from the store."""
    store = MemoryLocalStore()
    message = make_received()
    store.save_incoming_message(message)
    store.save_contact(make_contact(BOB))
    store.enqueue_outgoing(make_outgoing())

    store.list_received(message.conversation_id).clear()
    store.list_contacts().clear()
    store.list_outgoing().clear()

    assert len(store.list_received(message.conversation_id)) == 1
    assert len(store.list_contacts()) == 1
    assert len(store.list_outgoing()) == 1


def test_stored_messages_are_immutable_values() -> None:
    """A message obtained from the store is frozen: changing a field in place
    raises, so callers must go through ``update_outgoing``."""
    store = MemoryLocalStore()
    message = make_outgoing()
    store.enqueue_outgoing(message)

    stored = store.get_outgoing(message.message_id)
    assert stored is not None
    with pytest.raises(dataclasses.FrozenInstanceError):
        stored.attempts = 5
    assert store.get_outgoing(message.message_id) == message
