"""Tests for ``check_inbound_commit``, the consistency rule of one stream step.

Its behaviour through the store is covered by the LocalStore contract; these
tests cover the argument checks that the contract cannot reach with well-typed
calls.
"""

from __future__ import annotations

import pytest

from source.domain.messages import InboundState, check_inbound_commit
from source.domain.ordering import StreamSnapshot
from tests.support.builders import ALICE, BOB, conversation_of, make_received

FROM_BOB = conversation_of(BOB, ALICE)
EMPTY: InboundState = StreamSnapshot(1, (), frozenset())


def test_a_consistent_step_passes() -> None:
    check_inbound_commit(
        FROM_BOB,
        BOB.user_id,
        [make_received(sender=BOB, seq=1)],
        StreamSnapshot(2, (), frozenset()),
    )


@pytest.mark.parametrize(
    ("conversation_id", "sender"),
    [(FROM_BOB.value, BOB.user_id), (FROM_BOB, BOB.user_id.value)],
)
def test_the_key_must_be_typed(conversation_id: object, sender: object) -> None:
    with pytest.raises(ValueError):
        check_inbound_commit(conversation_id, sender, [], EMPTY)


def test_the_state_must_be_a_snapshot() -> None:
    with pytest.raises(ValueError):
        check_inbound_commit(FROM_BOB, BOB.user_id, [], "state")


def test_released_items_must_be_received_messages() -> None:
    released: list[object] = ["message"]
    with pytest.raises(ValueError):
        check_inbound_commit(FROM_BOB, BOB.user_id, released, EMPTY)
