"""Contract of LocalStorePort: behaviour EVERY implementation must have.

Subclass ``LocalStoreContract``, override the ``store`` fixture and, only for
durable adapters, the ``reopen`` method:

    class TestSqliteLocalStore(LocalStoreContract):
        @pytest.fixture
        def store(self, tmp_path: Path) -> LocalStorePort:
            return SqliteLocalStore(tmp_path / "user.db")

        def reopen(self, store: LocalStorePort) -> LocalStorePort:
            store.close()
            return SqliteLocalStore(store.path)

``reopen`` simulates a process restart: it must return a NEW store object
over the same underlying data. The default skips the durability tests, which
is the right answer for a purely in-memory adapter.

Not covered here (needs adapter internals, so it lives in the adapter's own
tests): atomicity of ``save_incoming_message`` when the process dies halfway.
"""

import dataclasses
from typing import Any

import pytest

from source.application.ports.errors import AlreadyExistsError, NotFoundError
from source.application.ports.local_store import LocalStorePort
from source.domain.contacts import ContactState
from source.domain.envelope import MessageKind
from source.domain.identifiers import MessageId
from source.domain.messages import InboundState, OutgoingMessage, ReceivedMessage
from source.domain.ordering import Arrival, InboundStream, Pending, StreamSnapshot
from source.domain.outgoing_status import OutgoingStatus
from tests.support.builders import (
    ALICE,
    BASE_TIME_MS,
    BOB,
    CARLA,
    DAVID,
    conversation_of,
    make_contact,
    make_identity,
    make_outgoing,
    make_received,
    next_message_id,
)

Q, A, D, E = (
    OutgoingStatus.QUEUED,
    OutgoingStatus.ACCEPTED,
    OutgoingStatus.DELIVERED,
    OutgoingStatus.EXPIRED,
)

FAR_FUTURE_MS = 10**13  # "now" large enough for every retry time to have passed
WITH_BOB = conversation_of(
    ALICE, BOB
)  # conversation of the local user (ALICE) with BOB
FROM_BOB = conversation_of(BOB, ALICE)  # same conversation, built from the other side


def _get(store: LocalStorePort, message_id: MessageId) -> OutgoingMessage:
    """Fetch an outgoing message that the test knows is there."""
    found = store.get_outgoing(message_id)
    assert found is not None, "expected the outgoing message to be stored"
    return found


class LocalStoreContract:
    @pytest.fixture
    def store(self) -> LocalStorePort:
        raise NotImplementedError(
            "override the 'store' fixture in the concrete test class"
        )

    def reopen(self, store: LocalStorePort) -> LocalStorePort:
        pytest.skip("adapter is not durable: restart tests do not apply")

    # -------------------------------------------------------------- identity

    def test_a_new_store_has_no_identity(self, store: LocalStorePort) -> None:
        """A store that was never written to reports no identity."""
        assert store.load_identity() is None

    def test_a_saved_identity_is_loaded_back_unchanged(
        self, store: LocalStorePort
    ) -> None:
        """What ``save_identity`` receives is exactly what ``load_identity`` returns."""
        identity = make_identity()
        store.save_identity(identity)
        assert store.load_identity() == identity

    @pytest.mark.parametrize(
        "second", [ALICE, BOB], ids=["same_identity", "different_identity"]
    )
    def test_a_device_holds_exactly_one_identity(
        self, store: LocalStorePort, second: Any
    ) -> None:
        """A second ``save_identity`` is rejected, whether or not it is the same
        identity, and the first one stays untouched."""
        first = make_identity(ALICE)
        store.save_identity(first)
        with pytest.raises(AlreadyExistsError):
            store.save_identity(make_identity(second))
        assert store.load_identity() == first

    # -------------------------------------------------------------- contacts

    def test_an_unknown_contact_is_none_and_the_list_is_empty(
        self, store: LocalStorePort
    ) -> None:
        """Looking up a contact that was never saved gives ``None``; the list is empty."""
        assert store.get_contact(BOB.user_id) is None
        assert store.list_contacts() == []

    def test_a_saved_contact_is_read_back_unchanged(
        self, store: LocalStorePort
    ) -> None:
        """A contact is retrievable by id and appears in the listing."""
        contact = make_contact(BOB)
        store.save_contact(contact)
        assert store.get_contact(BOB.user_id) == contact
        assert store.list_contacts() == [contact]

    def test_saving_a_contact_again_replaces_it(self, store: LocalStorePort) -> None:
        """Saving a contact with an existing ``user_id`` updates it; no duplicate appears."""
        store.save_contact(make_contact(BOB, state=ContactState.INCOMING_REQUEST))
        accepted = make_contact(BOB, state=ContactState.ACTIVE, alias="Bobby")
        store.save_contact(accepted)
        assert store.get_contact(BOB.user_id) == accepted
        assert store.list_contacts() == [accepted]

    def test_contacts_are_listed_sorted_by_user_id(self, store: LocalStorePort) -> None:
        """The listing is ordered by ``user_id`` whatever the insertion order
        (they are inserted in REVERSE order so a lucky insertion order cannot pass)."""
        people = sorted([BOB, CARLA, DAVID], key=lambda p: p.user_id)
        for person in reversed(people):
            store.save_contact(make_contact(person))
        assert [c.user_id for c in store.list_contacts()] == [p.user_id for p in people]

    def test_contacts_can_be_filtered_by_state(self, store: LocalStorePort) -> None:
        """``list_contacts(state)`` returns only contacts in that state; no filter returns all."""
        store.save_contact(make_contact(BOB, state=ContactState.ACTIVE))
        store.save_contact(make_contact(CARLA, state=ContactState.INCOMING_REQUEST))
        store.save_contact(make_contact(DAVID, state=ContactState.OUTGOING_REQUEST))
        assert [c.user_id for c in store.list_contacts(ContactState.ACTIVE)] == [
            BOB.user_id
        ]
        assert [
            c.user_id for c in store.list_contacts(ContactState.INCOMING_REQUEST)
        ] == [CARLA.user_id]
        assert [
            c.user_id for c in store.list_contacts(ContactState.OUTGOING_REQUEST)
        ] == [DAVID.user_id]
        assert len(store.list_contacts()) == 3

    def test_two_contacts_may_share_an_alias(self, store: LocalStorePort) -> None:
        """Aliases are not unique; the user id is the identity."""
        store.save_contact(make_contact(BOB, alias="Sam"))
        store.save_contact(make_contact(CARLA, alias="Sam"))
        assert len(store.list_contacts()) == 2

    def test_deleting_a_contact_reports_whether_it_existed(
        self, store: LocalStorePort
    ) -> None:
        """``delete_contact`` returns True the first time and False for an
        already-deleted or never-saved contact."""
        store.save_contact(make_contact(BOB))
        assert store.delete_contact(BOB.user_id) is True
        assert store.get_contact(BOB.user_id) is None
        assert store.delete_contact(BOB.user_id) is False
        assert store.delete_contact(CARLA.user_id) is False

    def test_deleting_a_contact_keeps_its_messages(self, store: LocalStorePort) -> None:
        """Removing a contact does not erase the conversation history."""
        store.save_contact(make_contact(BOB))
        message = make_received(sender=BOB)
        store.save_incoming_message(message)
        store.delete_contact(BOB.user_id)
        assert store.list_received(message.conversation_id) == [message]

    # -------------------------------------------------------------- received

    def test_a_new_message_is_stored_and_reported_as_new(
        self, store: LocalStorePort
    ) -> None:
        """The first ``save_incoming_message`` returns True, marks the id as
        received and makes the message appear in the history."""
        message = make_received()
        assert store.has_received(message.message_id) is False
        assert store.save_incoming_message(message) is True
        assert store.has_received(message.message_id) is True
        assert store.list_received(message.conversation_id) == [message]

    def test_an_unknown_message_id_has_not_been_received(
        self, store: LocalStorePort
    ) -> None:
        """``has_received`` is False for an id that was never stored."""
        assert store.has_received(next_message_id()) is False

    def test_storing_the_same_message_twice_is_idempotent(
        self, store: LocalStorePort
    ) -> None:
        """A repeated delivery returns False and leaves exactly one copy."""
        message = make_received()
        assert store.save_incoming_message(message) is True
        assert store.save_incoming_message(message) is False
        assert store.list_received(message.conversation_id) == [message]

    def test_a_duplicate_id_never_overwrites_the_first_copy(
        self, store: LocalStorePort
    ) -> None:
        """A second message with the same id but different content is ignored;
        the original content is kept."""
        original = make_received(body="original")
        store.save_incoming_message(original)
        forged = dataclasses.replace(original, body="forged")
        assert store.save_incoming_message(forged) is False
        assert store.list_received(original.conversation_id) == [original]

    def test_messages_are_kept_apart_by_conversation(
        self, store: LocalStorePort
    ) -> None:
        """Each conversation lists only its own messages; an unused one is empty."""
        from_bob = make_received(sender=BOB, body="from bob")
        from_carla = make_received(sender=CARLA, body="from carla")
        store.save_incoming_message(from_bob)
        store.save_incoming_message(from_carla)
        assert store.list_received(from_bob.conversation_id) == [from_bob]
        assert store.list_received(from_carla.conversation_id) == [from_carla]
        assert store.list_received(conversation_of(ALICE, DAVID)) == []

    def test_the_store_does_not_hold_back_messages_that_arrive_ahead_of_a_gap(
        self, store: LocalStorePort
    ) -> None:
        """The store keeps whatever it is given: seq 3 and 4 are stored even
        though seq 2 is missing. Deciding WHEN to store is the ordering domain's job."""
        for seq in (1, 3, 4):
            store.save_incoming_message(make_received(seq=seq))
        found = store.list_received(FROM_BOB)
        assert [m.seq for m in found] == [1, 3, 4]

    def test_every_message_kind_is_stored(self, store: LocalStorePort) -> None:
        """No ``MessageKind`` is silently dropped from the history."""
        for seq, kind in enumerate(MessageKind, start=1):
            store.save_incoming_message(make_received(seq=seq, kind=kind))
        assert {m.kind for m in store.list_received(FROM_BOB)} == set(MessageKind)

    @pytest.mark.parametrize(
        "body",
        [
            "ñandú 日本語 🔑",
            "line1\nline2\r\n\ttabbed",
            "nul\x00inside",
            "a" * 16_384,
        ],
        ids=["unicode", "whitespace", "nul_character", "large"],
    )
    def test_a_message_body_round_trips_unchanged(
        self, store: LocalStorePort, body: str
    ) -> None:
        """Unicode, line breaks, a NUL character and a large body come back byte for byte."""
        message = make_received(body=body)
        store.save_incoming_message(message)
        (found,) = store.list_received(message.conversation_id)
        assert found.body == body

    def test_history_is_returned_in_storage_order_not_sorted_by_seq(
        self, store: LocalStorePort
    ) -> None:
        """The store never reorders: messages come back oldest-stored first,
        even if their ``seq`` values are not increasing."""
        messages = [make_received(seq=seq) for seq in (3, 1, 2)]
        for message in messages:
            store.save_incoming_message(message)
        assert store.list_received(FROM_BOB) == messages

    def test_history_limit_keeps_the_last_stored_messages_in_order(
        self, store: LocalStorePort
    ) -> None:
        """With ``limit=n`` the LAST n stored messages are returned, still oldest first."""
        messages = [make_received(seq=seq) for seq in range(1, 6)]
        for message in messages:
            store.save_incoming_message(message)
        assert store.list_received(FROM_BOB, limit=2) == messages[-2:]
        assert store.list_received(FROM_BOB, limit=5) == messages
        assert store.list_received(FROM_BOB, limit=50) == messages

    def test_history_defaults_to_the_last_hundred_messages(
        self, store: LocalStorePort
    ) -> None:
        """Without a ``limit`` only the last 100 messages are returned."""
        messages = [make_received(seq=seq) for seq in range(1, 106)]
        for message in messages:
            store.save_incoming_message(message)
        assert store.list_received(FROM_BOB) == messages[-100:]

    @pytest.mark.parametrize("limit", [0, -1, -100])
    def test_a_history_limit_below_one_is_a_value_error(
        self, store: LocalStorePort, limit: int
    ) -> None:
        """A non-positive ``limit`` is rejected, never interpreted as "no limit"."""
        with pytest.raises(ValueError):
            store.list_received(FROM_BOB, limit=limit)

    # ---------------------------------------------------------------- outbox

    def test_an_outgoing_message_can_be_queued_and_read_back(
        self, store: LocalStorePort
    ) -> None:
        """A queued message is retrievable by id exactly as it was given."""
        message = make_outgoing()
        store.enqueue_outgoing(message)
        assert store.get_outgoing(message.message_id) == message

    def test_an_unknown_outgoing_message_is_none_and_the_outbox_is_empty(
        self, store: LocalStorePort
    ) -> None:
        """Looking up an id that was never queued gives ``None``; a new outbox is empty."""
        assert store.get_outgoing(next_message_id()) is None
        assert store.list_outgoing() == []

    def test_the_stored_envelope_is_byte_identical(self, store: LocalStorePort) -> None:
        """A retry must resend exactly the same signed bytes, so the envelope
        must not be re-serialised or altered by the store."""
        message = make_outgoing()
        store.enqueue_outgoing(message)
        stored = _get(store, message.message_id)
        assert stored.envelope.to_bytes() == message.envelope.to_bytes()

    def test_enqueuing_the_same_message_id_twice_is_rejected(
        self, store: LocalStorePort
    ) -> None:
        """A repeated ``message_id`` raises ``AlreadyExistsError`` and adds nothing."""
        message = make_outgoing()
        store.enqueue_outgoing(message)
        with pytest.raises(AlreadyExistsError):
            store.enqueue_outgoing(message)
        assert store.list_outgoing() == [message]

    def test_reusing_a_seq_in_the_same_conversation_is_rejected(
        self, store: LocalStorePort
    ) -> None:
        """Two different messages cannot share a ``seq`` in one conversation;
        the first one is kept."""
        first = make_outgoing(seq=1)
        store.enqueue_outgoing(first)
        with pytest.raises(AlreadyExistsError):
            store.enqueue_outgoing(make_outgoing(seq=1, body="different message"))
        assert store.list_outgoing() == [first]

    def test_the_same_seq_in_another_conversation_is_fine(
        self, store: LocalStorePort
    ) -> None:
        """``seq`` numbering is per conversation: seq 1 with Bob and seq 1 with Carla coexist."""
        store.enqueue_outgoing(make_outgoing(receiver=BOB, seq=1))
        store.enqueue_outgoing(make_outgoing(receiver=CARLA, seq=1))
        assert len(store.list_outgoing()) == 2

    def test_updating_replaces_status_attempts_and_retry_time(
        self, store: LocalStorePort
    ) -> None:
        """``update_outgoing`` stores the new delivery state in place, without
        creating a second entry."""
        message = make_outgoing()
        store.enqueue_outgoing(message)
        updated = dataclasses.replace(
            message, status=A, attempts=2, next_attempt_at_ms=5_000
        )
        store.update_outgoing(updated)
        assert store.get_outgoing(message.message_id) == updated
        assert store.list_outgoing() == [updated]

    def test_the_store_does_not_validate_status_transitions(
        self, store: LocalStorePort
    ) -> None:
        """Even a "backwards" change such as DELIVERED -> QUEUED is persisted:
        legal transitions are decided by the domain, not by the store."""
        message = make_outgoing(status=D)
        store.enqueue_outgoing(message)
        store.update_outgoing(dataclasses.replace(message, status=Q))
        assert _get(store, message.message_id).status == Q

    def test_updating_an_unknown_message_is_not_found(
        self, store: LocalStorePort
    ) -> None:
        """Updating a message that was never queued raises ``NotFoundError`` and
        does not create it."""
        with pytest.raises(NotFoundError):
            store.update_outgoing(make_outgoing())
        assert store.list_outgoing() == []

    @pytest.mark.parametrize(
        "changes",
        [
            {"seq": 2},
            {"body": "edited text"},
            {"kind": next(k for k in MessageKind if k != MessageKind.TEXT)},
            {"sent_at_ms": BASE_TIME_MS + 1},
            {"receiver": CARLA},
        ],
        ids=["seq", "body", "kind", "sent_at", "conversation"],
    )
    def test_an_update_cannot_change_the_signed_message(
        self, store: LocalStorePort, changes: dict[str, Any]
    ) -> None:
        """Content, seq, kind, send time and conversation are part of the signed
        envelope. An update that alters any of them raises ``ValueError`` and
        leaves the stored message untouched."""
        original = make_outgoing(seq=1)
        store.enqueue_outgoing(original)
        altered = make_outgoing(message_id=original.message_id, **{"seq": 1, **changes})
        with pytest.raises(ValueError):
            store.update_outgoing(altered)
        assert _get(store, original.message_id) == original

    # ------------------------------------------------------ listing outgoing

    def _fill(self, store: LocalStorePort) -> list[OutgoingMessage]:
        """Queue five messages in a deliberately scrambled order and return them."""
        messages = [
            make_outgoing(receiver=CARLA, seq=2, status=A),
            make_outgoing(receiver=BOB, seq=3, status=D),
            make_outgoing(receiver=BOB, seq=1, status=Q),
            make_outgoing(receiver=BOB, seq=2, status=E),
            make_outgoing(receiver=CARLA, seq=1, status=Q),
        ]
        for message in messages:
            store.enqueue_outgoing(message)
        return messages

    def test_outgoing_is_sorted_by_conversation_then_seq(
        self, store: LocalStorePort
    ) -> None:
        """The listing is ordered by ``(conversation_id, seq)`` regardless of
        insertion order."""
        self._fill(store)
        keys = [(m.conversation_id, m.seq) for m in store.list_outgoing()]
        assert keys == sorted(keys)
        assert len(keys) == 5

    def test_outgoing_can_be_filtered_by_conversation(
        self, store: LocalStorePort
    ) -> None:
        """Passing a conversation id returns only that conversation's messages, by seq."""
        self._fill(store)
        found = store.list_outgoing(WITH_BOB)
        assert [m.seq for m in found] == [1, 2, 3]
        assert {m.envelope.receiver for m in found} == {BOB.user_id}

    def test_outgoing_can_be_filtered_by_status(self, store: LocalStorePort) -> None:
        """Passing statuses returns only messages in any of them (list, set,
        tuple or frozenset are all accepted)."""
        self._fill(store)
        assert len(store.list_outgoing(statuses=[Q])) == 2
        assert len(store.list_outgoing(statuses={Q, A})) == 3
        assert len(store.list_outgoing(statuses=(D,))) == 1
        assert len(store.list_outgoing(statuses=frozenset(OutgoingStatus))) == 5

    def test_an_empty_status_collection_matches_nothing(
        self, store: LocalStorePort
    ) -> None:
        """An empty collection of statuses is a filter that matches nothing
        (it is NOT the same as ``None``, which means "no filter")."""
        self._fill(store)
        assert store.list_outgoing(statuses=[]) == []

    def test_conversation_and_status_filters_combine(
        self, store: LocalStorePort
    ) -> None:
        """Both filters at once return the intersection."""
        self._fill(store)
        found = store.list_outgoing(WITH_BOB, statuses=[Q, E])
        assert [m.seq for m in found] == [1, 2]

    # --------------------------------------------------------- due messages

    def test_only_queued_messages_are_due(self, store: LocalStorePort) -> None:
        """Messages that are accepted, delivered or expired are never due."""
        due = make_outgoing(seq=1, status=Q)
        store.enqueue_outgoing(due)
        for seq, status in ((2, A), (3, D), (4, E)):
            store.enqueue_outgoing(make_outgoing(seq=seq, status=status))
        assert store.list_due_outgoing(now_ms=FAR_FUTURE_MS) == [due]

    def test_a_message_is_due_exactly_at_its_retry_time(
        self, store: LocalStorePort
    ) -> None:
        """Just before ``next_attempt_at_ms`` it is not due; at that instant and
        later it is (the boundary counts as due)."""
        message = make_outgoing(next_attempt_at_ms=1_000)
        store.enqueue_outgoing(message)
        assert store.list_due_outgoing(now_ms=999) == []
        assert store.list_due_outgoing(now_ms=1_000) == [message]
        assert store.list_due_outgoing(now_ms=1_001) == [message]

    def test_a_message_with_no_retry_time_is_always_due(
        self, store: LocalStorePort
    ) -> None:
        """``next_attempt_at_ms=None`` means "send now", even at time 0."""
        message = make_outgoing(next_attempt_at_ms=None)
        store.enqueue_outgoing(message)
        assert store.list_due_outgoing(now_ms=0) == [message]

    def test_due_messages_go_oldest_first(self, store: LocalStorePort) -> None:
        """Due messages are ordered by ``sent_at_ms`` (expected order written by
        hand, not recomputed with the implementation's own sort key)."""
        early = make_outgoing(receiver=BOB, seq=1, sent_at_ms=1_000)
        middle_1 = make_outgoing(receiver=CARLA, seq=1, sent_at_ms=2_000)
        middle_2 = make_outgoing(receiver=CARLA, seq=2, sent_at_ms=2_500)
        late = make_outgoing(receiver=BOB, seq=2, sent_at_ms=3_000)
        for message in (late, middle_2, early, middle_1):
            store.enqueue_outgoing(message)
        assert store.list_due_outgoing(now_ms=FAR_FUTURE_MS) == [
            early,
            middle_1,
            middle_2,
            late,
        ]

    def test_messages_sent_at_the_same_time_break_ties_by_conversation_then_seq(
        self, store: LocalStorePort
    ) -> None:
        """With equal ``sent_at_ms`` the order is by ``conversation_id`` and then
        ``seq``, so the result is deterministic."""
        people = sorted([BOB, CARLA], key=lambda p: conversation_of(ALICE, p))
        expected = [
            make_outgoing(receiver=person, seq=seq, sent_at_ms=1_000)
            for person in people
            for seq in (1, 2)
        ]
        for message in reversed(expected):
            store.enqueue_outgoing(message)
        assert store.list_due_outgoing(now_ms=FAR_FUTURE_MS) == expected

    def test_the_limit_caps_the_number_of_due_messages(
        self, store: LocalStorePort
    ) -> None:
        """``limit`` returns at most that many, keeping the oldest ones."""
        for seq in range(1, 6):
            store.enqueue_outgoing(make_outgoing(seq=seq, sent_at_ms=1_000 * seq))
        found = store.list_due_outgoing(now_ms=FAR_FUTURE_MS, limit=2)
        assert [m.seq for m in found] == [1, 2]
        assert len(store.list_due_outgoing(now_ms=FAR_FUTURE_MS, limit=1)) == 1
        assert len(store.list_due_outgoing(now_ms=FAR_FUTURE_MS)) == 5

    @pytest.mark.parametrize("limit", [0, -1, -100])
    def test_a_due_limit_below_one_is_a_value_error(
        self, store: LocalStorePort, limit: int
    ) -> None:
        """A non-positive ``limit`` is rejected, never interpreted as "no limit"."""
        with pytest.raises(ValueError):
            store.list_due_outgoing(now_ms=0, limit=limit)

    def test_a_message_leaves_the_due_list_once_accepted(
        self, store: LocalStorePort
    ) -> None:
        """Updating the status from QUEUED to ACCEPTED removes it from the due list."""
        message = make_outgoing()
        store.enqueue_outgoing(message)
        assert store.list_due_outgoing(now_ms=0) == [message]
        store.update_outgoing(dataclasses.replace(message, status=A))
        assert store.list_due_outgoing(now_ms=0) == []

    # ---------------------------------------------------- last outgoing seq

    def test_the_last_seq_is_zero_for_a_new_conversation(
        self, store: LocalStorePort
    ) -> None:
        """With nothing sent yet the counter is 0, so the first message gets seq 1."""
        assert store.last_outgoing_seq(WITH_BOB) == 0

    def test_the_last_seq_is_the_highest_used_not_the_latest_inserted(
        self, store: LocalStorePort
    ) -> None:
        """Inserting seq 1, 5, 3 gives 5, not 3."""
        for seq in (1, 5, 3):
            store.enqueue_outgoing(make_outgoing(seq=seq))
        assert store.last_outgoing_seq(WITH_BOB) == 5

    def test_the_last_seq_is_per_conversation(self, store: LocalStorePort) -> None:
        """Each conversation has its own counter; an untouched one stays at 0."""
        store.enqueue_outgoing(make_outgoing(receiver=BOB, seq=7))
        store.enqueue_outgoing(make_outgoing(receiver=CARLA, seq=2))
        assert store.last_outgoing_seq(WITH_BOB) == 7
        assert store.last_outgoing_seq(conversation_of(ALICE, CARLA)) == 2
        assert store.last_outgoing_seq(conversation_of(ALICE, DAVID)) == 0

    def test_the_last_seq_counts_messages_in_every_status(
        self, store: LocalStorePort
    ) -> None:
        """Delivered and expired messages still count: a seq is never reused."""
        for seq, status in ((1, Q), (2, A), (3, D), (4, E)):
            store.enqueue_outgoing(make_outgoing(seq=seq, status=status))
        assert store.last_outgoing_seq(WITH_BOB) == 4

    def test_the_last_seq_ignores_received_messages(
        self, store: LocalStorePort
    ) -> None:
        """Only what the local user SENT moves the counter, not what arrived."""
        store.save_incoming_message(make_received(sender=BOB, seq=9))
        assert store.last_outgoing_seq(WITH_BOB) == 0

    # ------------------------------------------------------- max lamport

    def test_max_lamport_is_zero_for_a_new_conversation(
        self, store: LocalStorePort
    ) -> None:
        """With nothing known yet, the first message gets timestamp 1."""
        assert store.max_lamport(WITH_BOB) == 0

    def test_max_lamport_counts_sent_messages_of_every_kind(
        self, store: LocalStorePort
    ) -> None:
        """Contact messages are events too: they order what follows them."""
        store.enqueue_outgoing(make_outgoing(seq=1, lamport=3))
        store.enqueue_outgoing(
            make_outgoing(seq=2, lamport=8, kind=MessageKind.CONTACT_ACCEPT)
        )
        assert store.max_lamport(WITH_BOB) == 8

    def test_max_lamport_counts_received_messages(self, store: LocalStorePort) -> None:
        """What arrived counts as much as what was sent: the max of both."""
        store.enqueue_outgoing(make_outgoing(seq=1, lamport=4))
        store.save_incoming_message(make_received(seq=1, lamport=11))
        assert store.max_lamport(WITH_BOB) == 11

    def test_max_lamport_counts_messages_released_by_commit_inbound(
        self, store: LocalStorePort
    ) -> None:
        """Messages that reach the history through ordering count as well."""
        message = make_received(seq=1, lamport=6)
        store.commit_inbound(FROM_BOB, BOB.user_id, [message], _state(2))
        assert store.max_lamport(WITH_BOB) == 6

    def test_max_lamport_ignores_messages_still_buffered(
        self, store: LocalStorePort
    ) -> None:
        """A message waiting behind a gap has not been seen: it orders nothing yet."""
        waiting = make_received(seq=2, lamport=50)
        store.commit_inbound(FROM_BOB, BOB.user_id, [], _state(1, [waiting]))
        assert store.max_lamport(WITH_BOB) == 0
        loaded = store.load_inbound_stream(FROM_BOB, BOB.user_id)
        assert loaded is not None and loaded.pending[0].item.lamport == 50

    def test_max_lamport_is_per_conversation(self, store: LocalStorePort) -> None:
        """Each conversation has its own clock."""
        store.enqueue_outgoing(make_outgoing(receiver=BOB, seq=1, lamport=9))
        store.save_incoming_message(make_received(sender=CARLA, seq=1, lamport=4))
        assert store.max_lamport(WITH_BOB) == 9
        assert store.max_lamport(conversation_of(ALICE, CARLA)) == 4
        assert store.max_lamport(conversation_of(ALICE, DAVID)) == 0

    def test_the_lamport_timestamp_is_kept(self, store: LocalStorePort) -> None:
        """Stored messages come back with the timestamp they were saved with."""
        sent = make_outgoing(seq=1, lamport=12)
        received = make_received(seq=1, lamport=13)
        store.enqueue_outgoing(sent)
        store.save_incoming_message(received)
        assert _get(store, sent.message_id).lamport == 12
        assert store.list_received(FROM_BOB)[0].lamport == 13

    # -------------------------------------------- incoming ordering state

    def test_an_unknown_stream_has_no_state(self, store: LocalStorePort) -> None:
        """A stream that was never committed loads as ``None``."""
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) is None

    def test_a_committed_buffer_is_loaded_back_and_not_yet_history(
        self, store: LocalStorePort
    ) -> None:
        """Buffering only changes the stream state: the message is not in the
        history and its id is not seen until it is released."""
        ahead = make_received(sender=BOB, seq=3)
        state = _state(1, [ahead])

        store.commit_inbound(FROM_BOB, BOB.user_id, [], state)

        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == state
        assert store.list_received(FROM_BOB) == []
        assert store.has_received(ahead.message_id) is False

    def test_released_messages_join_the_history_in_the_given_order(
        self, store: LocalStorePort
    ) -> None:
        """Released messages are appended in order and marked as seen, and the
        state is replaced, in the same call."""
        released = [make_received(sender=BOB, seq=seq) for seq in (1, 2)]
        state = _state(3)

        store.commit_inbound(FROM_BOB, BOB.user_id, released, state)

        assert store.list_received(FROM_BOB) == released
        assert all(store.has_received(m.message_id) for m in released)
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == state

    def test_releasing_a_buffered_message_moves_it_from_buffer_to_history(
        self, store: LocalStorePort
    ) -> None:
        """A message buffered in one commit and released in the next ends up in
        the history and no longer in the buffer."""
        first, second = (make_received(sender=BOB, seq=seq) for seq in (1, 2))
        store.commit_inbound(FROM_BOB, BOB.user_id, [], _state(1, [second]))

        store.commit_inbound(FROM_BOB, BOB.user_id, [first, second], _state(3))

        assert store.list_received(FROM_BOB) == [first, second]
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == _state(3)

    def test_the_buffer_keeps_what_is_still_waiting(
        self, store: LocalStorePort
    ) -> None:
        """Only the released part leaves the buffer; the rest stays as it was."""
        first, third, fifth = (make_received(sender=BOB, seq=s) for s in (1, 3, 5))
        store.commit_inbound(FROM_BOB, BOB.user_id, [], _state(1, [third, fifth]))

        store.commit_inbound(FROM_BOB, BOB.user_id, [first], _state(2, [third, fifth]))

        loaded = store.load_inbound_stream(FROM_BOB, BOB.user_id)
        assert loaded == _state(2, [third, fifth])

    def test_a_skipped_gap_and_a_late_arrival_are_recorded(
        self, store: LocalStorePort
    ) -> None:
        """Skipped seqs are stored; a late release keeps next_seq and forgets the skip."""
        second, first = (make_received(sender=BOB, seq=seq) for seq in (2, 1))
        store.commit_inbound(FROM_BOB, BOB.user_id, [second], _state(3, skipped={1}))

        store.commit_inbound(FROM_BOB, BOB.user_id, [first], _state(3))

        assert store.list_received(FROM_BOB) == [second, first]  # storage order
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == _state(3)

    def test_committing_the_same_state_again_is_allowed(
        self, store: LocalStorePort
    ) -> None:
        """next_seq may stay the same: only going back is refused."""
        state = _state(1, [make_received(sender=BOB, seq=2)])
        store.commit_inbound(FROM_BOB, BOB.user_id, [], state)
        store.commit_inbound(FROM_BOB, BOB.user_id, [], state)
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == state

    def test_streams_are_kept_apart_by_conversation_and_sender(
        self, store: LocalStorePort
    ) -> None:
        """Each (conversation, sender) has its own state."""
        from_carla = conversation_of(CARLA, ALICE)
        bob_state = _state(1, [make_received(sender=BOB, seq=2)])
        carla_state = _state(5)
        store.commit_inbound(FROM_BOB, BOB.user_id, [], bob_state)
        store.commit_inbound(from_carla, CARLA.user_id, [], carla_state)

        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == bob_state
        assert store.load_inbound_stream(from_carla, CARLA.user_id) == carla_state

    def test_a_released_message_already_seen_rejects_the_whole_commit(
        self, store: LocalStorePort
    ) -> None:
        """If ANY released id was seen, nothing is written: not the other
        messages, not the new state."""
        seen = make_received(sender=BOB, seq=2)
        store.save_incoming_message(seen)
        before = _state(1, [make_received(sender=BOB, seq=3)])
        store.commit_inbound(FROM_BOB, BOB.user_id, [], before)
        fresh = make_received(sender=BOB, seq=1)

        with pytest.raises(AlreadyExistsError):
            store.commit_inbound(FROM_BOB, BOB.user_id, [fresh, seen], _state(4))

        assert store.has_received(fresh.message_id) is False
        assert store.list_received(FROM_BOB) == [seen]
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == before

    def test_next_seq_never_goes_back(self, store: LocalStorePort) -> None:
        """An old snapshot cannot overwrite a newer one."""
        store.commit_inbound(FROM_BOB, BOB.user_id, [], _state(5))
        with pytest.raises(ValueError):
            store.commit_inbound(FROM_BOB, BOB.user_id, [], _state(4))
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == _state(5)

    def test_a_buffered_seq_cannot_change_its_message(
        self, store: LocalStorePort
    ) -> None:
        """Buffered messages are immutable: the same seq must stay the same message."""
        original = _state(1, [make_received(sender=BOB, seq=2, body="one")])
        store.commit_inbound(FROM_BOB, BOB.user_id, [], original)
        replaced = _state(1, [make_received(sender=BOB, seq=2, body="two")])

        with pytest.raises(ValueError):
            store.commit_inbound(FROM_BOB, BOB.user_id, [], replaced)
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == original

    @pytest.mark.parametrize(
        "case",
        [
            "released_from_another_sender",
            "released_not_behind_next_seq",
            "released_twice",
            "buffered_from_another_sender",
            "buffered_entry_mismatch",
            "released_and_buffered",
        ],
    )
    def test_an_inconsistent_commit_is_refused_and_writes_nothing(
        self, store: LocalStorePort, case: str
    ) -> None:
        """Arguments that do not describe one step of one stream are a ValueError."""
        one = make_received(sender=BOB, seq=1)
        two = make_received(sender=BOB, seq=2)
        released: list[ReceivedMessage] = []
        state = _state(3)
        if case == "released_from_another_sender":
            released = [make_received(sender=CARLA, seq=1)]
        elif case == "released_not_behind_next_seq":
            released, state = [two], _state(2)
        elif case == "released_twice":
            released = [one, one]
        elif case == "buffered_from_another_sender":
            state = _state(1, [make_received(sender=CARLA, seq=2)])
        elif case == "buffered_entry_mismatch":
            state = StreamSnapshot(1, (Pending(3, two.message_id, two),), frozenset())
        else:  # released_and_buffered: the same id released AND still waiting
            released = [one]
            state = _state(2, [dataclasses.replace(one, seq=3)])

        with pytest.raises(ValueError):
            store.commit_inbound(FROM_BOB, BOB.user_id, released, state)
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) is None
        assert store.list_received(FROM_BOB) == []

    def test_an_inbound_stream_driven_through_the_store_gives_the_full_history(
        self, store: LocalStorePort
    ) -> None:
        """The way 1A.7 will use it: offer each arrival, commit every step that
        changed the stream, and rebuild the stream from the store before each
        arrival (as if the client restarted every time)."""
        messages = {seq: make_received(sender=BOB, seq=seq) for seq in range(1, 7)}
        for seq in (4, 2, 2, 6, 1, 5, 3, 1):
            saved = store.load_inbound_stream(FROM_BOB, BOB.user_id)
            stream: InboundStream[ReceivedMessage] = (
                InboundStream()
                if saved is None
                else InboundStream.restore(saved, now_ms=0)
            )
            message = messages[seq]
            result = stream.offer(seq, message.message_id, message, now_ms=0)
            if result.arrival in _CHANGES_STATE:
                store.commit_inbound(
                    FROM_BOB,
                    BOB.user_id,
                    [d.item for d in result.released],
                    stream.snapshot(),
                )

        assert store.list_received(FROM_BOB) == [messages[s] for s in range(1, 7)]
        assert store.load_inbound_stream(FROM_BOB, BOB.user_id) == _state(7)

    # -------------------------------------------------- durability (restart)

    def test_the_identity_survives_a_restart(self, store: LocalStorePort) -> None:
        """After reopening the store, the same identity is loaded."""
        identity = make_identity()
        store.save_identity(identity)
        assert self.reopen(store).load_identity() == identity

    def test_the_second_identity_is_still_rejected_after_a_restart(
        self, store: LocalStorePort
    ) -> None:
        """The "one identity per device" rule holds across restarts."""
        store.save_identity(make_identity(ALICE))
        with pytest.raises(AlreadyExistsError):
            self.reopen(store).save_identity(make_identity(BOB))

    def test_contacts_survive_a_restart(self, store: LocalStorePort) -> None:
        """Contacts, with their states, come back identical and in the same order."""
        contacts = [
            make_contact(BOB),
            make_contact(CARLA, state=ContactState.INCOMING_REQUEST),
        ]
        for contact in contacts:
            store.save_contact(contact)
        expected = store.list_contacts()
        assert self.reopen(store).list_contacts() == expected
        assert len(expected) == 2

    def test_max_lamport_survives_a_restart(self, store: LocalStorePort) -> None:
        """The clock is derived from stored messages: a restart cannot set it back."""
        store.enqueue_outgoing(make_outgoing(seq=1, lamport=5))
        store.save_incoming_message(make_received(seq=1, lamport=7))
        assert self.reopen(store).max_lamport(WITH_BOB) == 7

    def test_history_and_the_seen_set_survive_a_restart(
        self, store: LocalStorePort
    ) -> None:
        """History is kept, the id is still "seen", and a duplicate is still
        rejected after reopening."""
        message = make_received()
        store.save_incoming_message(message)
        reopened = self.reopen(store)
        assert reopened.has_received(message.message_id) is True
        assert reopened.list_received(message.conversation_id) == [message]
        assert reopened.save_incoming_message(message) is False

    def test_history_order_survives_a_restart(self, store: LocalStorePort) -> None:
        """Storage order is preserved after reopening."""
        messages = [make_received(seq=seq) for seq in (3, 1, 2)]
        for message in messages:
            store.save_incoming_message(message)
        assert self.reopen(store).list_received(FROM_BOB) == messages

    def test_the_outbox_survives_a_restart_exactly(self, store: LocalStorePort) -> None:
        """A queued message, including its delivery state and its envelope
        bytes, is identical after reopening."""
        message = make_outgoing(status=A, attempts=3, next_attempt_at_ms=9_999)
        store.enqueue_outgoing(message)
        stored = _get(self.reopen(store), message.message_id)
        assert stored == message
        assert stored.envelope.to_bytes() == message.envelope.to_bytes()

    def test_an_update_survives_a_restart(self, store: LocalStorePort) -> None:
        """A change made by ``update_outgoing`` is durable, not just the first enqueue."""
        message = make_outgoing()
        store.enqueue_outgoing(message)
        updated = dataclasses.replace(
            message, status=A, attempts=1, next_attempt_at_ms=2_000
        )
        store.update_outgoing(updated)
        assert _get(self.reopen(store), message.message_id) == updated

    def test_the_seq_counter_survives_a_restart(self, store: LocalStorePort) -> None:
        """``last_outgoing_seq`` is the same after reopening, so seq numbers are never reused."""
        store.enqueue_outgoing(make_outgoing(seq=4))
        assert self.reopen(store).last_outgoing_seq(WITH_BOB) == 4

    def test_a_deletion_survives_a_restart(self, store: LocalStorePort) -> None:
        """A deleted contact does not reappear after reopening."""
        store.save_contact(make_contact(BOB))
        store.delete_contact(BOB.user_id)
        assert self.reopen(store).get_contact(BOB.user_id) is None

    def test_a_rejected_write_leaves_nothing_behind_after_a_restart(
        self, store: LocalStorePort
    ) -> None:
        """A write refused with ``AlreadyExistsError`` leaves no partial data,
        also after reopening."""
        first = make_outgoing(seq=1)
        store.enqueue_outgoing(first)
        with pytest.raises(AlreadyExistsError):
            store.enqueue_outgoing(make_outgoing(seq=1, body="clash"))
        assert self.reopen(store).list_outgoing() == [first]

    def test_the_inbound_state_survives_a_restart(self, store: LocalStorePort) -> None:
        """next_seq, skipped seqs and buffered messages come back identical."""
        released = make_received(sender=BOB, seq=2)
        state = _state(3, [make_received(sender=BOB, seq=5)], skipped={1})
        store.commit_inbound(FROM_BOB, BOB.user_id, [released], state)

        reopened = self.reopen(store)

        assert reopened.load_inbound_stream(FROM_BOB, BOB.user_id) == state
        assert reopened.list_received(FROM_BOB) == [released]
        assert reopened.has_received(released.message_id) is True

    def test_next_seq_still_never_goes_back_after_a_restart(
        self, store: LocalStorePort
    ) -> None:
        """The "never go back" rule uses the durable state, not memory."""
        store.commit_inbound(FROM_BOB, BOB.user_id, [], _state(5))
        with pytest.raises(ValueError):
            self.reopen(store).commit_inbound(FROM_BOB, BOB.user_id, [], _state(4))


_CHANGES_STATE = {Arrival.DELIVERED, Arrival.BUFFERED, Arrival.LATE}


def _state(
    next_seq: int,
    pending: list[ReceivedMessage] | None = None,
    skipped: set[int] | None = None,
) -> InboundState:
    """A stream state whose buffer holds exactly ``pending`` (sorted by seq)."""
    return StreamSnapshot(
        next_seq=next_seq,
        pending=tuple(
            Pending(m.seq, m.message_id, m)
            for m in sorted(pending or [], key=lambda m: m.seq)
        ),
        skipped=frozenset(skipped or set()),
    )
