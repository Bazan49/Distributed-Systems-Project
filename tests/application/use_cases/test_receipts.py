"""Tests for the receipts use case: confirming messages that went through a mailbox."""

from __future__ import annotations

import pytest

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.receipts import ReceiptsService
from source.application.use_cases.receive_messages import ReceiveResult, ReceiveStatus
from source.application.use_cases.send_message import SendMessageService
from source.domain.contacts import ContactState
from source.domain.envelope import MAX_RECEIPT_IDS, MessageKind, receipt_body
from source.domain.identifiers import MessageId
from source.domain.messages import OutgoingMessage, ReceivedMessage
from source.domain.outgoing_status import OutgoingStatus
from tests.support.builders import (
    ALICE,
    BOB,
    CARLA,
    Person,
    make_contact,
    make_identity,
    make_outgoing,
    make_received,
)
from tests.support.logs import has_event

BOB_SESSION = UnlockedIdentity(make_identity(BOB), BOB.keys.private_key)
ALICE_SESSION = UnlockedIdentity(make_identity(ALICE), ALICE.keys.private_key)


@pytest.fixture
def store() -> MemoryLocalStore:
    store = MemoryLocalStore()
    store.save_contact(make_contact(ALICE, state=ContactState.ACTIVE))
    store.save_contact(make_contact(BOB, state=ContactState.ACTIVE))
    store.save_contact(make_contact(CARLA, state=ContactState.ACTIVE))
    return store


@pytest.fixture
def service(store: MemoryLocalStore) -> ReceiptsService:
    return ReceiptsService(
        store, SendMessageService(FakeCrypto(), store, ManualClock())
    )


def arrived(
    sender: Person = ALICE,
    kind: MessageKind = MessageKind.TEXT,
    status: ReceiveStatus = ReceiveStatus.DELIVERED,
) -> ReceiveResult:
    """What receiving one envelope from ``sender`` reported."""
    return ReceiveResult(MessageId.new(), status, sender=sender.user_id, kind=kind)


def confirmed_ids(receipt: OutgoingMessage) -> list[str]:
    return receipt.body.split(",")


# ---------------- receiver side: sending receipts ----------------


def test_one_receipt_per_sender_with_the_ids_of_its_texts(
    service: ReceiptsService,
) -> None:
    from_alice = [arrived(ALICE), arrived(ALICE)]
    from_carla = arrived(CARLA)

    receipts = service.send_receipts(BOB_SESSION, [*from_alice, from_carla])

    assert [r.kind for r in receipts] == [MessageKind.RECEIPT, MessageKind.RECEIPT]
    assert [r.receiver for r in receipts] == [ALICE.user_id, CARLA.user_id]
    assert confirmed_ids(receipts[0]) == [r.message_id.value for r in from_alice]
    assert confirmed_ids(receipts[1]) == [from_carla.message_id.value]


@pytest.mark.parametrize(
    "status", [ReceiveStatus.DELIVERED, ReceiveStatus.BUFFERED, ReceiveStatus.LATE]
)
def test_every_text_now_on_the_device_is_confirmed(
    service: ReceiptsService, status: ReceiveStatus
) -> None:
    """Buffered behind a gap counts: it is durably on the device already."""
    assert len(service.send_receipts(BOB_SESSION, [arrived(status=status)])) == 1


@pytest.mark.parametrize(
    "status",
    [
        ReceiveStatus.DUPLICATE,
        ReceiveStatus.CONFLICT,
        ReceiveStatus.TOO_FAR_AHEAD,
        ReceiveStatus.NOT_FOR_ME,
        ReceiveStatus.UNKNOWN_SENDER,
        ReceiveStatus.BAD_SIGNATURE,
        ReceiveStatus.UNREADABLE,
    ],
)
def test_nothing_is_confirmed_for_envelopes_not_stored_now(
    service: ReceiptsService, status: ReceiveStatus
) -> None:
    assert service.send_receipts(BOB_SESSION, [arrived(status=status)]) == []


@pytest.mark.parametrize(
    "kind",
    [MessageKind.RECEIPT, MessageKind.CONTACT_REQUEST, MessageKind.CONTACT_ACCEPT],
)
def test_only_texts_are_confirmed_so_receipts_never_answer_receipts(
    service: ReceiptsService, kind: MessageKind
) -> None:
    assert service.send_receipts(BOB_SESSION, [arrived(kind=kind)]) == []


def test_a_sender_who_is_no_longer_a_contact_gets_no_receipt(
    service: ReceiptsService, store: MemoryLocalStore
) -> None:
    store.delete_contact(ALICE.user_id)
    assert service.send_receipts(BOB_SESSION, [arrived(ALICE)]) == []


def test_a_long_list_is_split_into_several_receipts(service: ReceiptsService) -> None:
    results = [arrived() for _ in range(MAX_RECEIPT_IDS + 1)]

    receipts = service.send_receipts(BOB_SESSION, results)

    assert [len(confirmed_ids(r)) for r in receipts] == [MAX_RECEIPT_IDS, 1]


def test_receipts_are_queued_to_be_delivered_like_any_message(
    service: ReceiptsService, store: MemoryLocalStore
) -> None:
    (receipt,) = service.send_receipts(BOB_SESSION, [arrived()])
    assert store.get_outgoing(receipt.message_id) == receipt
    assert receipt.status is OutgoingStatus.QUEUED


# ---------------- sender side: applying receipts ----------------


def sent_to_bob(
    store: MemoryLocalStore, seq: int, status: OutgoingStatus
) -> OutgoingMessage:
    message = make_outgoing(sender=ALICE, receiver=BOB, seq=seq, status=status)
    store.enqueue_outgoing(message)
    return message


def receipt_from(sender: Person, *ids: MessageId) -> ReceivedMessage:
    """A receipt from ``sender`` to Alice confirming ``ids``, about to be stored."""
    return make_received(
        sender=sender, me=ALICE, kind=MessageKind.RECEIPT, body=receipt_body(ids)
    )


def status_of(store: MemoryLocalStore, message: OutgoingMessage) -> OutgoingStatus:
    stored = store.get_outgoing(message.message_id)
    assert stored is not None
    return stored.status


@pytest.mark.parametrize(
    "before",
    [OutgoingStatus.ACCEPTED, OutgoingStatus.QUEUED, OutgoingStatus.EXPIRED],
)
def test_a_receipt_marks_the_confirmed_messages_delivered(
    service: ReceiptsService,
    store: MemoryLocalStore,
    before: OutgoingStatus,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Whatever the sender believed, the receiver's word that it arrived wins."""
    caplog.set_level("INFO", logger="p08")
    message = sent_to_bob(store, 1, before)

    service.on_released(ALICE_SESSION, [receipt_from(BOB, message.message_id)])

    assert status_of(store, message) is OutgoingStatus.DELIVERED
    assert has_event(caplog, "receipt.applied", count=1)


def test_applying_a_receipt_twice_changes_nothing(
    service: ReceiptsService, store: MemoryLocalStore
) -> None:
    message = sent_to_bob(store, 1, OutgoingStatus.ACCEPTED)
    receipt = receipt_from(BOB, message.message_id)

    service.on_released(ALICE_SESSION, [receipt])
    service.on_released(ALICE_SESSION, [receipt])

    assert status_of(store, message) is OutgoingStatus.DELIVERED


def test_only_the_receiver_can_confirm_a_message(
    service: ReceiptsService, store: MemoryLocalStore
) -> None:
    to_bob = sent_to_bob(store, 1, OutgoingStatus.ACCEPTED)

    service.on_released(ALICE_SESSION, [receipt_from(CARLA, to_bob.message_id)])

    assert status_of(store, to_bob) is OutgoingStatus.ACCEPTED


def test_unknown_ids_and_messages_that_are_not_texts_are_ignored(
    service: ReceiptsService, store: MemoryLocalStore
) -> None:
    request = make_outgoing(
        sender=ALICE,
        receiver=BOB,
        seq=1,
        kind=MessageKind.CONTACT_REQUEST,
        status=OutgoingStatus.ACCEPTED,
    )
    store.enqueue_outgoing(request)

    service.on_released(
        ALICE_SESSION, [receipt_from(BOB, request.message_id, MessageId.new())]
    )

    assert status_of(store, request) is OutgoingStatus.ACCEPTED


def test_messages_that_are_not_receipts_are_left_alone(
    service: ReceiptsService, store: MemoryLocalStore
) -> None:
    message = sent_to_bob(store, 1, OutgoingStatus.ACCEPTED)
    text = make_received(sender=BOB, me=ALICE, body=message.message_id.value)

    service.on_released(ALICE_SESSION, [text])

    assert status_of(store, message) is OutgoingStatus.ACCEPTED
