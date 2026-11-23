"""Tests for the receive-messages use case.

The plan's "done when": an altered envelope is rejected, and two deliveries of
the same envelope leave a single message. Beyond that, these tests follow the
pipeline step by step (addressing, duplicates, sender, signature,
readability, ordering, storage, confirmation), check the gap timers and the
restart path, and close with property and end-to-end tests.

Alice is the local user throughout; Bob is her active contact.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from nacl import pwhash

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.system.crypto import NaClCrypto
from source.application.ports.errors import StoreError
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.receive_messages import (
    GapReport,
    ReceiveMessagesService,
    ReceiveResult,
    ReceiveStatus,
)
from source.application.use_cases.send_message import SendMessageService
from source.domain.contacts import Contact, ContactState
from source.domain.envelope import Envelope, MessageContent, MessageKind
from source.domain.identifiers import Alias, ConversationId, MessageId, UserId
from source.domain.identity import LocalIdentity
from source.domain.messages import InboundState, ReceivedMessage
from source.domain.ordering import OrderingPolicy
from source.domain.records import KeyPair
from tests.support.builders import (
    ALICE,
    BASE_TIME_MS,
    BOB,
    CARLA,
    Person,
    conversation_of,
    make_contact,
    make_identity,
)

CRYPTO = FakeCrypto()
FROM_BOB = conversation_of(BOB, ALICE)
TIMEOUT = 1_000
POLICY = OrderingPolicy(gap_timeout_ms=TIMEOUT)


def session_of(person: Person) -> UnlockedIdentity:
    return UnlockedIdentity(make_identity(person), person.keys.private_key)


ALICE_SESSION = session_of(ALICE)


def craft(
    *,
    seq: int = 1,
    body: str | None = None,
    kind: MessageKind = MessageKind.TEXT,
    sender: Person = BOB,
    receiver: Person = ALICE,
    encrypt_for: Person | None = None,
    sign_with: Person | None = None,
    conversation: ConversationId | None = None,
    plaintext: bytes | None = None,
    message_id: MessageId | None = None,
) -> Envelope:
    """Build an envelope by hand, so every kind of bad envelope can be made."""
    content = MessageContent(
        kind,
        conversation or conversation_of(sender, receiver),
        seq,
        BASE_TIME_MS,
        f"m{seq}" if body is None else body,
    )
    ciphertext = CRYPTO.encrypt_for(
        (encrypt_for or receiver).keys.public_key,
        content.to_bytes() if plaintext is None else plaintext,
    )
    return Envelope.build(
        message_id=message_id or MessageId.new(),
        sender=sender.user_id,
        receiver=receiver.user_id,
        ciphertext=ciphertext,
        sign=lambda data: CRYPTO.sign((sign_with or sender).keys.private_key, data),
    )


def history(store: MemoryLocalStore) -> list[str]:
    return [m.body for m in store.list_received(FROM_BOB)]


class CountingCrypto(FakeCrypto):
    """Counts the expensive operations, to check when they are skipped."""

    def __init__(self) -> None:
        super().__init__()
        self.verified = 0
        self.decrypted = 0

    def verify(self, public_key: bytes, message: bytes, signature: bytes) -> bool:
        self.verified += 1
        return super().verify(public_key, message, signature)

    def decrypt(self, private_key: bytes, ciphertext: bytes) -> bytes:
        self.decrypted += 1
        return super().decrypt(private_key, ciphertext)


class FlakyStore(MemoryLocalStore):
    """A store whose next ``commit_inbound`` fails, like a full disk would."""

    fail_next_commit = False

    def commit_inbound(
        self,
        conversation_id: ConversationId,
        sender: UserId,
        released: Sequence[ReceivedMessage],
        state: InboundState,
    ) -> None:
        if self.fail_next_commit:
            self.fail_next_commit = False
            raise StoreError("disk full")
        super().commit_inbound(conversation_id, sender, released, state)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def store() -> MemoryLocalStore:
    store = MemoryLocalStore()
    store.save_contact(make_contact(BOB))
    return store


@pytest.fixture
def crypto() -> CountingCrypto:
    return CountingCrypto()


@pytest.fixture
def service(
    crypto: CountingCrypto, store: MemoryLocalStore, clock: ManualClock
) -> ReceiveMessagesService:
    return ReceiveMessagesService(crypto, store, clock, POLICY)


def receive(service: ReceiveMessagesService, envelope: Envelope) -> ReceiveResult:
    return service.receive(ALICE_SESSION, envelope)


# ---------------- the happy path ----------------


def test_a_valid_envelope_is_stored_and_confirmed(
    service: ReceiveMessagesService, store: MemoryLocalStore, clock: ManualClock
) -> None:
    envelope = craft(seq=1, body="hola")

    result = receive(service, envelope)

    assert result.status is ReceiveStatus.DELIVERED
    assert result.acknowledge
    assert result.message_id == envelope.message_id
    (stored,) = store.list_received(FROM_BOB)
    assert stored == ReceivedMessage(
        message_id=envelope.message_id,
        conversation_id=FROM_BOB,
        sender=BOB.user_id,
        seq=1,
        kind=MessageKind.TEXT,
        body="hola",
        sent_at_ms=BASE_TIME_MS,
        received_at_ms=clock.now_ms(),
    )
    assert [d.item for d in result.released] == [stored]
    assert store.has_received(envelope.message_id)


# ---------------- duplicates ----------------


def test_two_deliveries_of_the_same_envelope_leave_one_message(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    envelope = craft(seq=1)

    first = receive(service, envelope)
    second = receive(service, envelope)

    assert (first.status, second.status) == (
        ReceiveStatus.DELIVERED,
        ReceiveStatus.DUPLICATE,
    )
    assert second.acknowledge  # confirm it, so the mailboxes stop sending it
    assert second.released == ()
    assert history(store) == ["m1"]


def test_a_duplicate_costs_no_cryptography(
    service: ReceiveMessagesService, crypto: CountingCrypto
) -> None:
    envelope = craft(seq=1)
    receive(service, envelope)
    before = (crypto.verified, crypto.decrypted)

    for _ in range(3):  # e.g. the copies from the three mailbox replicas
        receive(service, envelope)

    assert (crypto.verified, crypto.decrypted) == before


def test_a_copy_of_a_buffered_message_is_a_duplicate(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    ahead = craft(seq=2)
    receive(service, ahead)

    assert receive(service, ahead).status is ReceiveStatus.DUPLICATE
    assert store.load_inbound_stream(FROM_BOB, BOB.user_id).pending[0].seq == 2  # type: ignore[union-attr]


# ---------------- tampering ----------------


def test_an_altered_ciphertext_is_rejected(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    envelope = craft(seq=1)
    flipped = bytes([envelope.ciphertext[0] ^ 1]) + envelope.ciphertext[1:]
    altered = dataclasses.replace(envelope, ciphertext=flipped)

    result = receive(service, altered)

    assert result.status is ReceiveStatus.BAD_SIGNATURE
    assert result.acknowledge  # refused for good: no point in seeing it again
    assert history(store) == []
    assert not store.has_received(envelope.message_id)


def test_an_altered_signature_is_rejected(service: ReceiveMessagesService) -> None:
    envelope = craft(seq=1)
    altered = dataclasses.replace(
        envelope, signature=bytes([envelope.signature[0] ^ 1]) + envelope.signature[1:]
    )
    assert receive(service, altered).status is ReceiveStatus.BAD_SIGNATURE


def test_someone_impersonating_a_contact_is_rejected(
    service: ReceiveMessagesService, crypto: CountingCrypto
) -> None:
    """Carla claims to be Bob, but she can only sign with HER key."""
    forged = craft(seq=1, sign_with=CARLA)

    assert receive(service, forged).status is ReceiveStatus.BAD_SIGNATURE
    assert crypto.decrypted == 0  # nothing forged is ever decrypted


def test_a_rejected_forgery_does_not_block_the_real_message(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    receive(service, craft(seq=1, sign_with=CARLA, body="fake"))
    assert receive(service, craft(seq=1, body="real")).status is ReceiveStatus.DELIVERED
    assert history(store) == ["real"]


# ---------------- unreadable content ----------------


def test_a_message_encrypted_for_someone_else_is_unreadable(
    service: ReceiveMessagesService,
) -> None:
    result = receive(service, craft(encrypt_for=CARLA))
    assert result.status is ReceiveStatus.UNREADABLE
    assert result.acknowledge


def test_a_content_that_is_not_valid_is_unreadable(
    service: ReceiveMessagesService,
) -> None:
    assert (
        receive(service, craft(plaintext=b"not json")).status
        is ReceiveStatus.UNREADABLE
    )


def test_a_ciphertext_lifted_from_another_conversation_is_unreadable(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    """Bob re-sends to Alice a content written for his chat with Carla."""
    other = conversation_of(BOB, CARLA)
    result = receive(service, craft(conversation=other))
    assert result.status is ReceiveStatus.UNREADABLE
    assert store.load_inbound_stream(FROM_BOB, BOB.user_id) is None


# ---------------- addressing and senders ----------------


def test_an_envelope_for_someone_else_is_not_ours(
    service: ReceiveMessagesService, crypto: CountingCrypto
) -> None:
    result = receive(service, craft(receiver=CARLA))
    assert result.status is ReceiveStatus.NOT_FOR_ME
    assert not result.acknowledge
    assert crypto.verified == 0


def test_an_unknown_sender_is_left_for_later(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    result = receive(service, craft(sender=CARLA))
    assert result.status is ReceiveStatus.UNKNOWN_SENDER
    assert not result.acknowledge  # 1A.9 may turn the sender into a contact
    assert store.list_received(conversation_of(CARLA, ALICE)) == []


@pytest.mark.parametrize(
    "state", [ContactState.OUTGOING_REQUEST, ContactState.INCOMING_REQUEST]
)
def test_messages_from_a_contact_in_any_state_are_ordered_and_stored(
    service: ReceiveMessagesService, store: MemoryLocalStore, state: ContactState
) -> None:
    """The contact state is NOT checked on arrival: a text may overtake the
    acceptance that precedes it, and ordering must still see both."""
    store.save_contact(make_contact(CARLA, state=state))
    accept = craft(sender=CARLA, seq=1, kind=MessageKind.CONTACT_ACCEPT, body="")
    text = craft(sender=CARLA, seq=2, body="hola")

    assert receive(service, text).status is ReceiveStatus.BUFFERED
    released = receive(service, accept).released

    assert [d.item.kind for d in released] == [
        MessageKind.CONTACT_ACCEPT,
        MessageKind.TEXT,
    ]


# ---------------- ordering ----------------


def test_an_early_message_is_buffered_durably_and_confirmed(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    result = receive(service, craft(seq=2))

    assert result.status is ReceiveStatus.BUFFERED
    assert result.acknowledge  # durable: it is in the committed stream state
    assert history(store) == []
    saved = store.load_inbound_stream(FROM_BOB, BOB.user_id)
    assert saved is not None and [p.seq for p in saved.pending] == [2]


def test_filling_the_gap_stores_the_run_in_order(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    receive(service, craft(seq=3))
    receive(service, craft(seq=2))

    result = receive(service, craft(seq=1))

    assert result.status is ReceiveStatus.DELIVERED
    assert [d.item.seq for d in result.released] == [1, 2, 3]
    assert history(store) == ["m1", "m2", "m3"]


def test_a_reused_seq_is_a_conflict_and_the_first_message_wins(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    receive(service, craft(seq=2, body="first"))
    result = receive(service, craft(seq=2, body="second"))
    receive(service, craft(seq=1))

    assert result.status is ReceiveStatus.CONFLICT
    assert result.acknowledge
    assert history(store) == ["m1", "first"]


def test_too_far_ahead_is_not_confirmed_and_works_later(
    crypto: CountingCrypto, store: MemoryLocalStore, clock: ManualClock
) -> None:
    service = ReceiveMessagesService(crypto, store, clock, OrderingPolicy(max_ahead=1))
    far = craft(seq=3)

    first = receive(service, far)
    receive(service, craft(seq=1))

    assert first.status is ReceiveStatus.TOO_FAR_AHEAD
    assert not first.acknowledge
    assert receive(service, far).status is ReceiveStatus.BUFFERED


# ---------------- gap timers ----------------


def test_no_deadline_without_a_gap(service: ReceiveMessagesService) -> None:
    receive(service, craft(seq=1))
    assert service.next_deadline_ms() is None
    assert service.expire_gaps(ALICE_SESSION) == []


def test_a_gap_reports_its_deadline(
    service: ReceiveMessagesService, clock: ManualClock
) -> None:
    receive(service, craft(seq=2))
    assert service.next_deadline_ms() == clock.monotonic_ms() + TIMEOUT


def test_an_expired_gap_is_skipped_and_what_waited_is_stored(
    service: ReceiveMessagesService, store: MemoryLocalStore, clock: ManualClock
) -> None:
    second = craft(seq=2)
    receive(service, second)
    clock.advance(TIMEOUT - 1)
    assert service.expire_gaps(ALICE_SESSION) == []

    clock.advance(1)
    (report,) = service.expire_gaps(ALICE_SESSION)

    assert report == GapReport(FROM_BOB, BOB.user_id, range(1, 2), report.released)
    assert [d.item.message_id for d in report.released] == [second.message_id]
    assert history(store) == ["m2"]
    assert service.next_deadline_ms() is None


def test_a_skipped_message_that_arrives_later_is_stored_as_late(
    service: ReceiveMessagesService, store: MemoryLocalStore, clock: ManualClock
) -> None:
    receive(service, craft(seq=2))
    clock.advance(TIMEOUT)
    service.expire_gaps(ALICE_SESSION)

    result = receive(service, craft(seq=1))

    assert result.status is ReceiveStatus.LATE
    assert result.released[0].late
    assert history(store) == ["m2", "m1"]  # storage order: as it happened


# ---------------- restarts and failures ----------------


def test_a_buffered_message_survives_a_restart(
    crypto: CountingCrypto, store: MemoryLocalStore, clock: ManualClock
) -> None:
    before = ReceiveMessagesService(crypto, store, clock, POLICY)
    receive(before, craft(seq=2))

    after = ReceiveMessagesService(crypto, store, clock, POLICY)  # new process
    result = receive(after, craft(seq=1))

    assert [d.item.seq for d in result.released] == [1, 2]
    assert history(store) == ["m1", "m2"]


def test_resume_restarts_the_gap_timers_after_a_restart(
    crypto: CountingCrypto, store: MemoryLocalStore, clock: ManualClock
) -> None:
    receive(ReceiveMessagesService(crypto, store, clock, POLICY), craft(seq=2))
    after = ReceiveMessagesService(crypto, store, clock, POLICY)
    clock.advance(TIMEOUT * 5)
    assert (
        after.expire_gaps(ALICE_SESSION) == []
    )  # nothing loaded: nothing would ever expire

    after.resume(ALICE_SESSION)
    assert after.next_deadline_ms() == clock.monotonic_ms() + TIMEOUT
    clock.advance(TIMEOUT)

    assert [r.skipped for r in after.expire_gaps(ALICE_SESSION)] == [range(1, 2)]
    assert history(store) == ["m2"]


def test_resume_ignores_a_contact_that_is_yourself(
    service: ReceiveMessagesService, store: MemoryLocalStore
) -> None:
    store.save_contact(make_contact(ALICE))
    service.resume(ALICE_SESSION)
    assert service.next_deadline_ms() is None


def test_a_failed_commit_loses_nothing(
    crypto: CountingCrypto, clock: ManualClock
) -> None:
    """If the store fails, the error propagates, nothing is confirmed, and the
    envelope received again later is processed as if for the first time."""
    store = FlakyStore()
    store.save_contact(make_contact(BOB))
    service = ReceiveMessagesService(crypto, store, clock, POLICY)
    second = craft(seq=2)

    store.fail_next_commit = True
    with pytest.raises(StoreError):
        receive(service, second)
    assert store.load_inbound_stream(FROM_BOB, BOB.user_id) is None

    assert receive(service, second).status is ReceiveStatus.BUFFERED
    receive(service, craft(seq=1))
    assert history(store) == ["m1", "m2"]


def test_a_failed_gap_commit_is_retried_by_the_next_expire(
    crypto: CountingCrypto, clock: ManualClock
) -> None:
    store = FlakyStore()
    store.save_contact(make_contact(BOB))
    service = ReceiveMessagesService(crypto, store, clock, POLICY)
    receive(service, craft(seq=2))
    clock.advance(TIMEOUT)

    store.fail_next_commit = True
    with pytest.raises(StoreError):
        service.expire_gaps(ALICE_SESSION)
    service.resume(ALICE_SESSION)  # the dropped stream is reloaded
    clock.advance(TIMEOUT)

    assert [r.skipped for r in service.expire_gaps(ALICE_SESSION)] == [range(1, 2)]
    assert history(store) == ["m2"]


# ---------------- batches ----------------


def test_a_batch_reports_what_to_acknowledge(
    service: ReceiveMessagesService,
) -> None:
    second, first = craft(seq=2), craft(seq=1)
    stranger = craft(sender=CARLA)

    batch = service.receive_batch(ALICE_SESSION, [second, second, stranger, first])

    assert [r.status for r in batch.results] == [
        ReceiveStatus.BUFFERED,
        ReceiveStatus.DUPLICATE,
        ReceiveStatus.UNKNOWN_SENDER,
        ReceiveStatus.DELIVERED,
    ]
    assert batch.to_acknowledge == [second.message_id, first.message_id]
    assert [d.item.seq for d in batch.released] == [1, 2]


# ---------------- argument checks ----------------


def test_receive_checks_its_argument_types(service: ReceiveMessagesService) -> None:
    with pytest.raises(TypeError):
        service.receive("session", craft())
    with pytest.raises(TypeError):
        service.receive(ALICE_SESSION, craft().to_bytes())


# ---------------- properties and end to end ----------------


@settings(max_examples=150)
@given(st.data())
def test_any_shuffle_and_duplication_gives_the_sent_order(data: st.DataObject) -> None:
    n = data.draw(st.integers(min_value=1, max_value=15))
    envelopes = [craft(seq=seq) for seq in range(1, n + 1)]
    extra = data.draw(st.lists(st.sampled_from(envelopes), max_size=2 * n))
    arrivals = data.draw(st.permutations(envelopes + extra))
    store = MemoryLocalStore()
    store.save_contact(make_contact(BOB))
    service = ReceiveMessagesService(FakeCrypto(), store, ManualClock(), POLICY)

    batch = service.receive_batch(ALICE_SESSION, arrivals)

    assert history(store) == [f"m{seq}" for seq in range(1, n + 1)]
    assert all(r.acknowledge for r in batch.results)
    assert set(batch.to_acknowledge) == {e.message_id for e in envelopes}


def test_send_then_receive_with_real_cryptography() -> None:
    """Bob's SendMessageService produces what Alice's ReceiveMessagesService
    stores, with NaClCrypto and through the wire format."""
    crypto = NaClCrypto(
        opslimit=pwhash.argon2id.OPSLIMIT_MIN, memlimit=pwhash.argon2id.MEMLIMIT_MIN
    )
    alice_keys, bob_keys = crypto.generate_key_pair(), crypto.generate_key_pair()
    alice_id = UserId.from_public_key(alice_keys.public_key)
    bob_id = UserId.from_public_key(bob_keys.public_key)

    def side(
        keys: KeyPair, me: UserId, other: UserId, other_pk: bytes
    ) -> tuple[UnlockedIdentity, MemoryLocalStore]:
        """One user's session, with the other one as an active contact."""
        store = MemoryLocalStore()
        store.save_contact(Contact(other, other_pk, Alias("x"), ContactState.ACTIVE, 0))
        identity = LocalIdentity(me, keys.public_key, Alias("me"), b"locked", 0)
        return UnlockedIdentity(identity, keys.private_key), store

    bob_session, bob_store = side(bob_keys, bob_id, alice_id, alice_keys.public_key)
    alice_session, alice_store = side(alice_keys, alice_id, bob_id, bob_keys.public_key)
    clock = ManualClock()
    sender = SendMessageService(crypto, bob_store, clock)
    receiver = ReceiveMessagesService(crypto, alice_store, clock)

    sent = [sender.send_text(bob_session, alice_id, f"msg {i}") for i in range(3)]
    wire = [Envelope.from_bytes(m.envelope.to_bytes()) for m in reversed(sent)]
    receiver.receive_batch(alice_session, wire + wire)

    conversation = ConversationId.between(alice_id, bob_id)
    assert [m.body for m in alice_store.list_received(conversation)] == [
        "msg 0",
        "msg 1",
        "msg 2",
    ]
