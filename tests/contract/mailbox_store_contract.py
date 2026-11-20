"""Contract every MailboxStorePort implementation must satisfy."""

from source.application.ports import MailboxStorePort
from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.records import PutOutcome

ALICE = UserId.from_public_key(b"alice")
BOB = UserId.from_public_key(b"bob")
CAROL = UserId.from_public_key(b"carol")
NOW = 1_000.0


def make_envelope(receiver: UserId = BOB) -> Envelope:
    """A real, valid Envelope (a fresh random MessageId each time)."""
    return Envelope.build(
        message_id=MessageId.new(),
        sender=ALICE,
        receiver=receiver,
        ciphertext=b"opaque",
        sign=lambda _bytes: b"sig",
    )


class MailboxStoreContract:
    """Subclass it and override the ``store`` fixture."""

    def test_put_then_fetch(self, store: MailboxStorePort) -> None:
        env = make_envelope()
        assert store.put(env, NOW + 100) is PutOutcome.STORED
        assert store.fetch(BOB, 10, NOW) == [env]

    def test_put_is_idempotent(self, store: MailboxStorePort) -> None:
        env = make_envelope()
        store.put(env, NOW + 100)
        assert store.put(env, NOW + 999) is PutOutcome.ALREADY_PRESENT
        assert store.fetch(BOB, 10, NOW) == [env]

    def test_fetch_returns_oldest_first(self, store: MailboxStorePort) -> None:
        envs = [make_envelope() for _ in range(3)]
        for env in envs:
            store.put(env, NOW + 100)
        assert store.fetch(BOB, 10, NOW) == envs

    def test_fetch_respects_limit(self, store: MailboxStorePort) -> None:
        for _ in range(3):
            store.put(make_envelope(), NOW + 100)
        assert len(store.fetch(BOB, 2, NOW)) == 2

    def test_fetch_never_returns_expired(self, store: MailboxStorePort) -> None:
        old, fresh = make_envelope(), make_envelope()
        store.put(old, NOW - 1)
        store.put(fresh, NOW + 100)
        assert store.fetch(BOB, 10, NOW) == [fresh]

    def test_receivers_are_isolated(self, store: MailboxStorePort) -> None:
        store.put(make_envelope(BOB), NOW + 100)
        assert store.fetch(CAROL, 10, NOW) == []

    def test_delete_removes_and_counts(self, store: MailboxStorePort) -> None:
        keep, drop = make_envelope(), make_envelope()
        store.put(keep, NOW + 100)
        store.put(drop, NOW + 100)
        assert store.delete(BOB, [drop.message_id, MessageId.new()]) == 1
        assert store.fetch(BOB, 10, NOW) == [keep]

    def test_purge_expired_counts_and_removes(self, store: MailboxStorePort) -> None:
        store.put(make_envelope(), NOW - 1)
        store.put(make_envelope(), NOW + 100)
        assert store.purge_expired(NOW) == 1
        assert store.purge_expired(NOW) == 0
