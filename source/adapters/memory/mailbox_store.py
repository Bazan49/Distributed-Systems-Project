from source.domain.envelope import Envelope
from source.domain.identifiers import MessageId, UserId
from source.domain.records import PutOutcome


class InMemoryMailboxStore:
    """Dict-based MailboxStorePort for tests. Keeps insertion order (oldest first)."""

    def __init__(self) -> None:
        self._queues: dict[UserId, dict[MessageId, tuple[Envelope, float]]] = {}

    def put(self, envelope: Envelope, expires_at: float) -> PutOutcome:
        queue = self._queues.setdefault(envelope.receiver, {})
        if envelope.message_id in queue:
            return PutOutcome.ALREADY_PRESENT
        queue[envelope.message_id] = (envelope, expires_at)
        return PutOutcome.STORED

    def fetch(self, receiver: UserId, limit: int, now: float) -> list[Envelope]:
        queue = self._queues.get(receiver, {})
        alive = [env for env, expires_at in queue.values() if expires_at > now]
        return alive[:limit]

    def delete(self, receiver: UserId, message_ids: list[MessageId]) -> int:
        queue = self._queues.get(receiver, {})
        removed = 0
        for message_id in message_ids:
            if queue.pop(message_id, None) is not None:
                removed += 1
        return removed

    def purge_expired(self, now: float) -> int:
        removed = 0
        for queue in self._queues.values():
            for message_id in [m for m, (_, exp) in queue.items() if exp <= now]:
                del queue[message_id]
                removed += 1
        return removed
