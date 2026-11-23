"""Per-sender ordering and duplicate suppression of incoming messages.

The problem:
Messages from one sender reach the receiver over two independent paths (direct
and mailbox), with retries. So they can arrive out of order, more than once,
or not at all. This module turns that arrival sequence back into the order the
sender wrote them in.

The guarantee: per-sender FIFO

Every message carries ``seq``: the sender's counter for the conversation,
starting at 1 with no holes (it lives inside the encrypted ``MessageContent``,
so only the receiver sees it). An ``InboundStream`` releases the messages of
ONE sender in exactly ``seq`` order, each one exactly once:

* a message with the expected ``seq`` is released at once, together with every
  buffered message that directly follows it;
* a message from further ahead waits in a bounded buffer;
* a message already released, or already buffered, is a duplicate and is
  dropped. Its ``MessageId`` is the same on every path and retry, but the
  ``seq`` alone is enough here, since it identifies a message within the stream.

If a ``seq`` never arrives (the message expired in the mailboxes, or the sender
lost its device), waiting forever would block everything behind it. So a gap
has a deadline: when it passes, the stream gives up on the missing numbers,
reports them, and moves on. If a skipped message shows up later anyway it is
still released, flagged as ``late``: arriving out of place is better than being
lost. This is the ONLY case in which FIFO order is broken, and it is always
visible to the caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

from source.domain.canonical import is_int
from source.domain.identifiers import MessageId

__all__ = [
    "DEFAULT_GAP_TIMEOUT_MS",
    "DEFAULT_LATE_WINDOW",
    "DEFAULT_MAX_AHEAD",
    "Arrival",
    "Delivery",
    "GapResult",
    "InboundStream",
    "OfferResult",
    "OrderingPolicy",
    "Pending",
    "StreamSnapshot",
]

T = TypeVar("T")

# How long the stream waits for a missing seq before skipping it. Long enough
# to cover a sender retrying a deposit with backoff; short enough that one lost
# message does not freeze a conversation.

DEFAULT_GAP_TIMEOUT_MS = 2 * 60 * 1000  # 2 minutes

# How far beyond the expected seq a message may be and still be buffered. It
# also bounds the buffer: at most this many messages are ever held.

DEFAULT_MAX_AHEAD = 256  # maximum number of messages that can be buffered

# How many seqs behind the expected one the stream remembers which ones it
# skipped, so a late arrival can still be released.

DEFAULT_LATE_WINDOW = 1024  # how many seqs behind the expected one the stream remembers


def _require_int(value: int, minimum: int, what: str) -> int:
    """Return ``value`` if it is a real ``int`` (not ``bool``) >= ``minimum``."""
    if not is_int(value) or value < minimum:
        raise ValueError(f"{what} must be an integer >= {minimum}")
    return value


# ordering policy


@dataclass(frozen=True)
class OrderingPolicy:
    """Tunable limits of an ``InboundStream``. Immutable, validated on creation.

    Attributes:
        gap_timeout_ms: how long a gap may stay open before it is skipped.
            ``0`` skips it on the first ``expire`` call.
        max_ahead: the largest distance ``seq - next_seq`` that is buffered;
            anything further is refused (``TOO_FAR_AHEAD``). It bounds memory:
            a buggy or hostile sender cannot make the receiver hold more than
            ``max_ahead`` messages per stream.
        late_window: how many seqs behind ``next_seq`` the stream remembers
            the skipped ones. A skipped message arriving later than that is
            treated as a duplicate and dropped. ``0`` disables late delivery.
    """

    gap_timeout_ms: int = DEFAULT_GAP_TIMEOUT_MS
    max_ahead: int = DEFAULT_MAX_AHEAD
    late_window: int = DEFAULT_LATE_WINDOW

    def __post_init__(self) -> None:
        _require_int(self.gap_timeout_ms, 0, "gap_timeout_ms")
        _require_int(self.max_ahead, 1, "max_ahead")
        _require_int(self.late_window, 0, "late_window")


# results


class Arrival(str, Enum):
    """What the stream did with one offered message."""

    DELIVERED = "delivered"
    """It was the expected seq: released, possibly with buffered followers."""

    BUFFERED = "buffered"
    """Ahead of a gap: held until the gap is filled or skipped."""

    LATE = "late"
    """Its seq had already been skipped: released now, out of FIFO order."""

    DUPLICATE = "duplicate"
    """Already released or already buffered: dropped."""

    CONFLICT = "conflict"
    """Its seq (or id) is already taken by a DIFFERENT message: dropped.

    Only the sender can sign a message, so this is a sender bug or a sender
    misbehaving in its own conversation. The first message wins.
    """

    TOO_FAR_AHEAD = "too_far_ahead"
    """Beyond ``max_ahead``: refused for now. It must be offered again later
    (it stays in the mailbox, or the sender retries)."""

    @property
    def final(self) -> bool:
        """True if this message never needs to be offered again.

        The caller may acknowledge a final message (to the mailbox or to the
        sender) once everything released by the same call is stored. A
        ``BUFFERED`` message is not final: the buffer lives in memory, so it
        must not be acknowledged until it is released, unless the caller
        persists the stream (``snapshot``).
        """
        return self not in (Arrival.BUFFERED, Arrival.TOO_FAR_AHEAD)


@dataclass(frozen=True)
class Pending(Generic[T]):
    """A buffered message: its position, its id and the caller's payload."""

    seq: int
    message_id: MessageId
    item: T

    def __post_init__(self) -> None:
        _require_int(self.seq, 1, "seq")
        if not isinstance(self.message_id, MessageId):
            raise ValueError("message_id must be a MessageId")


@dataclass(frozen=True)
class Delivery(Generic[T]):
    """A message released by the stream, ready to be stored in the history.

    ``late`` is True when the message arrived after its gap was skipped: it is
    released after messages with a higher seq, and the UI may want to say so.
    """

    seq: int
    message_id: MessageId
    item: T
    late: bool = False


@dataclass(frozen=True)
class OfferResult(Generic[T]):
    """Outcome of ``InboundStream.offer``.

    ``released`` is in the order the messages must be stored. It is empty for
    every arrival except ``DELIVERED`` (one or more) and ``LATE`` (exactly one).
    """

    arrival: Arrival
    released: tuple[Delivery[T], ...] = ()


@dataclass(frozen=True)
class GapResult(Generic[T]):
    """Outcome of ``InboundStream.expire``.

    ``skipped`` is the range of seqs given up on (``None`` if nothing expired)
    and ``released`` the buffered messages that the skip unblocked, in order.
    """

    skipped: range | None = None
    released: tuple[Delivery[T], ...] = ()


@dataclass(frozen=True)
class StreamSnapshot(Generic[T]):
    """Everything needed to rebuild an ``InboundStream`` after a restart.

    It holds no time: deadlines are monotonic readings, meaningless in another
    process, so ``InboundStream.restore`` starts the gap timer afresh.

    Validated on creation, so a snapshot read back from storage is either
    consistent or rejected. The rules do not depend on any policy:

    * ``next_seq`` >= 1;
    * ``pending`` is sorted by seq, every seq is AHEAD of ``next_seq`` (the
      one at ``next_seq`` would already have been released) and no seq or
      ``MessageId`` appears twice;
    * every ``skipped`` seq is BEHIND ``next_seq``.

    Raises:
        ValueError: any rule above is broken.
    """

    next_seq: int
    pending: tuple[Pending[T], ...]
    skipped: frozenset[int]

    def __post_init__(self) -> None:
        _require_int(self.next_seq, 1, "next_seq")
        if not isinstance(self.pending, tuple):
            raise ValueError("pending must be a tuple")
        previous = self.next_seq
        ids: set[MessageId] = set()
        for entry in self.pending:
            if not isinstance(entry, Pending):
                raise ValueError("pending entries must be Pending")
            if entry.seq <= previous:
                raise ValueError(
                    f"buffered seq {entry.seq} is not ahead of next_seq or not sorted"
                )
            if entry.message_id in ids:
                raise ValueError(f"buffered message {entry.message_id} is repeated")
            ids.add(entry.message_id)
            previous = entry.seq
        if not isinstance(self.skipped, frozenset):
            raise ValueError("skipped must be a frozenset")
        for seq in self.skipped:
            if not is_int(seq) or not 1 <= seq < self.next_seq:
                raise ValueError(f"skipped seq {seq!r} is not behind next_seq")


# the stream


class InboundStream(Generic[T]):
    """Reorders the messages of ONE sender in ONE conversation.

    ``T`` is whatever the caller wants back for each message (typically the
    decrypted ``ReceivedMessage``); the stream never looks inside it. Keep one
    stream per ``(conversation_id, sender)``. Not thread-safe.
    """

    def __init__(self, policy: OrderingPolicy | None = None) -> None:
        self._policy = policy if policy is not None else OrderingPolicy()
        self._next_seq = 1
        self._pending: dict[int, Pending[T]] = {}
        self._pending_ids: dict[MessageId, int] = {}
        self._skipped: set[int] = set()
        self._deadline_ms: int | None = None

    # persistence

    def snapshot(self) -> StreamSnapshot[T]:
        """The current state, for the caller to persist.

        When to call it: after every ``offer`` or ``expire`` that changed the
        state, i.e. whose arrival is ``DELIVERED``, ``BUFFERED`` or ``LATE``,
        or whose ``GapResult`` skipped something. Save it in the SAME
        transaction as the messages that call released, so that after a crash
        the history and the stream state always agree. ``DUPLICATE``,
        ``CONFLICT``, ``TOO_FAR_AHEAD`` and an ``expire`` that skipped nothing
        leave the state untouched: there is nothing new to save.
        """
        return StreamSnapshot(
            next_seq=self._next_seq,
            pending=tuple(self._pending[seq] for seq in sorted(self._pending)),
            skipped=frozenset(self._skipped),
        )

    @classmethod
    def restore(
        cls,
        snapshot: StreamSnapshot[T],
        now_ms: int,
        policy: OrderingPolicy | None = None,
    ) -> InboundStream[T]:
        """Rebuild a stream from ``snapshot``. A pending gap restarts its timer at ``now_ms``.

        When to call it: once per stream when the client starts (after the
        user logs in), with the last saved snapshot and the current monotonic
        time. A stream with no saved snapshot is simply ``InboundStream()``.
        The gap timer restarts from ``now_ms`` because the old deadline was a
        monotonic reading of the previous process and means nothing here.

        Raises:
            ValueError: ``now_ms`` is invalid, ``snapshot`` is not a
                ``StreamSnapshot``, or it buffers a message further ahead than
                ``policy.max_ahead`` allows. (Structural consistency is
                already guaranteed by ``StreamSnapshot`` itself.)
        """
        if not isinstance(snapshot, StreamSnapshot):
            raise ValueError("snapshot must be a StreamSnapshot")
        _require_int(now_ms, 0, "now_ms")
        stream: InboundStream[T] = cls(policy)
        # The snapshot validated its own structure; only the policy limit is
        # left to check, since a stricter policy may now refuse what it holds.
        stream._next_seq = snapshot.next_seq
        for pending in snapshot.pending:
            if pending.seq - stream._next_seq > stream._policy.max_ahead:
                raise ValueError(f"buffered seq {pending.seq} is beyond max_ahead")
            stream._pending[pending.seq] = pending
            stream._pending_ids[pending.message_id] = pending.seq
        stream._skipped = set(snapshot.skipped)
        stream._forget_old_skips()
        if stream._pending:
            stream._deadline_ms = now_ms + stream._policy.gap_timeout_ms
        return stream

    # inspection

    @property
    def policy(self) -> OrderingPolicy:
        return self._policy

    @property
    def next_seq(self) -> int:
        """The seq the stream is waiting for. Everything below it is settled."""
        return self._next_seq

    @property
    def buffered(self) -> int:
        """How many messages are waiting behind a gap."""
        return len(self._pending)

    @property
    def gap(self) -> range | None:
        """The seqs currently missing (``next_seq`` up to the first buffered one), or None."""
        if not self._pending:
            return None
        return range(self._next_seq, min(self._pending))

    @property
    def deadline_ms(self) -> int | None:
        """Monotonic time at which the current gap may be skipped, or None if there is no gap.

        The caller schedules a timer for it and then calls ``expire``.
        """
        return self._deadline_ms

    # operations

    def offer(
        self, seq: int, message_id: MessageId, item: T, now_ms: int
    ) -> OfferResult[T]:
        """Hand one arrived message to the stream.

        When to call it: every time a message of this sender arrives, by any
        path (direct or mailbox), after its signature was verified, its
        content decrypted, and its ``MessageId`` checked against the store
        (``LocalStorePort.has_received``). Store everything in ``released``,
        in order, then save ``snapshot()`` if the state changed.

        ``now_ms`` (monotonic) is only used to start the timer of a new gap.
        A missing message that arrives after the deadline but before
        ``expire`` is called still fills its gap in order.

        Raises:
            ValueError: ``seq`` or ``now_ms`` is not a valid integer, or
                ``message_id`` is not a ``MessageId``. These are caller bugs,
                not network conditions: ``seq`` was already validated when the
                content was parsed.
        """
        _require_int(seq, 1, "seq")
        _require_int(now_ms, 0, "now_ms")
        if not isinstance(message_id, MessageId):
            raise ValueError("message_id must be a MessageId")

        # Case 1: seq is BEHIND the expected one. Everything below next_seq is
        # already settled: either released, or skipped by ``expire``.
        if seq < self._next_seq:
            # 1a. Its gap was skipped and it finally showed up: release it now,
            # out of FIFO order and flagged as late, and forget the skip so a
            # second copy is a duplicate. next_seq does not move.
            if seq in self._skipped:
                self._skipped.discard(seq)
                late = Delivery(seq, message_id, item, late=True)
                return OfferResult(Arrival.LATE, (late,))
            # 1b. It was already released (a retry, or the copy that came by
            # the other path or from another replica). Drop it; state unchanged.
            return OfferResult(Arrival.DUPLICATE)

        # Case 2: seq is already WAITING in the buffer.
        held = self._pending.get(seq)
        if held is not None:
            # 2a. Same id: another copy of the buffered message. Drop it.
            # 2b. Different id: the sender reused a seq. The first one wins.
            same = held.message_id == message_id
            return OfferResult(Arrival.DUPLICATE if same else Arrival.CONFLICT)
        # Case 3: the id is already buffered under ANOTHER seq. A message has
        # one seq for life, so this is the sender misbehaving. Drop it.
        if message_id in self._pending_ids:
            return OfferResult(Arrival.CONFLICT)

        # Case 4: it is EXACTLY the expected seq. Release it together with the
        # buffered run that follows it (seq+1, seq+2, ... until the next hole).
        # next_seq moves past the run; if messages are still buffered, they sit
        # behind a NEW hole, whose timer starts now.
        if seq == self._next_seq:
            released = (Delivery(seq, message_id, item), *self._drain(seq + 1))
            self._restart_timer(now_ms)
            return OfferResult(Arrival.DELIVERED, released)

        # Case 5: it is AHEAD, but too far to keep (bounded memory). It is not
        # taken: it must NOT be acknowledged, so it stays in the mailbox (or
        # the sender retries) and is offered again once the stream catches up.
        if seq - self._next_seq > self._policy.max_ahead:
            return OfferResult(Arrival.TOO_FAR_AHEAD)

        # Case 6: it is AHEAD within range: there is a hole before it. Keep it
        # until the hole is filled (case 4) or skipped (``expire``). If this
        # opens the hole, start its timer; if the hole was already open, its
        # deadline does not move, so new arrivals cannot postpone the skip.
        self._pending[seq] = Pending(seq, message_id, item)
        self._pending_ids[message_id] = seq
        if self._deadline_ms is None:
            self._deadline_ms = now_ms + self._policy.gap_timeout_ms
        return OfferResult(Arrival.BUFFERED)

    def expire(self, now_ms: int) -> GapResult[T]:
        """Skip the current gap if its deadline has passed.

        When to call it: when the timer the caller scheduled for
        ``deadline_ms`` fires, and also when convenient (e.g. after a mailbox
        collect or when the client starts): calling it early is harmless, it
        just does nothing. Store ``released`` in order, show the user that
        ``skipped`` is missing, and save ``snapshot()`` if something was
        skipped. Then schedule a timer for the new ``deadline_ms``, if any.

        At most ONE gap is skipped per call. If the release uncovers another
        gap, that one gets a full ``gap_timeout_ms`` from ``now_ms``: its
        messages were blocked behind the first gap, so they had no chance to
        arrive in time.

        Raises:
            ValueError: ``now_ms`` is not a non-negative integer.
        """
        _require_int(now_ms, 0, "now_ms")
        # Case 1: there is no hole (nothing buffered), or its deadline has not
        # arrived yet. Keep waiting; state unchanged.
        if self._deadline_ms is None or now_ms < self._deadline_ms:
            return GapResult()
        # Case 2: the deadline passed. Give up on the missing seqs: from
        # next_seq up to (not including) the first buffered one.
        first_buffered = min(self._pending)
        skipped = range(self._next_seq, first_buffered)
        # Remember them, so that if one arrives later it is released as late
        # instead of being taken for a duplicate (see ``offer``, case 1a).
        self._skipped.update(skipped)
        # Continue as if the hole had been filled: release the buffered run
        # that starts right after it, and move next_seq past that run.
        released = self._drain(first_buffered)
        # If more messages are still buffered, they are behind ANOTHER hole,
        # which gets a full timeout starting now.
        self._restart_timer(now_ms)
        return GapResult(skipped=skipped, released=released)

    # internals

    def _drain(self, start: int) -> tuple[Delivery[T], ...]:
        """Release the buffered run that starts at ``start`` and advance ``next_seq``."""
        released: list[Delivery[T]] = []
        seq = start
        while seq in self._pending:
            pending = self._pending.pop(seq)
            del self._pending_ids[pending.message_id]
            released.append(Delivery(pending.seq, pending.message_id, pending.item))
            seq += 1
        self._next_seq = seq
        self._forget_old_skips()
        return tuple(released)

    def _restart_timer(self, now_ms: int) -> None:
        """After ``next_seq`` moved: a remaining gap is a NEW gap, timed from now."""
        self._deadline_ms = (
            now_ms + self._policy.gap_timeout_ms if self._pending else None
        )

    def _forget_old_skips(self) -> None:
        """Keep only the skipped seqs inside ``late_window``: bounded memory."""
        if not self._skipped:
            return
        horizon = self._next_seq - self._policy.late_window
        self._skipped = {seq for seq in self._skipped if seq >= horizon}
