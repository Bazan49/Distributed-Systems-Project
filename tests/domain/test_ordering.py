"""Tests for per-sender ordering and duplicate suppression.

The first part checks each rule on hand-written arrival sequences. The second
part checks the guarantees with hypothesis, over arbitrary shuffles,
duplicates, clock advances and restarts:

* no gap timeout: any shuffle and duplication gives exactly the ordered
  history (the "done when" criterion of the plan);
* any timing: no message is ever lost or released twice, and FIFO is only
  broken for messages that were explicitly skipped (flagged ``late``);
* snapshot and restore at any point do not change the outcome.
"""

from __future__ import annotations

from collections import deque

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from source.domain.identifiers import MessageId
from source.domain.ordering import (
    Arrival,
    Delivery,
    GapResult,
    InboundStream,
    OfferResult,
    OrderingPolicy,
    Pending,
    StreamSnapshot,
)

TIMEOUT = 1_000


def mid(seq: int, variant: int = 0) -> MessageId:
    """A deterministic MessageId per (seq, variant): variant != 0 is a different message."""
    return MessageId(f"{variant:016x}{seq:016x}")


def stream(**policy: int) -> InboundStream[str]:
    return InboundStream(OrderingPolicy(**{"gap_timeout_ms": TIMEOUT, **policy}))


def offer(
    s: InboundStream[str], seq: int, now_ms: int = 0, variant: int = 0
) -> OfferResult[str]:
    return s.offer(seq, mid(seq, variant), f"m{seq}", now_ms)


def seqs(deliveries: tuple[Delivery[str], ...]) -> list[int]:
    return [d.seq for d in deliveries]


# ---------------- in order ----------------


def test_a_new_stream_waits_for_seq_1() -> None:
    s = stream()
    assert s.next_seq == 1
    assert s.buffered == 0
    assert s.gap is None
    assert s.deadline_ms is None


def test_in_order_messages_are_released_one_by_one() -> None:
    s = stream()
    for seq in (1, 2, 3):
        result = offer(s, seq)
        assert result.arrival is Arrival.DELIVERED
        assert result.released == (Delivery(seq, mid(seq), f"m{seq}"),)
    assert s.next_seq == 4


def test_the_item_is_returned_untouched() -> None:
    payload = object()
    s: InboundStream[object] = InboundStream()
    (delivery,) = s.offer(1, mid(1), payload, 0).released
    assert delivery.item is payload


# ---------------- out of order ----------------


def test_a_message_ahead_is_buffered_and_opens_a_gap() -> None:
    s = stream()
    result = offer(s, 3, now_ms=50)

    assert result.arrival is Arrival.BUFFERED
    assert result.released == ()
    assert s.buffered == 1
    assert s.gap == range(1, 3)
    assert s.deadline_ms == 50 + TIMEOUT


def test_filling_the_gap_releases_the_whole_run_in_order() -> None:
    s = stream()
    offer(s, 4)
    offer(s, 2)
    offer(s, 3)

    result = offer(s, 1)

    assert result.arrival is Arrival.DELIVERED
    assert seqs(result.released) == [1, 2, 3, 4]
    assert s.buffered == 0
    assert s.gap is None
    assert s.deadline_ms is None


def test_releasing_stops_at_the_next_hole() -> None:
    s = stream()
    offer(s, 2)
    offer(s, 4)

    result = offer(s, 1)

    assert seqs(result.released) == [1, 2]
    assert s.next_seq == 3
    assert s.gap == range(3, 4)


def test_the_deadline_is_set_by_the_first_buffered_message_only() -> None:
    s = stream()
    offer(s, 3, now_ms=10)
    offer(s, 5, now_ms=500)
    assert s.deadline_ms == 10 + TIMEOUT


def test_a_remaining_gap_after_a_release_is_timed_from_that_moment() -> None:
    s = stream()
    offer(s, 2, now_ms=0)
    offer(s, 4, now_ms=0)

    offer(s, 1, now_ms=700)  # releases 1-2; the hole at 3 is a NEW gap

    assert s.deadline_ms == 700 + TIMEOUT


# ---------------- duplicates and conflicts ----------------


def test_a_resent_delivered_message_is_a_duplicate() -> None:
    s = stream()
    offer(s, 1)
    assert offer(s, 1).arrival is Arrival.DUPLICATE
    assert s.next_seq == 2


def test_a_resent_buffered_message_is_a_duplicate() -> None:
    s = stream()
    offer(s, 3)
    assert offer(s, 3).arrival is Arrival.DUPLICATE
    assert s.buffered == 1


def test_a_different_message_with_a_buffered_seq_is_a_conflict() -> None:
    s = stream()
    offer(s, 3)

    result = offer(s, 3, variant=1)

    assert result.arrival is Arrival.CONFLICT
    # The first one wins.
    assert s.snapshot().pending == (Pending(3, mid(3), "m3"),)


def test_a_buffered_id_reused_with_another_seq_is_a_conflict() -> None:
    s = stream()
    s.offer(3, mid(99), "x", 0)
    assert s.offer(4, mid(99), "y", 0).arrival is Arrival.CONFLICT
    assert s.buffered == 1


# ---------------- bounded buffer ----------------


def test_max_ahead_is_the_furthest_seq_that_is_buffered() -> None:
    s = stream(max_ahead=3)
    assert offer(s, 4).arrival is Arrival.BUFFERED  # 4 - 1 == 3
    assert offer(s, 5).arrival is Arrival.TOO_FAR_AHEAD
    assert s.buffered == 1


def test_too_far_ahead_is_accepted_once_the_stream_catches_up() -> None:
    s = stream(max_ahead=2)
    assert offer(s, 4).arrival is Arrival.TOO_FAR_AHEAD
    offer(s, 1)
    offer(s, 2)
    assert offer(s, 4).arrival is Arrival.BUFFERED


# ---------------- gap timeout ----------------


def test_expire_does_nothing_without_a_gap() -> None:
    s = stream()
    offer(s, 1)
    assert s.expire(10**9) == GapResult()


def test_expire_does_nothing_before_the_deadline() -> None:
    s = stream()
    offer(s, 3, now_ms=0)
    assert s.expire(TIMEOUT - 1) == GapResult()
    assert s.next_seq == 1


def test_expire_skips_the_gap_at_the_deadline() -> None:
    s = stream()
    offer(s, 3, now_ms=0)
    offer(s, 4, now_ms=0)

    result = s.expire(TIMEOUT)

    assert result.skipped == range(1, 3)
    assert seqs(result.released) == [3, 4]
    assert all(not d.late for d in result.released)
    assert s.next_seq == 5
    assert s.deadline_ms is None


def test_expire_skips_one_gap_and_restarts_the_timer_for_the_next() -> None:
    s = stream()
    offer(s, 2, now_ms=0)
    offer(s, 4, now_ms=0)

    first = s.expire(5_000)

    assert first.skipped == range(1, 2)
    assert seqs(first.released) == [2]
    assert s.gap == range(3, 4)
    assert s.deadline_ms == 5_000 + TIMEOUT
    assert s.expire(5_000 + TIMEOUT - 1) == GapResult()
    assert s.expire(5_000 + TIMEOUT).skipped == range(3, 4)


def test_a_missing_message_arriving_after_the_deadline_still_fills_in_order() -> None:
    s = stream()
    offer(s, 2, now_ms=0)

    result = offer(s, 1, now_ms=TIMEOUT * 10)  # expire was never called

    assert result.arrival is Arrival.DELIVERED
    assert seqs(result.released) == [1, 2]


def test_zero_timeout_skips_on_the_first_expire() -> None:
    s = stream(gap_timeout_ms=0)
    offer(s, 2, now_ms=42)
    assert s.expire(42).skipped == range(1, 2)


# ---------------- late arrivals ----------------


def test_a_skipped_message_that_arrives_later_is_released_as_late() -> None:
    s = stream()
    offer(s, 3)
    s.expire(TIMEOUT)

    result = offer(s, 1)

    assert result.arrival is Arrival.LATE
    assert result.released == (Delivery(1, mid(1), "m1", late=True),)
    assert s.next_seq == 4  # a late arrival does not move the stream


def test_a_late_message_is_released_only_once() -> None:
    s = stream()
    offer(s, 2)
    s.expire(TIMEOUT)
    offer(s, 1)
    assert offer(s, 1).arrival is Arrival.DUPLICATE


def test_skipped_seqs_beyond_the_late_window_are_forgotten() -> None:
    s = stream(late_window=2)
    offer(s, 2)
    s.expire(TIMEOUT)  # skips 1; next_seq = 3
    offer(s, 3)
    offer(s, 4)  # next_seq = 5: seq 1 is now 4 behind, outside the window

    assert offer(s, 1).arrival is Arrival.DUPLICATE


def test_late_window_zero_disables_late_delivery() -> None:
    s = stream(late_window=0)
    offer(s, 2)
    s.expire(TIMEOUT)
    assert offer(s, 1).arrival is Arrival.DUPLICATE


# ---------------- arrival semantics ----------------


@pytest.mark.parametrize(
    ("arrival", "final"),
    [
        (Arrival.DELIVERED, True),
        (Arrival.LATE, True),
        (Arrival.DUPLICATE, True),
        (Arrival.CONFLICT, True),
        (Arrival.BUFFERED, False),
        (Arrival.TOO_FAR_AHEAD, False),
    ],
)
def test_which_arrivals_are_final(arrival: Arrival, final: bool) -> None:
    assert arrival.final is final


# ---------------- validation ----------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"gap_timeout_ms": -1},
        {"gap_timeout_ms": 1.5},
        {"max_ahead": 0},
        {"max_ahead": True},
        {"late_window": -1},
    ],
)
def test_policy_rejects_invalid_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        OrderingPolicy(**kwargs)


def test_policy_defaults_are_valid() -> None:
    policy = OrderingPolicy()
    assert InboundStream(policy).policy is policy


@pytest.mark.parametrize(
    ("seq", "message_id", "now_ms"),
    [
        (0, mid(1), 0),
        (True, mid(1), 0),
        ("1", mid(1), 0),
        (1, "0" * 32, 0),
        (1, mid(1), -1),
    ],
)
def test_offer_rejects_invalid_arguments(
    seq: object, message_id: object, now_ms: object
) -> None:
    with pytest.raises(ValueError):
        InboundStream().offer(seq, message_id, "x", now_ms)


def test_expire_rejects_a_negative_time() -> None:
    with pytest.raises(ValueError):
        InboundStream().expire(-1)


@pytest.mark.parametrize(("seq", "message_id"), [(0, mid(1)), (1, "id")])
def test_pending_validates_its_fields(seq: int, message_id: object) -> None:
    with pytest.raises(ValueError):
        Pending(seq, message_id, "x")


# ---------------- snapshot and restore ----------------


def test_snapshot_captures_the_state() -> None:
    s = stream()
    offer(s, 4)
    offer(s, 2)
    s.expire(TIMEOUT)  # skips 1, releases 2; 4 still waits behind 3

    snap = s.snapshot()

    assert snap == StreamSnapshot(
        next_seq=3, pending=(Pending(4, mid(4), "m4"),), skipped=frozenset({1})
    )


def test_restore_continues_where_the_snapshot_left_off() -> None:
    s = stream()
    offer(s, 4)
    offer(s, 2)
    s.expire(TIMEOUT)

    restored = InboundStream.restore(s.snapshot(), now_ms=0, policy=s.policy)

    assert seqs(offer(restored, 3).released) == [3, 4]
    assert offer(restored, 1).arrival is Arrival.LATE


def test_restore_restarts_the_gap_timer() -> None:
    snap: StreamSnapshot[str] = StreamSnapshot(
        3, (Pending(5, mid(5), "m5"),), frozenset()
    )
    restored = InboundStream.restore(
        snap, now_ms=77, policy=OrderingPolicy(gap_timeout_ms=10)
    )
    assert restored.deadline_ms == 87


def test_restore_without_pending_has_no_deadline() -> None:
    restored: InboundStream[str] = InboundStream.restore(
        StreamSnapshot(9, (), frozenset()), now_ms=5
    )
    assert restored.next_seq == 9
    assert restored.deadline_ms is None


def test_restore_applies_the_late_window() -> None:
    snap: StreamSnapshot[str] = StreamSnapshot(100, (), frozenset({1, 99}))
    restored = InboundStream.restore(
        snap, now_ms=0, policy=OrderingPolicy(late_window=5)
    )
    assert restored.snapshot().skipped == frozenset({99})


@pytest.mark.parametrize(
    ("next_seq", "pending", "skipped"),
    [
        (0, (), frozenset()),
        (True, (), frozenset()),
        (3, (Pending(3, mid(3), "x"),), frozenset()),  # should have been released
        (3, (Pending(2, mid(2), "x"),), frozenset()),  # behind next_seq
        (
            1,
            (Pending(3, mid(3), "x"), Pending(2, mid(2), "y")),
            frozenset(),
        ),  # unsorted
        (1, (Pending(2, mid(2), "x"), Pending(2, mid(2, 1), "y")), frozenset()),
        (1, (Pending(2, mid(9), "x"), Pending(3, mid(9), "y")), frozenset()),
        (1, ("not pending",), frozenset()),
        (1, [Pending(2, mid(2), "x")], frozenset()),  # not a tuple
        (3, (), frozenset({3})),  # skipped must be behind next_seq
        (3, (), frozenset({0})),
        (3, (), frozenset({True})),
        (3, (), {1}),  # not a frozenset
    ],
)
def test_an_inconsistent_snapshot_cannot_be_built(
    next_seq: int, pending: object, skipped: object
) -> None:
    with pytest.raises(ValueError):
        StreamSnapshot(next_seq, pending, skipped)


def test_restore_rejects_a_snapshot_beyond_max_ahead() -> None:
    snap: StreamSnapshot[str] = StreamSnapshot(
        1, (Pending(300, mid(300), "x"),), frozenset()
    )
    with pytest.raises(ValueError):
        InboundStream.restore(snap, now_ms=0, policy=OrderingPolicy(max_ahead=10))


def test_restore_rejects_something_that_is_not_a_snapshot() -> None:
    with pytest.raises(ValueError):
        InboundStream.restore("snapshot", now_ms=0)


def test_restore_rejects_a_negative_time() -> None:
    with pytest.raises(ValueError):
        InboundStream.restore(StreamSnapshot(1, (), frozenset()), now_ms=-1)


# ---------------- properties ----------------


@st.composite
def arrivals(draw: st.DrawFn, max_messages: int = 40) -> tuple[int, list[int]]:
    """``n`` messages, each arriving at least once, some several times, in any order."""
    n = draw(st.integers(min_value=1, max_value=max_messages))
    extra = draw(st.lists(st.integers(min_value=1, max_value=n), max_size=2 * n))
    order = draw(st.permutations(list(range(1, n + 1)) + extra))
    return n, list(order)


@given(arrivals())
def test_any_shuffle_and_duplication_gives_the_same_ordered_history(
    case: tuple[int, list[int]],
) -> None:
    n, order = case
    s = stream(max_ahead=n)
    history: list[Delivery[str]] = []

    for seq in order:
        history.extend(offer(s, seq).released)

    assert history == [Delivery(seq, mid(seq), f"m{seq}") for seq in range(1, n + 1)]
    assert s.buffered == 0
    assert s.deadline_ms is None


@settings(max_examples=300)
@given(arrivals(max_messages=30), st.data())
def test_no_message_is_lost_or_repeated_whatever_the_timing(
    case: tuple[int, list[int]], data: st.DataObject
) -> None:
    n, order = case
    s = stream(
        gap_timeout_ms=data.draw(st.integers(min_value=0, max_value=100)),
        max_ahead=data.draw(st.integers(min_value=1, max_value=n + 1)),
        late_window=n,
    )
    queue = deque(order)
    now = 0
    history: list[Delivery[str]] = []
    skipped: set[int] = set()

    while queue:
        now += data.draw(st.integers(min_value=0, max_value=60))
        if data.draw(st.booleans()):
            gap = s.expire(now)
            skipped.update(gap.skipped or ())
            history.extend(gap.released)
        seq = queue.popleft()
        result = offer(s, seq, now_ms=now)
        if result.arrival is Arrival.TOO_FAR_AHEAD:
            queue.append(seq)  # not taken: it stays in the mailbox and comes back
        history.extend(result.released)

    released = [d.seq for d in history]
    # Every message exactly once: nothing lost, nothing repeated.
    assert sorted(released) == list(range(1, n + 1))
    # FIFO holds for everything that was not explicitly skipped...
    in_order = [d.seq for d in history if not d.late]
    assert in_order == sorted(in_order)
    # ...and every out-of-order delivery was announced as skipped first.
    assert {d.seq for d in history if d.late} <= skipped
    assert all(d.item == f"m{d.seq}" for d in history)
    assert s.next_seq == n + 1


@given(arrivals(), st.data())
def test_a_restart_at_any_point_changes_nothing(
    case: tuple[int, list[int]], data: st.DataObject
) -> None:
    n, order = case
    cut = data.draw(st.integers(min_value=0, max_value=len(order)))
    policy = OrderingPolicy(gap_timeout_ms=TIMEOUT, max_ahead=n)
    s: InboundStream[str] = InboundStream(policy)
    history: list[int] = []

    for seq in order[:cut]:
        history.extend(seqs(offer(s, seq).released))
    s = InboundStream.restore(s.snapshot(), now_ms=0, policy=policy)
    for seq in order[cut:]:
        history.extend(seqs(offer(s, seq).released))

    assert history == list(range(1, n + 1))
