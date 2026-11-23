"""Tests for the signed mailbox requests and the replay guard (pure domain)."""

import json
from dataclasses import replace
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from source.domain.canonical import FormatError
from source.domain.identifiers import MessageId
from source.domain.mailbox_access import (
    MAX_ACK_IDS,
    MAX_COLLECT_LIMIT,
    MailboxAction,
    MailboxRequest,
    ReplayGuard,
    new_nonce,
)
from tests.support.builders import BOB, CARLA, ack_request, collect_request

NOW = 1_700_000_000_000


# ---------------- shape ----------------
def test_build_derives_the_receiver_from_the_key() -> None:
    request = collect_request(BOB, NOW)
    assert request.receiver == BOB.user_id and request.identity_matches()


def test_build_collapses_repeated_ids() -> None:
    mid = MessageId.new()
    assert ack_request(BOB, NOW, [mid, mid]).message_ids == (mid,)


def test_a_mismatched_receiver_is_detected() -> None:
    assert not replace(
        collect_request(BOB, NOW), receiver=CARLA.user_id
    ).identity_matches()


@pytest.mark.parametrize(
    "changes",
    [
        {"limit": 0},
        {"limit": MAX_COLLECT_LIMIT + 1},
        {"limit": True},
        {"message_ids": (MessageId.new(),)},
        {"issued_at_ms": -1},
        {"issued_at_ms": 1.5},
        {"nonce": "short"},
        {"nonce": "G" * 32},
        {"signature": b""},
        {"signature": b"x" * 129},
        {"public_key": b""},
        {"version": 2},
        {"message_ids": [MessageId.new()]},  # a list, not a tuple
    ],
)
def test_invalid_collect_requests_are_rejected(changes: dict[str, Any]) -> None:
    with pytest.raises(FormatError):
        replace(collect_request(BOB, NOW), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"limit": 5},
        {"message_ids": ()},
        {"message_ids": tuple(MessageId.new() for _ in range(MAX_ACK_IDS + 1))},
    ],
)
def test_invalid_acknowledge_requests_are_rejected(changes: dict[str, Any]) -> None:
    with pytest.raises(FormatError):
        replace(ack_request(BOB, NOW, [MessageId.new()]), **changes)


def test_repeated_ids_built_by_hand_are_rejected() -> None:
    mid = MessageId.new()
    request = ack_request(BOB, NOW, [mid])
    with pytest.raises(FormatError, match="repeated"):
        replace(request, message_ids=(mid, mid))


# ---------------- signing bytes ----------------
def test_signing_bytes_have_their_own_domain_prefix() -> None:
    assert (
        collect_request(BOB, NOW)
        .signing_bytes()
        .startswith(b"mensajeria/mailbox-request/v1\0")
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"issued_at_ms": NOW + 1},
        {"nonce": "0" * 32},
        {"limit": 11},
    ],
)
def test_every_field_is_covered_by_the_signature(changes: dict[str, Any]) -> None:
    request = collect_request(BOB, NOW, limit=10)
    assert replace(request, **changes).signing_bytes() != request.signing_bytes()


def test_the_action_is_covered_by_the_signature() -> None:
    mid = MessageId.new()
    ack = ack_request(BOB, NOW, [mid], nonce="1" * 32)
    collect = collect_request(BOB, NOW, nonce="1" * 32)
    assert ack.signing_bytes() != collect.signing_bytes()


# ---------------- serialization ----------------
@given(
    action=st.sampled_from(list(MailboxAction)),
    ids=st.integers(min_value=1, max_value=5),
)
def test_round_trip(action: MailboxAction, ids: int) -> None:
    if action is MailboxAction.COLLECT:
        request = collect_request(BOB, NOW, limit=ids)
    else:
        request = ack_request(BOB, NOW, [MessageId.new() for _ in range(ids)])
    assert MailboxRequest.from_bytes(request.to_bytes()) == request


@pytest.mark.parametrize(
    "mutate",
    [
        lambda o: o.pop("nonce"),
        lambda o: o.update(extra=1),
        lambda o: o.update(ids="not a list"),
        lambda o: o.update(action="delete_everything"),
        lambda o: o.update(to="nope"),
        lambda o: o.update(ids=["nope"]),
    ],
)
def test_from_bytes_rejects_anything_inexact(mutate: Any) -> None:
    obj = json.loads(ack_request(BOB, NOW, [MessageId.new()]).to_bytes())
    mutate(obj)
    with pytest.raises(FormatError):
        MailboxRequest.from_bytes(json.dumps(obj).encode())


def test_from_bytes_rejects_non_json() -> None:
    with pytest.raises(FormatError):
        MailboxRequest.from_bytes(b"\xff not json")


# ---------------- replay guard ----------------
def test_new_nonces_are_valid_and_distinct() -> None:
    a, b = new_nonce(), new_nonce()
    assert a != b and len(a) == 32


def test_replay_guard_accepts_once() -> None:
    guard = ReplayGuard(max_skew_ms=1_000)
    assert guard.check_and_remember("a" * 32, NOW, NOW)
    assert not guard.check_and_remember("a" * 32, NOW, NOW + 500)


def test_replay_guard_rejects_stale_requests_both_ways() -> None:
    guard = ReplayGuard(max_skew_ms=1_000)
    assert not guard.check_and_remember("a" * 32, NOW - 1_001, NOW)
    assert not guard.check_and_remember("b" * 32, NOW + 1_001, NOW)
    assert guard.check_and_remember("c" * 32, NOW - 1_000, NOW)


def test_replay_guard_forgets_nonces_that_could_no_longer_pass() -> None:
    guard = ReplayGuard(max_skew_ms=1_000)
    guard.check_and_remember("a" * 32, NOW, NOW)
    # Once the window has passed, the old request is stale anyway ...
    assert not guard.check_and_remember("a" * 32, NOW, NOW + 1_001)
    # ... and the guard no longer needs to remember it.
    assert guard._seen == {}


def test_replay_guard_rejects_a_bad_window() -> None:
    with pytest.raises(ValueError):
        ReplayGuard(max_skew_ms=0)
