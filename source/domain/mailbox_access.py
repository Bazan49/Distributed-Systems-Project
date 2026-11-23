"""Authenticated access to a mailbox: who may read and delete a receiver's queue.

Without this, anyone who knows Bob's ``UserId`` could list his pending
envelopes (and learn how many he has and from whom) and, knowing their ids,
delete them. Pure rules, standard library only.

Design:

* Only the receiver may ``collect`` or ``acknowledge``. Every request is signed
  by the receiver's identity key.
* The request is SELF-CERTIFYING, like a directory record: it carries the
  receiver's public key, and ``receiver`` must be the hash of that key. A node
  can check it without asking the directory (which might be unavailable or
  stale).
* The signature covers every field (action, receiver, key, limit, ids, time,
  nonce), with its own domain prefix: it cannot be replayed as an envelope or
  directory signature, nor turned from a ``collect`` into an ``acknowledge``.
* Freshness: ``issued_at_ms`` (client wall clock) must be within
  ``max_skew_ms`` of the node's clock, and the ``nonce`` must not have been
  seen inside that window (``ReplayGuard``). A retry is a NEW request with a
  new nonce, never a resend.
* The request is not bound to one node: the client sends the same request to
  its 3 nodes. Replaying it to another node within the window only repeats
  what the receiver already asked for (deleting the same ids, or reading
  envelopes that were already on the wire). Accepted and documented.
"""

from __future__ import annotations

import secrets
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from enum import Enum

from source.domain.canonical import (
    FormatError,
    b64d,
    b64e,
    canonical_json,
    is_int,
    parse_json_object,
    require_keys,
)
from source.domain.envelope import MAX_SIGNATURE_BYTES
from source.domain.identifiers import MessageId, UserId

MAILBOX_REQUEST_VERSION = 1
MAX_PUBLIC_KEY_BYTES = 256
MAX_ACK_IDS = 256  # ids per acknowledge request
MAX_COLLECT_LIMIT = 1_000
MAX_REQUEST_BYTES = 32 * 1024
DEFAULT_MAX_SKEW_MS = 5 * 60 * 1000  # tolerated clock difference client/node

_NONCE_HEX_LEN = 32
_SIGNING_DOMAIN = b"mensajeria/mailbox-request/v1\0"
_KEYS = {"v", "action", "to", "pk", "limit", "ids", "at", "nonce", "sig"}


class MailboxAction(str, Enum):
    COLLECT = "collect"
    ACKNOWLEDGE = "acknowledge"


def new_nonce() -> str:
    """128 random bits as 32 lowercase hex characters."""
    return secrets.token_hex(_NONCE_HEX_LEN // 2)


def _is_nonce(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == _NONCE_HEX_LEN
        and all(c in "0123456789abcdef" for c in value)
    )


@dataclass(frozen=True)
class MailboxRequest:
    """A receiver's signed request to ``collect`` from or ``acknowledge`` to a mailbox.

    * ``COLLECT``: ``limit >= 1`` and no ``message_ids``.
    * ``ACKNOWLEDGE``: ``limit == 0`` and 1 to ``MAX_ACK_IDS`` distinct ids.

    Build it with ``MailboxRequest.build`` so the signature is correct.
    """

    action: MailboxAction
    receiver: UserId
    public_key: bytes
    limit: int
    message_ids: tuple[MessageId, ...]
    issued_at_ms: int
    nonce: str
    signature: bytes
    version: int = MAILBOX_REQUEST_VERSION

    def __post_init__(self) -> None:
        if self.version != MAILBOX_REQUEST_VERSION or not is_int(self.version):
            raise FormatError("unsupported mailbox request version")
        if not isinstance(self.action, MailboxAction):
            raise FormatError("invalid action")
        if not isinstance(self.receiver, UserId):
            raise FormatError("receiver must be a UserId")
        if not isinstance(self.public_key, bytes) or not (
            1 <= len(self.public_key) <= MAX_PUBLIC_KEY_BYTES
        ):
            raise FormatError(f"public_key must be 1 to {MAX_PUBLIC_KEY_BYTES} bytes")
        if not isinstance(self.message_ids, tuple) or not all(
            isinstance(m, MessageId) for m in self.message_ids
        ):
            raise FormatError("message_ids must be a tuple of MessageId")
        if not is_int(self.limit):
            raise FormatError("limit must be an integer")
        if self.action is MailboxAction.COLLECT:
            if not 1 <= self.limit <= MAX_COLLECT_LIMIT:
                raise FormatError(f"collect limit must be 1 to {MAX_COLLECT_LIMIT}")
            if self.message_ids:
                raise FormatError("a collect request carries no message ids")
        else:
            if self.limit != 0:
                raise FormatError("an acknowledge request has limit 0")
            if not 1 <= len(self.message_ids) <= MAX_ACK_IDS:
                raise FormatError(f"acknowledge needs 1 to {MAX_ACK_IDS} message ids")
            if len(set(self.message_ids)) != len(self.message_ids):
                raise FormatError("repeated message ids")
        if not is_int(self.issued_at_ms) or self.issued_at_ms < 0:
            raise FormatError("issued_at_ms must be an integer >= 0")
        if not _is_nonce(self.nonce):
            raise FormatError("nonce must be 32 lowercase hexadecimal characters")
        if not isinstance(self.signature, bytes) or not (
            1 <= len(self.signature) <= MAX_SIGNATURE_BYTES
        ):
            raise FormatError(f"signature must be 1 to {MAX_SIGNATURE_BYTES} bytes")

    # -- signature ----------------------------------------------------------

    def _fields(self) -> dict[str, object]:
        return {
            "v": self.version,
            "action": self.action.value,
            "to": self.receiver.value,
            "pk": b64e(self.public_key),
            "limit": self.limit,
            "ids": [m.value for m in self.message_ids],
            "at": self.issued_at_ms,
            "nonce": self.nonce,
        }

    def signing_bytes(self) -> bytes:
        """Exact bytes the receiver signs. Every field except the signature."""
        return _SIGNING_DOMAIN + canonical_json(self._fields())

    def identity_matches(self) -> bool:
        """Is ``receiver`` the hash of the carried public key?"""
        return UserId.from_public_key(self.public_key) == self.receiver

    @classmethod
    def build(
        cls,
        *,
        action: MailboxAction,
        public_key: bytes,
        issued_at_ms: int,
        sign: Callable[[bytes], bytes],
        limit: int = 0,
        message_ids: Iterable[MessageId] = (),
        nonce: str | None = None,
    ) -> MailboxRequest:
        """Create a signed request. ``receiver`` is derived from the key.

        Repeated ``message_ids`` are collapsed (first occurrence kept).
        """
        draft = cls(
            action=action,
            receiver=UserId.from_public_key(public_key),
            public_key=public_key,
            limit=limit,
            message_ids=tuple(dict.fromkeys(message_ids)),
            issued_at_ms=issued_at_ms,
            nonce=nonce if nonce is not None else new_nonce(),
            signature=b"\x00",  # placeholder, replaced below
        )
        return replace(draft, signature=sign(draft.signing_bytes()))

    # -- serialization ------------------------------------------------------

    def to_bytes(self) -> bytes:
        return canonical_json({**self._fields(), "sig": b64e(self.signature)})

    @classmethod
    def from_bytes(cls, data: bytes) -> MailboxRequest:
        """Read a request received from the network; reject anything inexact."""
        obj = parse_json_object(data, MAX_REQUEST_BYTES)
        require_keys(obj, _KEYS)
        if not isinstance(obj["ids"], list):
            raise FormatError("ids must be a list")
        try:
            return cls(
                version=obj["v"],
                action=MailboxAction(obj["action"]),
                receiver=UserId(obj["to"]),
                public_key=b64d(obj["pk"]),
                limit=obj["limit"],
                message_ids=tuple(MessageId(m) for m in obj["ids"]),
                issued_at_ms=obj["at"],
                nonce=obj["nonce"],
                signature=b64d(obj["sig"]),
            )
        except ValueError as exc:
            if isinstance(exc, FormatError):
                raise
            raise FormatError(str(exc)) from exc


class ReplayGuard:
    """Remembers recent nonces so a captured request cannot be used twice.

    Pure: the caller passes the node's ``now_ms``. Memory is bounded by the
    number of requests inside one window, because a nonce only needs to be
    remembered while its request would still pass the freshness check.

    Limitation: kept in memory, so a node restart forgets it; a request
    captured less than ``max_skew_ms`` before the restart could be replayed
    once. Acceptable for Phase 1 (documented in the decision log).
    """

    def __init__(self, max_skew_ms: int = DEFAULT_MAX_SKEW_MS) -> None:
        if not is_int(max_skew_ms) or max_skew_ms < 1:
            raise ValueError("max_skew_ms must be a positive integer")
        self._max_skew = max_skew_ms
        self._seen: dict[str, int] = {}  # nonce -> forget after (ms)

    def is_fresh(self, issued_at_ms: int, now_ms: int) -> bool:
        """Is the request time within the tolerated clock skew?"""
        return abs(now_ms - issued_at_ms) <= self._max_skew

    def check_and_remember(self, nonce: str, issued_at_ms: int, now_ms: int) -> bool:
        """True the first time a fresh ``nonce`` is seen; False if stale or repeated."""
        self._forget_old(now_ms)
        if not self.is_fresh(issued_at_ms, now_ms) or nonce in self._seen:
            return False
        self._seen[nonce] = issued_at_ms + self._max_skew
        return True

    def _forget_old(self, now_ms: int) -> None:
        for nonce in [n for n, until in self._seen.items() if until < now_ms]:
            del self._seen[nonce]
