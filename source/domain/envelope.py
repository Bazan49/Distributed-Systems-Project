"""Wire format of the envelope (Envelope) and of its encrypted content (MessageContent).

A message travels in two layers:

* ``Envelope`` (outer layer): the only thing mailboxes and the network see. It
  carries the bare minimum to route (receiver), deduplicate (id) and prove
  authorship (sender and signature). Everything else is encrypted.
* ``MessageContent`` (inner layer): what is encrypted for the receiver. Only
  the receiver can read it.

This module neither encrypts nor signs: it does not know any cryptography. It
defines WHICH bytes are encrypted and WHICH bytes are signed; the crypto port
does the rest.
"""

from __future__ import annotations

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
from source.domain.identifiers import ConversationId, MessageId, UserId

ENVELOPE_VERSION = 1

MAX_BODY_BYTES = 16 * 1024
MAX_RECEIPT_IDS = 256  # message ids confirmed by one receipt
MAX_CIPHERTEXT_BYTES = 64 * 1024
MAX_SIGNATURE_BYTES = 128
MAX_ENVELOPE_BYTES = (
    128 * 1024
)  # cap applied when reading bytes received from the network

_SIGNING_DOMAIN = b"mensajeria/envelope/v1\0"


# Encrypted content
class MessageKind(str, Enum):
    TEXT = "text"
    CONTACT_REQUEST = "contact_request"
    CONTACT_ACCEPT = "contact_accept"
    # Delivery receipt: "I stored these messages". ``body`` holds the confirmed
    # MessageIds (see ``receipt_body``). It is how a sender learns that a
    # message deposited in a mailbox arrived (OutgoingStatus.DELIVERED). A
    # receipt is never itself confirmed by another receipt.
    RECEIPT = "receipt"


_RECEIPT_SEPARATOR = ","


def receipt_body(message_ids: Iterable[MessageId]) -> str:
    """Body of a RECEIPT confirming ``message_ids`` (repeats collapsed, order kept)."""
    return _RECEIPT_SEPARATOR.join(m.value for m in dict.fromkeys(message_ids))


def _parse_receipt_body(body: str) -> tuple[MessageId, ...]:
    if not body:
        raise FormatError("a receipt must confirm at least one message")
    try:
        ids = tuple(MessageId(part) for part in body.split(_RECEIPT_SEPARATOR))
    except ValueError as exc:
        raise FormatError("a receipt body must be comma-separated message ids") from exc
    if len(ids) > MAX_RECEIPT_IDS:
        raise FormatError(f"a receipt confirms at most {MAX_RECEIPT_IDS} messages")
    if len(set(ids)) != len(ids):
        raise FormatError("a receipt repeats a message id")
    return ids


@dataclass(frozen=True)
class MessageContent:
    """What goes inside the encryption. Only the receiver sees it.

    ``seq`` is the per-sender, per-conversation counter (starts at 1) used to
    order messages and detect gaps. Every kind, receipts included, uses it.

    ``peer_seq`` is the highest ``seq`` the sender had received CONTIGUOUSLY
    from the other side of the conversation when this message was created
    (0 if none). It makes the merged order of both sides causal: a reply is
    always shown after the message it answers, whatever the wall clocks say
    (see ``source.domain.conversation_order``).

    Both live here and not in the envelope so that mailboxes cannot observe
    the pace or order of a conversation.
    """

    kind: MessageKind
    conversation_id: ConversationId
    seq: int
    sent_at_ms: int
    body: str
    peer_seq: int

    def __post_init__(self) -> None:
        if not isinstance(self.kind, MessageKind):
            raise FormatError("invalid kind")
        if not isinstance(self.conversation_id, ConversationId):
            raise FormatError("invalid conversation_id")
        if not is_int(self.seq) or self.seq < 1:
            raise FormatError("seq must be an integer >= 1")
        if not is_int(self.sent_at_ms) or self.sent_at_ms < 0:
            raise FormatError("sent_at_ms must be an integer >= 0")
        if not is_int(self.peer_seq) or self.peer_seq < 0:
            raise FormatError("peer_seq must be an integer >= 0")
        if not isinstance(self.body, str):
            raise FormatError("body must be text")
        try:
            body_size = len(self.body.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise FormatError("body is not valid UTF-8") from exc
        if body_size > MAX_BODY_BYTES:
            raise FormatError("body too large")
        if self.kind is MessageKind.TEXT and not self.body:
            raise FormatError("a text message cannot be empty")
        if self.kind is MessageKind.RECEIPT:
            _parse_receipt_body(self.body)

    def receipt_ids(self) -> tuple[MessageId, ...]:
        """The message ids confirmed by this RECEIPT.

        Raises:
            FormatError: this content is not a receipt.
        """
        if self.kind is not MessageKind.RECEIPT:
            raise FormatError("not a receipt")
        return _parse_receipt_body(self.body)

    def to_bytes(self) -> bytes:
        """Bytes handed to the crypto port to be encrypted."""
        return canonical_json(
            {
                "v": ENVELOPE_VERSION,
                "kind": self.kind.value,
                "conv": self.conversation_id.value,
                "seq": self.seq,
                "sent_at_ms": self.sent_at_ms,
                "body": self.body,
                "peer_seq": self.peer_seq,
            }
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> MessageContent:
        """Rebuild the content from bytes that have already been decrypted."""
        obj = parse_json_object(data, MAX_CIPHERTEXT_BYTES)
        require_keys(
            obj, {"v", "kind", "conv", "seq", "sent_at_ms", "body", "peer_seq"}
        )
        if obj["v"] != ENVELOPE_VERSION:
            raise FormatError("unsupported content version")
        try:
            return cls(
                kind=MessageKind(obj["kind"]),
                conversation_id=ConversationId(obj["conv"]),
                seq=obj["seq"],
                sent_at_ms=obj["sent_at_ms"],
                body=obj["body"],
                peer_seq=obj["peer_seq"],
            )
        except ValueError as exc:
            if isinstance(exc, FormatError):
                raise
            raise FormatError(str(exc)) from exc


# Envelope
@dataclass(frozen=True)
class Envelope:
    """Outer layer of a message: the only thing mailboxes see.

    ``ciphertext`` is opaque to the domain (it includes whatever the encryption
    scheme needs, such as a nonce or an ephemeral key). ``signature`` covers
    every other field and is produced through ``Envelope.build``.
    """

    message_id: MessageId
    sender: UserId
    receiver: UserId
    ciphertext: bytes
    signature: bytes
    version: int = ENVELOPE_VERSION

    def __post_init__(self) -> None:
        if self.version != ENVELOPE_VERSION or not is_int(self.version):
            raise FormatError("unsupported envelope version")
        if not isinstance(self.message_id, MessageId):
            raise FormatError("invalid message_id")
        if not isinstance(self.sender, UserId) or not isinstance(self.receiver, UserId):
            raise FormatError("sender and receiver must be UserId")
        if self.sender == self.receiver:
            raise FormatError("sender and receiver must be different")
        if not isinstance(self.ciphertext, bytes) or not (
            1 <= len(self.ciphertext) <= MAX_CIPHERTEXT_BYTES
        ):
            raise FormatError("ciphertext must be 1 to 65536 bytes")
        if not isinstance(self.signature, bytes) or not (
            1 <= len(self.signature) <= MAX_SIGNATURE_BYTES
        ):
            raise FormatError("signature must be 1 to 128 bytes")

    # -- signature ----------------------------------------------------------

    def signing_bytes(self) -> bytes:
        """Exact bytes the sender signs and the receiver verifies.

        Covers version, id, sender, receiver and ciphertext (encrypt-then-sign),
        with a domain prefix so this signature is useless in any other context.
        It does not include the signature itself.
        """
        return _SIGNING_DOMAIN + canonical_json(
            {
                "v": self.version,
                "id": self.message_id.value,
                "from": self.sender.value,
                "to": self.receiver.value,
                "ct": b64e(self.ciphertext),
            }
        )

    @classmethod
    def build(
        cls,
        *,
        message_id: MessageId,
        sender: UserId,
        receiver: UserId,
        ciphertext: bytes,
        sign: Callable[[bytes], bytes],
    ) -> Envelope:
        """Create a signed envelope. ``sign`` receives the bytes to sign and returns the signature."""
        draft = cls(
            message_id=message_id,
            sender=sender,
            receiver=receiver,
            ciphertext=ciphertext,
            signature=b"\x00",  # placeholder, replaced below
        )
        return replace(draft, signature=sign(draft.signing_bytes()))

    # -- consistency with the content ---------------------------------------

    def expected_conversation_id(self) -> ConversationId:
        return ConversationId.between(self.sender, self.receiver)

    def ensure_content_matches(self, content: MessageContent) -> None:
        """The receiver calls this after decrypting: the content must belong to
        the conversation between this envelope's sender and receiver."""
        if content.conversation_id != self.expected_conversation_id():
            raise FormatError("content does not belong to the envelope's conversation")

    # -- serialization ------------------------------------------------------

    def to_bytes(self) -> bytes:
        """Bytes that travel over the network and are stored in a mailbox."""
        return canonical_json(
            {
                "v": self.version,
                "id": self.message_id.value,
                "from": self.sender.value,
                "to": self.receiver.value,
                "ct": b64e(self.ciphertext),
                "sig": b64e(self.signature),
            }
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> Envelope:
        """Read an envelope received from the network; reject anything inexact."""
        obj = parse_json_object(data, MAX_ENVELOPE_BYTES)
        require_keys(obj, {"v", "id", "from", "to", "ct", "sig"})
        if not is_int(obj["v"]):
            raise FormatError("invalid version")
        try:
            return cls(
                version=obj["v"],
                message_id=MessageId(obj["id"]),
                sender=UserId(obj["from"]),
                receiver=UserId(obj["to"]),
                ciphertext=b64d(obj["ct"]),
                signature=b64d(obj["sig"]),
            )
        except ValueError as exc:
            if isinstance(exc, FormatError):
                raise
            raise FormatError(str(exc)) from exc
