"""Envelope and MessageContent format.

A message travels in two layers:

* ``Envelope`` (outer layer): the only thing mailboxes and the network see.
  It carries the minimum needed to route, deduplicate, and prove authorship.
  Everything else is encrypted.
* ``MessageContent`` (inner layer): what gets encrypted for the receiver.
  Only the receiver can read it.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import Enum

from domain.identifiers import ConversationId, MessageId, UserId
from domain.utils import _b64d, _b64e, _canonical_json, _is_int, _parse_object, _require_keys

ENVELOPE_VERSION = 1

MAX_BODY_BYTES = 16 * 1024
MAX_CIPHERTEXT_BYTES = 64 * 1024
MAX_SIGNATURE_BYTES = 128
MAX_ENVELOPE_BYTES = 128 * 1024  # hard cap when reading bytes from the network

# Domain separation prefix for signatures. Prevents cross-protocol replay
# attacks: a signature made for an envelope cannot be reused as a directory
# registration signature, and vice versa. The trailing null byte prevents
# prefix-ambiguity attacks ("envelope/v1X" vs "envelope/v1" + "X").
_SIGNING_DOMAIN = b"messaging/envelope/v1\0"


class EnvelopeError(ValueError):
    """The envelope or its content does not conform to the format."""

# Encrypted content
class MessageKind(str, Enum):
    TEXT = "text"
    CONTACT_REQUEST = "contact_request"
    CONTACT_ACCEPT = "contact_accept"

@dataclass(frozen=True)
class MessageContent:
    """What goes inside the encryption. Only the receiver can read it.

    ``seq`` is the per-sender, per-conversation counter (starts at 1) used to
    order and detect gaps. It lives here and not in the envelope so that
    mailboxes cannot see the rhythm or order of the conversation.
    """

    kind: MessageKind
    conversation_id: ConversationId
    seq: int
    sent_at_ms: int
    body: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, MessageKind):
            raise EnvelopeError("invalid kind")
        if not isinstance(self.conversation_id, ConversationId):
            raise EnvelopeError("invalid conversation_id")
        if not _is_int(self.seq) or self.seq < 1:
            raise EnvelopeError("seq must be an integer >= 1")
        if not _is_int(self.sent_at_ms) or self.sent_at_ms < 0:
            raise EnvelopeError("sent_at_ms must be an integer >= 0")
        if not isinstance(self.body, str):
            raise EnvelopeError("body must be text")
        try:
            body_size = len(self.body.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise EnvelopeError("body is not valid UTF-8") from exc
        if body_size > MAX_BODY_BYTES:
            raise EnvelopeError("body too large")
        if self.kind is MessageKind.TEXT and not self.body:
            raise EnvelopeError("a text message cannot be empty")

    def to_bytes(self) -> bytes:
        """Bytes handed to the crypto port for encryption."""
        return _canonical_json(
            {
                "v": ENVELOPE_VERSION,
                "kind": self.kind.value,
                "conv": self.conversation_id.value,
                "seq": self.seq,
                "sent_at_ms": self.sent_at_ms,
                "body": self.body,
            }
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "MessageContent":
        """Rebuild the content from already-decrypted bytes."""
        obj = _parse_object(data, MAX_CIPHERTEXT_BYTES)
        _require_keys(obj, {"v", "kind", "conv", "seq", "sent_at_ms", "body"})
        if obj["v"] != ENVELOPE_VERSION:
            raise EnvelopeError("unsupported content version")
        try:
            return cls(
                kind=MessageKind(obj["kind"]),
                conversation_id=ConversationId(obj["conv"]),
                seq=obj["seq"],
                sent_at_ms=obj["sent_at_ms"],
                body=obj["body"],
            )
        except ValueError as exc:
            if isinstance(exc, EnvelopeError):
                raise
            raise EnvelopeError(str(exc)) from exc

# Envelope
@dataclass(frozen=True)
class Envelope:
    """Outer layer of the message: the only thing mailboxes see.

    ``ciphertext`` is opaque to the domain (it includes whatever the
    encryption scheme needs, such as nonce or ephemeral key). ``signature``
    covers all other fields and is obtained with ``Envelope.build``.
    """

    message_id: MessageId
    sender: UserId
    receiver: UserId
    ciphertext: bytes
    signature: bytes
    version: int = ENVELOPE_VERSION

    def __post_init__(self) -> None:
        if self.version != ENVELOPE_VERSION or not _is_int(self.version):
            raise EnvelopeError("unsupported envelope version")
        if not isinstance(self.message_id, MessageId):
            raise EnvelopeError("invalid message_id")
        if not isinstance(self.sender, UserId) or not isinstance(
            self.receiver, UserId
        ):
            raise EnvelopeError("sender and receiver must be UserId")
        if self.sender == self.receiver:
            raise EnvelopeError("sender and receiver must differ")
        if not isinstance(self.ciphertext, bytes) or not (
            1 <= len(self.ciphertext) <= MAX_CIPHERTEXT_BYTES
        ):
            raise EnvelopeError(
                "ciphertext must be between 1 and 65536 bytes"
            )
        if not isinstance(self.signature, bytes) or not (
            1 <= len(self.signature) <= MAX_SIGNATURE_BYTES
        ):
            raise EnvelopeError(
                "signature must be between 1 and 128 bytes"
            )

    # -- signing -------------------------------------------------------------

    def signing_bytes(self) -> bytes:
        """Exact bytes signed by the sender and verified by the receiver.

        Covers version, id, sender, receiver, and ciphertext (encrypt then
        sign), with a domain prefix so this signature cannot be reused in
        another context. Does not include the signature itself.
        """
        return _SIGNING_DOMAIN + _canonical_json(
            {
                "v": self.version,
                "id": self.message_id.value,
                "from": self.sender.value,
                "to": self.receiver.value,
                "ct": _b64e(self.ciphertext),
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
    ) -> "Envelope":
        """Create a signed envelope.

        ``sign`` receives the bytes to sign and returns the signature.
        It is injected by the application layer (usually a method of the
        crypto port). The domain never knows which algorithm is used.
        """
        draft = cls(
            message_id=message_id,
            sender=sender,
            receiver=receiver,
            ciphertext=ciphertext,
            signature=b"\x00",  # placeholder: replaced below
        )
        return replace(draft, signature=sign(draft.signing_bytes()))

    # -- consistency with content --------------------------------------------

    def expected_conversation_id(self) -> ConversationId:
        return ConversationId.between(self.sender, self.receiver)

    def ensure_content_matches(self, content: MessageContent) -> None:
        """The receiver calls this after decrypting: the content must belong
        to the conversation between the sender and receiver of this envelope.
        """
        if content.conversation_id != self.expected_conversation_id():
            raise EnvelopeError(
                "content does not match the envelope conversation"
            )

    # -- serialization -------------------------------------------------------

    def to_bytes(self) -> bytes:
        """Bytes that travel over the network and are stored in the mailbox."""
        return _canonical_json(
            {
                "v": self.version,
                "id": self.message_id.value,
                "from": self.sender.value,
                "to": self.receiver.value,
                "ct": _b64e(self.ciphertext),
                "sig": _b64e(self.signature),
            }
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "Envelope":
        """Read an envelope received from the network; reject anything inexact."""
        obj = _parse_object(data, MAX_ENVELOPE_BYTES)
        _require_keys(obj, {"v", "id", "from", "to", "ct", "sig"})
        if not _is_int(obj["v"]):
            raise EnvelopeError("invalid version")
        try:
            return cls(
                version=obj["v"],
                message_id=MessageId(obj["id"]),
                sender=UserId(obj["from"]),
                receiver=UserId(obj["to"]),
                ciphertext=_b64d(obj["ct"]),
                signature=_b64d(obj["sig"]),
            )
        except ValueError as exc:
            if isinstance(exc, EnvelopeError):
                raise
            raise EnvelopeError(str(exc)) from exc