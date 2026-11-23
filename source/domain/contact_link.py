"""Contact link: the text a user shares so that others can add them.

A link is everything needed to start a conversation with someone, and
nothing else::

    p08:contact:<base64url of canonical JSON>

    {"v": 1, "id": <UserId>, "pk": <public key>, "alias": <alias>, "sig": <signature>}

* ``pk`` is the user's public key: it verifies their signatures and encrypts
  messages for them. It is the only field that is strictly needed.
* ``id`` is the ``UserId``, which must be the hash of ``pk``. It is redundant
  on purpose: a link whose identifier does not match its key is rejected, so a
  corrupted or tampered link never yields a contact.
* ``alias`` is the display name the user suggests. It is not an identity.
* ``sig`` is the owner's signature over all the other fields. It proves the
  link was made by whoever holds the private key, and that nobody changed the
  alias on the way.

A link holds no address, node or IP: where a user can be reached is resolved
through the directory by ``UserId``, so a link never goes stale.

The link is also the body of a ``CONTACT_REQUEST`` message: a requester sends
their own link, which is how the receiver learns a key it has never seen.

This module defines the format and checks its structure; verifying ``sig``
needs cryptography and is done by the contacts use case through the crypto
port.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace

from source.domain.canonical import (
    FormatError,
    b64d,
    b64e,
    canonical_json,
    is_int,
    parse_json_object,
    require_keys,
)
from source.domain.identifiers import Alias, UserId

__all__ = [
    "LINK_PREFIX",
    "LINK_VERSION",
    "MAX_LINK_CHARS",
    "ContactLink",
]

LINK_VERSION = 1
LINK_PREFIX = "p08:contact:"

# Generous bounds that still refuse absurd input before decoding it.
MAX_PUBLIC_KEY_BYTES = 256
MAX_SIGNATURE_BYTES = 128
MAX_LINK_CHARS = 2048

_SIGNING_DOMAIN = b"mensajeria/contact-link/v1\0"


@dataclass(frozen=True)
class ContactLink:
    """A user's shareable identity card. Validated on creation.

    Raises:
        FormatError: a field has the wrong type or size, or ``user_id`` is
            not the hash of ``public_key``.
    """

    user_id: UserId
    public_key: bytes
    alias: Alias
    signature: bytes
    version: int = LINK_VERSION

    def __post_init__(self) -> None:
        if not is_int(self.version) or self.version != LINK_VERSION:
            raise FormatError("unsupported contact link version")
        if not isinstance(self.user_id, UserId) or not isinstance(self.alias, Alias):
            raise FormatError("invalid user_id or alias")
        if not isinstance(self.public_key, bytes) or not (
            1 <= len(self.public_key) <= MAX_PUBLIC_KEY_BYTES
        ):
            raise FormatError("public key has an invalid size")
        if not isinstance(self.signature, bytes) or not (
            1 <= len(self.signature) <= MAX_SIGNATURE_BYTES
        ):
            raise FormatError("signature has an invalid size")
        if UserId.from_public_key(self.public_key) != self.user_id:
            raise FormatError("the link's identifier does not match its key")

    # signature

    def signing_bytes(self) -> bytes:
        """Exact bytes the owner signs: every field except the signature.

        Domain-separated, so a link signature can never be passed off as an
        envelope signature or the other way round.
        """
        return _SIGNING_DOMAIN + canonical_json(self._fields())

    @classmethod
    def build(
        cls,
        *,
        public_key: bytes,
        alias: Alias,
        sign: Callable[[bytes], bytes],
    ) -> ContactLink:
        """Create a signed link. ``sign`` receives the bytes to sign."""
        draft = cls(
            user_id=UserId.from_public_key(public_key),
            public_key=public_key,
            alias=alias,
            signature=b"\x00",  # placeholder, replaced below
        )
        return replace(draft, signature=sign(draft.signing_bytes()))

    # text form

    def to_text(self) -> str:
        """The link as the user copies and pastes it."""
        payload = {**self._fields(), "sig": b64e(self.signature)}
        return LINK_PREFIX + b64e(canonical_json(payload))

    @classmethod
    def from_text(cls, text: str) -> ContactLink:
        """Parse a pasted link strictly. Surrounding whitespace is ignored.

        Only the structure is checked here, including that the identifier is
        the hash of the key. The signature must still be verified with the
        crypto port before the link is trusted.

        Raises:
            FormatError: not a contact link, or any part of it is invalid.
        """
        if not isinstance(text, str):
            raise FormatError("a contact link is text")
        text = text.strip()
        if len(text) > MAX_LINK_CHARS:
            raise FormatError("contact link too long")
        if not text.startswith(LINK_PREFIX):
            raise FormatError("this is not a contact link")
        obj = parse_json_object(b64d(text[len(LINK_PREFIX) :]), MAX_LINK_CHARS)
        require_keys(obj, {"v", "id", "pk", "alias", "sig"})
        if not is_int(obj["v"]):
            raise FormatError("invalid version")
        if not isinstance(obj["alias"], str):
            raise FormatError("invalid alias")
        try:
            return cls(
                version=obj["v"],
                user_id=UserId(obj["id"]),
                public_key=b64d(obj["pk"]),
                alias=Alias(obj["alias"]),
                signature=b64d(obj["sig"]),
            )
        except ValueError as exc:
            if isinstance(exc, FormatError):
                raise
            raise FormatError(str(exc)) from exc

    # internals

    def _fields(self) -> dict[str, object]:
        return {
            "v": self.version,
            "id": self.user_id.value,
            "pk": b64e(self.public_key),
            "alias": self.alias.value,
        }
