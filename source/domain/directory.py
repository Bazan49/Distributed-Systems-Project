"""Directory records: a user's signed announcement.

A record says: "I am ``user_id``, my public key is ``payload``, and I am present
until ``expires_at_ms``". Pure rules, standard library only.

Design:

* The record is SELF-CERTIFYING. ``user_id`` is the SHA-256 of the public key,
  and the key travels in ``payload``. Anyone can check that the key matches the
  id, and then check the signature with that key. Nobody can publish a record for
  Alice without Alice's private key.
* The signature covers EVERY field (id, version, expiry and key), so a captured
  record cannot be altered, for example to make a presence last longer.
* ``version`` only goes up: of two valid records, the higher version wins.
* ``expires_at_ms`` is the presence lease: if the user does not
  renew it, the user looks offline.
* The signing context has its own prefix, so a directory signature can never be
  replayed as an envelope signature, or the other way round.

Phase 1 keeps the payload as just the raw public key. A richer payload would need
a format version.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from source.domain.canonical import is_int
from source.domain.envelope import MAX_SIGNATURE_BYTES
from source.domain.identifiers import UserId
from source.domain.records import DirectoryRecord

DIRECTORY_FORMAT_VERSION = 1
MAX_PUBLIC_KEY_BYTES = 256

# Different from the envelope's prefix on purpose (cross-protocol replay protection).
_SIGNING_DOMAIN = b"mensajeria/directory/v1\0"


class DirectoryRecordError(ValueError):
    """The record is not well formed."""


def validate_shape(record: DirectoryRecord) -> None:
    """Check types and sizes of every field.

    Raises:
        DirectoryRecordError: something is out of range or of the wrong type.
    """
    if not isinstance(record.user_id, UserId):
        raise DirectoryRecordError("user_id must be a UserId")
    if not is_int(record.version) or record.version < 1:
        raise DirectoryRecordError("version must be an integer >= 1")
    if not is_int(record.expires_at_ms) or record.expires_at_ms < 0:
        raise DirectoryRecordError(
            "expires_at_ms must be an integer >= 0 (milliseconds)"
        )
    if (
        not isinstance(record.payload, bytes)
        or not 1 <= len(record.payload) <= MAX_PUBLIC_KEY_BYTES
    ):
        raise DirectoryRecordError(f"payload must be 1 to {MAX_PUBLIC_KEY_BYTES} bytes")
    if (
        not isinstance(record.signature, bytes)
        or not 1 <= len(record.signature) <= MAX_SIGNATURE_BYTES
    ):
        raise DirectoryRecordError(
            f"signature must be 1 to {MAX_SIGNATURE_BYTES} bytes"
        )


def public_key_of(record: DirectoryRecord) -> bytes:
    """The public key announced by the record (the payload, in Phase 1)."""
    return record.payload


def identity_matches(record: DirectoryRecord) -> bool:
    """Does ``user_id`` equal the hash of the announced public key?"""
    return UserId.from_public_key(record.payload) == record.user_id


def signing_bytes(record: DirectoryRecord) -> bytes:
    """The exact bytes the user signs. Covers every field except the signature."""
    validate_shape(record)
    body = {
        "v": DIRECTORY_FORMAT_VERSION,
        "user": record.user_id.value,
        "ver": record.version,
        "exp": record.expires_at_ms,
        "pk": record.payload.hex(),
    }
    canonical = json.dumps(
        body, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return _SIGNING_DOMAIN + canonical.encode("ascii")


def build_record(
    *,
    public_key: bytes,
    version: int,
    expires_at_ms: int,
    sign: Callable[[bytes], bytes],
) -> DirectoryRecord:
    """Create a signed record. ``user_id`` is derived from the key, so it matches by construction.

    ``sign`` receives the bytes to sign and returns the signature; the application
    layer injects it (usually a method of the crypto port).
    """
    draft = DirectoryRecord(
        user_id=UserId.from_public_key(public_key),
        version=version,
        payload=public_key,
        signature=b"\x00",  # placeholder, replaced below
        expires_at_ms=expires_at_ms,
    )
    signature = sign(signing_bytes(draft))
    return DirectoryRecord(
        user_id=draft.user_id,
        version=version,
        payload=public_key,
        signature=signature,
        expires_at_ms=expires_at_ms,
    )
