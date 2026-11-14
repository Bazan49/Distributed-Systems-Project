import base64
import binascii
import json
from typing import Any


class EnvelopeError(ValueError):
    """The envelope or its content does not conform to the format."""


# Canonical serialization utilities


def _canonical_json(obj: dict[str, Any]) -> bytes:
    """Deterministic JSON: sorted keys, no whitespace, pure ASCII.

    Required for cryptographic signing: the signer and verifier must produce
    byte-identical output for identical data. Any variation in key order or
    whitespace would invalidate otherwise-valid signatures.
    """
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _b64e(data: bytes) -> str:
    """Encode bytes as unpadded base64url (URL-safe, JSON-friendly).

    Padding is stripped so the encoded form is canonical: a given byte sequence
    has exactly one valid representation.
    """
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64d(text: Any) -> bytes:
    """Decode unpadded base64url and reject any non-canonical form.

    Canonical check matters for signatures: if an attacker can flip the padding
    or use a different base64 variant for the same bytes, the signature bytes
    would differ even though the decoded content is identical. Re-encoding and
    comparing ensures we only accept the exact canonical representation.
    """
    if not isinstance(text, str):
        raise EnvelopeError("expected base64url text")
    try:
        data = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError) as exc:
        raise EnvelopeError("invalid base64url") from exc
    if _b64e(data) != text:
        raise EnvelopeError("non-canonical base64url")
    return data


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject JSON objects containing duplicate keys.

    Without this, ``{"to": "alice", "to": "bob"}`` is accepted by Python's
    default parser (last value wins), but different parsers may pick different
    values. An attacker could sign a payload that means different things to
    different nodes, breaking the integrity guarantee.
    """
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EnvelopeError(f"duplicate key: {key}")
        result[key] = value
    return result


def _parse_object(data: bytes, max_bytes: int) -> dict[str, Any]:
    """Defensive JSON object parser.

    Checks (in order, cheapest first):
      1. Input is ``bytes`` (not str, not bytearray).
      2. Size does not exceed ``max_bytes`` (before touching the parser).
      3. Content decodes as UTF-8.
      4. Content is valid JSON.
      5. JSON root is an object (not array, string, number, etc.).
      6. No duplicate keys anywhere in the object.
    """
    if not isinstance(data, bytes):
        raise EnvelopeError("expected bytes")
    if len(data) > max_bytes:
        raise EnvelopeError("data too large")
    try:
        parsed = json.loads(data.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EnvelopeError("not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise EnvelopeError("expected a JSON object")
    return parsed


def _require_keys(obj: dict[str, Any], expected: set[str]) -> None:
    """Require exactly the given key set (no more, no less).

    Extra keys are rejected on purpose: a permissive parser lets an attacker
    inject ``"admin": true`` or ``"role": "root"`` into the payload, and any
    downstream code that reads those keys inherits the injected privilege.
    """
    if set(obj) != expected:
        raise EnvelopeError("fields do not match expected set")


def _is_int(value: Any) -> bool:
    """True only for genuine ``int``, not ``bool``.

    In Python, ``bool`` is a subclass of ``int``: ``True == 1`` and
    ``False == 0``. Without this check, ``{"seq": true}`` would silently
    parse as ``seq=1``, corrupting ordering logic.
    """
    return type(value) is int  # noqa: E721  # must exclude bool, isinstance would include it
