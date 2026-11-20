"""Deterministic serialization helpers shared by every signed or hashed structure.

Anything that gets signed must serialize to exactly the same bytes on every
machine, so all signed structures go through ``canonical_json``.
"""

from __future__ import annotations

import base64
import binascii
import json
from typing import Any


class FormatError(ValueError):
    """Data does not follow the expected format."""


def canonical_json(obj: dict[str, Any]) -> bytes:
    """Deterministic JSON: sorted keys, no whitespace, pure ASCII."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def b64e(data: bytes) -> str:
    """Encode bytes as unpadded base64url."""
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def b64d(text: Any) -> bytes:
    """Decode unpadded base64url, rejecting every non-canonical form."""
    if not isinstance(text, str):
        raise FormatError("expected base64url text")
    try:
        data = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError) as exc:
        raise FormatError("invalid base64url") from exc
    if b64e(data) != text:
        raise FormatError("non-canonical base64url")
    return data


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FormatError(f"duplicate key: {key}")
        result[key] = value
    return result


def parse_json_object(data: bytes, max_bytes: int) -> dict[str, Any]:
    """Strictly parse bytes received from the network into a JSON object."""
    if not isinstance(data, bytes):
        raise FormatError("expected bytes")
    if len(data) > max_bytes:
        raise FormatError("data too large")
    try:
        parsed = json.loads(data.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FormatError("not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise FormatError("expected a JSON object")
    return parsed


def require_keys(obj: dict[str, Any], expected: set[str]) -> None:
    """Fail unless the object has exactly the expected keys (no extra, none missing)."""
    if set(obj) != expected:
        raise FormatError("unexpected set of fields")


def is_int(value: Any) -> bool:
    """True only for genuine ``int``, not ``bool``.

    In Python, ``bool`` is a subclass of ``int``: ``True == 1`` and
    ``False == 0``. Without this check, ``{"seq": true}`` would silently
    parse as ``seq=1``, corrupting ordering logic.
    """
    return isinstance(value, int) and not isinstance(
        value, bool
    )  # must exclude bool, isinstance would include it
