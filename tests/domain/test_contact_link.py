"""Tests for the contact link format.

A link whose identifier does not match its key is
rejected. Beyond that, the parser must refuse anything inexact, since a link
is pasted by users and may come from anyone.
"""

from __future__ import annotations

from typing import Any

import pytest

from source.adapters.memory.crypto import FakeCrypto
from source.domain.canonical import FormatError, b64e, canonical_json
from source.domain.contact_link import LINK_PREFIX, MAX_LINK_CHARS, ContactLink
from source.domain.identifiers import Alias, UserId
from tests.support.builders import BOB, CARLA, Person

CRYPTO = FakeCrypto()


def link_of(person: Person, alias: str | None = None) -> ContactLink:
    return ContactLink.build(
        public_key=person.keys.public_key,
        alias=Alias(alias or person.name),
        sign=lambda data: CRYPTO.sign(person.keys.private_key, data),
    )


def raw(**overrides: Any) -> str:
    """A link text built field by field, to produce malformed links."""
    fields: dict[str, Any] = {
        "v": 1,
        "id": BOB.user_id.value,
        "pk": b64e(BOB.keys.public_key),
        "alias": "Bob",
        "sig": b64e(b"\x01" * 32),
    }
    fields.update(overrides)
    fields = {k: v for k, v in fields.items() if v is not ...}
    return LINK_PREFIX + b64e(canonical_json(fields))


# ---------------- building and round trip ----------------


def test_build_derives_the_id_from_the_key() -> None:
    link = link_of(BOB)
    assert link.user_id == BOB.user_id
    assert link.public_key == BOB.keys.public_key
    assert link.alias == Alias("Bob")


def test_the_signature_covers_every_other_field() -> None:
    link = link_of(BOB)
    assert CRYPTO.verify(BOB.keys.public_key, link.signing_bytes(), link.signature)
    renamed = ContactLink(
        link.user_id, link.public_key, Alias("Mallory"), link.signature
    )
    assert renamed.signing_bytes() != link.signing_bytes()
    assert not CRYPTO.verify(
        BOB.keys.public_key, renamed.signing_bytes(), link.signature
    )


def test_the_signed_bytes_are_domain_separated() -> None:
    assert link_of(BOB).signing_bytes().startswith(b"mensajeria/contact-link/v1\0")


def test_text_round_trip() -> None:
    link = link_of(BOB, "Bob Esponja")
    text = link.to_text()

    assert text.startswith(LINK_PREFIX)
    assert text.isascii() and " " not in text
    assert ContactLink.from_text(text) == link


def test_surrounding_whitespace_is_ignored() -> None:
    link = link_of(BOB)
    assert ContactLink.from_text(f"  {link.to_text()}\n") == link


def test_a_link_fits_comfortably_in_a_message() -> None:
    assert len(link_of(BOB, "x" * 64).to_text()) < MAX_LINK_CHARS


# ---------------- malformed links ----------------


def test_a_link_whose_identifier_does_not_match_its_key_is_rejected() -> None:
    with pytest.raises(FormatError, match="does not match"):
        ContactLink.from_text(raw(id=CARLA.user_id.value))


def test_the_same_rule_holds_when_building_directly() -> None:
    with pytest.raises(FormatError):
        ContactLink(CARLA.user_id, BOB.keys.public_key, Alias("Bob"), b"sig")


# ---------------- strict parsing ----------------


@pytest.mark.parametrize(
    "text",
    [
        "",
        "hola",
        "https://example.com/contact",
        "p08:contacto:" + raw()[len(LINK_PREFIX) :],  # wrong prefix
        LINK_PREFIX,  # nothing after it
        LINK_PREFIX + "!!!",  # not base64url
        LINK_PREFIX + "ñandú",  # not ASCII
        LINK_PREFIX + b64e(b"not json"),
        LINK_PREFIX + b64e(b"[1, 2]"),  # JSON, but not an object
        raw() + "A",  # trailing garbage breaks canonical base64
        LINK_PREFIX + "A" * MAX_LINK_CHARS,  # too long
    ],
)
def test_garbage_is_not_a_link(text: str) -> None:
    with pytest.raises(FormatError):
        ContactLink.from_text(text)


def test_bytes_are_not_a_link() -> None:
    with pytest.raises(FormatError):
        ContactLink.from_text(raw().encode())


@pytest.mark.parametrize(
    "overrides",
    [
        {"v": 2},
        {"v": True},
        {"v": "1"},
        {"id": "not-hex"},
        {"id": BOB.user_id.value.upper()},
        {"pk": ""},
        {"pk": b64e(b"\x00" * 257)},
        {"pk": "not base64!"},
        {"alias": ""},
        {"alias": " Bob"},
        {"alias": 5},
        {"sig": ""},
        {"sig": b64e(b"\x00" * 129)},
        {"extra": "field"},
        {"sig": ...},  # missing field
        {"alias": ...},
    ],
)
def test_a_malformed_field_is_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(FormatError):
        ContactLink.from_text(raw(**overrides))


def test_a_well_formed_link_with_a_bad_signature_still_parses() -> None:
    """Parsing checks the structure only; the signature is checked by the
    contacts use case with the crypto port."""
    link = ContactLink.from_text(raw())
    assert link.user_id == BOB.user_id
    assert not CRYPTO.verify(link.public_key, link.signing_bytes(), link.signature)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"user_id": BOB.user_id.value},
        {"alias": "Bob"},
        {"public_key": "key"},
        {"signature": None},
        {"version": 2},
    ],
)
def test_the_constructor_validates_types(kwargs: dict[str, Any]) -> None:
    fields: dict[str, Any] = {
        "user_id": BOB.user_id,
        "public_key": BOB.keys.public_key,
        "alias": Alias("Bob"),
        "signature": b"sig",
    }
    fields.update(kwargs)
    with pytest.raises(FormatError):
        ContactLink(**fields)


def test_ids_compare_by_value() -> None:
    assert link_of(BOB).user_id == UserId.from_public_key(BOB.keys.public_key)
