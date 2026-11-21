import dataclasses
import hashlib
import hmac
import json
import random
from typing import Any

import pytest

from source.domain.canonical import FormatError
from source.domain.envelope import (
    MAX_BODY_BYTES,
    MAX_CIPHERTEXT_BYTES,
    MAX_ENVELOPE_BYTES,
    Envelope,
    MessageContent,
    MessageKind,
)
from source.domain.identifiers import ConversationId, MessageId, UserId

ALICE = UserId.from_public_key(b"alice")
BOB = UserId.from_public_key(b"bob")
CARLA = UserId.from_public_key(b"carla")
CONV = ConversationId.between(ALICE, BOB)
MSG_ID = MessageId("0123456789abcdef0123456789abcdef")
KEY = b"test-key"  # FAKE signature, used only in tests; the real one lives behind CryptoPort


def fake_sign(data: bytes) -> bytes:
    return hmac.new(KEY, data, hashlib.sha256).digest()


def fake_verify(env: Envelope) -> bool:
    return hmac.compare_digest(fake_sign(env.signing_bytes()), env.signature)


def make_envelope(**over: Any) -> Envelope:
    args: dict[str, Any] = {
        "message_id": MSG_ID,
        "sender": ALICE,
        "receiver": BOB,
        "ciphertext": b"\x01\x02ciphertext\xff",
        "sign": fake_sign,
    }
    args.update(over)
    return Envelope.build(**args)


def make_content(**over: Any) -> MessageContent:
    args: dict[str, Any] = {
        "kind": MessageKind.TEXT,
        "conversation_id": CONV,
        "seq": 1,
        "sent_at_ms": 1_700_000_000_000,
        "body": "hi",
    }
    args.update(over)
    return MessageContent(**args)


# ----------------------------- Envelope: serialization ----------------------


def test_envelope_round_trip() -> None:
    env = make_envelope()
    assert Envelope.from_bytes(env.to_bytes()) == env


def test_envelope_serialization_is_canonical_and_deterministic() -> None:
    raw = make_envelope().to_bytes()
    assert raw == make_envelope().to_bytes()
    obj = json.loads(raw)
    assert list(obj) == sorted(obj)  # sorted keys
    assert b" " not in raw and b"\n" not in raw
    assert raw.isascii()


def test_envelope_exposes_only_the_six_expected_fields() -> None:
    # Privacy: no seq, no conversation, no time, no plaintext in the envelope.
    assert set(json.loads(make_envelope().to_bytes())) == {
        "v",
        "id",
        "from",
        "to",
        "ct",
        "sig",
    }


def test_envelope_round_trip_with_random_data() -> None:
    rng = random.Random(1234)
    for _ in range(300):
        ct = rng.randbytes(rng.randint(1, 2000))
        env = make_envelope(
            ciphertext=ct, message_id=MessageId(rng.randbytes(16).hex())
        )
        assert Envelope.from_bytes(env.to_bytes()) == env


def test_envelope_is_immutable() -> None:
    env = make_envelope()
    with pytest.raises(dataclasses.FrozenInstanceError):
        env.ciphertext = b"other"  


# ----------------------------- Envelope: signature --------------------------


def test_build_signs_exactly_the_signing_bytes() -> None:
    env = make_envelope()
    assert env.signature == fake_sign(env.signing_bytes())
    assert fake_verify(env)


@pytest.mark.parametrize(
    "field,value",
    [
        ("message_id", MessageId("f" * 32)),
        ("sender", CARLA),
        ("receiver", CARLA),
        ("ciphertext", b"other-text"),
    ],
)
def test_changing_any_field_invalidates_the_signature(field: str, value: Any) -> None:
    original = make_envelope()
    tampered = dataclasses.replace(original, **{field: value})
    assert tampered.signing_bytes() != original.signing_bytes()
    assert not fake_verify(tampered)


def test_signing_bytes_do_not_depend_on_the_signature() -> None:
    env = make_envelope()
    assert (
        dataclasses.replace(env, signature=b"anything").signing_bytes()
        == env.signing_bytes()
    )


def test_signing_bytes_carry_a_domain_prefix() -> None:
    assert make_envelope().signing_bytes().startswith(b"mensajeria/envelope/v1\0")


def test_an_envelope_cannot_be_redirected_to_another_receiver() -> None:
    # A malicious mailbox changes the receiver while keeping the original signature.
    original = make_envelope()
    redirected = dataclasses.replace(original, receiver=CARLA)
    assert not fake_verify(redirected)


# ----------------------------- Envelope: validation -------------------------


def test_rejects_sender_equal_to_receiver() -> None:
    with pytest.raises(FormatError):
        make_envelope(receiver=ALICE)


@pytest.mark.parametrize("ct", [b"", b"x" * (MAX_CIPHERTEXT_BYTES + 1), "text", None])
def test_rejects_invalid_ciphertext(ct: Any) -> None:
    with pytest.raises(FormatError):
        make_envelope(ciphertext=ct)


def test_accepts_ciphertext_at_the_limits() -> None:
    make_envelope(ciphertext=b"x")
    make_envelope(ciphertext=b"x" * MAX_CIPHERTEXT_BYTES)


@pytest.mark.parametrize("sig", [b"", b"x" * 129, None])
def test_rejects_invalid_signature(sig: Any) -> None:
    with pytest.raises(FormatError):
        make_envelope(sign=lambda _data: sig)


def test_rejects_unknown_version() -> None:
    env = make_envelope()
    with pytest.raises(FormatError):
        dataclasses.replace(env, version=2)


def test_rejects_wrong_types_for_ids() -> None:
    with pytest.raises(FormatError):
        make_envelope(sender="alice")
    with pytest.raises(FormatError):
        make_envelope(message_id="0123456789abcdef0123456789abcdef")


# ----------------------------- Envelope: strict parsing ---------------------


def _obj() -> dict[str, Any]:
    obj: dict[str, Any] = json.loads(make_envelope().to_bytes())
    return obj


def _dump(obj: dict[str, Any]) -> bytes:
    return json.dumps(obj).encode()


def test_from_bytes_rejects_extra_or_missing_fields() -> None:
    extra = _obj() | {"seq": 3}
    with pytest.raises(FormatError):
        Envelope.from_bytes(_dump(extra))
    missing = _obj()
    del missing["sig"]
    with pytest.raises(FormatError):
        Envelope.from_bytes(_dump(missing))


def test_from_bytes_rejects_duplicate_keys() -> None:
    raw = make_envelope().to_bytes().decode()
    duplicated = raw[:-1] + ',"v":1}'
    with pytest.raises(FormatError):
        Envelope.from_bytes(duplicated.encode())


@pytest.mark.parametrize(
    "raw", [b"", b"not json", b"[]", b"123", b'"x"', b"\xff\xfe\x00"]
)
def test_from_bytes_rejects_garbage(raw: bytes) -> None:
    with pytest.raises(FormatError):
        Envelope.from_bytes(raw)


def test_from_bytes_rejects_oversized_input() -> None:
    with pytest.raises(FormatError):
        Envelope.from_bytes(b" " * (MAX_ENVELOPE_BYTES + 1))


def test_from_bytes_rejects_text_instead_of_bytes() -> None:
    with pytest.raises(FormatError):
        Envelope.from_bytes("{}")  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["ct", "sig"])
@pytest.mark.parametrize("bad", ["abc=", "a b", "***", "ab+/", 123, None])
def test_from_bytes_rejects_non_canonical_base64(field: str, bad: Any) -> None:
    obj = _obj()
    obj[field] = bad
    with pytest.raises(FormatError):
        Envelope.from_bytes(_dump(obj))


@pytest.mark.parametrize("field", ["id", "from", "to"])
def test_from_bytes_rejects_malformed_ids(field: str) -> None:
    obj = _obj()
    obj[field] = "xyz"
    with pytest.raises(FormatError):
        Envelope.from_bytes(_dump(obj))


@pytest.mark.parametrize("v", [0, 2, "1", True, 1.0, None])
def test_from_bytes_rejects_invalid_version(v: Any) -> None:
    obj = _obj()
    obj["v"] = v
    with pytest.raises(FormatError):
        Envelope.from_bytes(_dump(obj))


def test_an_envelope_altered_in_transit_fails_verification() -> None:
    # A mailbox changes one character of the ciphertext in the JSON it forwards.
    obj = _obj()
    ct = list(obj["ct"])
    ct[0] = "A" if ct[0] != "A" else "B"
    obj["ct"] = "".join(ct)
    tampered = Envelope.from_bytes(_dump(obj))
    assert not fake_verify(tampered)


# ----------------------------- MessageContent -------------------------------


def test_content_round_trip_with_unicode() -> None:
    for text in ["hello", "ñandú ✓ 日本語 🚀", 'line1\nline2\t"quotes" \\']:
        c = make_content(body=text)
        assert MessageContent.from_bytes(c.to_bytes()) == c


def test_content_is_deterministic_and_canonical() -> None:
    c = make_content()
    assert c.to_bytes() == make_content().to_bytes()
    keys = list(json.loads(c.to_bytes()))
    assert keys == sorted(keys)
    assert set(keys) == {"v", "kind", "conv", "seq", "sent_at_ms", "body"}


@pytest.mark.parametrize("kind", list(MessageKind))
def test_content_accepts_every_kind(kind: MessageKind) -> None:
    body = "hi" if kind is MessageKind.TEXT else ""
    c = make_content(kind=kind, body=body)
    assert MessageContent.from_bytes(c.to_bytes()) == c


@pytest.mark.parametrize("seq", [0, -1, True, 1.5, "1", None])
def test_content_rejects_invalid_seq(seq: Any) -> None:
    with pytest.raises(FormatError):
        make_content(seq=seq)


@pytest.mark.parametrize("t", [-1, True, 1.5, "0", None])
def test_content_rejects_invalid_time(t: Any) -> None:
    with pytest.raises(FormatError):
        make_content(sent_at_ms=t)


def test_content_rejects_empty_text_but_not_empty_contact_messages() -> None:
    with pytest.raises(FormatError):
        make_content(body="")
    make_content(kind=MessageKind.CONTACT_ACCEPT, body="")


def test_content_limits_size_in_bytes_not_characters() -> None:
    make_content(body="a" * MAX_BODY_BYTES)
    with pytest.raises(FormatError):
        make_content(body="a" * (MAX_BODY_BYTES + 1))
    with pytest.raises(FormatError):
        make_content(body="é" * (MAX_BODY_BYTES // 2 + 1))  # 2 bytes each


def test_content_rejects_lone_surrogates() -> None:
    with pytest.raises(FormatError):
        make_content(body="\ud800")


@pytest.mark.parametrize(
    "change",
    [{"kind": "other"}, {"conv": "xyz"}, {"seq": 0}, {"v": 2}, {"extra": 1}],
)
def test_content_from_bytes_rejects_invalid_data(change: dict[str, Any]) -> None:
    obj = json.loads(make_content().to_bytes()) | change
    with pytest.raises(FormatError):
        MessageContent.from_bytes(_dump(obj))


def test_content_from_bytes_rejects_garbage() -> None:
    with pytest.raises(FormatError):
        MessageContent.from_bytes(b"\x00\x01")


# ----------------------------- Envelope + content ---------------------------


def test_content_must_belong_to_the_envelopes_conversation() -> None:
    env = make_envelope()
    env.ensure_content_matches(make_content())  # same conversation: ok
    foreign = make_content(conversation_id=ConversationId.between(ALICE, CARLA))
    with pytest.raises(FormatError):
        env.ensure_content_matches(foreign)


def test_the_conversation_is_the_same_in_both_directions() -> None:
    from_alice = make_envelope()
    from_bob = make_envelope(sender=BOB, receiver=ALICE)
    assert (
        from_alice.expected_conversation_id()
        == from_bob.expected_conversation_id()
        == CONV
    )
