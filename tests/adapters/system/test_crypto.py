"""NaClCrypto must pass the shared CryptoContract plus its own format checks."""

import struct
from typing import Any

import pytest
from nacl import public as nacl_public
from nacl import pwhash
from nacl import signing as nacl_signing

from source.adapters.system.crypto import (
    PRIVATE_KEY_SIZE,
    PUBLIC_KEY_SIZE,
    SIGNATURE_SIZE,
    NaClCrypto,
)
from source.application.ports.crypto import CryptoPort
from source.application.ports.errors import DecryptionError, WrongPasswordError
from source.domain.identifiers import UserId
from source.domain.records import KeyPair
from tests.contracts.crypto_contract import CryptoContract

FAST_OPS = pwhash.argon2id.OPSLIMIT_MIN
FAST_MEM = pwhash.argon2id.MEMLIMIT_MIN
PASSWORD = "correct horse battery staple"


def fast_crypto() -> NaClCrypto:
    """Cheapest Argon2id cost, so the password tests do not take minutes."""
    return NaClCrypto(opslimit=FAST_OPS, memlimit=FAST_MEM)


class TestNaClCryptoContract(CryptoContract):
    @pytest.fixture
    def crypto(self) -> CryptoPort:
        return fast_crypto()


@pytest.fixture
def crypto() -> NaClCrypto:
    return fast_crypto()


@pytest.fixture
def keys(crypto: NaClCrypto) -> KeyPair:
    return crypto.generate_key_pair()


# --------------------------------------------------------------- port / keys


def test_it_satisfies_the_port() -> None:
    assert isinstance(NaClCrypto(), CryptoPort)


def test_keys_have_the_documented_sizes(keys: KeyPair) -> None:
    assert len(keys.public_key) == PUBLIC_KEY_SIZE == 64
    assert len(keys.private_key) == PRIVATE_KEY_SIZE == 64


def test_the_two_halves_of_a_key_are_independent(crypto: NaClCrypto) -> None:
    # Signing and encryption must never share key material.
    keys = crypto.generate_key_pair()
    assert keys.public_key[:32] != keys.public_key[32:]
    assert keys.private_key[:32] != keys.private_key[32:]


def test_keys_are_plain_bytes_not_nacl_objects(keys: KeyPair) -> None:
    assert isinstance(keys.public_key, bytes) and isinstance(keys.private_key, bytes)


def test_the_private_key_never_appears_in_repr(keys: KeyPair) -> None:
    assert keys.private_key.hex() not in repr(keys).lower()
    assert repr(keys.private_key) not in repr(keys)


def test_the_user_id_commits_to_the_whole_public_key(crypto: NaClCrypto) -> None:
    keys = crypto.generate_key_pair()
    # Same signing half, different encryption half.
    swapped = keys.public_key[:32] + bytes(32)
    assert UserId.from_public_key(keys.public_key) != UserId.from_public_key(swapped)


# --------------------------------------------------------------- signatures


def test_ed25519_known_answer_rfc_8032_test_1(crypto: NaClCrypto) -> None:
    # RFC 8032, section 7.1, TEST 1: proves this is standard Ed25519.
    seed = bytes.fromhex(
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"
    )
    verify_key = bytes.fromhex(
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    )
    expected = bytes.fromhex(
        "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33b"
        "acc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
    )
    private_key = seed + bytes(32)
    public_key = verify_key + bytes(32)
    assert crypto.sign(private_key, b"") == expected
    assert crypto.verify(public_key, b"", expected) is True


def test_signatures_have_the_ed25519_size(crypto: NaClCrypto, keys: KeyPair) -> None:
    assert len(crypto.sign(keys.private_key, b"data")) == SIGNATURE_SIZE == 64


def test_a_signature_from_raw_pynacl_verifies(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    # Pins the documented layout: the signing seed is the first half of the private key.
    raw = nacl_signing.SigningKey(keys.private_key[:32]).sign(b"hello").signature
    assert crypto.verify(keys.public_key, b"hello", raw) is True


@pytest.mark.parametrize("bad", [None, "text", 5, bytearray(64)])
def test_verify_does_not_raise_on_wrongly_typed_arguments(
    crypto: NaClCrypto, keys: KeyPair, bad: Any
) -> None:
    signature = crypto.sign(keys.private_key, b"data")
    assert crypto.verify(bad, b"data", signature) is False
    assert crypto.verify(keys.public_key, bad, signature) is False
    assert crypto.verify(keys.public_key, b"data", bad) is False


def test_verify_rejects_a_public_key_with_an_invalid_signing_half(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    signature = crypto.sign(keys.private_key, b"data")
    for half in (bytes(32), b"\xff" * 32, b"\x01" + bytes(31)):  # not valid / low order
        assert crypto.verify(half + keys.public_key[32:], b"data", signature) is False


def test_a_signature_does_not_verify_with_swapped_key_halves(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    signature = crypto.sign(keys.private_key, b"data")
    swapped = keys.public_key[32:] + keys.public_key[:32]
    assert crypto.verify(swapped, b"data", signature) is False


# --------------------------------------------------------------- encryption


def test_the_ciphertext_overhead_is_exactly_the_sealed_box_one(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    for size in (0, 1, 1000):
        assert len(crypto.encrypt_for(keys.public_key, b"x" * size)) == size + 48


def test_a_ciphertext_from_raw_pynacl_decrypts(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    # Pins the documented layout: the encryption key is the second half of each key.
    sealed = nacl_public.SealedBox(nacl_public.PublicKey(keys.public_key[32:]))
    assert crypto.decrypt(keys.private_key, sealed.encrypt(b"interop")) == b"interop"


def test_decrypt_does_not_raise_anything_but_decryption_error(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    for bad in (b"", b"\x00" * 47, b"\x00" * 48, b"\x00" * 100, None, "text", 5):
        with pytest.raises(DecryptionError):
            crypto.decrypt(keys.private_key, bad)


def test_decryption_uses_only_the_encryption_half_of_the_private_key(
    crypto: NaClCrypto,
) -> None:
    alice, bob = crypto.generate_key_pair(), crypto.generate_key_pair()
    ciphertext = crypto.encrypt_for(alice.public_key, b"secret")
    # Alice's encryption half is enough, whatever the signing half is...
    mixed = bob.private_key[:32] + alice.private_key[32:]
    assert crypto.decrypt(mixed, ciphertext) == b"secret"
    # ...and Alice's signing half is useless without it.
    with pytest.raises(DecryptionError):
        crypto.decrypt(alice.private_key[:32] + bob.private_key[32:], ciphertext)


# ------------------------------------------------------------- fingerprint


def test_the_fingerprint_is_ten_groups_of_four_hex_digits(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    groups = crypto.fingerprint(keys.public_key).split(" ")
    assert len(groups) == 10
    assert all(len(g) == 4 and set(g) <= set("0123456789abcdef") for g in groups)


def test_the_fingerprint_changes_if_either_half_changes(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    base = crypto.fingerprint(keys.public_key)
    assert crypto.fingerprint(bytes(32) + keys.public_key[32:]) != base
    assert crypto.fingerprint(keys.public_key[:32] + bytes(32)) != base


@pytest.mark.parametrize("bad", [b"", b"short", b"x" * 65, None, "text"])
def test_the_fingerprint_of_a_malformed_key_is_a_value_error(
    crypto: NaClCrypto, bad: Any
) -> None:
    with pytest.raises(ValueError):
        crypto.fingerprint(bad)


# -------------------------------------------------- protected private key


def test_the_blob_is_self_describing(crypto: NaClCrypto, keys: KeyPair) -> None:
    blob = crypto.protect_private_key(keys.private_key, PASSWORD)
    version, ops, mem = struct.unpack_from(">BII", blob)
    assert (version, ops, mem) == (1, FAST_OPS, FAST_MEM)
    # header (9) + salt (16) + nonce (24) + key (64) + MAC (16)
    assert len(blob) == 9 + 16 + 24 + PRIVATE_KEY_SIZE + 16


def test_every_blob_uses_a_fresh_salt_and_a_fresh_nonce(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    # A repeated salt would let an attacker reuse one Argon2id computation for many blobs.
    first = crypto.protect_private_key(keys.private_key, PASSWORD)
    second = crypto.protect_private_key(keys.private_key, PASSWORD)
    assert first[9:25] != second[9:25]  # salt
    assert first[25:49] != second[25:49]  # SecretBox nonce


def test_a_blob_unlocks_with_an_instance_configured_differently(
    keys: KeyPair,
) -> None:
    blob = fast_crypto().protect_private_key(keys.private_key, PASSWORD)
    other = NaClCrypto(opslimit=FAST_OPS + 1, memlimit=FAST_MEM * 2)
    assert other.unlock_private_key(blob, PASSWORD) == keys.private_key


def test_the_production_defaults_round_trip(keys: KeyPair) -> None:
    # The only test that pays the real Argon2id cost (about a second).
    crypto = NaClCrypto()
    blob = crypto.protect_private_key(keys.private_key, PASSWORD)
    assert crypto.unlock_private_key(blob, PASSWORD) == keys.private_key
    with pytest.raises(WrongPasswordError):
        crypto.unlock_private_key(blob, PASSWORD + "!")


def test_the_same_password_in_different_unicode_forms_unlocks(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    composed, decomposed = "café", "café"
    blob = crypto.protect_private_key(keys.private_key, composed)
    assert crypto.unlock_private_key(blob, decomposed) == keys.private_key


def test_a_header_with_an_unknown_version_is_rejected(
    crypto: NaClCrypto, keys: KeyPair
) -> None:
    blob = bytearray(crypto.protect_private_key(keys.private_key, PASSWORD))
    blob[0] = 2
    with pytest.raises(WrongPasswordError):
        crypto.unlock_private_key(bytes(blob), PASSWORD)


@pytest.mark.parametrize(
    "ops, mem",
    [(0, FAST_MEM), (FAST_OPS, 0), (2**32 - 1, FAST_MEM), (FAST_OPS, 2**32 - 1)],
)
def test_an_absurd_cost_in_the_header_is_rejected_without_computing_it(
    crypto: NaClCrypto, keys: KeyPair, ops: int, mem: int
) -> None:
    # A corrupted or hostile blob must not make us allocate gigabytes.
    blob = crypto.protect_private_key(keys.private_key, PASSWORD)
    forged = struct.pack(">BII", 1, ops, mem) + blob[9:]
    with pytest.raises(WrongPasswordError):
        crypto.unlock_private_key(forged, PASSWORD)


def test_a_password_that_cannot_be_encoded(crypto: NaClCrypto, keys: KeyPair) -> None:
    with pytest.raises(ValueError):
        crypto.protect_private_key(keys.private_key, "bad\ud800")
    blob = crypto.protect_private_key(keys.private_key, PASSWORD)
    with pytest.raises(WrongPasswordError):
        crypto.unlock_private_key(blob, "bad\ud800")


# ------------------------------------------------------------ construction


@pytest.mark.parametrize(
    "kwargs",
    [
        {"opslimit": 0},
        {"opslimit": 5},
        {"memlimit": FAST_MEM - 1},
        {"memlimit": 2**31},
    ],
)
def test_the_constructor_rejects_cost_parameters_out_of_range(
    kwargs: dict[str, int],
) -> None:
    with pytest.raises(ValueError):
        NaClCrypto(**kwargs)


def test_the_constructor_only_accepts_keyword_arguments() -> None:
    with pytest.raises(TypeError):
        NaClCrypto(2, 8192)
