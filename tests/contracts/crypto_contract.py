"""Contract of CryptoPort: behaviour EVERY implementation must have.

Subclass ``CryptoContract`` in a test module and override the ``crypto`` fixture:

    class TestNaClCrypto(CryptoContract):
        @pytest.fixture
        def crypto(self) -> CryptoPort:
            return NaClCrypto()
"""

import dataclasses

import pytest

from source.application.ports.crypto import CryptoPort
from source.application.ports.errors import DecryptionError, WrongPasswordError
from source.domain.envelope import (
    MAX_BODY_BYTES,
    MAX_CIPHERTEXT_BYTES,
    MAX_SIGNATURE_BYTES,
    Envelope,
    MessageContent,
    MessageKind,
)
from source.domain.identifiers import Alias, ConversationId, MessageId, UserId
from source.domain.identity import LocalIdentity
from source.domain.records import KeyPair

MSG_ID = MessageId("0123456789abcdef0123456789abcdef")
OTHER_MSG_ID = MessageId("fedcba9876543210fedcba9876543210")
PASSWORD = "correct horse battery staple"

MALFORMED_KEYS = [b"", b"short", b"x" * 1000]
GARBAGE_BYTES = [
    b"",
    b"x",
    b"\x00" * 5,
    bytes(range(32)),
    bytes(range(64)),
    b"\xff" * 1000,
]
PLAINTEXTS = [
    b"",
    b"x",
    b"hello",
    bytes(range(256)),
    b"a" * 20_000,
    b"b" * 70_000,
]


def flip(data: bytes, position: int) -> bytes:
    """Return ``data`` with one bit changed at ``position``."""
    mutable = bytearray(data)
    mutable[position] ^= 0x01
    return bytes(mutable)


class CryptoContract:
    @pytest.fixture
    def crypto(self) -> CryptoPort:
        raise NotImplementedError(
            "override the 'crypto' fixture in the concrete test class"
        )

    @pytest.fixture
    def alice(self, crypto: CryptoPort) -> KeyPair:
        return crypto.generate_key_pair()

    @pytest.fixture
    def bob(self, crypto: CryptoPort) -> KeyPair:
        return crypto.generate_key_pair()

    # ------------------------------------------------------------------ keys

    def test_a_keypair_has_non_empty_byte_keys(self, alice: KeyPair) -> None:
        assert isinstance(alice.public_key, bytes) and alice.public_key
        assert isinstance(alice.private_key, bytes) and alice.private_key

    def test_public_and_private_keys_differ(self, alice: KeyPair) -> None:
        assert alice.public_key != alice.private_key

    def test_every_generated_keypair_is_new(self, crypto: CryptoPort) -> None:
        pairs = [crypto.generate_key_pair() for _ in range(5)]
        assert len({p.public_key for p in pairs}) == 5
        assert len({p.private_key for p in pairs}) == 5

    def test_the_public_key_can_identify_a_user(
        self, alice: KeyPair, bob: KeyPair
    ) -> None:
        assert UserId.from_public_key(alice.public_key) != UserId.from_public_key(
            bob.public_key
        )

    # ------------------------------------------------------------ signatures

    def test_a_signature_is_non_empty_bytes_within_the_envelope_limit(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        signature = crypto.sign(alice.private_key, b"data")
        assert isinstance(signature, bytes)
        assert 1 <= len(signature) <= MAX_SIGNATURE_BYTES

    def test_signing_is_deterministic(self, crypto: CryptoPort, alice: KeyPair) -> None:
        assert crypto.sign(alice.private_key, b"data") == crypto.sign(
            alice.private_key, b"data"
        )

    @pytest.mark.parametrize("data", [b"", b"x", bytes(range(256)), b"z" * 100_000])
    def test_a_valid_signature_verifies(
        self, crypto: CryptoPort, alice: KeyPair, data: bytes
    ) -> None:
        signature = crypto.sign(alice.private_key, data)
        assert crypto.verify(alice.public_key, data, signature) is True

    def test_different_data_gives_a_different_signature(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        assert crypto.sign(alice.private_key, b"one") != crypto.sign(
            alice.private_key, b"two"
        )

    def test_verification_fails_if_the_data_changes(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        signature = crypto.sign(alice.private_key, b"original")
        assert crypto.verify(alice.public_key, b"original!", signature) is False
        assert crypto.verify(alice.public_key, b"originaL", signature) is False
        assert crypto.verify(alice.public_key, b"", signature) is False

    def test_verification_fails_under_another_public_key(
        self, crypto: CryptoPort, alice: KeyPair, bob: KeyPair
    ) -> None:
        signature = crypto.sign(alice.private_key, b"data")
        assert crypto.verify(bob.public_key, b"data", signature) is False

    def test_one_user_cannot_pass_off_a_signature_as_another(
        self, crypto: CryptoPort, alice: KeyPair, bob: KeyPair
    ) -> None:
        by_bob = crypto.sign(bob.private_key, b"data")
        assert crypto.verify(alice.public_key, b"data", by_bob) is False

    def test_verification_fails_for_a_tampered_signature(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        signature = crypto.sign(alice.private_key, b"data")
        for position in (0, len(signature) // 2, len(signature) - 1):
            assert (
                crypto.verify(alice.public_key, b"data", flip(signature, position))
                is False
            )
        assert crypto.verify(alice.public_key, b"data", signature[:-1]) is False
        assert crypto.verify(alice.public_key, b"data", signature + b"\x00") is False
        assert crypto.verify(alice.public_key, b"data", b"") is False

    @pytest.mark.parametrize("bad_key", GARBAGE_BYTES)
    @pytest.mark.parametrize("bad_signature", GARBAGE_BYTES)
    def test_verify_returns_false_and_never_raises_on_garbage(
        self, crypto: CryptoPort, bad_key: bytes, bad_signature: bytes
    ) -> None:
        # A hostile network must not be able to crash us with malformed input.
        assert crypto.verify(bad_key, b"data", bad_signature) is False

    @pytest.mark.parametrize("bad_key", MALFORMED_KEYS)
    def test_signing_with_a_malformed_private_key_is_a_value_error(
        self, crypto: CryptoPort, bad_key: bytes
    ) -> None:
        with pytest.raises(ValueError):
            crypto.sign(bad_key, b"data")

    # ------------------------------------------------------------ encryption

    @pytest.mark.parametrize("plaintext", PLAINTEXTS, ids=lambda p: f"{len(p)}-bytes")
    def test_encrypting_then_decrypting_returns_the_original(
        self, crypto: CryptoPort, alice: KeyPair, plaintext: bytes
    ) -> None:
        ciphertext = crypto.encrypt_for(alice.public_key, plaintext)
        assert crypto.decrypt(alice.private_key, ciphertext) == plaintext

    def test_the_ciphertext_hides_the_plaintext(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        plaintext = b"attack at dawn, bring the documents " * 3
        ciphertext = crypto.encrypt_for(alice.public_key, plaintext)
        assert ciphertext != plaintext
        assert b"attack at dawn" not in ciphertext

    def test_encryption_is_randomized(self, crypto: CryptoPort, alice: KeyPair) -> None:
        # Otherwise equal messages would be recognisable from outside.
        first = crypto.encrypt_for(alice.public_key, b"same message")
        second = crypto.encrypt_for(alice.public_key, b"same message")
        assert first != second
        assert crypto.decrypt(alice.private_key, first) == crypto.decrypt(
            alice.private_key, second
        )

    @pytest.mark.parametrize("size", [0, 1, 1_000, 20_000])
    def test_encryption_overhead_is_small_but_not_zero(
        self, crypto: CryptoPort, alice: KeyPair, size: int
    ) -> None:
        ciphertext = crypto.encrypt_for(alice.public_key, b"x" * size)
        assert 0 < len(ciphertext) - size <= 256

    def test_only_the_intended_receiver_can_decrypt(
        self, crypto: CryptoPort, alice: KeyPair, bob: KeyPair
    ) -> None:
        ciphertext = crypto.encrypt_for(alice.public_key, b"for alice only")
        with pytest.raises(DecryptionError):
            crypto.decrypt(bob.private_key, ciphertext)

    def test_changing_any_single_byte_of_the_ciphertext_is_detected(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        ciphertext = crypto.encrypt_for(alice.public_key, b"short message")
        for position in range(len(ciphertext)):
            with pytest.raises(DecryptionError):
                crypto.decrypt(alice.private_key, flip(ciphertext, position))

    def test_truncated_extended_or_garbage_ciphertexts_fail_cleanly(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        ciphertext = crypto.encrypt_for(alice.public_key, b"a message")
        broken = [b"", b"x", b"\x00" * 10, b"\xff" * 47, b"\xff" * 48, b"\xff" * 200]
        broken += [
            ciphertext[:-1],
            ciphertext[1:],
            ciphertext + b"\x00",
            ciphertext[: len(ciphertext) // 2],
        ]
        for item in broken:
            with pytest.raises(DecryptionError):
                crypto.decrypt(alice.private_key, item)

    @pytest.mark.parametrize("bad_key", MALFORMED_KEYS)
    def test_encrypting_to_a_malformed_public_key_is_a_value_error(
        self, crypto: CryptoPort, bad_key: bytes
    ) -> None:
        # Can happen at runtime: a hostile contact link may carry a malformed key.
        with pytest.raises(ValueError):
            crypto.encrypt_for(bad_key, b"data")

    @pytest.mark.parametrize("bad_key", MALFORMED_KEYS)
    def test_decrypting_with_a_malformed_private_key_is_a_value_error(
        self, crypto: CryptoPort, alice: KeyPair, bad_key: bytes
    ) -> None:
        ciphertext = crypto.encrypt_for(alice.public_key, b"data")
        with pytest.raises(ValueError):
            crypto.decrypt(bad_key, ciphertext)

    # ------------------------------------------------- password protection

    def test_a_protected_secret_is_recovered_with_the_right_password(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        protected = crypto.protect_private_key(alice.private_key, PASSWORD)
        assert crypto.unlock_private_key(protected, PASSWORD) == alice.private_key

    def test_the_protected_form_does_not_contain_the_secret(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        protected = crypto.protect_private_key(alice.private_key, PASSWORD)
        assert alice.private_key not in protected
        assert len(protected) > len(alice.private_key)

    def test_protection_is_randomized(self, crypto: CryptoPort, alice: KeyPair) -> None:
        # A random salt: the same secret and password never give the same output twice.
        assert crypto.protect_private_key(
            alice.private_key, PASSWORD
        ) != crypto.protect_private_key(alice.private_key, PASSWORD)

    @pytest.mark.parametrize(
        "wrong",
        ["wrong", "Correct horse battery staple", " " + PASSWORD, PASSWORD + " ", ""],
    )
    def test_a_wrong_password_is_rejected(
        self, crypto: CryptoPort, alice: KeyPair, wrong: str
    ) -> None:
        protected = crypto.protect_private_key(alice.private_key, PASSWORD)
        with pytest.raises(WrongPasswordError):
            crypto.unlock_private_key(protected, wrong)

    def test_corrupted_protected_data_is_reported_as_a_wrong_password(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        protected = crypto.protect_private_key(alice.private_key, PASSWORD)
        broken = [
            flip(protected, p) for p in (0, len(protected) // 2, len(protected) - 1)
        ]
        broken += [b"", b"x", b"\x00" * 50, protected[:-1], protected + b"\x00"]
        for item in broken:
            with pytest.raises(WrongPasswordError):
                crypto.unlock_private_key(item, PASSWORD)

    @pytest.mark.parametrize("password", ["contraseña-ñ-日本-🔑", "p" * 10_000])
    def test_unusual_passwords_work(
        self, crypto: CryptoPort, alice: KeyPair, password: str
    ) -> None:
        protected = crypto.protect_private_key(alice.private_key, password)
        assert crypto.unlock_private_key(protected, password) == alice.private_key

    def test_logging_in_restores_a_working_identity(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        # The whole login flow: store the key protected, later unlock it and use it.
        stored = crypto.protect_private_key(alice.private_key, PASSWORD)
        unlocked = crypto.unlock_private_key(stored, PASSWORD)
        signature = crypto.sign(unlocked, b"proof")
        assert crypto.verify(alice.public_key, b"proof", signature) is True

    # ----------------------------------------------------------- fingerprint

    def test_a_fingerprint_is_short_printable_text(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        fingerprint = crypto.fingerprint(alice.public_key)
        assert isinstance(fingerprint, str)
        assert 1 <= len(fingerprint) <= 80 and fingerprint.isprintable()

    def test_a_fingerprint_is_deterministic_and_distinguishes_keys(
        self, crypto: CryptoPort, alice: KeyPair, bob: KeyPair
    ) -> None:
        assert crypto.fingerprint(alice.public_key) == crypto.fingerprint(
            alice.public_key
        )
        assert crypto.fingerprint(alice.public_key) != crypto.fingerprint(
            bob.public_key
        )

    # --------------------------------- works with the REAL domain structures

    def _sender_view(
        self, crypto: CryptoPort, alice: KeyPair, bob: KeyPair, body: str = "hello Bob"
    ) -> tuple[MessageContent, Envelope]:
        sender, receiver = (
            UserId.from_public_key(alice.public_key),
            UserId.from_public_key(bob.public_key),
        )
        content = MessageContent(
            MessageKind.TEXT,
            ConversationId.between(sender, receiver),
            1,
            1_700_000_000_000,
            body,
        )
        envelope = Envelope.build(
            message_id=MSG_ID,
            sender=sender,
            receiver=receiver,
            ciphertext=crypto.encrypt_for(bob.public_key, content.to_bytes()),
            sign=lambda data: crypto.sign(alice.private_key, data),
        )
        return content, envelope

    def test_a_full_message_travels_from_alice_to_bob(
        self, crypto: CryptoPort, alice: KeyPair, bob: KeyPair
    ) -> None:
        content, envelope = self._sender_view(crypto, alice, bob)

        received = Envelope.from_bytes(envelope.to_bytes())  # over the wire
        assert received.sender == UserId.from_public_key(alice.public_key)
        assert crypto.verify(
            alice.public_key, received.signing_bytes(), received.signature
        )
        decrypted = MessageContent.from_bytes(
            crypto.decrypt(bob.private_key, received.ciphertext)
        )
        received.ensure_content_matches(decrypted)
        assert decrypted == content

    def test_the_largest_legal_message_fits_in_an_envelope(
        self, crypto: CryptoPort, alice: KeyPair, bob: KeyPair
    ) -> None:
        _content, envelope = self._sender_view(
            crypto, alice, bob, body="a" * MAX_BODY_BYTES
        )
        assert len(envelope.ciphertext) <= MAX_CIPHERTEXT_BYTES

    def test_a_tampered_or_redirected_envelope_fails_verification(
        self, crypto: CryptoPort, alice: KeyPair, bob: KeyPair
    ) -> None:
        _content, envelope = self._sender_view(crypto, alice, bob)
        carla = UserId.from_public_key(crypto.generate_key_pair().public_key)
        tampered = dataclasses.replace(
            envelope, ciphertext=flip(envelope.ciphertext, 0)
        )
        redirected = dataclasses.replace(envelope, receiver=carla)
        for forged in (tampered, redirected):
            assert not crypto.verify(
                alice.public_key, forged.signing_bytes(), forged.signature
            )

    def test_a_protected_key_fits_in_a_local_identity(
        self, crypto: CryptoPort, alice: KeyPair
    ) -> None:
        identity = LocalIdentity(
            user_id=UserId.from_public_key(alice.public_key),
            public_key=alice.public_key,
            alias=Alias("Alice"),
            protected_private_key=crypto.protect_private_key(
                alice.private_key, PASSWORD
            ),
            created_at_ms=1,
        )
        recovered = crypto.unlock_private_key(identity.protected_private_key, PASSWORD)
        assert recovered == alice.private_key
