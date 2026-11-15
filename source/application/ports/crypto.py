from typing import Protocol, runtime_checkable

from source.domain.records import KeyPair


@runtime_checkable
class CryptoPort(Protocol):
    """Every cryptographic operation the system needs.

    Phase 1 adapter: PyNaCl (libsodium). The domain never imports ``nacl``.
    Encryption is end to end WITHOUT forward secrecy: a leaked private key
    decrypts old messages (documented scope limit of Phase 1).

    How it plugs into the domain (see tests/integration):
      * encrypt:  ``encrypt_for(receiver_pk, content.to_bytes())`` -> ciphertext
      * sign:     ``Envelope.build(..., sign=lambda b: crypto.sign(sk, b))``
      * verify:   ``verify(sender_pk, envelope.signing_bytes(), envelope.signature)``
      * decrypt:  ``MessageContent.from_bytes(decrypt(sk, envelope.ciphertext))``
    """

    def generate_key_pair(self) -> KeyPair:
        """Create a new identity key pair."""
        ...

    def sign(self, private_key: bytes, message: bytes) -> bytes:
        """Return a signature of ``message`` made with ``private_key``."""
        ...

    def verify(self, public_key: bytes, message: bytes, signature: bytes) -> bool:
        """Return True only if ``signature`` is valid. Never raises on a bad signature."""
        ...

    def encrypt_for(self, receiver_public_key: bytes, plaintext: bytes) -> bytes:
        """Encrypt so that only the owner of the matching private key can read it."""
        ...

    def decrypt(self, private_key: bytes, ciphertext: bytes) -> bytes:
        """Decrypt ``ciphertext``.

        Raises:
            DecryptionError: wrong key, or the ciphertext was altered.
        """
        ...

    def fingerprint(self, public_key: bytes) -> str:
        """Human-comparable fingerprint used to verify a contact out of band."""
        ...

    def protect_private_key(self, private_key: bytes, password: str) -> bytes:
        """Encrypt the private key with a key derived from ``password`` (for storage)."""
        ...

    def unlock_private_key(self, protected: bytes, password: str) -> bytes:
        """Reverse of ``protect_private_key``.

        Raises:
            WrongPasswordError: the password is incorrect or the blob is corrupted.
        """
        ...
