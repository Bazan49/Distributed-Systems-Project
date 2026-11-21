"""FakeCrypto: an INSECURE stand-in for CryptoPort, for tests ONLY.

It is not real cryptography. It exists so that business logic can be tested
fast and deterministically, without PyNaCl, while still behaving like the real
thing: wrong keys fail, tampering is detected, encryption is randomized, and
secrets need the right password. It deliberately breaks a security property:
anyone who knows a public key can forge that user's signatures and decrypt
messages sent to them, because the "secret" parts are derived from public
values. It is a test double, which is the one exception to the port's rule
"never implement cryptography yourself".
"""

import hashlib
import hmac
import os
from collections.abc import Callable

from source.application.ports.crypto import CryptoPort
from source.application.ports.errors import DecryptionError, WrongPasswordError
from source.domain.records import KeyPair

_PUBLIC_SIZE = 32
_PRIVATE_SIZE = 64  # public key (32) followed by the secret part (32)
_NONCE_SIZE = 16
_TAG_SIZE = 16
_SALT_SIZE = 16


# helpers
def _sha256(*parts: bytes) -> bytes:
    return hashlib.sha256(b"".join(parts)).digest()


def _keystream(key: bytes, nonce: bytes, length: int) -> bytes:
    blocks = (length + 31) // 32
    stream = b"".join(_sha256(key, nonce, i.to_bytes(8, "big")) for i in range(blocks))
    return stream[:length]


def _xor(data: bytes, stream: bytes) -> bytes:
    return (int.from_bytes(data, "big") ^ int.from_bytes(stream, "big")).to_bytes(
        len(data), "big"
    )


def _tag(key: bytes, nonce: bytes, body: bytes) -> bytes:
    return hmac.new(key, nonce + body, hashlib.sha256).digest()[:_TAG_SIZE]


class FakeCrypto(CryptoPort):
    INSECURE = True

    def __init__(self) -> None:
        self._randbytes: Callable[[int], bytes]
        self._randbytes = os.urandom

    @staticmethod
    def _public_of(secret: bytes) -> bytes:
        return _sha256(b"fake/pub", secret)

    def generate_key_pair(self) -> KeyPair:
        """Create a new identity key pair."""
        secret = self._randbytes(32)
        public = self._public_of(secret)
        return KeyPair(public_key=public, private_key=public + secret)

    @staticmethod
    def _split_private(private_key: bytes) -> bytes:
        """Validate a private key and return its public half."""
        if not isinstance(private_key, bytes) or len(private_key) != _PRIVATE_SIZE:
            raise ValueError("malformed private key")
        public, secret = private_key[:_PUBLIC_SIZE], private_key[_PUBLIC_SIZE:]
        if FakeCrypto._public_of(secret) != public:
            raise ValueError("malformed private key")
        return public

    @staticmethod
    def _signature(public_key: bytes, data: bytes) -> bytes:
        return hmac.new(public_key, b"fake/sig" + data, hashlib.sha256).digest()

    def sign(self, private_key: bytes, message: bytes) -> bytes:
        """Return a signature of ``message`` made with ``private_key``."""
        return self._signature(self._split_private(private_key), message)

    def verify(self, public_key: bytes, message: bytes, signature: bytes) -> bool:
        """Return True only if ``signature`` is valid. Never raises on a bad signature."""
        if not (
            isinstance(public_key, bytes)
            and isinstance(message, bytes)
            and isinstance(signature, bytes)
            and len(public_key) == _PUBLIC_SIZE
        ):
            return False
        return hmac.compare_digest(self._signature(public_key, message), signature)

    # encryption to a public key
    @staticmethod
    def _check_public(public_key: bytes) -> None:
        if not isinstance(public_key, bytes) or len(public_key) != _PUBLIC_SIZE:
            raise ValueError("malformed public key")

    def encrypt_for(self, receiver_public_key: bytes, plaintext: bytes) -> bytes:
        """Encrypt so that only the owner of the matching private key can read it."""
        self._check_public(receiver_public_key)
        nonce = self._randbytes(_NONCE_SIZE)
        keystream = _keystream(
            _sha256(b"fake/enc", receiver_public_key), nonce, len(plaintext)
        )
        body = _xor(plaintext, keystream)
        return (
            nonce + body + _tag(_sha256(b"fake/mac", receiver_public_key), nonce, body)
        )

    def decrypt(self, private_key: bytes, ciphertext: bytes) -> bytes:
        """Decrypt ``ciphertext``.

        Raises:
            DecryptionError: wrong key, or the ciphertext was altered.
        """
        public = self._split_private(private_key)
        if (
            not isinstance(ciphertext, bytes)
            or len(ciphertext) < _NONCE_SIZE + _TAG_SIZE
        ):
            raise DecryptionError("ciphertext too short")
        nonce = ciphertext[:_NONCE_SIZE]
        body = ciphertext[_NONCE_SIZE:-_TAG_SIZE]
        tag = ciphertext[-_TAG_SIZE:]
        if not hmac.compare_digest(
            _tag(_sha256(b"fake/mac", public), nonce, body), tag
        ):
            raise DecryptionError("wrong key or corrupted ciphertext")
        return _xor(body, _keystream(_sha256(b"fake/enc", public), nonce, len(body)))

    # password protection
    @staticmethod
    def _password_key(salt: bytes, password: str) -> bytes:
        return _sha256(
            b"fake/kdf", salt, password.encode("utf-8")
        )  # no real KDF: it is a fake

    def protect_private_key(self, private_key: bytes, password: str) -> bytes:
        """Encrypt the private key with a key derived from ``password``"""
        salt = self._randbytes(_SALT_SIZE)
        key = self._password_key(salt, password)
        body = _xor(private_key, _keystream(key, salt, len(private_key)))
        return salt + body + _tag(key, salt, body)

    def unlock_private_key(self, protected: bytes, password: str) -> bytes:
        """Reverse of ``protect_private_key``.

        Raises:
            WrongPasswordError: the password is incorrect or the blob is corrupted.
        """
        if not isinstance(protected, bytes) or len(protected) < _SALT_SIZE + _TAG_SIZE:
            raise WrongPasswordError("protected data too short")
        salt = protected[:_SALT_SIZE]
        body = protected[_SALT_SIZE:-_TAG_SIZE]
        tag = protected[-_TAG_SIZE:]
        key = self._password_key(salt, password)
        if not hmac.compare_digest(_tag(key, salt, body), tag):
            raise WrongPasswordError("wrong password or corrupted data")
        return _xor(body, _keystream(key, salt, len(body)))

    # fingerprint
    def fingerprint(self, public_key: bytes) -> str:
        """Human-comparable fingerprint used to verify a contact out of band."""
        if not isinstance(public_key, bytes) or not public_key:
            raise ValueError("public key must be non-empty bytes")
        digest = hashlib.sha256(b"fake/fp" + public_key).hexdigest()[:24]
        return " ".join(digest[i : i + 4] for i in range(0, 24, 4))
