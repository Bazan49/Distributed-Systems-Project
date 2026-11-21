"""NaClCrypto: the production implementation of ``CryptoPort``, built on PyNaCl.

PyNaCl is a thin binding to libsodium, a widely audited cryptographic library.
This module composes its primitives; it does not implement any cryptography
itself.

Keys
----
A user's identity is one *composite* key pair made of two independent pairs, so
that signing and encryption never share key material:

====================  ==========================================  =========
Part                  Primitive                                   Size
====================  ==========================================  =========
signing key pair      Ed25519 (seed / verify key)                 32 + 32 B
encryption key pair   Curve25519 (secret key / public key)        32 + 32 B
====================  ==========================================  =========

``public_key  = verify_key (32 B) || encryption_public_key (32 B)``  (64 bytes)
``private_key = signing_seed (32 B) || encryption_secret_key (32 B)`` (64 bytes)

Both are opaque to every other layer: only this module knows the layout. The
``UserId`` is the SHA-256 of the whole 64-byte public key, so it commits to both
halves.

Operations
----------
* **sign / verify**: Ed25519. Signatures are 64 bytes and deterministic.
* **encrypt_for / decrypt**: libsodium *sealed box* (an ephemeral Curve25519 key
  agreement plus XSalsa20-Poly1305). The sender needs no key of its own and the
  ciphertext is 48 bytes longer than the plaintext. Sender authenticity comes
  from the envelope signature (encrypt-then-sign), not from the sealed box.
* **protect_private_key / unlock_private_key**: the password is stretched with
  Argon2id (memory-hard) into a key for XSalsa20-Poly1305 (``SecretBox``). The
  blob is self-describing, so the cost parameters can be raised in the future
  without breaking blobs that already exist.
* **fingerprint**: a truncated, domain-separated SHA-256 of the public key.

Known limitation
----------------
A sealed box has no forward secrecy with respect to the recipient: whoever
obtains a user's private key can read every message ever sent to that user.
This is an accepted trade-off for asynchronous, mailbox-based delivery.

Python cannot reliably erase secrets from memory, so keys live in ordinary
``bytes`` objects for as long as the interpreter keeps them.
"""

from __future__ import annotations

import hashlib
import struct
import unicodedata

from nacl import exceptions as nacl_exceptions
from nacl import public, pwhash, secret, signing, utils

from source.application.ports.crypto import CryptoPort
from source.application.ports.errors import DecryptionError, WrongPasswordError
from source.domain.records import KeyPair

__all__ = ["PRIVATE_KEY_SIZE", "PUBLIC_KEY_SIZE", "SIGNATURE_SIZE", "NaClCrypto"]

_ARGON2ID = pwhash.argon2id

# -- key layout ---------------------------------------------------------------

_HALF_SIZE = 32  # every half of the composite keys is 32 bytes
PUBLIC_KEY_SIZE = 2 * _HALF_SIZE
PRIVATE_KEY_SIZE = 2 * _HALF_SIZE
SIGNATURE_SIZE = 64  # Ed25519 signature length

_SEALED_BOX_OVERHEAD = 48  # ephemeral public key (32) + Poly1305 tag (16)

# -- fingerprint --------------------------------------------------------------

_FINGERPRINT_DOMAIN = b"mensajeria/fingerprint/v1\0"
_FINGERPRINT_BYTES = 20  # 160 bits: plenty for a human to compare out of band
_FINGERPRINT_GROUP = 4  # hex characters per visual group

# -- protected private key blob ----------------------------------------------
#
#   offset  size  field
#   0       1     format version (currently 1)
#   1       4     Argon2id opslimit, big-endian
#   5       4     Argon2id memlimit in bytes, big-endian
#   9       16    random salt
#   25      ...   SecretBox output: nonce (24) || ciphertext || MAC (16)

_BLOB_VERSION = 1
_BLOB_HEADER = struct.Struct(">BII")
_SALT_SIZE = _ARGON2ID.SALTBYTES
_BLOB_PREFIX_SIZE = _BLOB_HEADER.size + _SALT_SIZE
_SECRETBOX_OVERHEAD = secret.SecretBox.NONCE_SIZE + secret.SecretBox.MACBYTES
_MIN_BLOB_SIZE = _BLOB_PREFIX_SIZE + _SECRETBOX_OVERHEAD

# Hard ceilings for the Argon2id cost. They stop a corrupted or hostile blob from
# making ``unlock_private_key`` allocate gigabytes before the password is checked.
_MAX_OPSLIMIT = _ARGON2ID.OPSLIMIT_SENSITIVE
_MAX_MEMLIMIT = _ARGON2ID.MEMLIMIT_SENSITIVE
_MIN_OPSLIMIT = _ARGON2ID.OPSLIMIT_MIN
_MIN_MEMLIMIT = _ARGON2ID.MEMLIMIT_MIN


def _require_key(value: object, size: int, what: str) -> bytes:
    """Return ``value`` if it is ``bytes`` of exactly ``size``; else ValueError."""
    if not isinstance(value, bytes) or len(value) != size:
        raise ValueError(f"malformed {what}: expected {size} bytes")
    return value


def _password_bytes(password: str) -> bytes:
    """Normalise (NFKC) and UTF-8 encode a password.

    Normalisation makes the same password typed on different systems derive the
    same key, e.g. a precomposed ``é`` and ``e`` followed by a combining accent.

    Raises:
        ValueError: ``password`` cannot be encoded (it contains a lone surrogate).
    """
    return unicodedata.normalize("NFKC", password).encode("utf-8")


def _derive_key(password: str, salt: bytes, opslimit: int, memlimit: int) -> bytes:
    return bytes(
        _ARGON2ID.kdf(
            secret.SecretBox.KEY_SIZE,
            _password_bytes(password),
            salt,
            opslimit=opslimit,
            memlimit=memlimit,
        )
    )


class NaClCrypto(CryptoPort):
    """``CryptoPort`` backed by PyNaCl. Stateless, so it is safe to share.

    Args:
        opslimit: Argon2id time cost used when protecting a private key.
        memlimit: Argon2id memory cost, in bytes, used when protecting a private key.

    The defaults are libsodium's *moderate* profile (3 passes over 256 MiB, roughly
    half a second on a laptop): a key is unlocked once per login, so the cost is
    paid rarely while it makes offline password guessing against a stolen device
    expensive. Tests pass the minimum values to stay fast. The cost only applies
    to *new* protected blobs; existing ones carry their own parameters.

    Raises:
        ValueError: a cost parameter is outside the range libsodium (or this
            module) accepts.
    """

    def __init__(
        self,
        *,
        opslimit: int = _ARGON2ID.OPSLIMIT_MODERATE,
        memlimit: int = _ARGON2ID.MEMLIMIT_MODERATE,
    ) -> None:
        if not _MIN_OPSLIMIT <= opslimit <= _MAX_OPSLIMIT:
            raise ValueError(
                f"opslimit must be between {_MIN_OPSLIMIT} and {_MAX_OPSLIMIT}"
            )
        if not _MIN_MEMLIMIT <= memlimit <= _MAX_MEMLIMIT:
            raise ValueError(
                f"memlimit must be between {_MIN_MEMLIMIT} and {_MAX_MEMLIMIT} bytes"
            )
        self._opslimit = opslimit
        self._memlimit = memlimit

    # -- identity key pair ----------------------------------------------------

    def generate_key_pair(self) -> KeyPair:
        """Create a new identity key pair (see the module docstring for the layout).

        The randomness comes from the operating system's CSPRNG through libsodium.
        """
        signing_key = signing.SigningKey.generate()
        encryption_key = public.PrivateKey.generate()
        return KeyPair(
            public_key=bytes(signing_key.verify_key) + bytes(encryption_key.public_key),
            private_key=bytes(signing_key) + bytes(encryption_key),
        )

    # -- signatures -----------------------------------------------------------

    def sign(self, private_key: bytes, message: bytes) -> bytes:
        """Sign ``message`` with Ed25519 and return the 64-byte signature.

        Signing is deterministic: the same key and message give the same bytes.

        Raises:
            ValueError: ``private_key`` is not a well-formed 64-byte private key.
        """
        key = _require_key(private_key, PRIVATE_KEY_SIZE, "private key")
        signing_key = signing.SigningKey(key[:_HALF_SIZE])
        return bytes(signing_key.sign(message).signature)

    def verify(self, public_key: bytes, message: bytes, signature: bytes) -> bool:
        """Return ``True`` only if ``signature`` is a valid signature of ``message``.

        This never raises: a hostile peer must not be able to crash the caller by
        sending a malformed key or signature, so every failure is simply ``False``.
        """
        if not (
            isinstance(public_key, bytes)
            and isinstance(message, bytes)
            and isinstance(signature, bytes)
            and len(public_key) == PUBLIC_KEY_SIZE
            and len(signature) == SIGNATURE_SIZE
        ):
            return False
        try:
            signing.VerifyKey(public_key[:_HALF_SIZE]).verify(message, signature)
        except (nacl_exceptions.CryptoError, ValueError, TypeError):
            return False
        return True

    # -- encryption to a public key -------------------------------------------

    def encrypt_for(self, receiver_public_key: bytes, plaintext: bytes) -> bytes:
        """Encrypt ``plaintext`` so that only the owner of the matching key can read it.

        Uses a sealed box: every call uses a fresh ephemeral key, so encrypting the
        same plaintext twice gives different ciphertexts. The ciphertext is exactly
        48 bytes longer than the plaintext.

        Raises:
            ValueError: ``receiver_public_key`` is not a well-formed 64-byte public key.
                This can happen at runtime, e.g. with a hostile contact link.
        """
        key = _require_key(receiver_public_key, PUBLIC_KEY_SIZE, "public key")
        sealed_box = public.SealedBox(public.PublicKey(key[_HALF_SIZE:]))
        return bytes(sealed_box.encrypt(plaintext))

    def decrypt(self, private_key: bytes, ciphertext: bytes) -> bytes:
        """Decrypt a ciphertext produced by ``encrypt_for``.

        Raises:
            ValueError: ``private_key`` is not a well-formed 64-byte private key.
            DecryptionError: the key is not the intended one, or the ciphertext is
                truncated, corrupted or tampered with. The cases are not told apart.
        """
        key = _require_key(private_key, PRIVATE_KEY_SIZE, "private key")
        if not isinstance(ciphertext, bytes) or len(ciphertext) < _SEALED_BOX_OVERHEAD:
            raise DecryptionError("ciphertext is too short")
        sealed_box = public.SealedBox(public.PrivateKey(key[_HALF_SIZE:]))
        try:
            return bytes(sealed_box.decrypt(ciphertext))
        except (nacl_exceptions.CryptoError, ValueError, TypeError) as exc:
            raise DecryptionError("wrong key or corrupted ciphertext") from exc

    # -- fingerprint ----------------------------------------------------------

    def fingerprint(self, public_key: bytes) -> str:
        """Return a short text two people can read aloud to verify a contact.

        Ten groups of four hex characters, e.g. ``"3f2a 9c1e ..."``: the first
        160 bits of a domain-separated SHA-256 of the public key.

        Raises:
            ValueError: ``public_key`` is not a well-formed 64-byte public key.
        """
        key = _require_key(public_key, PUBLIC_KEY_SIZE, "public key")
        digest = hashlib.sha256(_FINGERPRINT_DOMAIN + key).digest()
        digits = digest[:_FINGERPRINT_BYTES].hex()
        step = _FINGERPRINT_GROUP
        return " ".join(digits[i : i + step] for i in range(0, len(digits), step))

    # -- password protection --------------------------------------------------

    def protect_private_key(self, private_key: bytes, password: str) -> bytes:
        """Encrypt ``private_key`` under a key derived from ``password`` (Argon2id).

        A fresh random salt and nonce are used every time, so protecting the same
        key twice gives different blobs. The blob records the Argon2id cost it was
        made with, so ``unlock_private_key`` works whatever this instance's cost is.

        Password *policy* (minimum length, strength) is the caller's business.

        Raises:
            ValueError: ``password`` cannot be encoded (it contains a lone surrogate).
        """
        salt = bytes(utils.random(_SALT_SIZE))
        key = _derive_key(password, salt, self._opslimit, self._memlimit)
        sealed = secret.SecretBox(key).encrypt(private_key)
        header = _BLOB_HEADER.pack(_BLOB_VERSION, self._opslimit, self._memlimit)
        return header + salt + bytes(sealed)

    def unlock_private_key(self, protected: bytes, password: str) -> bytes:
        """Reverse of ``protect_private_key``.

        Raises:
            WrongPasswordError: the password is wrong, or the blob is corrupted,
                truncated or in an unknown format. A wrong password and a damaged
                blob cannot be told apart, because the authentication tag fails
                the same way for both.
        """
        if not isinstance(protected, bytes) or len(protected) < _MIN_BLOB_SIZE:
            raise WrongPasswordError("protected key is too short")
        version, opslimit, memlimit = _BLOB_HEADER.unpack_from(protected)
        if (
            version != _BLOB_VERSION
            or not _MIN_OPSLIMIT <= opslimit <= _MAX_OPSLIMIT
            or not _MIN_MEMLIMIT <= memlimit <= _MAX_MEMLIMIT
        ):
            raise WrongPasswordError("unknown or corrupted protected key format")
        salt = protected[_BLOB_HEADER.size : _BLOB_PREFIX_SIZE]
        try:
            key = _derive_key(password, salt, opslimit, memlimit)
        except ValueError as exc:  # a password that cannot even be encoded
            raise WrongPasswordError("wrong password") from exc
        try:
            return bytes(secret.SecretBox(key).decrypt(protected[_BLOB_PREFIX_SIZE:]))
        except (nacl_exceptions.CryptoError, ValueError) as exc:
            raise WrongPasswordError("wrong password or corrupted data") from exc
