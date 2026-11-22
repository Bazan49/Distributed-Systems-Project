"""Errors that ports may raise. Adapters translate library errors into these."""


class PortError(Exception):
    """Base class for every error raised through a port."""


# -- storage ------------------------------------------------------------------


class StoreError(PortError):
    """Base class of storage errors."""


class AlreadyExistsError(StoreError):
    """The item to create already exists (duplicate key)."""


class NotFoundError(StoreError):
    """The item to update does not exist."""


# -- cryptography -------------------------------------------------------------


class CryptoError(PortError):
    """Base class of cryptography errors."""


class DecryptionError(CryptoError):
    """The data could not be decrypted: wrong key, corrupted or forged ciphertext."""


class WrongPasswordError(CryptoError):
    """The password does not unlock the protected private key."""
