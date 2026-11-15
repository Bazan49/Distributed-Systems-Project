"""Errors that ports may raise. Adapters translate library errors into these."""


class PortError(Exception):
    """Base class for every error raised through a port."""


class DecryptionError(PortError):
    """Ciphertext is corrupted, tampered with, or not meant for this key."""


class WrongPasswordError(PortError):
    """The password does not unlock the protected private key."""


class StorageError(PortError):
    """The local storage failed (I/O error, corrupted database...)."""
