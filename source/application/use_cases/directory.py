"""Directory service: the rules of one node's user directory.

``DirectoryStorePort`` only stores. THIS service decides:

* a record is accepted only if it is well formed, its ``user_id`` matches the
  public key it carries, and its signature is valid. A forged or altered
  record never reaches the store;
* of two valid records for one user, the higher version wins; a lower version,
  or a different record with the same version, is rejected;
* the presence lease must be in the future and not longer than the policy allows;
* an expired record is never returned: the user simply looks offline.

``check_record_authenticity`` is also meant for the CLIENT: a node is not
trusted, so whoever resolves a record should check it again.

Depends only on ports and the domain: no Ray, no SQLite.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.directory_store import DirectoryStorePort
from source.domain.canonical import is_int
from source.domain.directory import (
    DirectoryRecordError,
    identity_matches,
    public_key_of,
    signing_bytes,
    validate_shape,
)
from source.domain.identifiers import UserId
from source.domain.records import DirectoryRecord

DEFAULT_MAX_LEASE_MS = (
    10 * 60 * 1000
)  # a presence can be announced for at most 10 minutes


class RecordCheck(Enum):
    VALID = "valid"
    MALFORMED = "malformed"
    BAD_IDENTITY = "bad_identity"  # user_id is not the hash of the announced key
    INVALID_SIGNATURE = "invalid_signature"


class PublishOutcome(Enum):
    STORED = "stored"
    UNCHANGED = "unchanged"  # the identical record was already here (idempotent)
    REJECTED_MALFORMED = "rejected_malformed"
    REJECTED_BAD_IDENTITY = "rejected_bad_identity"
    REJECTED_INVALID_SIGNATURE = "rejected_invalid_signature"
    REJECTED_EXPIRED = "rejected_expired"
    REJECTED_LEASE_TOO_LONG = "rejected_lease_too_long"
    REJECTED_STALE = (
        "rejected_stale"  # a newer (or conflicting) record is already stored
    )

    @property
    def accepted(self) -> bool:
        return self in (PublishOutcome.STORED, PublishOutcome.UNCHANGED)


def check_record_authenticity(
    record: DirectoryRecord, crypto: CryptoPort
) -> RecordCheck:
    """Is this record well formed, self-consistent and signed by its owner?

    Says nothing about time or versions: those are policy of whoever receives it.
    """
    try:
        validate_shape(record)
    except DirectoryRecordError:
        return RecordCheck.MALFORMED
    if not identity_matches(record):
        return RecordCheck.BAD_IDENTITY
    if not crypto.verify(
        public_key_of(record), signing_bytes(record), record.signature
    ):
        return RecordCheck.INVALID_SIGNATURE
    return RecordCheck.VALID


@dataclass(frozen=True)
class DirectoryPolicy:
    max_lease_ms: int = DEFAULT_MAX_LEASE_MS

    def __post_init__(self) -> None:
        if not is_int(self.max_lease_ms) or self.max_lease_ms < 1:
            raise ValueError("max_lease_ms must be a positive integer")


_FROM_CHECK = {
    RecordCheck.MALFORMED: PublishOutcome.REJECTED_MALFORMED,
    RecordCheck.BAD_IDENTITY: PublishOutcome.REJECTED_BAD_IDENTITY,
    RecordCheck.INVALID_SIGNATURE: PublishOutcome.REJECTED_INVALID_SIGNATURE,
}


class DirectoryService:
    def __init__(
        self,
        store: DirectoryStorePort,
        crypto: CryptoPort,
        clock: ClockPort,
        policy: DirectoryPolicy | None = None,
    ) -> None:
        self._store = store
        self._crypto = crypto
        self._clock = clock
        self._policy = policy if policy is not None else DirectoryPolicy()

    def publish(self, record: DirectoryRecord) -> PublishOutcome:
        """Validate ``record`` and store it if it is acceptable.

        Order of checks: shape, lease, identity, signature, version. A rejected
        record never changes anything.
        """
        try:
            validate_shape(record)
        except DirectoryRecordError:
            return PublishOutcome.REJECTED_MALFORMED

        now = self._clock.now_ms()
        if record.expires_at <= now:
            return PublishOutcome.REJECTED_EXPIRED
        if record.expires_at - now > self._policy.max_lease_ms:
            return PublishOutcome.REJECTED_LEASE_TOO_LONG

        check = check_record_authenticity(record, self._crypto)
        if check is not RecordCheck.VALID:
            return _FROM_CHECK[check]

        stored = self._store.get(record.user_id)
        if stored is not None:
            if stored == record:
                return PublishOutcome.UNCHANGED
            if record.version <= stored.version:
                return PublishOutcome.REJECTED_STALE
        self._store.put(record)
        return PublishOutcome.STORED

    def resolve(self, user_id: UserId) -> DirectoryRecord | None:
        """The current record of ``user_id``, or None if unknown or expired.

        An expired record is never returned, even if it is still stored.
        """
        record = self._store.get(user_id)
        if record is None or record.expires_at <= self._clock.now_ms():
            return None
        return record

    def purge_expired(self) -> int:
        """Physically remove every expired record; return how many."""
        return self._store.delete_expired(self._clock.now_ms())
