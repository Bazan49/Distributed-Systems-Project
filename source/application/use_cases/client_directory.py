"""The directory as the CLIENT uses it: announce my presence and find contacts.

* ``publish``: sign a new record of my own (my public key and a lease) and
  store it on my nodes with the write quorum (2 of 3).
* ``renew_if_due``: publish again when the lease is close to its end. The app
  calls it periodically (every few seconds); most calls do nothing.
* ``resolve_contact``: the newest valid record of a contact, or None if the
  contact is unknown or offline (lease ended).

The rules (version numbers, when to renew) live in ``source.domain.presence``;
the quorum (which nodes, how to combine answers, checking every record again)
in ``QuorumClient``. This service only adds the user's key and the clock.

The node side of the directory is ``DirectoryService``; this is the client side.

Logs (see ``docs/logging.md``), once per publication:

* ``presence.published`` (DEBUG: it happens every ~10 s) with the ``version``
  and ``expires_in_ms``, how long the new presence lasts;
* ``presence.renew_failed`` (WARNING) with the ``version`` that was not
  accepted and ``expires_in_ms``, how long the PREVIOUS presence still lasts
  (0: the user already looks offline).

Depends only on ports, other use cases and the domain: no Ray, no SQLite.
"""

from __future__ import annotations

from dataclasses import dataclass

from source.application.observability import get_logger
from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.use_cases.directory import DEFAULT_MAX_LEASE_MS
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.quorum import QuorumClient, WriteResult
from source.domain.canonical import is_int
from source.domain.directory import build_record
from source.domain.identifiers import UserId
from source.domain.presence import next_version, renewal_due
from source.domain.records import DirectoryRecord

log = get_logger(__name__)

DEFAULT_LEASE_MS = 30_000  # a presence lasts 30 s unless renewed
DEFAULT_RENEW_MARGIN_MS = 20_000  # renew when 20 s are left: about every 10 s


@dataclass(frozen=True)
class PresencePolicy:
    """How long a presence lasts and how early it is renewed.

    With the defaults the record is renewed about every 10 s, so if one
    attempt fails (no quorum) there is still time for one or two retries
    before the user looks offline.
    """

    lease_ms: int = DEFAULT_LEASE_MS
    renew_margin_ms: int = DEFAULT_RENEW_MARGIN_MS

    def __post_init__(self) -> None:
        if not is_int(self.lease_ms) or not 1 <= self.lease_ms <= DEFAULT_MAX_LEASE_MS:
            # Nodes refuse longer leases (DirectoryPolicy.max_lease_ms).
            raise ValueError(
                f"lease_ms must be an integer from 1 to {DEFAULT_MAX_LEASE_MS}"
            )
        if not is_int(self.renew_margin_ms) or not (
            0 < self.renew_margin_ms < self.lease_ms
        ):
            raise ValueError("renew_margin_ms must be an integer in (0, lease_ms)")


@dataclass(frozen=True)
class PublishAttempt:
    """One attempt to publish my presence."""

    record: DirectoryRecord  # what was sent (signed, with its version and lease)
    result: WriteResult  # which nodes confirmed it

    @property
    def accepted(self) -> bool:
        return self.result.accepted


class DirectoryClient:
    def __init__(
        self,
        me: UnlockedIdentity,
        quorum: QuorumClient,
        crypto: CryptoPort,
        clock: ClockPort,
        policy: PresencePolicy | None = None,
    ) -> None:
        self._me = me
        self._quorum = quorum
        self._crypto = crypto
        self._clock = clock
        self._policy = policy if policy is not None else PresencePolicy()
        self._last_version = 0  # highest version this session ever signed
        self._online_until_ms: int | None = None  # lease of the last ACCEPTED record

    # ---- my presence ----
    def publish(self) -> PublishAttempt:
        """Sign and publish a new record of mine now, with a full lease.

        Every call uses a new, higher version (even if a previous attempt
        failed: some node may have stored it anyway). If the quorum is not
        reached, my presence is not considered renewed and ``renew_if_due``
        will try again.
        """
        now = self._clock.now_ms()
        version = next_version(self._last_version, now)
        record = build_record(
            public_key=self._me.public_key,
            version=version,
            expires_at_ms=now + self._policy.lease_ms,
            sign=lambda data: self._crypto.sign(self._me.private_key, data),
        )
        self._last_version = version
        result = self._quorum.publish_record(record)
        if result.accepted:
            self._online_until_ms = record.expires_at_ms
            log.debug(
                "presence.published",
                version=version,
                expires_in_ms=record.expires_at_ms - now,
            )
        else:
            left = 0 if self._online_until_ms is None else self._online_until_ms - now
            log.warning(
                "presence.renew_failed", version=version, expires_in_ms=max(left, 0)
            )
        return PublishAttempt(record=record, result=result)

    def renew_if_due(self) -> PublishAttempt | None:
        """Publish if there is no accepted presence or its lease is about to end.

        Returns the attempt, or None if nothing had to be done. Meant to be
        called periodically (a timer or the app's main loop).
        """
        if not renewal_due(
            self._clock.now_ms(), self._online_until_ms, self._policy.renew_margin_ms
        ):
            return None
        return self.publish()

    @property
    def online_until_ms(self) -> int | None:
        """End of my current presence lease, or None if I look offline now."""
        if (
            self._online_until_ms is None
            or self._online_until_ms <= self._clock.now_ms()
        ):
            return None
        return self._online_until_ms

    # ---- contacts ----
    def resolve_contact(self, user_id: UserId) -> DirectoryRecord | None:
        """The newest valid record of ``user_id``, or None if unknown or offline.

        Every record was already checked by ``QuorumClient.resolve`` (identity,
        signature, lease), so a node cannot forge or extend a presence.
        """
        return self._quorum.resolve(user_id)

    def is_online(self, user_id: UserId) -> bool:
        """True if ``user_id`` has a presence whose lease has not ended."""
        return self.resolve_contact(user_id) is not None
