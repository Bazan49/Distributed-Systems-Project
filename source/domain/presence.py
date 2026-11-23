"""Presence rules of the client: when to renew the record and with which version.

A user announces "I am online until T" by publishing a signed directory record
with a lease (``expires_at_ms``). If the user stops renewing it, the lease
ends and the user simply looks offline: nobody has to announce a logout.

Pure standard library: the time is passed in as a value.

Versions. Every node keeps the highest version it ever stored for a user and
rejects anything not higher (see ``DirectoryStorePort.highest_version``). So
each record the client publishes must have a higher version than every
previous one, INCLUDING those published before the app was restarted, which
the client no longer remembers. The rule is::

    next version = max(previous version + 1, now in milliseconds)

* After a restart (previous = 0) the wall clock is larger than any version
  published before, as long as the clock did not go back in time.
* Inside one session, ``previous + 1`` keeps versions growing even if the
  clock goes back (an NTP correction) or two publishes fall in the same ms.
"""

from __future__ import annotations

from source.domain.canonical import is_int


def next_version(previous: int, now_ms: int) -> int:
    """The version of the next record: higher than ``previous``, normally ``now_ms``.

    Raises:
        ValueError: ``previous`` or ``now_ms`` is not a non-negative integer.
    """
    if not is_int(previous) or previous < 0:
        raise ValueError("previous must be an integer >= 0")
    if not is_int(now_ms) or now_ms < 0:
        raise ValueError("now_ms must be an integer >= 0")
    return max(previous + 1, now_ms)


def renewal_due(now_ms: int, expires_at_ms: int | None, renew_margin_ms: int) -> bool:
    """Must the presence be published (again) now?

    Yes if there is no accepted presence yet (``expires_at_ms`` is None), or
    if less than ``renew_margin_ms`` of the lease is left. Renewing BEFORE the
    end leaves time for a failed attempt to be retried while the user still
    looks online.
    """
    return expires_at_ms is None or now_ms >= expires_at_ms - renew_margin_ms
