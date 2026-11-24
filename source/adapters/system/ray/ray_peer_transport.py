"""RayPeerTransport: direct client-to-client delivery over Ray.

The real ``PeerTransportPort``. A client that is listening owns a small actor,
its INBOX, registered in Ray under ``client-<user_id>``. Being reachable is
exactly "my inbox exists": ``start_listening`` creates it and
``stop_listening`` kills it, so the name disappears and senders fall back to
the mailboxes.

How one direct send works
=========================

1. The sender looks the receiver's inbox up by name. No inbox: UNREACHABLE.
2. It calls ``inbox.deliver(envelope_bytes)`` and waits at most ``timeout_ms``.
3. The inbox queues the envelope and waits for the receiver's verdict.
4. The receiver's transport runs a listening thread that keeps asking its
   inbox for the next envelope (``inbox.next``, a long poll). It rebuilds
   the envelope with ``Envelope.from_bytes`` (strict validation), hands it to
   the client, and reports back with ``inbox.settle``: True if the client
   settled it, False if the client raised.
5. ``deliver`` returns that verdict: DELIVERED only if the receiver settled
   the envelope. Anything else (no verdict in time, a dead actor, a refusal)
   is UNREACHABLE, and the sender uses the mailboxes. Never an exception.

Why the receiver PULLS instead of being called: the client's code runs in the
process that called ``start_listening`` (a client actor, or a test process),
and a Ray actor cannot call back into an arbitrary process. Asking the inbox
works from anywhere.

A send that timed out is not cancelled: its envelope may still be handed to
the receiver later. That is harmless. The same envelope also goes through the
mailboxes, and the receiver drops duplicates by message id.

Wire format: envelopes cross processes as bytes (``to_bytes``), like the
node gateway, and are always validated on arrival.
"""

from __future__ import annotations

import asyncio
import itertools
import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Any

import ray
from ray.exceptions import GetTimeoutError, RayError

from source.application.observability import get_logger, short
from source.domain.canonical import is_int
from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.records import DirectSendResult

__all__ = [
    "INBOX_PREFIX",
    "Inbox",
    "InboxActor",
    "RayPeerTransport",
    "inbox_name",
]

log = get_logger(__name__)

INBOX_PREFIX = "client-"
POLL_WAIT_S = 0.5
"""How long one ``inbox.next`` waits for an envelope before returning None."""
NAME_RELEASE_WAIT_S = 30.0
"""How long ``stop_listening`` waits for Ray to drop the inbox's name."""

Handler = Callable[[Envelope], None]


def inbox_name(user_id: UserId) -> str:
    """Name under which the inbox of ``user_id`` is registered in Ray."""
    return f"{INBOX_PREFIX}{user_id.value}"


class Inbox:
    """The inbox of one listening client, without Ray.

    An asyncio class: as a Ray actor every call runs on one event loop, so
    many senders can wait for their verdicts at the same time while the
    receiver keeps asking for the next envelope. No locks are needed.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[tuple[int, bytes]] = asyncio.Queue()
        self._verdicts: dict[int, asyncio.Future[bool]] = {}
        self._keys = itertools.count()

    async def deliver(self, envelope: bytes, timeout_s: float) -> bool:
        """Queue ``envelope`` and wait for the receiver's verdict.

        Returns True only if the receiver settled it within ``timeout_s``.
        """
        key = next(self._keys)
        verdict: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._verdicts[key] = verdict
        await self._queue.put((key, envelope))
        try:
            return await asyncio.wait_for(verdict, timeout_s)
        except asyncio.TimeoutError:
            return False
        finally:
            self._verdicts.pop(key, None)

    async def next(self, wait_s: float) -> tuple[int, bytes] | None:
        """The next queued envelope with its key, or None after ``wait_s``."""
        try:
            return await asyncio.wait_for(self._queue.get(), wait_s)
        except asyncio.TimeoutError:
            return None

    async def settle(self, key: int, settled: bool) -> None:
        """The receiver's verdict on envelope ``key`` (ignored if nobody waits)."""
        verdict = self._verdicts.get(key)
        if verdict is not None and not verdict.done():
            verdict.set_result(settled)


# Not restarted: an inbox that dies means "offline", and senders use the
# mailboxes until the client listens again.
InboxActor = ray.remote(max_restarts=0, max_task_retries=0)(Inbox)


class RayPeerTransport:
    """``PeerTransportPort`` over Ray: the direct transport of ONE client.

    Args:
        user: the client this transport belongs to (its inbox is named after it).
        namespace: Ray namespace shared by every client. ``None`` = the
            namespace of the current job.
        guard: held while the client's handler runs, so that envelopes are
            received one at a time and never while the client is busy with
            other work (the client is not thread-safe). ``None`` = no guard.

    Ray must already be initialised (``ray.init``).
    """

    def __init__(
        self,
        user: UserId,
        namespace: str | None = None,
        guard: AbstractContextManager[Any] | None = None,
    ) -> None:
        self._user = user
        self._namespace = namespace
        self._guard: AbstractContextManager[Any] = (
            guard if guard is not None else nullcontext()
        )
        self._inbox: Any | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ---- sending ----
    def send_direct(
        self, receiver: UserId, envelope: Envelope, timeout_ms: int
    ) -> DirectSendResult:
        if not is_int(timeout_ms) or timeout_ms < 0:
            raise ValueError("timeout_ms must be a non-negative integer (milliseconds)")
        timeout_s = timeout_ms / 1000
        try:
            inbox = ray.get_actor(inbox_name(receiver), namespace=self._namespace)
        except ValueError:  # no inbox: the receiver is not listening
            return DirectSendResult.UNREACHABLE
        try:
            settled = ray.get(
                inbox.deliver.remote(envelope.to_bytes(), timeout_s), timeout=timeout_s
            )
        except (GetTimeoutError, RayError) as exc:
            log.debug(
                "direct.unreachable",
                to=short(receiver.value),
                msg=envelope.message_id.value,
                reason=type(exc).__name__,
            )
            return DirectSendResult.UNREACHABLE
        return (
            DirectSendResult.DELIVERED
            if settled is True
            else DirectSendResult.UNREACHABLE
        )

    # ---- receiving ----
    def start_listening(self, on_envelope: Handler) -> None:
        """Create this client's inbox and start taking envelopes from it.

        Listening again replaces the previous handler (and inbox).
        """
        self.stop_listening()
        self._inbox = self._create_inbox()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._listen,
            args=(self._inbox, on_envelope, self._stop),
            name=f"inbox-{short(self._user.value)}",
            daemon=True,
        )
        self._thread.start()
        log.info("direct.listening", user=short(self._user.value))

    def stop_listening(self) -> None:
        """Kill the inbox and wait until its name is free: from then on, senders
        get UNREACHABLE. Idempotent."""
        inbox = self._inbox
        if inbox is None:
            return
        self._inbox = self._thread = None
        # The listening thread is not joined: it may be waiting for the guard
        # that our caller holds. It ends by itself: the stop flag is set and
        # its next call to the dead inbox fails.
        self._stop.set()
        ray.kill(inbox, no_restart=True)
        self._wait_until_name_is_free()
        log.info("direct.stopped", user=short(self._user.value))

    def _create_inbox(self) -> Any:
        """Register a new inbox; retry while a previous one is still releasing the name."""
        deadline = time.monotonic() + NAME_RELEASE_WAIT_S
        while True:
            try:
                inbox: Any = InboxActor.options(
                    name=inbox_name(self._user), namespace=self._namespace
                ).remote()
                ray.get(inbox.next.remote(0))  # wait until it is really up
            except ValueError:  # the name is still taken
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)
            else:
                return inbox

    def _wait_until_name_is_free(self) -> None:
        deadline = time.monotonic() + NAME_RELEASE_WAIT_S
        while time.monotonic() < deadline:
            try:
                ray.get_actor(inbox_name(self._user), namespace=self._namespace)
            except ValueError:
                return
            time.sleep(0.05)

    def _listen(self, inbox: Any, handler: Handler, stop: threading.Event) -> None:
        """The listening thread: take, hand over, report. Ends when stopped."""
        while not stop.is_set():
            try:
                item = ray.get(inbox.next.remote(POLL_WAIT_S))
            except RayError:  # the inbox was killed (stop_listening)
                return
            if item is None:
                continue
            key, raw = item
            settled = self._hand_over(raw, handler)
            try:
                inbox.settle.remote(key, settled)
            except RayError:
                return

    def _hand_over(self, raw: bytes, handler: Handler) -> bool:
        """Validate and hand one envelope to the client; True if it settled it."""
        try:
            envelope = Envelope.from_bytes(raw)
        except Exception as exc:  # malformed input from an untrusted peer
            log.warning("direct.rejected", reason=type(exc).__name__)
            return False
        try:
            with self._guard:
                handler(envelope)
        except Exception as exc:  # the client did not settle it: not delivered
            log.debug(
                "direct.not_settled",
                msg=envelope.message_id.value,
                reason=type(exc).__name__,
            )
            return False
        return True
