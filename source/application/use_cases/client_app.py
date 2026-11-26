"""ClientApp: the client's public API. One object that wires every client use case.

Two lifetimes:

* **Always available** (built once): identity, sending, receiving, contacts,
  membership and the quorum client. They need no private key.
* **Per session** (built on login, dropped on logout): the peer transport,
  the directory client and the delivery policy. They all depend on WHO is
  logged in: the transport is addressed by the user's id (in Ray, the inbox
  ``client-<id>``), and the directory client signs presence records with the
  private key. So ``ClientApp`` receives a factory, ``peers_for(user_id)``,
  rather than a transport.

The periodic work of a connected client lives in ``tick()``, in this order:

1. refresh the membership view, so the next calls ask live nodes;
2. renew the presence record if its lease is ending;
3. collect from the mailboxes, receive, and acknowledge what was settled;
4. skip ordering gaps whose deadline passed;
5. deliver whatever is due in the outbox (including the answers that
   receiving may have queued, such as contact acceptances).

``tick()`` is cheap to call often: steps 1 and 3 run at most once per
interval of ``ClientPolicy``; steps 2, 4 and 5 do nothing when nothing is due.
The caller decides the cadence (a CLI loop, a Ray actor timer, a test).

Direct envelopes from peers arrive through ``receive_direct``: the peer
transport is told to call it on login. An envelope is confirmed to its sender
only if it was settled; otherwise the transport reports a failure and the
sender falls back to the mailboxes.

Change listeners (``add_change_listener``) are called when a conversation
may look different: messages were stored in the history (texts, contact
messages), or a sent message changed state (delivered, accepted by the
mailboxes, expired). Never for a tick that changed nothing, so an
interface can wait for them instead of polling.

Not thread-safe: the caller serialises calls (the Ray client actor holds one lock).
It depends only on ports, the other use cases and the domain: no Ray, no SQLite.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum

from source.application.observability import get_logger
from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.local_store import LocalStorePort
from source.application.ports.node_gateway import NodeGatewayPort
from source.application.ports.peer_transport import PeerTransportPort
from source.application.use_cases.client_directory import (
    DirectoryClient,
    PresencePolicy,
)
from source.application.use_cases.contacts import (
    AddResult,
    ContactsService,
    ContactView,
)
from source.application.use_cases.delivery import (
    DeliveryOutcome,
    DeliveryPolicy,
    DeliveryReport,
    DeliveryService,
)
from source.application.use_cases.identity import IdentityService, UnlockedIdentity
from source.application.use_cases.membership import MembershipService
from source.application.use_cases.quorum import QuorumClient, QuorumPolicy
from source.application.use_cases.receive_messages import (
    BatchResult,
    GapReport,
    ReceiveMessagesService,
    ReceiveResult,
)
from source.application.use_cases.send_message import SendMessageService
from source.domain.canonical import is_int
from source.domain.contacts import ContactState
from source.domain.envelope import Envelope, MessageKind
from source.domain.identifiers import Alias, ConversationId, MessageId, UserId
from source.domain.mailbox_access import (
    MAX_ACK_IDS,
    MAX_COLLECT_LIMIT,
    MailboxAction,
    MailboxRequest,
)
from source.domain.messages import OutgoingMessage, ReceivedMessage
from source.domain.ordering import OrderingPolicy
from source.domain.outgoing_status import OutgoingStatus

__all__ = [
    "ChangeListener",
    "ClientApp",
    "ClientError",
    "ClientPolicy",
    "EntryKind",
    "HistoryEntry",
    "NotLoggedInError",
    "Profile",
    "TickReport",
]

log = get_logger(__name__)


ChangeListener = Callable[[], None]
"""Called with no arguments when a conversation may look different."""

# A sent message changed state (its status in the history) in these outcomes.
_STATUS_CHANGES = frozenset(
    {
        DeliveryOutcome.DELIVERED_DIRECT,
        DeliveryOutcome.ACCEPTED_BY_MAILBOX,
        DeliveryOutcome.EXPIRED,
    }
)

# errors


class ClientError(Exception):
    """Base class of the errors raised by ``ClientApp`` itself."""


class NotLoggedInError(ClientError):
    """The operation needs a logged-in user."""


class EnvelopeNotAcceptedError(ClientError):
    """A direct envelope was not settled; its sender must use the mailboxes."""


# policy


@dataclass(frozen=True)
class ClientPolicy:
    """How often ``tick()`` does its periodic network work.

    Attributes:
        membership_refresh_ms: ping every node at most this often. Must stay
            well below the membership's dead timeout (15 s by default): a
            node not heard from for that long is considered dead.
        collect_interval_ms: collect from the mailboxes at most this often.
            Must stay well below the receiver's gap timeout (2 min), so that
            a message waiting in a mailbox fills its gap before it is skipped.
        collect_limit: most envelopes asked for per collect.
    """

    membership_refresh_ms: int = 5_000
    collect_interval_ms: int = 10_000
    collect_limit: int = 100

    def __post_init__(self) -> None:
        for name in ("membership_refresh_ms", "collect_interval_ms", "collect_limit"):
            value = getattr(self, name)
            if not is_int(value) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.collect_limit > MAX_COLLECT_LIMIT:
            raise ValueError(f"collect_limit must be at most {MAX_COLLECT_LIMIT}")


# views


@dataclass(frozen=True)
class Profile:
    """The logged-in user, as the interface shows it."""

    user_id: UserId
    alias: Alias
    fingerprint: str
    link: str


class EntryKind(str, Enum):
    RECEIVED = "received"
    SENT = "sent"
    MISSING = "missing"
    """A seq of the contact that never arrived: "waiting for this message"."""


@dataclass(frozen=True)
class HistoryEntry:
    """One line of a conversation.

    ``status`` is set only for SENT entries; ``message_id`` for every entry
    except MISSING ones; ``body`` is empty for MISSING ones.

    ``lamport`` is the message's Lamport timestamp, the history's order. A
    MISSING entry takes the one of the message that follows it, since its own
    is unknown until it arrives.
    """

    kind: EntryKind
    seq: int
    sent_at_ms: int
    lamport: int
    body: str = ""
    status: OutgoingStatus | None = None
    message_id: MessageId | None = None


@dataclass(frozen=True)
class TickReport:
    """What one ``tick()`` did."""

    membership_refreshed: bool
    presence_published: bool | None
    """None if the presence did not need renewing; else whether it was accepted."""
    collected: BatchResult | None
    """None if it was not time to collect."""
    gaps: tuple[GapReport, ...]
    delivery: DeliveryReport

    @property
    def stored(self) -> tuple[ReceivedMessage, ...]:
        """Every message stored in the history by this tick, in storage order."""
        from_mailbox = () if self.collected is None else self.collected.released
        from_gaps = tuple(d for gap in self.gaps for d in gap.released)
        return tuple(d.item for d in (*from_mailbox, *from_gaps))


# the app


@dataclass
class _Session:
    identity: UnlockedIdentity
    peers: PeerTransportPort
    directory: DirectoryClient
    delivery: DeliveryService


class ClientApp:
    """The client: account, contacts, conversations and the periodic network work."""

    def __init__(
        self,
        *,
        crypto: CryptoPort,
        store: LocalStorePort,
        clock: ClockPort,
        gateway: NodeGatewayPort,
        peers_for: Callable[[UserId], PeerTransportPort],
        policy: ClientPolicy | None = None,
        ordering: OrderingPolicy | None = None,
        delivery_policy: DeliveryPolicy | None = None,
        quorum_policy: QuorumPolicy | None = None,
        presence_policy: PresencePolicy | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._crypto = crypto
        self._store = store
        self._clock = clock
        self._peers_for = peers_for
        self._policy = policy if policy is not None else ClientPolicy()
        self._delivery_policy = delivery_policy
        self._presence_policy = presence_policy
        self._rng = rng

        self._identity = IdentityService(crypto, store, clock)
        self._membership = MembershipService(clock)
        self._quorum = QuorumClient(
            gateway, self._membership, crypto, clock, quorum_policy
        )
        self._sender = SendMessageService(crypto, store, clock)
        self._contacts = ContactsService(crypto, store, clock, self._sender)
        self._receiver = ReceiveMessagesService(
            crypto, store, clock, ordering, contacts=self._contacts
        )
        self._session: _Session | None = None
        self._last_run_ms: dict[str, int] = {}
        self._listeners: list[ChangeListener] = []

    def add_change_listener(self, listener: ChangeListener) -> None:
        """Call ``listener`` whenever a conversation may look different.

        It runs in the thread that did the work (a tick, a direct envelope, a
        send), while the caller holds its lock: it must be quick and must not
        call the app back. Typically it only wakes someone up.
        """
        self._listeners.append(listener)

    def _changed(self) -> None:
        for listener in list(self._listeners):
            listener()

    # account

    def is_registered(self) -> bool:
        """True if this device already holds an identity."""
        return self._identity.is_registered()

    @property
    def logged_in(self) -> bool:
        """True if a user is logged in (the session is open)."""
        return self._session is not None

    def register(self, alias: str, password: str) -> Profile:
        """Create this device's identity and log in.

        Raises:
            ValueError: invalid alias.
            WeakPasswordError: the password does not satisfy the policy.
            AlreadyRegisteredError: this device already holds an identity.
        """
        return self._start(self._identity.register(alias, password))

    def login(self, password: str) -> Profile:
        """Unlock the identity and go online. Logging in again replaces the session.

        Raises:
            NotRegisteredError: no identity on this device.
            WrongPasswordError: wrong password.
        """
        return self._start(self._identity.unlock(password))

    def logout(self) -> None:
        """Go offline and forget the unlocked key.

        Nothing is announced: the presence simply expires with its lease, and
        senders fall back to the mailboxes. Idempotent.
        """
        if self._session is None:
            return
        user = self._session.identity.user_id
        self._session.peers.stop_listening()
        self._session = None
        log.info("session.ended", user=user)

    def profile(self) -> Profile:
        """The logged-in user: id, alias, fingerprint and contact link."""
        return self._profile(self._require_session().identity)

    # contacts

    def add_contact(self, link: str) -> AddResult:
        """Add the owner of ``link`` (see ``ContactsService.add_contact``) and send at once."""
        session = self._require_session()
        result = self._contacts.add_contact(session.identity, link)
        self._deliver(session)
        return result

    def contacts(self, state: ContactState | None = None) -> list[ContactView]:
        """Every contact (or those in ``state``), with fingerprint and look-alikes."""
        self._require_session()
        return self._contacts.contacts(state)

    def pending_requests(self) -> list[ContactView]:
        """The requests inbox, oldest first."""
        self._require_session()
        return self._contacts.pending_requests()

    def accept(self, user_id: UserId) -> ContactView:
        """Accept a pending request and send the acceptance at once."""
        session = self._require_session()
        view = self._contacts.accept(session.identity, user_id)
        self._deliver(session)
        return view

    def reject(self, user_id: UserId) -> None:
        """Reject a pending request (it is deleted; nothing is sent)."""
        self._require_session()
        self._contacts.reject(user_id)

    def remove_contact(self, user_id: UserId) -> bool:
        """Delete a contact in any state; return whether it existed."""
        self._require_session()
        return self._contacts.remove(user_id)

    # conversations

    def send(self, to: UserId, body: str) -> OutgoingMessage:
        """Send a text to an active contact; return it with its delivery state.

        The message is queued durably first, then a delivery round runs at
        once, so with the receiver online it usually comes back DELIVERED, and
        otherwise ACCEPTED (in the mailboxes) or still QUEUED (to be retried).

        Raises:
            UnknownContactError, ContactNotActiveError: see ``send_text``.
            ValueError: invalid body (empty or too large).
        """
        session = self._require_session()
        queued = self._sender.send_text(session.identity, to, body)
        self._deliver(session)
        stored = self._store.get_outgoing(queued.message_id)
        return stored if stored is not None else queued

    def history(self, contact: UserId, limit: int = 50) -> list[HistoryEntry]:
        """The conversation with ``contact``: texts from both sides, oldest first.

        * Received texts are in the contact's own order (``seq``), so a message
          that arrived late is shown in its place.
        * A ``seq`` that never arrived, between two that did, is shown as a
          MISSING entry ("waiting for this message"). Contact and receipt
          messages share the seq space, so they are used for this and then
          hidden.
        * The two sides are interleaved by Lamport timestamp, so a reply is
          always after what it answers, even if the wall clocks disagree.
          Ties (two messages written at the same time, without seeing each
          other) go by sender id. Both participants therefore see the same
          order.

        ``limit`` bounds how many messages of EACH side are read.

        Raises:
            ValueError: ``limit`` is less than 1.
        """
        session = self._require_session()
        if not is_int(limit) or limit < 1:
            raise ValueError("limit must be a positive integer")
        me = session.identity.user_id
        conversation = ConversationId.between(me, contact)
        received = _received_entries(
            self._store.list_received(conversation, limit), contact, limit
        )
        sent = [
            HistoryEntry(
                EntryKind.SENT,
                m.seq,
                m.sent_at_ms,
                m.lamport,
                m.body,
                status=m.status,
                message_id=m.message_id,
            )
            for m in self._store.list_outgoing(conversation_id=conversation)[-limit:]
            if m.kind is MessageKind.TEXT
        ]
        # Sorted rather than merged: nothing assumes a peer's timestamps grow.
        ordered = sorted(
            [(e, contact) for e in received] + [(e, me) for e in sent],
            key=lambda pair: (pair[0].lamport, pair[1].value, pair[0].seq),
        )
        return [entry for entry, _ in ordered]

    # network

    def tick(self) -> TickReport:
        """Do the periodic work of a connected client (see the module docstring).

        Raises:
            NotLoggedInError: nobody is logged in.
            StoreError: the local store failed; nothing unsafe was confirmed.
        """
        session = self._require_session()
        refreshed = self._refresh_membership_if_due()
        attempt = session.directory.renew_if_due()
        collected = (
            self._collect(session)
            if self._due("collect", self._policy.collect_interval_ms)
            else None
        )
        gaps = tuple(self._receiver.expire_gaps(session.identity))
        delivery = session.delivery.deliver_due()
        report = TickReport(
            membership_refreshed=refreshed,
            presence_published=None if attempt is None else attempt.accepted,
            collected=collected,
            gaps=gaps,
            delivery=delivery,
        )
        if report.stored or _changes_status(delivery):
            self._changed()
        _log_tick(report)
        return report

    def receive_direct(self, envelope: Envelope) -> ReceiveResult:
        """Receive an envelope a peer sent straight to us.

        Raises:
            NotLoggedInError: nobody is logged in.
        """
        session = self._require_session()
        result = self._receiver.receive(session.identity, envelope)
        if result.released:
            self._changed()
        return result

    # internals

    def _start(self, identity: UnlockedIdentity) -> Profile:
        """Open a session for ``identity``: listen, resume ordering, announce presence."""
        self.logout()
        peers = self._peers_for(identity.user_id)
        directory = DirectoryClient(
            identity, self._quorum, self._crypto, self._clock, self._presence_policy
        )
        delivery = DeliveryService(
            self._store,
            peers,
            self._quorum,
            directory,
            self._clock,
            self._delivery_policy,
            self._rng,
        )
        self._session = _Session(identity, peers, directory, delivery)
        self._last_run_ms.clear()  # first tick refreshes and collects at once
        peers.start_listening(self._on_direct)
        self._receiver.resume(identity)
        self._refresh_membership_if_due()
        directory.renew_if_due()
        log.info("session.started", user=identity.user_id)
        return self._profile(identity)

    def _on_direct(self, envelope: Envelope) -> None:
        """Listener for the peer transport: raising means "not delivered"."""
        if not self.receive_direct(envelope).acknowledge:
            raise EnvelopeNotAcceptedError("envelope not settled")

    def _deliver(self, session: _Session) -> DeliveryReport:
        self._refresh_membership_if_due()
        report = session.delivery.deliver_due()
        if _changes_status(report):
            self._changed()
        return report

    def _collect(self, session: _Session) -> BatchResult:
        identity = session.identity
        request = self._mailbox_request(
            identity, MailboxAction.COLLECT, limit=self._policy.collect_limit
        )
        envelopes = self._quorum.collect(request).envelopes
        batch = self._receiver.receive_batch(identity, envelopes)
        ids = batch.to_acknowledge
        for start in range(0, len(ids), MAX_ACK_IDS):
            self._quorum.acknowledge(
                self._mailbox_request(
                    identity,
                    MailboxAction.ACKNOWLEDGE,
                    message_ids=ids[start : start + MAX_ACK_IDS],
                )
            )
        return batch

    def _mailbox_request(
        self,
        identity: UnlockedIdentity,
        action: MailboxAction,
        *,
        limit: int = 0,
        message_ids: Sequence[MessageId] = (),
    ) -> MailboxRequest:
        return MailboxRequest.build(
            action=action,
            public_key=identity.public_key,
            issued_at_ms=self._clock.now_ms(),
            limit=limit,
            message_ids=message_ids,
            sign=lambda data: self._crypto.sign(identity.private_key, data),
        )

    def _refresh_membership_if_due(self) -> bool:
        if not self._due("membership", self._policy.membership_refresh_ms):
            return False
        self._quorum.refresh_membership()
        return True

    def _due(self, task: str, interval_ms: int) -> bool:
        """True (and mark it as run) if ``task`` has not run in the last ``interval_ms``."""
        now = self._clock.monotonic_ms()
        last = self._last_run_ms.get(task)
        if last is not None and now - last < interval_ms:
            return False
        self._last_run_ms[task] = now
        return True

    def _require_session(self) -> _Session:
        if self._session is None:
            raise NotLoggedInError("log in first")
        return self._session

    def _profile(self, identity: UnlockedIdentity) -> Profile:
        return Profile(
            user_id=identity.user_id,
            alias=identity.alias,
            fingerprint=self._contacts.my_fingerprint(identity),
            link=self._contacts.my_link(identity),
        )


def _changes_status(report: DeliveryReport) -> bool:
    """True if some sent message changed state in this delivery round."""
    return any(outcome in _STATUS_CHANGES for _, outcome in report.outcomes)


def _received_entries(
    messages: Sequence[ReceivedMessage], contact: UserId, limit: int
) -> list[HistoryEntry]:
    """Received texts in seq order, with a MISSING entry for every seq that never came.

    If fewer than ``limit`` messages came back, the whole conversation was
    read, so the gaps are counted from seq 1; otherwise only between the
    oldest and newest message read.
    """
    ordered = sorted((m for m in messages if m.sender == contact), key=lambda m: m.seq)
    if not ordered:
        return []
    entries: list[HistoryEntry] = []
    expected = 1 if len(messages) < limit else ordered[0].seq
    for message in ordered:
        for missing in range(expected, message.seq):
            entries.append(
                HistoryEntry(
                    EntryKind.MISSING, missing, message.sent_at_ms, message.lamport
                )
            )
        expected = message.seq + 1
        if message.kind is MessageKind.TEXT:
            entries.append(
                HistoryEntry(
                    EntryKind.RECEIVED,
                    message.seq,
                    message.sent_at_ms,
                    message.lamport,
                    message.body,
                    message_id=message.message_id,
                )
            )
    return entries


def _log_tick(report: TickReport) -> None:
    """A summary of one tick, only when it did something."""
    collected = 0 if report.collected is None else len(report.collected.results)
    delivery = report.delivery
    delivered = delivery.count(DeliveryOutcome.DELIVERED_DIRECT) + delivery.count(
        DeliveryOutcome.ACCEPTED_BY_MAILBOX
    )
    retried = delivery.count(DeliveryOutcome.RETRY_SCHEDULED)
    stored = len(report.stored)
    if collected or stored or report.gaps or delivered or retried:
        log.debug(
            "client.tick",
            collected=collected,
            stored=stored,
            gaps=len(report.gaps),
            delivered=delivered,
            retried=retried,
        )
