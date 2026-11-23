"""Contacts use case: share a link, request, accept or reject, and check fingerprints.

The handshake, between Alice (who adds) and Bob (who shares his link)::

    Bob    my_link()           -> prints his signed link (shared by any means)
    Alice  add_contact(link)   -> Bob is saved as OUTGOING_REQUEST and a
                                  CONTACT_REQUEST is queued whose body is
                                  ALICE's own link
    Bob    (receives it)       -> introduce(): Alice becomes INCOMING_REQUEST
           pending_requests()  -> the requests inbox, with fingerprints
           accept(alice)       -> Alice becomes ACTIVE; CONTACT_ACCEPT queued
           reject(alice)       -> Alice is deleted (she may ask again)
    Alice  (receives accept)   -> on_released(): Bob becomes ACTIVE

Why the request carries the requester's link: Bob has never seen Alice's key,
so he cannot verify her envelope with a key from his contacts. ``introduce``
decrypts the request (it is encrypted for Bob), reads Alice's link from it,
checks that the link is hers and properly signed, and only THEN verifies the
envelope's signature with that key. Nothing is stored before every check
passes.

Fingerprints: a short form of a public key that two people compare out of
band (in person, on a call). It is the only defence against someone who
swaps a link in transit: cryptography proves "this is the owner of this key",
only people can confirm "this key is Bob's".

Key changes: a ``UserId`` IS the hash of its key, so a contact's key can
never change silently; a different key is a different user. What can happen
is a NEW user with the SAME alias as an existing contact (Bob on a new
device, or someone impersonating him). Every view that shows a newcomer
lists those look-alikes, so the interface can warn before accepting.

Order of writes: the store cannot save a contact and queue a message in one
transaction, so each operation queues the message FIRST and saves the
contact AFTER. If the second write fails, repeating the operation is safe:
the message is simply queued again, and the receiver treats a repeated
request or acceptance as a no-op. The other order could leave a contact
saved whose message was never queued, and a retry would not resend it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from enum import Enum

from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.errors import DecryptionError
from source.application.ports.local_store import LocalStorePort
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.send_message import SendMessageService
from source.domain.canonical import FormatError
from source.domain.contact_link import ContactLink
from source.domain.contacts import Contact, ContactState
from source.domain.envelope import Envelope, MessageContent, MessageKind
from source.domain.identifiers import UserId
from source.domain.messages import ReceivedMessage

__all__ = [
    "MAX_PENDING_REQUESTS",
    "AddOutcome",
    "AddResult",
    "ContactError",
    "ContactView",
    "ContactsService",
    "InvalidLinkError",
    "NoPendingRequestError",
]

# Bounds the requests inbox: anyone holding your link can send requests, so
# a flood must not grow local storage without limit. Requests beyond it are
# not dropped: they wait in the mailboxes and are retried until they expire.
MAX_PENDING_REQUESTS = 50


# errors


class ContactError(Exception):
    """Base class of the errors raised by the contacts use case."""


class InvalidLinkError(ContactError, ValueError):
    """The text is not a valid, correctly signed contact link of another user."""


class NoPendingRequestError(ContactError):
    """There is no pending request from that user to accept or reject."""


# results


class AddOutcome(str, Enum):
    """What ``add_contact`` did."""

    REQUEST_SENT = "request_sent"
    """A contact request was queued; waiting for them to accept."""
    ALREADY_REQUESTED = "already_requested"
    """A request to them was already sent; nothing new was queued."""
    ACCEPTED = "accepted"
    """They had already asked us: adding them accepted their request."""
    ALREADY_CONTACT = "already_contact"
    """They were already an active contact; nothing changed."""


@dataclass(frozen=True)
class ContactView:
    """A contact as the interface shows it.

    ``look_alikes`` are OTHER contacts with the same alias (ignoring case).
    If it is not empty, the interface should warn: it may be the same person
    with a new identity, or someone impersonating them, and the fingerprint
    is how to tell.
    """

    contact: Contact
    fingerprint: str
    look_alikes: tuple[Contact, ...] = ()


@dataclass(frozen=True)
class AddResult:
    """Outcome of ``add_contact``: what happened, and to whom."""

    outcome: AddOutcome
    view: ContactView


# use case


class ContactsService:
    """Contact links, the request handshake and the requests inbox.

    It also implements the ``ContactGate`` that ``ReceiveMessagesService``
    calls: ``introduce`` for an unknown sender, ``on_released`` for contact
    messages once they are in order.
    """

    def __init__(
        self,
        crypto: CryptoPort,
        store: LocalStorePort,
        clock: ClockPort,
        sender: SendMessageService,
    ) -> None:
        self._crypto = crypto
        self._store = store
        self._clock = clock
        self._sender = sender

    # my identity

    def my_link(self, session: UnlockedIdentity) -> str:
        """The signed link that others use to add the local user."""
        return self._link_of(session).to_text()

    def my_fingerprint(self, session: UnlockedIdentity) -> str:
        """The local user's fingerprint, to read out when someone verifies it."""
        return self._crypto.fingerprint(session.public_key)

    # adding

    def add_contact(self, session: UnlockedIdentity, link_text: str) -> AddResult:
        """Add the owner of ``link_text`` as a contact.

        * a new user: saved as ``OUTGOING_REQUEST`` and a request is queued;
        * a user who already asked us: their request is accepted;
        * a request already sent, or an active contact: nothing changes.

        The link's alias becomes the contact's alias.

        Raises:
            InvalidLinkError: the text is not a contact link, its identifier
                does not match its key, its signature is invalid, or it is
                the local user's own link.
        """
        link = self._parse_link(link_text)
        if link.user_id == session.user_id:
            raise InvalidLinkError("this is your own contact link")

        existing = self._store.get_contact(link.user_id)
        if existing is not None:
            if existing.state is ContactState.INCOMING_REQUEST:
                return AddResult(AddOutcome.ACCEPTED, self._accept(session, existing))
            outcome = (
                AddOutcome.ALREADY_CONTACT
                if existing.state is ContactState.ACTIVE
                else AddOutcome.ALREADY_REQUESTED
            )
            return AddResult(outcome, self._view(existing))

        contact = Contact(
            user_id=link.user_id,
            public_key=link.public_key,
            alias=link.alias,
            state=ContactState.OUTGOING_REQUEST,
            created_at_ms=self._clock.now_ms(),
        )
        # Queue first, save after (see the module docstring).
        self._sender.queue(
            session, contact, MessageKind.CONTACT_REQUEST, self.my_link(session)
        )
        self._store.save_contact(contact)
        return AddResult(AddOutcome.REQUEST_SENT, self._view(contact))

    # the requests inbox

    def pending_requests(self) -> list[ContactView]:
        """Requests waiting for an answer, oldest first, with fingerprints and look-alikes."""
        pending = self._store.list_contacts(ContactState.INCOMING_REQUEST)
        pending.sort(key=lambda c: (c.created_at_ms, c.user_id))
        return [self._view(contact) for contact in pending]

    def accept(self, session: UnlockedIdentity, user_id: UserId) -> ContactView:
        """Accept the pending request of ``user_id``: they become an active contact.

        Raises:
            NoPendingRequestError: no pending request from ``user_id``.
        """
        return self._accept(session, self._pending(user_id))

    def reject(self, user_id: UserId) -> None:
        """Reject the pending request of ``user_id`` by deleting it.

        Nothing is sent to them, and they are not blocked: they may ask again.
        Messages already received from them stay in the history.

        Raises:
            NoPendingRequestError: no pending request from ``user_id``.
        """
        self._store.delete_contact(self._pending(user_id).user_id)

    # contacts

    def remove(self, user_id: UserId) -> bool:
        """Delete a contact in any state; return whether it existed.

        It cancels a request we sent, or ends a contact. Nothing is sent to
        them; their messages stay in the history, and anything new from them
        is treated as coming from a stranger. Adding them again later sends a
        fresh request.
        """
        return self._store.delete_contact(user_id)

    def view(self, user_id: UserId) -> ContactView | None:
        """A contact with its fingerprint and look-alikes, or None if unknown."""
        contact = self._store.get_contact(user_id)
        return None if contact is None else self._view(contact)

    def contacts(self, state: ContactState | None = None) -> list[ContactView]:
        """Every contact (or those in ``state``), sorted by alias, then id."""
        found = self._store.list_contacts(state)
        found.sort(key=lambda c: (c.alias.value.casefold(), c.user_id))
        return [self._view(contact) for contact in found]

    # ContactGate (called by ReceiveMessagesService)

    def introduce(
        self, session: UnlockedIdentity, envelope: Envelope
    ) -> Contact | None:
        """Turn the unknown sender of a valid contact request into a pending request.

        Returns the new ``INCOMING_REQUEST`` contact, or None if the envelope
        is not an authentic contact request, or the inbox is full. Never
        raises for anything a peer can send.

        This is the ONLY place that decrypts before verifying: the key that
        verifies the envelope travels inside it. It is safe because the
        content is parsed strictly, the key must hash to the sender's id,
        both signatures are checked, and nothing is stored before that.
        """
        if envelope.receiver != session.user_id:
            return None
        if len(self._store.list_contacts(ContactState.INCOMING_REQUEST)) >= (
            MAX_PENDING_REQUESTS
        ):
            return None
        try:
            content = MessageContent.from_bytes(
                self._crypto.decrypt(session.private_key, envelope.ciphertext)
            )
            envelope.ensure_content_matches(content)
            if content.kind is not MessageKind.CONTACT_REQUEST:
                return None
            link = ContactLink.from_text(content.body)
        except (DecryptionError, FormatError):
            return None
        if link.user_id != envelope.sender:
            return None
        if not self._link_is_authentic(link):
            return None
        if not self._crypto.verify(
            link.public_key, envelope.signing_bytes(), envelope.signature
        ):
            return None
        contact = Contact(
            user_id=link.user_id,
            public_key=link.public_key,
            alias=link.alias,
            state=ContactState.INCOMING_REQUEST,
            created_at_ms=self._clock.now_ms(),
        )
        self._store.save_contact(contact)
        return contact

    def on_released(
        self, session: UnlockedIdentity, messages: Sequence[ReceivedMessage]
    ) -> None:
        """React to contact messages, in the order the senders wrote them.

        Every transition is idempotent, because a message may be handled
        again if storing it fails afterwards:

        ==================  ================  ==================================
        message             our state         effect
        ==================  ================  ==================================
        CONTACT_ACCEPT      OUTGOING_REQUEST  they accepted: ACTIVE
        CONTACT_REQUEST     OUTGOING_REQUEST  both asked at once: ACTIVE
        CONTACT_REQUEST     ACTIVE            they lost us: accept again
        anything else       any               nothing
        ==================  ================  ==================================
        """
        for message in messages:
            contact = self._store.get_contact(message.sender)
            if contact is None:
                continue
            if contact.state is ContactState.OUTGOING_REQUEST and message.kind in (
                MessageKind.CONTACT_ACCEPT,
                MessageKind.CONTACT_REQUEST,
            ):
                self._store.save_contact(replace(contact, state=ContactState.ACTIVE))
            elif (
                contact.state is ContactState.ACTIVE
                and message.kind is MessageKind.CONTACT_REQUEST
            ):
                self._sender.queue(session, contact, MessageKind.CONTACT_ACCEPT, "")

    # internals

    def _accept(self, session: UnlockedIdentity, contact: Contact) -> ContactView:
        # Queue first, save after.
        self._sender.queue(session, contact, MessageKind.CONTACT_ACCEPT, "")
        active = replace(contact, state=ContactState.ACTIVE)
        self._store.save_contact(active)
        return self._view(active)

    def _pending(self, user_id: UserId) -> Contact:
        contact = self._store.get_contact(user_id)
        if contact is None or contact.state is not ContactState.INCOMING_REQUEST:
            raise NoPendingRequestError(f"no pending request from {user_id}")
        return contact

    def _link_of(self, session: UnlockedIdentity) -> ContactLink:
        return ContactLink.build(
            public_key=session.public_key,
            alias=session.alias,
            sign=lambda data: self._crypto.sign(session.private_key, data),
        )

    def _parse_link(self, text: str) -> ContactLink:
        try:
            link = ContactLink.from_text(text)
        except FormatError as exc:
            raise InvalidLinkError(f"invalid contact link: {exc}") from exc
        if not self._link_is_authentic(link):
            raise InvalidLinkError("the contact link's signature is not valid")
        return link

    def _link_is_authentic(self, link: ContactLink) -> bool:
        return self._crypto.verify(
            link.public_key, link.signing_bytes(), link.signature
        )

    def _view(self, contact: Contact) -> ContactView:
        alias = contact.alias.value.casefold()
        look_alikes = tuple(
            other
            for other in self._store.list_contacts()
            if other.user_id != contact.user_id
            and other.alias.value.casefold() == alias
        )
        return ContactView(
            contact=contact,
            fingerprint=self._crypto.fingerprint(contact.public_key),
            look_alikes=look_alikes,
        )
