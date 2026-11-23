"""Send-message use case: turn what the user typed into a sealed envelope in the outbox.

Sending is split in two on purpose:

1. This use case builds the message and makes it durable: it
   numbers it, encrypts it for the receiver, signs it, and stores it in the
   local outbox (``LocalStorePort.enqueue_outgoing``) with status ``QUEUED``.
   It does no network I/O, so it succeeds even when the user is offline.
2. The delivery policy later takes queued messages from the
   outbox and tries the direct path, then the mailboxes, with retries.

Because the envelope is final once queued, every retry resends exactly the
same bytes with the same ``MessageId``, which is what lets the receiver and
the mailboxes suppress duplicates.

Order of the protections (encrypt-then-sign):

* the content (kind, conversation, seq, time, body) is encrypted for the
  receiver's public key, so only the receiver can read it, and the ``seq``
  that orders the conversation is hidden from the mailboxes;
* the envelope (version, id, sender, receiver, ciphertext) is signed with the
  sender's private key, so anyone can check who sent it and that nothing was
  changed, without being able to read it.
"""

from __future__ import annotations

from source.application.ports.clock import ClockPort
from source.application.ports.crypto import CryptoPort
from source.application.ports.local_store import LocalStorePort
from source.application.use_cases.identity import UnlockedIdentity
from source.domain.contacts import Contact, ContactState
from source.domain.envelope import Envelope, MessageContent, MessageKind
from source.domain.identifiers import ConversationId, MessageId, UserId
from source.domain.messages import OutgoingMessage
from source.domain.outgoing_status import OutgoingStatus

__all__ = [
    "ContactNotActiveError",
    "SendError",
    "SendMessageService",
    "UnknownContactError",
]


# errors


class SendError(Exception):
    """Base class of the errors raised by the send-message use case."""


class UnknownContactError(SendError):
    """The receiver is not among the local user's contacts."""


class ContactNotActiveError(SendError):
    """The receiver is a contact, but the contact request is not accepted yet."""


# use case


class SendMessageService:
    """Build, encrypt, sign and queue outgoing messages.

    Not thread-safe: the ``seq`` of a message is "last used + 1", so two sends
    to the same conversation must not run at the same time. The client
    (one process per device) serialises them; if it did not, the store would
    refuse the second one (``AlreadyExistsError``) rather than reuse a seq.
    """

    def __init__(
        self, crypto: CryptoPort, store: LocalStorePort, clock: ClockPort
    ) -> None:
        self._crypto = crypto
        self._store = store
        self._clock = clock

    def send_text(
        self, session: UnlockedIdentity, receiver: UserId, body: str
    ) -> OutgoingMessage:
        """Queue a text message for ``receiver``; return it as stored.

        The receiver must be an ACTIVE contact: chatting needs a contact
        request accepted by both sides, and the contact holds the public key
        the message is encrypted for.

        Raises:
            UnknownContactError: ``receiver`` is not a contact.
            ContactNotActiveError: the contact request is not accepted yet.
            ValueError: ``receiver`` is the local user, or ``body`` is empty
                or longer than ``MAX_BODY_BYTES`` once encoded.
        """
        if session.user_id == receiver:
            raise ValueError("cannot send a message to yourself")
        contact = self._store.get_contact(receiver)
        if contact is None:
            raise UnknownContactError(f"{receiver} is not a contact")
        if contact.state is not ContactState.ACTIVE:
            raise ContactNotActiveError(
                f"the contact request with {receiver} is not accepted yet"
            )
        return self.queue(session, contact, MessageKind.TEXT, body)

    def queue(
        self,
        session: UnlockedIdentity,
        contact: Contact,
        kind: MessageKind,
        body: str,
    ) -> OutgoingMessage:
        """Build, encrypt, sign and queue a message of any ``kind`` for ``contact``.

        The building block behind ``send_text``. Prefer ``send_text`` for user messages.

        Every message kind shares the conversation's ``seq`` space, so the
        receiver's ordering sees contact messages and text in the order they
        were sent.

        Raises:
            ValueError: the contact is the local user, ``body`` is invalid
                for ``kind``, or the contact's public key is malformed.
            AlreadyExistsError: the store refused the message because its
                ``seq`` was taken meanwhile (concurrent sends).
        """
        if not isinstance(session, UnlockedIdentity):
            raise TypeError("session must be an UnlockedIdentity")
        if not isinstance(contact, Contact):
            raise TypeError("contact must be a Contact")
        sender = session.user_id
        receiver = contact.user_id
        if sender == receiver:
            raise ValueError("cannot send a message to yourself")

        conversation_id = ConversationId.between(sender, receiver)
        seq = self._store.last_outgoing_seq(conversation_id) + 1
        sent_at_ms = self._clock.now_ms()
        # Validates kind and body (non-empty text, size limit) before any
        # cryptography runs.
        content = MessageContent(kind, conversation_id, seq, sent_at_ms, body)

        ciphertext = self._crypto.encrypt_for(contact.public_key, content.to_bytes())
        envelope = Envelope.build(
            message_id=MessageId.new(),
            sender=sender,
            receiver=receiver,
            ciphertext=ciphertext,
            sign=lambda data: self._crypto.sign(session.private_key, data),
        )
        message = OutgoingMessage(
            envelope=envelope,
            conversation_id=conversation_id,
            seq=seq,
            kind=kind,
            body=body,
            sent_at_ms=sent_at_ms,
            status=OutgoingStatus.QUEUED,
            attempts=0,
            next_attempt_at_ms=None,  # due at once: the delivery policy picks it up
        )
        self._store.enqueue_outgoing(message)
        return message
