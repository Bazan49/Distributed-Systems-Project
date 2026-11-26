"""Delivery receipts: how a sender learns that a message left in a mailbox arrived.

A message sent DIRECTLY is confirmed by the call itself: the receiver's device
answers that it stored it, and the sender marks it DELIVERED at once. A
message left in the MAILBOXES is only ACCEPTED: the receiver may collect it
days later, and the sender is not there to see it. So the receiver writes
back:

* ``send_receipts`` (receiver side): after collecting from the mailboxes, it
  queues one RECEIPT per conversation with the ids of the text messages it
  just stored (or durably buffered). A receipt is an ordinary message:
  encrypted, signed, ordered and retried like any other, so the nodes cannot
  read it, fake it or tell it apart from a text.
* ``on_released`` (sender side): when a RECEIPT is stored, the messages it
  names go to DELIVERED. It runs in the same step that stores the receipt
  (``ReceiveMessagesService`` calls it before committing), and applying a
  receipt twice changes nothing, so a crash cannot lose or double it.

Only the receiver of a message can confirm it: a receipt naming a message
sent to someone else, or in another conversation, is ignored. Receipts only
confirm text messages and are never themselves confirmed, so two clients can
never answer each other's receipts forever.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import replace

from source.application.observability import get_logger
from source.application.ports.local_store import LocalStorePort
from source.application.use_cases.identity import UnlockedIdentity
from source.application.use_cases.receive_messages import ReceiveResult, ReceiveStatus
from source.application.use_cases.send_message import SendMessageService
from source.domain.envelope import (
    MAX_RECEIPT_IDS,
    MessageContent,
    MessageKind,
    receipt_body,
)
from source.domain.identifiers import MessageId, UserId
from source.domain.messages import OutgoingMessage, ReceivedMessage
from source.domain.outgoing_status import OutgoingStatus, advance

__all__ = ["ReceiptsService"]

log = get_logger(__name__)

# Statuses of a received envelope that mean "it is on this device now".
_STORED = frozenset(
    {ReceiveStatus.DELIVERED, ReceiveStatus.BUFFERED, ReceiveStatus.LATE}
)


class ReceiptsService:
    """Send receipts for collected messages, and apply the receipts that arrive."""

    def __init__(self, store: LocalStorePort, sender: SendMessageService) -> None:
        self._store = store
        self._sender = sender

    # receiver side
    def send_receipts(
        self, session: UnlockedIdentity, results: Iterable[ReceiveResult]
    ) -> list[OutgoingMessage]:
        """Queue receipts for the text messages among ``results``; return them.

        Call it with what one mailbox collect received, BEFORE acknowledging
        to the nodes. One receipt per sender (that is, per conversation),
        split if it names more than ``MAX_RECEIPT_IDS`` messages. Senders that
        are no longer contacts get none.
        """
        confirmed: dict[UserId, list[MessageId]] = {}
        for result in results:
            if (
                result.status in _STORED
                and result.kind is MessageKind.TEXT
                and result.sender is not None
            ):
                confirmed.setdefault(result.sender, []).append(result.message_id)

        queued: list[OutgoingMessage] = []
        for peer, ids in confirmed.items():
            contact = self._store.get_contact(peer)
            if contact is None:
                continue
            for start in range(0, len(ids), MAX_RECEIPT_IDS):
                chunk = ids[start : start + MAX_RECEIPT_IDS]
                receipt = self._sender.queue(
                    session, contact, MessageKind.RECEIPT, receipt_body(chunk)
                )
                queued.append(receipt)
                log.debug(
                    "receipt.queued",
                    msg=receipt.message_id,
                    peer=peer,
                    count=len(chunk),
                )
        return queued

    # sender side
    def on_released(
        self, session: UnlockedIdentity, messages: Sequence[ReceivedMessage]
    ) -> None:
        """Mark DELIVERED the messages confirmed by the receipts among ``messages``.

        Called with messages about to be stored, in order. Idempotent.
        """
        for message in messages:
            if message.kind is MessageKind.RECEIPT:
                self._apply(message)

    def _apply(self, receipt: ReceivedMessage) -> None:
        content = MessageContent(
            receipt.kind,
            receipt.conversation_id,
            receipt.seq,
            receipt.sent_at_ms,
            receipt.body,
            receipt.lamport,
        )
        confirmed = 0
        for message_id in content.receipt_ids():
            message = self._store.get_outgoing(message_id)
            if (
                message is None
                or message.receiver != receipt.sender
                or message.conversation_id != receipt.conversation_id
                or message.kind is not MessageKind.TEXT
            ):
                continue  # not ours to confirm by this peer: ignored
            status = advance(message.status, OutgoingStatus.DELIVERED)
            if status is not message.status:
                self._store.update_outgoing(replace(message, status=status))
                confirmed += 1
        if confirmed:
            log.info(
                "receipt.applied",
                msg=receipt.message_id,
                peer=receipt.sender,
                count=confirmed,
            )
