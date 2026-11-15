from collections.abc import Callable
from typing import Protocol, runtime_checkable

from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.records import DirectSendResult


@runtime_checkable
class PeerTransportPort(Protocol):
    """Direct client-to-client delivery (used when both users are online).

    Phase 1 adapter: Ray actor to actor. Phase 2: our own connection. A failed
    direct send is a normal outcome, not an error: the delivery policy falls
    back to the mailbox.
    """

    def send_direct(
        self, receiver: UserId, envelope: Envelope, timeout: float
    ) -> DirectSendResult:
        """Try to hand ``envelope`` straight to ``receiver``.

        Returns ``DELIVERED`` only if the peer received it; any failure
        (offline, slow, transport error) is ``UNREACHABLE``. Never raises for those.
        """
        ...

    def start_listening(self, on_envelope: Callable[[Envelope], None]) -> None:
        """Start receiving direct envelopes; call ``on_envelope`` for each one."""
        ...

    def stop_listening(self) -> None:
        """Stop receiving. The user then looks offline to direct senders."""
        ...
