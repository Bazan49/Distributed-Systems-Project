"""In-memory fake of direct client-to-client delivery.

One ``InMemoryPeerNetwork`` plays "the network"; each client gets its own
transport from ``network.transport_for(user)``.
"""

from collections.abc import Callable

from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.records import DirectSendResult

Handler = Callable[[Envelope], None]


class InMemoryPeerNetwork:
    def __init__(self) -> None:
        self._listeners: dict[UserId, Handler] = {}

    def transport_for(self, user: UserId) -> "InMemoryPeerTransport":
        return InMemoryPeerTransport(self, user)

    def _register(self, user: UserId, handler: Handler) -> None:
        self._listeners[user] = handler

    def _unregister(self, user: UserId) -> None:
        self._listeners.pop(user, None)

    def _listener(self, user: UserId) -> Handler | None:
        return self._listeners.get(user)


class InMemoryPeerTransport:
    """The transport of ONE client (``user``)."""

    def __init__(self, network: InMemoryPeerNetwork, user: UserId) -> None:
        self._network = network
        self._user = user

    def send_direct(
        self, receiver: UserId, envelope: Envelope, timeout: float
    ) -> DirectSendResult:
        handler = self._network._listener(receiver)
        if handler is None:
            return DirectSendResult.UNREACHABLE  # offline or unknown
        try:
            handler(envelope)
        except Exception:
            # A failing peer is a normal outcome, not an error: the caller falls back to the mailbox.
            return DirectSendResult.UNREACHABLE
        return DirectSendResult.DELIVERED

    def start_listening(self, on_envelope: Handler) -> None:
        self._network._register(self._user, on_envelope)

    def stop_listening(self) -> None:
        self._network._unregister(self._user)
