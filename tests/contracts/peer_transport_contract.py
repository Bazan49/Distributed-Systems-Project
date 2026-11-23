"""Contract every PeerTransportPort implementation must satisfy.

``make_transport(user)`` returns the transport of one client; all transports
made by the same fixture share the same "network".
"""

from collections.abc import Callable

from source.application.ports import PeerTransportPort
from source.domain.envelope import Envelope
from source.domain.identifiers import UserId
from source.domain.records import DirectSendResult
from tests.contracts.mailbox_store_contract import ALICE, BOB, CAROL, make_envelope

TIMEOUT_MS = 1_000
TransportFactory = Callable[[UserId], PeerTransportPort]


class PeerTransportContract:
    """Subclass it and override the ``make_transport`` fixture."""

    def test_delivers_to_a_listening_peer(
        self, make_transport: TransportFactory
    ) -> None:
        received: list[Envelope] = []
        make_transport(BOB).start_listening(received.append)
        envelope = make_envelope(BOB)
        result = make_transport(ALICE).send_direct(BOB, envelope, TIMEOUT_MS)
        assert result is DirectSendResult.DELIVERED
        assert received == [envelope]

    def test_each_send_is_delivered_exactly_once(
        self, make_transport: TransportFactory
    ) -> None:
        received: list[Envelope] = []
        make_transport(BOB).start_listening(received.append)
        make_transport(ALICE).send_direct(BOB, make_envelope(BOB), TIMEOUT_MS)
        assert len(received) == 1

    def test_unreachable_when_the_peer_never_listened(
        self, make_transport: TransportFactory
    ) -> None:
        result = make_transport(ALICE).send_direct(BOB, make_envelope(BOB), TIMEOUT_MS)
        assert result is DirectSendResult.UNREACHABLE

    def test_unreachable_after_stop_listening(
        self, make_transport: TransportFactory
    ) -> None:
        received: list[Envelope] = []
        bob = make_transport(BOB)
        bob.start_listening(received.append)
        bob.stop_listening()
        result = make_transport(ALICE).send_direct(BOB, make_envelope(BOB), TIMEOUT_MS)
        assert result is DirectSendResult.UNREACHABLE
        assert received == []

    def test_can_listen_again_after_stopping(
        self, make_transport: TransportFactory
    ) -> None:
        received: list[Envelope] = []
        bob = make_transport(BOB)
        bob.start_listening(received.append)
        bob.stop_listening()
        bob.start_listening(received.append)
        result = make_transport(ALICE).send_direct(BOB, make_envelope(BOB), TIMEOUT_MS)
        assert result is DirectSendResult.DELIVERED

    def test_a_failing_receiver_means_unreachable_not_an_exception(
        self, make_transport: TransportFactory
    ) -> None:
        def broken(_envelope: Envelope) -> None:
            raise RuntimeError("receiver crashed")

        make_transport(BOB).start_listening(broken)
        result = make_transport(ALICE).send_direct(BOB, make_envelope(BOB), TIMEOUT_MS)
        assert result is DirectSendResult.UNREACHABLE

    def test_messages_go_only_to_their_receiver(
        self, make_transport: TransportFactory
    ) -> None:
        bob_got: list[Envelope] = []
        carol_got: list[Envelope] = []
        make_transport(BOB).start_listening(bob_got.append)
        make_transport(CAROL).start_listening(carol_got.append)
        make_transport(ALICE).send_direct(BOB, make_envelope(BOB), TIMEOUT_MS)
        assert len(bob_got) == 1 and carol_got == []
