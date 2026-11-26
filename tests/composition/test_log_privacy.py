"""Privacy of the logs.

A whole session runs at the most detailed level (DEBUG): accounts, a contact
request, an accepted contact, messages both ways, a message that waits in the
mailboxes while its receiver is offline. Then the log file is searched for
every secret that went through the system. None may appear. Every event added
to the code is covered by this test.
"""

from __future__ import annotations

import random
from pathlib import Path

from source.adapters.memory.clock import ManualClock
from source.adapters.memory.crypto import FakeCrypto
from source.adapters.memory.local_store import MemoryLocalStore
from source.adapters.memory.node_gateway import InMemoryNodeGateway
from source.adapters.memory.peer_transport import InMemoryPeerNetwork
from source.application.observability import log_context, short
from source.application.use_cases.client_app import ClientApp
from source.composition.logging_setup import configure_logging
from source.domain.node_id import NodeId

SECRET_PHRASE = "la clave de la caja fuerte es 4512"
LATE_PHRASE = "nos vemos a las siete en el parque"
PASSWORD = "contraseña-ultra-secreta"
ALIASES = ("Alice Secreta", "Bob Secreto")


def test_no_secret_ever_reaches_the_log(tmp_path: Path) -> None:
    path = configure_logging(process="client-test", log_dir=tmp_path, level="DEBUG")
    clock = ManualClock()
    crypto = FakeCrypto()
    gateway = InMemoryNodeGateway(
        [NodeId(f"node-{i}") for i in (1, 2, 3)], clock, crypto
    )
    network = InMemoryPeerNetwork()

    def device() -> ClientApp:
        return ClientApp(
            crypto=crypto,
            store=MemoryLocalStore(),
            clock=clock,
            gateway=gateway,
            peers_for=network.transport_for,
            rng=random.Random(0),
        )

    alice, bob = device(), device()
    a = alice.register(ALIASES[0], PASSWORD)
    b = bob.register(ALIASES[1], PASSWORD)
    with log_context(client=short(a.user_id.value)):
        alice.add_contact(b.link)
    with log_context(client=short(b.user_id.value)):
        bob.accept(a.user_id)
        bob.send(a.user_id, SECRET_PHRASE)
        bob.logout()
    with log_context(client=short(a.user_id.value)):
        alice.send(b.user_id, LATE_PHRASE)
    bob.login(PASSWORD)
    with log_context(client=short(b.user_id.value)):
        clock.advance(60_000)
        bob.tick()

    log = path.read_text(encoding="utf-8")
    secrets = [
        SECRET_PHRASE,
        LATE_PHRASE,
        PASSWORD,
        *ALIASES,
        a.link,
        b.link,
        a.fingerprint,
        b.fingerprint,
        a.user_id.value,
        b.user_id.value,
    ]
    assert log  # the session was logged
    for secret in secrets:
        assert secret not in log, f"a secret reached the log: {secret[:12]}..."
