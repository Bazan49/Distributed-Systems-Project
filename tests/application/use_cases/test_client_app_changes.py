"""ClientApp's change listeners: called when a conversation may look different.

An interface waits for them instead of polling, so they must come when
something changed, and NOT for work that changed nothing.
"""

from __future__ import annotations

import pytest

from source.application.use_cases.client_app import ClientApp
from source.domain.outgoing_status import OutgoingStatus
from tests.application.use_cases.test_client_app import NODES, PASSWORD, World, befriend


class Counter:
    def __init__(self, app: ClientApp) -> None:
        self.calls = 0
        app.add_change_listener(self)

    def __call__(self) -> None:
        self.calls += 1


@pytest.fixture
def world() -> World:
    return World()


def test_a_direct_message_tells_both_sides(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    on_alice, on_bob = Counter(alice), Counter(bob)

    sent = alice.send(bob.profile().user_id, "hola")

    assert sent.status is OutgoingStatus.DELIVERED
    assert on_bob.calls == 1  # stored in his history
    assert on_alice.calls == 1  # her message changed state


def test_idle_work_tells_nobody(world: World) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    on_alice = Counter(alice)
    world.pass_time(120_000, alice, bob)  # many ticks: presence, collects...
    assert on_alice.calls == 0


def test_a_message_collected_from_the_mailboxes_tells_the_receiver(
    world: World,
) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    bob_id = bob.profile().user_id
    bob.logout()
    alice.send(bob_id, "te dejo esto")
    on_bob = Counter(bob)

    bob.login(PASSWORD)
    bob.tick()  # the first tick collects

    assert on_bob.calls == 1


def test_a_message_that_stays_queued_tells_nobody_until_it_is_accepted(
    world: World,
) -> None:
    alice, bob = world.user("alice"), world.user("bob")
    befriend(alice, bob)
    bob_id = bob.profile().user_id
    bob.logout()
    for node in NODES:
        world.gateway.set_down(node)
    on_alice = Counter(alice)

    queued = alice.send(bob_id, "¿estás?")
    assert queued.status is OutgoingStatus.QUEUED
    assert on_alice.calls == 0  # retry scheduled: nothing to show yet

    for node in NODES:
        world.gateway.set_up(node)
    world.pass_time(60_000, alice)
    assert on_alice.calls == 1  # now in the mailboxes
