"""The screens of the CLI, with a scripted user and real clients (no Ray).

Alice uses the interface; the other users act through their clients
directly, at the moment the script says (a function in the script).
"""

from __future__ import annotations

import logging
from datetime import timezone
from typing import Any

import pytest

from source.adapters.cli.app import Cli
from source.adapters.cli.client_api import ClientApi, ClientUnavailableError
from source.adapters.system.ray.ray_client import ClientHost
from source.application.ports.errors import StoreError
from source.domain.contacts import ContactState
from tests.adapters.cli.support import (
    NODES,
    PASSWORD,
    Arrives,
    NothingNew,
    ScriptedConsole,
    World,
    befriend,
)
from tests.support.logs import has_event


@pytest.fixture
def world() -> World:
    return World()


def run(client: ClientApi, *answers: Any) -> ScriptedConsole:
    console = ScriptedConsole(*answers)
    Cli(client, console, device="alice", tz=timezone.utc).run()
    assert console.unused == 0, "the script has more answers than questions"
    return console


def test_a_client_host_is_a_client_api(world: World) -> None:
    host: ClientApi = world.device()  # mypy checks the types
    assert isinstance(host, ClientApi)


# ---------------- account ----------------
def test_creating_an_account_shows_the_profile_and_the_link(world: World) -> None:
    alice = world.device()
    out = run(alice, "Alice", PASSWORD, "0")
    assert "¡Cuenta creada! Hola, Alice." in out.output
    assert alice.profile().link in out.lines
    assert alice.is_registered() and alice.logged_in()
    assert out.lines[-1] == "Hasta luego."


@pytest.mark.parametrize(
    ("alias", "password", "message"),
    [
        ("Alice", "corta", "al menos 8 caracteres"),
        ("A" * 65, PASSWORD, "El alias no es válido"),
    ],
)
def test_an_invalid_account_is_explained_and_asked_again(
    world: World, alias: str, password: str, message: str
) -> None:
    alice = world.device()
    out = run(alice, alias, password, "")  # "" = exit
    assert message in out.output
    assert not alice.is_registered()


def test_logging_in_after_a_wrong_password(world: World) -> None:
    alice = world.user("Alice")
    alice.logout()
    out = run(alice, "mala contraseña", PASSWORD, "0")
    assert "Contraseña incorrecta." in out.output
    assert "Hola de nuevo, Alice." in out.output
    assert alice.logged_in()


def test_logging_out_goes_back_to_the_log_in(world: World) -> None:
    alice = world.user("Alice")
    out = run(alice, "6", "")  # log out, then exit at the log-in
    assert "Sesión cerrada" in out.output
    assert not alice.logged_in()


def test_an_unknown_option_is_said_and_the_menu_comes_back(world: World) -> None:
    out = run(world.user("Alice"), "9", "0")
    assert "Opción no válida." in out.output
    assert out.prompts.count("Elige una opción: ") == 2


def test_ctrl_d_exits_cleanly(world: World) -> None:
    out = run(world.user("Alice"))  # no answers: the first question ends the input
    assert out.lines[-1] == "Hasta luego."


# ---------------- contacts ----------------
def test_adding_a_contact_then_chatting_both_ways(world: World) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")

    def bob_accepts() -> str:
        bob.accept(alice.profile().user_id)
        return "1"  # conversations

    def bob_answers() -> str:
        bob.send(alice.profile().user_id, "hola, Alice")
        return ""  # refresh

    out = run(
        alice,
        "2",
        bob.profile().link,
        bob_accepts,
        "1",  # Bob
        "hola, Bob",
        bob_answers,
        None,  # Esc
        "0",
    )
    assert "Solicitud enviada a Bob" in out.output
    assert "Esc: volver al menú" in out.output
    assert "Tú: hola, Bob   ✓✓ entregado" in out.output  # straight to Bob
    assert any(line.endswith("] Bob: hola, Alice") for line in out.lines)
    bob_history = bob.history(alice.profile().user_id)
    assert [e.body for e in bob_history] == ["hola, Bob", "hola, Alice"]


def test_a_message_to_an_offline_contact_waits_in_the_mailboxes(world: World) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")
    befriend(alice, bob)
    bob.logout()
    out = run(alice, "1", "1", "¿estás?", None, "0")
    assert "Tú: ¿estás?   ✓ en los buzones" in out.output


def test_an_invalid_link_or_my_own_link_is_explained(world: World) -> None:
    alice = world.user("Alice")
    out = run(alice, "2", "esto no es un enlace", "2", alice.profile().link, "0")
    assert "El enlace no es válido" in out.output
    assert "Ese es tu propio enlace." in out.output


def test_accepting_and_rejecting_pending_requests(world: World) -> None:
    alice, bob, carol = world.user("Alice"), world.user("Bob"), world.user("Carol")
    bob.add_contact(alice.profile().link)
    world.clock.advance(1_000)
    carol.add_contact(alice.profile().link)

    out = run(alice, "3", "a", "r", "0")  # oldest first: Bob, then Carol

    assert "Solicitudes pendientes (2)" in out.output
    assert "Bob y tú ya sois contactos." in out.output
    assert "Solicitud rechazada." in out.output
    [active] = alice.contacts(ContactState.ACTIVE)
    assert active.contact.alias.value == "Bob"
    assert alice.pending_requests() == []


def test_a_request_can_wait_for_later(world: World) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")
    bob.add_contact(alice.profile().link)
    run(alice, "3", "", "0")
    assert len(alice.pending_requests()) == 1


def test_a_look_alike_is_warned_about(world: World) -> None:
    alice = world.user("Alice")
    first, impostor = world.user("Bob"), world.user("Bob")  # same alias, other keys
    befriend(alice, first)
    impostor.add_contact(alice.profile().link)
    out = run(alice, "3", "", "0")
    assert "Otros contactos también se llaman «Bob»" in out.output


def test_removing_a_contact_asks_first(world: World) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")
    befriend(alice, bob)
    run(alice, "4", "1", "n", "0")  # changed my mind
    assert len(alice.contacts()) == 1
    out = run(alice, "4", "1", "s", "0")
    assert "Bob eliminado de tus contactos." in out.output
    assert "  1. Bob" in out.lines
    assert not any("[activo]" in line or "huella:" in line for line in out.lines)
    assert alice.contacts() == []


def test_without_contacts_the_screens_say_so(world: World) -> None:
    out = run(world.user("Alice"), "1", "3", "4", "0")
    assert "Aún no tienes contactos activos" in out.output
    assert "No tienes solicitudes pendientes." in out.output
    assert "Todavía no tienes contactos." in out.output


def test_choosing_a_number_that_does_not_exist(world: World) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")
    befriend(alice, bob)
    out = run(alice, "1", "7", "0")
    assert "Número no válido." in out.output


def test_the_profile_shows_the_link_to_share(world: World) -> None:
    alice = world.user("Alice")
    out = run(alice, "5", "0")
    profile = alice.profile()
    assert profile.link in out.lines
    assert f"Alias:  {profile.alias.value}" in out.lines
    assert f"Id:     {profile.user_id.value}" not in out.lines
    assert f"Huella: {profile.fingerprint}" not in out.lines


def test_a_message_too_long_is_explained(world: World) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")
    befriend(alice, bob)
    out = run(alice, "1", "1", "x" * 20_000, None, "0")
    assert "El mensaje está vacío o es demasiado largo." in out.output


# ---------------- failures ----------------
class Broken:
    """A client whose ``method`` raises ``error``; everything else is ``host``'s."""

    def __init__(self, host: ClientHost, method: str, error: Exception) -> None:
        self._host, self._method, self._error = host, method, error

    def __getattr__(self, name: str) -> Any:
        if name == self._method:

            def fail(*_args: object) -> Any:
                raise self._error

            return fail
        return getattr(self._host, name)


def test_a_store_failure_is_explained_and_logged(
    world: World, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="p08")
    client = Broken(world.user("Alice"), "pending_requests", StoreError("disk"))
    out = run(client, "3", "0")
    assert "No se pudo leer o guardar en este dispositivo." in out.output
    assert has_event(caplog, "cli.store_failed", screen="requests")


def test_an_unexpected_error_is_not_hidden(world: World) -> None:
    client = Broken(world.user("Alice"), "profile", RuntimeError("bug"))
    with pytest.raises(RuntimeError, match="bug"):
        Cli(client, ScriptedConsole("0")).run()


def test_a_client_that_stops_answering_closes_the_interface(world: World) -> None:
    client = Broken(world.user("Alice"), "tick", ClientUnavailableError("gone"))
    out = run(client)
    assert "dejó de responder" in out.output
    assert out.lines[-1] == "Hasta luego."


# ---------------- automatic updates (no polling) ----------------
def test_a_message_that_arrives_redraws_the_conversation_by_itself(
    world: World,
) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")
    befriend(alice, bob)
    alice_id = alice.profile().user_id
    out = run(
        alice,
        "1",
        "1",
        Arrives(lambda: bob.send(alice_id, "¿me lees?")),
        None,
        "0",
    )
    assert out.redraws == 1
    assert any(line.endswith("] Bob: ¿me lees?") for line in out.lines)


def test_nothing_is_redrawn_while_nothing_arrives(world: World) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")
    befriend(alice, bob)
    out = run(
        alice,
        "1",
        "1",
        NothingNew(lambda: (alice.tick(), bob.tick(), alice.tick())),  # idle work
        None,
        "0",
    )
    assert out.redraws == 0


def test_a_message_of_another_conversation_does_not_redraw_this_one(
    world: World,
) -> None:
    alice, bob, carol = world.user("Alice"), world.user("Bob"), world.user("Carol")
    befriend(alice, bob)
    world.clock.advance(1_000)
    befriend(alice, carol)
    alice_id = alice.profile().user_id
    out = run(
        alice,
        "1",
        "1",  # Bob (the oldest contact)
        NothingNew(lambda: carol.send(alice_id, "soy Carol")),
        None,
        "0",
    )
    assert out.redraws == 0
    assert "soy Carol" not in out.output


def test_my_message_is_redrawn_when_its_state_changes(world: World) -> None:
    alice, bob = world.user("Alice"), world.user("Bob")
    befriend(alice, bob)
    bob.logout()
    for node in NODES:
        world.gateway.set_down(node)

    def the_nodes_come_back() -> None:
        for node in NODES:
            world.gateway.set_up(node)
        world.clock.advance(60_000)  # past the retry backoff
        alice.tick()  # the retry: now the mailboxes accept it

    out = run(
        alice,
        "1",
        "1",
        "¿estás?",  # no node and Bob offline: it stays queued
        Arrives(the_nodes_come_back),
        None,
        "0",
    )
    assert "Tú: ¿estás?   · en cola" in out.output
    assert out.redraws == 1
    assert any(line.endswith("Tú: ¿estás?   ✓ en los buzones") for line in out.lines)


def test_the_requests_count_is_read_again_on_entering_the_menu(
    world: World,
) -> None:
    alice, carol = world.user("Alice"), world.user("Carol")

    def carol_asks() -> str:
        carol.add_contact(alice.profile().link)
        return "5"  # any screen; coming back to the menu reads it again

    out = run(alice, carol_asks, "0")
    assert out.output.count(" 3. Solicitudes pendientes (0)") == 1
    assert out.output.count(" 3. Solicitudes pendientes (1)") == 1
