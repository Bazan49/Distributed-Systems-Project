"""The screens of the command-line interface.

Flow::

    start ──► no account on this device ──► create account ──┐
         └──► account, logged out ────────► log in ──────────┤
                                                              ▼
                                                         main menu
        1 conversations   (choose an active contact, then chat)
        2 add a contact   (paste their contact link)
        3 pending requests (accept or reject each one)
        4 my contacts     (every contact and its state; remove one)
        5 my profile      (id, fingerprint and my contact link to share)
        6 log out         (back to the log-in screen)
        0 exit

Nothing is refreshed on a timer:

* a conversation is redrawn when something in it changes (a message arrives,
  or one of mine changes state). A background thread waits in the client
  (``wait_for_change``, it blocks until the client stores something) and
  wakes the console only if the conversation on screen is different;
* the main menu (and its count of pending requests) and the requests screen
  read the latest state each time they are entered.

Every call to the client goes through ``_attempt``: the errors a user can
cause (wrong password, invalid link...) become a message and the screen goes
on; a failure of the local store is also logged (``cli.store_failed``);
anything else is a bug and is not hidden.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from datetime import tzinfo
from typing import TypeVar

from source.adapters.cli.client_api import ClientApi, ClientUnavailableError
from source.adapters.cli.console import Console, Wakeup
from source.adapters.cli.render import (
    ADD_OUTCOME,
    STATUS_LABEL,
    contact_line,
    describe_error,
    header,
    history_lines,
    look_alike_warning,
    profile_lines,
    short_id,
)
from source.application.observability import get_logger
from source.application.ports.errors import StoreError
from source.application.use_cases.client_app import HistoryEntry
from source.application.use_cases.contacts import ContactView
from source.domain.contacts import ContactState
from source.domain.identifiers import UserId

__all__ = ["Cli"]

log = get_logger(__name__)
T = TypeVar("T")

INVALID_ALIAS = "El alias no es válido: de 1 a 64 caracteres, sin espacios al principio ni al final."
INVALID_BODY = "El mensaje está vacío o es demasiado largo."


class _Failed:
    """What ``_attempt`` returns when the call failed (the user was told why)."""


_FAILED = _Failed()

# How long one wait in the client lasts before it is renewed. Only a bound
# for the blocked call: nothing is redrawn when it ends without a change.
CHANGE_WAIT_S = 20.0


class _ChangeWatcher:
    """One background thread that waits for the client's changes.

    While a screen is watched (``watch``), each change runs its ``check``
    (in this thread); if it says the screen is out of date, the console is
    woken up to redraw it. One thread for the whole session, so at most one
    call waits in the client at any time.
    """

    def __init__(self, client: ClientApi, wakeup: Wakeup) -> None:
        self._client = client
        self._wakeup = wakeup
        self._check: Callable[[], bool] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def watch(self, check: Callable[[], bool]) -> None:
        """Watch a screen. Call it BEFORE drawing the screen: no change is missed."""
        self._check = check
        if self._thread is None:
            start = self._client.wait_for_change(-1, 0)  # the count right now
            self._thread = threading.Thread(
                target=self._run, args=(start,), daemon=True
            )
            self._thread.start()

    def unwatch(self) -> None:
        self._check = None

    def stop(self) -> None:
        self._check = None
        self._stop.set()

    def _run(self, version: int) -> None:
        try:
            while not self._stop.is_set():
                latest = self._client.wait_for_change(version, CHANGE_WAIT_S)
                if latest == version:
                    continue  # the wait ended without changes: nothing to do
                version = latest
                check = self._check
                if check is None:
                    continue
                try:
                    changed = check()
                except ClientUnavailableError:
                    raise
                except Exception:  # let the screen redraw and show the error
                    changed = True
                if changed:
                    self._wakeup.set()
        except Exception:
            # The client stopped answering (or the program is ending): the
            # screen will find out on its next call to the client.
            return


class Cli:
    """The interface of one device.

    Args:
        client: the device's client.
        console: where to read and write.
        device: the device's name, shown in the titles.
        tz: time zone of the times shown (None = the computer's).
        history_limit: messages of each side shown in a conversation.
    """

    def __init__(
        self,
        client: ClientApi,
        console: Console,
        *,
        device: str = "",
        tz: tzinfo | None = None,
        history_limit: int = 50,
    ) -> None:
        self._client = client
        self._io = console
        self._device = device
        self._tz = tz
        self._history_limit = history_limit
        self._wakeup = Wakeup()
        self._watcher = _ChangeWatcher(client, self._wakeup)
        self._shown: list[HistoryEntry] | None = None  # the conversation on screen

    def run(self) -> None:
        """Run until the user exits (option 0, Enter at the log-in, Ctrl+D or Ctrl+C)."""
        try:
            while True:
                if not self._client.logged_in() and not self._enter():
                    break
                if not self._main_menu():
                    break
        except (EOFError, KeyboardInterrupt):
            self._io.show()
        except ClientUnavailableError:
            self._io.show("El cliente de este dispositivo dejó de responder.")
        finally:
            self._watcher.stop()
        self._io.show("Hasta luego.")

    # ---- calls to the client ----
    def _attempt(
        self,
        screen: str,
        action: Callable[..., T],
        *args: object,
        invalid_value: str | None = None,
    ) -> T | _Failed:
        """Run ``action(*args)``; on an error the user can understand, say it and go on."""
        try:
            return action(*args)
        except ClientUnavailableError:
            raise
        except Exception as exc:
            message = describe_error(exc, invalid_value)
            if message is None:
                raise  # a bug: do not hide it
            if isinstance(exc, StoreError):
                log.exception("cli.store_failed", screen=screen)
            self._io.show(f"  ✗ {message}")
            return _FAILED

    # ---- account ----
    def _enter(self) -> bool:
        """Create the account or log in; False if the user wants to exit."""
        where = f" · dispositivo {self._device}" if self._device else ""
        self._io.show(header(f"Mensajería segura{where}"))
        if self._client.is_registered():
            return self._login()
        return self._register()

    def _register(self) -> bool:
        self._io.show("Este dispositivo todavía no tiene cuenta. Vamos a crear una.")
        while True:
            alias = self._io.ask("Tu alias (Enter vacío para salir): ").strip()
            if not alias:
                return False
            password = self._io.ask("Contraseña: ")

            profile = self._attempt(
                "register",
                self._client.register,
                alias,
                password,
                invalid_value=INVALID_ALIAS,
            )
            if isinstance(profile, _Failed):
                continue
            self._io.show(f"\n¡Cuenta creada! Hola, {profile.alias.value}.")
            for line in profile_lines(profile):
                self._io.show(line)
            return True

    def _login(self) -> bool:
        while True:
            password = self._io.secret("Contraseña (Enter vacío para salir): ")
            if not password:
                return False
            profile = self._attempt("login", self._client.login, password)
            if not isinstance(profile, _Failed):
                self._io.show(f"Hola de nuevo, {profile.alias.value}.")
                return True

    # ---- main menu ----
    def _main_menu(self) -> bool:
        """Until the user logs out (True) or exits (False)."""
        actions: dict[str, Callable[[], None]] = {
            "1": self._conversations,
            "2": self._add_contact,
            "3": self._requests,
            "4": self._contacts,
            "5": self._profile,
        }
        while True:
            if not self._render_main_menu():
                return False
            choice = self._io.ask("Elige una opción: ").strip()
            if choice == "0":
                return False
            if choice == "6":
                self._client.logout()
                self._io.show("Sesión cerrada: ya no recibes mensajes directos.")
                return True
            action = actions.get(choice)
            if action is None:
                self._io.show("  ✗ Opción no válida.")
            else:
                action()

    def _render_main_menu(self) -> bool:
        self._client.tick()
        profile = self._attempt("menu", self._client.profile)
        contacts = self._attempt("menu", self._client.contacts)
        if isinstance(profile, _Failed) or isinstance(contacts, _Failed):
            return False
        active = sum(v.contact.state is ContactState.ACTIVE for v in contacts)
        pending = sum(
            v.contact.state is ContactState.INCOMING_REQUEST for v in contacts
        )
        self._io.show(header(f"{profile.alias.value} ({short_id(profile.user_id)})"))
        self._io.show(f"Contactos: {active} · Solicitudes pendientes: {pending}")
        self._io.show(" 1. Conversaciones")
        self._io.show(" 2. Agregar contacto")
        self._io.show(f" 3. Solicitudes pendientes ({pending})")
        self._io.show(" 4. Mis contactos")
        self._io.show(" 5. Mi perfil y mi enlace")
        self._io.show(" 6. Cerrar sesión")
        self._io.show(" 0. Salir")
        return True

    def _choose(
        self,
        views: Sequence[ContactView],
        prompt: str,
        *,
        line: Callable[[int, ContactView], str] = contact_line,
    ) -> ContactView | None:
        for number, view in enumerate(views, 1):
            self._io.show(line(number, view))
        answer = self._io.ask(prompt).strip()
        if not answer:
            return None
        if answer.isdigit() and 1 <= int(answer) <= len(views):
            return views[int(answer) - 1]
        self._io.show("  ✗ Número no válido.")
        return None

    # ---- 1. conversations ----
    def _conversations(self) -> None:
        self._io.show(header("Conversaciones"))
        active = self._attempt(
            "conversations", self._client.contacts, ContactState.ACTIVE
        )
        if isinstance(active, _Failed):
            return
        if not active:
            self._io.show("Aún no tienes contactos activos: agrega uno (opción 2).")
            return
        chosen = self._choose(
            active,
            "¿Con quién? (número, Enter para volver): ",
            line=lambda number, view: f"{number:>3}. {view.contact.alias.value}",
        )
        if chosen is not None:
            self._chat(chosen)

    def _chat(self, view: ContactView) -> None:
        contact = view.contact
        self._watcher.watch(lambda: self._chat_changed(view))
        self._wakeup.clear()  # a wakeup left by another screen is not news here
        self._render_chat(view)
        try:
            while True:
                text = self._io.ask_or_escape(
                    "> ",
                    wakeup=self._wakeup,
                    on_wakeup=lambda: self._render_chat(view),
                )
                if text is None:
                    return
                if not text.strip():
                    self._client.tick()
                    self._show_history(view)
                    continue
                sent = self._attempt(
                    "chat",
                    self._client.send,
                    contact.user_id,
                    text,
                    invalid_value=INVALID_BODY,
                )
                if not isinstance(sent, _Failed):
                    self._io.show(f"  Tú: {text}   {STATUS_LABEL[sent.status]}")
        finally:
            self._watcher.unwatch()
            self._shown = None

    def _chat_changed(self, view: ContactView) -> bool:
        """True if the conversation is no longer the one on screen (watcher thread)."""
        latest = self._client.history(view.contact.user_id, self._history_limit)
        return latest != self._shown

    def _render_chat(self, view: ContactView) -> None:
        alias = view.contact.alias.value
        self._io.show(header(f"Conversación con {alias}"))
        for line in look_alike_warning(view):
            self._io.show(line)
        self._io.show(
            "Los mensajes nuevos aparecen solos · Enter vacío: actualizar · "
            "Esc: volver al menú"
        )
        self._show_history(view)

    def _show_history(self, view: ContactView) -> None:
        entries = self._attempt(
            "chat", self._client.history, view.contact.user_id, self._history_limit
        )
        if isinstance(entries, _Failed):
            return
        self._shown = entries
        self._io.show("─" * 40)
        for line in history_lines(entries, view.contact.alias.value, self._tz):
            self._io.show(line)
        self._io.show("─" * 40)

    # ---- 2. add a contact ----
    def _add_contact(self) -> None:
        self._io.show(header("Agregar contacto"))
        self._io.show("Pega el enlace de contacto de la otra persona.")
        link = self._io.ask("Enlace (Enter vacío para volver): ").strip()
        if not link:
            return
        result = self._attempt("add_contact", self._client.add_contact, link)
        if isinstance(result, _Failed):
            return
        alias = result.view.contact.alias.value
        self._io.show(f"  ✓ {ADD_OUTCOME[result.outcome].format(alias=alias)}")
        self._io.show(
            f"  Huella de {alias}: {result.view.fingerprint}"
            " (compruébala con esa persona por otro medio)"
        )
        for line in look_alike_warning(result.view):
            self._io.show(line)

    # ---- 3. pending requests ----
    def _requests(self) -> None:
        self._client.tick()
        pending = self._attempt("requests", self._client.pending_requests)
        if isinstance(pending, _Failed):
            return
        if not pending:
            self._io.show(header("Solicitudes pendientes"))
            self._io.show("No tienes solicitudes pendientes.")
            return

        handled: set[UserId] = set()
        shown = {view.contact.user_id for view in pending}
        self._render_pending_requests(pending)

        while True:
            view = next(
                (view for view in pending if view.contact.user_id not in handled),
                None,
            )
            if view is None:
                return
            alias = view.contact.alias.value
            answer = (
                self._io.ask(
                    f"¿Qué hacer con {alias}? [a]ceptar · [r]echazar · "
                    "Enter: decidir más tarde: "
                )
                .strip()
                .lower()
            )
            user = view.contact.user_id
            handled.add(user)
            if answer == "a":
                accepted = self._attempt("requests", self._client.accept, user)
                if not isinstance(accepted, _Failed):
                    self._io.show(f"  ✓ {alias} y tú ya sois contactos.")
            elif answer == "r":
                rejected = self._attempt("requests", self._client.reject, user)
                if not isinstance(rejected, _Failed):
                    self._io.show("  ✓ Solicitud rechazada.")
            self._client.tick()
            latest = self._attempt("requests", self._client.pending_requests)
            if isinstance(latest, _Failed):
                return
            new_requests = [
                candidate
                for candidate in latest
                if candidate.contact.user_id not in shown
            ]
            if new_requests:
                self._io.show("Nuevas solicitudes:")
                for number, new_view in enumerate(new_requests, 1):
                    self._io.show(contact_line(number, new_view))
                    for line in look_alike_warning(new_view):
                        self._io.show(line)
                shown.update(view.contact.user_id for view in new_requests)
            pending = latest

    def _render_pending_requests(self, pending: Sequence[ContactView]) -> None:
        self._io.show(header("Solicitudes pendientes"))
        for number, view in enumerate(pending, 1):
            self._io.show(contact_line(number, view))
            for line in look_alike_warning(view):
                self._io.show(line)

    # ---- 4. my contacts ----
    def _contacts(self) -> None:
        self._io.show(header("Mis contactos"))
        views = self._attempt("contacts", self._client.contacts)
        if isinstance(views, _Failed):
            return
        if not views:
            self._io.show("Todavía no tienes contactos.")
            return
        chosen = self._choose(
            views,
            "Número de un contacto para eliminarlo (Enter para volver): ",
            line=lambda number, view: f"{number:>3}. {view.contact.alias.value}",
        )
        if chosen is None:
            return
        alias = chosen.contact.alias.value
        sure = self._io.ask(f"¿Seguro que quieres eliminar a {alias}? (s/n): ")
        if sure.strip().lower() != "s":
            return
        user = chosen.contact.user_id
        removed = self._attempt("contacts", self._client.remove_contact, user)
        if not isinstance(removed, _Failed):
            self._io.show(f"  ✓ {alias} eliminado de tus contactos.")

    # ---- 5. my profile ----
    def _profile(self) -> None:
        self._io.show(header("Mi perfil"))
        profile = self._attempt("profile", self._client.profile)
        if isinstance(profile, _Failed):
            return
        for line in profile_lines(profile, include_identity=False):
            self._io.show(line)
