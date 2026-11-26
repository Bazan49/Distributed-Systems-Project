"""Reading and writing the terminal.

The screens never call ``input`` or ``print`` themselves: they use a
``Console``, so the tests can play a user with a script.

Nothing is redrawn on a timer. While the user types in a conversation, the
console waits for TWO things at once: a key, or a ``Wakeup`` that another
thread sets when something new arrived. Only then it redraws the screen
(``on_wakeup``), keeping what the user was typing.
"""

from __future__ import annotations

import contextlib
import getpass
import os
import select
import sys
import threading
import time
from collections.abc import Callable
from typing import Protocol, cast

__all__ = ["Console", "TerminalConsole", "Wakeup"]


class Wakeup:
    """Lets another thread interrupt a console that waits for the keyboard.

    ``set`` may be called from any thread. On POSIX it also writes one byte
    to a pipe, so a ``select`` can wait for the keyboard and for the wakeup
    at the same time, with no timeout.
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self._read_fd = self._write_fd = -1
        if os.name != "nt":
            self._read_fd, self._write_fd = os.pipe()
            os.set_blocking(self._read_fd, False)
            os.set_blocking(self._write_fd, False)

    def fileno(self) -> int:
        """The descriptor to ``select`` on (POSIX only; -1 elsewhere)."""
        return self._read_fd

    def set(self) -> None:
        self._event.set()
        if self._write_fd >= 0:
            # BlockingIOError: a wakeup is already pending; OSError: closed.
            with contextlib.suppress(OSError):
                os.write(self._write_fd, b"!")

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout_s: float) -> bool:
        """Block until it is set (True) or ``timeout_s`` passes (False)."""
        return self._event.wait(timeout_s)

    def clear(self) -> None:
        self._event.clear()
        if self._read_fd >= 0:
            with contextlib.suppress(OSError):  # BlockingIOError: drained
                while os.read(self._read_fd, 64):
                    pass

    def close(self) -> None:
        for fd in (self._read_fd, self._write_fd):
            if fd >= 0:
                os.close(fd)
        self._read_fd = self._write_fd = -1


class Console(Protocol):
    def ask(self, prompt: str) -> str:
        """A line typed by the user, without the newline.

        Raises:
            EOFError: the input ended (Ctrl+D, or the end of a script).
        """
        ...

    def ask_or_escape(
        self,
        prompt: str,
        *,
        wakeup: Wakeup | None = None,
        on_wakeup: Callable[[], None] | None = None,
    ) -> str | None:
        """Read a line, or return None if the user presses Esc.

        While waiting, each time ``wakeup`` is set the screen is redrawn with
        ``on_wakeup`` and what was typed so far is kept.
        """
        ...

    def secret(self, prompt: str) -> str:
        """Like ``ask``, but what is typed is not shown (passwords)."""
        ...

    def show(self, text: str = "") -> None:
        """Write ``text`` and a newline."""
        ...


class TerminalConsole:
    """The real terminal."""

    def ask(self, prompt: str) -> str:
        return input(prompt)

    def ask_or_escape(
        self,
        prompt: str,
        *,
        wakeup: Wakeup | None = None,
        on_wakeup: Callable[[], None] | None = None,
    ) -> str | None:
        if not sys.stdin.isatty():
            answer = input(prompt)
            return None if answer == "\x1b" else answer
        return self._read_terminal(prompt, wakeup, on_wakeup, escape=True)

    @staticmethod
    def _redraw(prompt: str, chars: list[str], on_wakeup: Callable[[], None]) -> None:
        sys.stdout.write("\x1b[2J\x1b[H")
        on_wakeup()
        sys.stdout.write(prompt + "".join(chars))
        sys.stdout.flush()

    @classmethod
    def _read_terminal(
        cls,
        prompt: str,
        wakeup: Wakeup | None,
        on_wakeup: Callable[[], None] | None,
        *,
        escape: bool,
    ) -> str | None:
        if os.name == "nt":
            return cls._read_windows(prompt, wakeup, on_wakeup, escape=escape)
        return cls._read_posix(prompt, wakeup, on_wakeup, escape=escape)

    @staticmethod
    def _read_posix(
        prompt: str,
        wakeup: Wakeup | None,
        on_wakeup: Callable[[], None] | None,
        *,
        escape: bool,
    ) -> str | None:
        import codecs
        import termios
        import tty

        fd = sys.stdin.fileno()
        settings = termios.tcgetattr(fd)
        chars: list[str] = []
        decoder = codecs.getincrementaldecoder(sys.stdin.encoding or "utf-8")()
        watched = [fd] if wakeup is None else [fd, wakeup.fileno()]
        try:
            tty.setcbreak(fd)
            sys.stdout.write(prompt)
            sys.stdout.flush()
            while True:
                ready, _, _ = select.select(watched, [], [])  # no timeout
                if wakeup is not None and wakeup.fileno() in ready:
                    wakeup.clear()
                    if on_wakeup is not None:
                        TerminalConsole._redraw(prompt, chars, on_wakeup)
                if fd not in ready:
                    continue
                raw = os.read(fd, 1)
                if not raw:
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    raise EOFError
                char = decoder.decode(raw)
                if not char:
                    continue
                if char == "\x1b":
                    if escape:
                        sys.stdout.write("\n")
                        sys.stdout.flush()
                        return None
                    continue
                if char in ("\r", "\n"):
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    return "".join(chars)
                if char in ("\b", "\x7f"):
                    if chars:
                        chars.pop()
                        sys.stdout.write("\b \b")
                        sys.stdout.flush()
                    continue
                if char == "\x04":
                    if not chars:
                        sys.stdout.write("\n")
                        sys.stdout.flush()
                        raise EOFError
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    return "".join(chars)
                if char.isprintable() or char == "\t":
                    chars.append(char)
                    sys.stdout.write(char)
                    sys.stdout.flush()
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, settings)

    @staticmethod
    def _read_windows(
        prompt: str,
        wakeup: Wakeup | None,
        on_wakeup: Callable[[], None] | None,
        *,
        escape: bool,
    ) -> str | None:
        import msvcrt

        class _KeyboardInput(Protocol):
            def kbhit(self) -> bool: ...
            def getwch(self) -> str: ...

        keyboard = cast(_KeyboardInput, msvcrt)
        sys.stdout.write(prompt)
        sys.stdout.flush()
        chars: list[str] = []
        while True:
            # msvcrt cannot wait for a key and an event together: it checks
            # the keyboard every 50 ms, but redraws only after a wakeup.
            if wakeup is not None and wakeup.is_set():
                wakeup.clear()
                if on_wakeup is not None:
                    TerminalConsole._redraw(prompt, chars, on_wakeup)
            if not keyboard.kbhit():
                time.sleep(0.05)
                continue
            char = keyboard.getwch()
            if char == "\x1b":
                if escape:
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    return None
                continue
            if char in ("\r", "\n"):
                sys.stdout.write("\n")
                sys.stdout.flush()
                return "".join(chars)
            if char in ("\x00", "\xe0"):
                keyboard.getwch()
                continue
            if char in ("\b", "\x7f"):
                if chars:
                    chars.pop()
                    sys.stdout.write("\b \b")
                    sys.stdout.flush()
                continue
            if char == "\x03":
                raise KeyboardInterrupt
            if char == "\x04":
                if not chars:
                    raise EOFError
                sys.stdout.write("\n")
                sys.stdout.flush()
                return "".join(chars)
            if char.isprintable() or char == "\t":
                chars.append(char)
                sys.stdout.write(char)
                sys.stdout.flush()

    def secret(self, prompt: str) -> str:
        return getpass.getpass(prompt)

    def show(self, text: str = "") -> None:
        print(text, flush=True)
