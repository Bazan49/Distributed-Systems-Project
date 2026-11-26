"""The command-line interface: a person uses the client from a terminal.

A driving adapter: it turns what the user types into calls to the client and
shows the answers. It only knows the client through ``ClientApi``, so the
same screens work with:

* a ``ClientHost`` in this process (the tests, with in-memory adapters);
* the client actor of a Ray cluster, through ``RemoteClient`` (real use,
  started by ``python -m source.composition.cli``).

Modules:

* ``client_api``: the operations the interface needs from a client;
* ``console``: reading and writing the terminal (replaceable in tests);
* ``render``: how every piece of the app is shown, as plain text;
* ``app``: the screens and menus (``Cli``).

``RemoteClient`` (the client actor of a Ray cluster as a ``ClientApi``) lives
with the rest of the Ray code, in ``source/adapters/system/ray/remote_client.py``:
nothing in this package imports Ray.
"""
