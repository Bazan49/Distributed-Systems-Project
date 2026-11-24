"""Configure logging for one process: JSON Lines to ``<log_dir>/<process>.jsonl``.

Call ``configure_logging`` ONCE, at the start of every process:

* a client actor, when it builds its client::

      configure_logging(process=f"client-{device}", device=device)

* a Ray node actor, in its ``__init__`` (each actor is its own process, and
  the configuration of the driver does NOT reach it)::

      configure_logging(process=f"node-{node_id}", node=str(node_id))

Nothing is written to the console: the console belongs to the users. Each
record is written and flushed at once, so the file can be followed live
(``tail -f logs/client-alice.jsonl``). Runs append to the same file; each run gets
its own ``run`` id so they can be told apart.

The folder comes from the argument, else from ``P08_LOG_DIR``, else
``logs`` in the current directory. The level comes from the argument, else
from ``P08_LOG_LEVEL``, else ``INFO``. The environment variables exist for
Ray: a launcher sets them once (``runtime_env={"env_vars": {...}}``) and
every actor process picks them up, with an ABSOLUTE folder shared by all.
"""

from __future__ import annotations

import logging
import os
import re
import secrets
from pathlib import Path

from source.adapters.system.json_logs import JsonLinesFormatter
from source.application.observability import get_logger, set_process_context

__all__ = [
    "DEFAULT_LOG_DIR",
    "DIR_ENV_VAR",
    "LEVELS",
    "LEVEL_ENV_VAR",
    "configure_logging",
]

DEFAULT_LOG_DIR = Path("logs")
LEVEL_ENV_VAR = "P08_LOG_LEVEL"
DIR_ENV_VAR = "P08_LOG_DIR"
LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

# Third-party loggers that would otherwise flood the file.
_QUIET = ("ray", "asyncio", "urllib3", "grpc")
_PROCESS_NAME = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")

log = get_logger(__name__)


def configure_logging(
    *,
    process: str,
    log_dir: Path | None = None,
    level: str | None = None,
    **context: object,
) -> Path:
    """Send every log record of this process to ``<log_dir>/<process>.jsonl``.

    Replaces any handler configured before, so calling it again (for example
    in a restarted Ray actor) does not duplicate lines.

    Args:
        process: names the file and the ``process`` field (``node-2``,
            ``client-alice``...).
        log_dir: folder of the log files; created if missing.
            Defaults to ``P08_LOG_DIR``, else ``logs``.
        level: ``DEBUG``, ``INFO``, ``WARNING`` or ``ERROR``.
        context: extra fields for every event of the process, e.g.
            ``node="node-2"``.

    Returns:
        The path of the log file.

    Raises:
        ValueError: invalid ``process`` name or ``level``.
    """
    if not _PROCESS_NAME.fullmatch(process):
        raise ValueError("process must be lowercase letters, digits, '.', '-' or '_'")
    chosen = (level or os.environ.get(LEVEL_ENV_VAR) or "INFO").upper()
    if chosen not in LEVELS:
        raise ValueError(f"level must be one of {', '.join(LEVELS)}")

    folder = log_dir or Path(os.environ.get(DIR_ENV_VAR) or DEFAULT_LOG_DIR)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{process}.jsonl"
    handler = logging.FileHandler(path, mode="a", encoding="utf-8")
    handler.setFormatter(JsonLinesFormatter())

    root = logging.getLogger()
    for old in root.handlers[:]:
        root.removeHandler(old)
        old.close()
    root.addHandler(handler)
    root.setLevel(chosen)
    for name in _QUIET:
        logging.getLogger(name).setLevel(logging.WARNING)
    logging.captureWarnings(True)

    set_process_context(process=process, run=secrets.token_hex(4), **context)
    log.info("logging.configured", log_level=chosen, file=str(path))
    return path
