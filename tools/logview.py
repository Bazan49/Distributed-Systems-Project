"""Read the JSON Lines logs as short, aligned lines; follow one message across processes.

    # live, while a client runs (in another terminal)
    tail -f logs/client-alice.jsonl | python tools/logview.py

    # every process merged in time order (nodes and clients)
    python tools/logview.py logs/*.jsonl

    # the journey of one message through every process
    python tools/logview.py logs/*.jsonl --msg 9c1e04b7

    # only warnings and errors, or only some events
    python tools/logview.py logs/*.jsonl --level WARNING
    python tools/logview.py logs/*.jsonl --event delivery. --event receive.

Lines that are not valid JSON are shown as they are, so nothing is hidden.
Standard library only; see docs/logging.md.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path

LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
# Shown in fixed columns, or not at all.
_HIDDEN = {"ts", "level", "logger", "event", "pid", "process", "run", "client", "node"}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read p08 JSON Lines logs.")
    parser.add_argument(
        "files", nargs="*", type=Path, help="log files (default: stdin)"
    )
    parser.add_argument(
        "--msg", help="only events of this message id (a prefix is enough)"
    )
    parser.add_argument(
        "--event",
        action="append",
        default=[],
        help="only events starting with this (repeatable)",
    )
    parser.add_argument(
        "--level", default="DEBUG", choices=list(LEVELS), help="minimum level"
    )
    parser.add_argument(
        "--who", help="only events of this client id or node (a prefix is enough)"
    )
    return parser.parse_args(argv)


def read(lines: Iterable[str]) -> Iterator[dict[str, object] | str]:
    for raw in lines:
        text = raw.strip()
        if not text:
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            yield text
            continue
        yield parsed if isinstance(parsed, dict) else text


def keep(event: dict[str, object], args: argparse.Namespace) -> bool:
    if LEVELS.get(str(event.get("level")), 0) < LEVELS[args.level]:
        return False
    if args.msg and not str(event.get("msg", "")).startswith(args.msg):
        return False
    if args.event and not any(
        str(event.get("event", "")).startswith(e) for e in args.event
    ):
        return False
    return not (args.who and not _who(event, "").startswith(args.who))


def render(event: dict[str, object]) -> str:
    time = str(event.get("ts", ""))[11:23]
    level = str(event.get("level", ""))[:4]
    who = _who(event, "-")
    fields = " ".join(
        f"{k}={_value(v)}"
        for k, v in event.items()
        if k not in _HIDDEN or (k == "node" and str(v) != who)  # a node it talks about
    )
    return f"{time} {level:<4} {who:<10} {event.get('event', '')}  {fields}".rstrip()


def _who(event: dict[str, object], default: str) -> str:
    """Who emitted the event: the client of the call, else the process.

    ``node`` comes last: it is also a FIELD of events about a node (a client
    logs ``node.state_changed node=node-2``), so it does not always say who
    is writing; ``process`` does.
    """
    who = event.get("client") or event.get("process") or event.get("node")
    return str(who or default)


def _value(value: object) -> str:
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    if isinstance(value, str) and " " in value:
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.files:
        events: list[dict[str, object] | str] = []
        for path in args.files:
            events.extend(read(path.read_text(encoding="utf-8").splitlines()))
        parsed = sorted(
            (e for e in events if isinstance(e, dict)),
            key=lambda e: str(e.get("ts", "")),
        )
        stream: Iterable[dict[str, object] | str] = [
            *(e for e in events if isinstance(e, str)),
            *parsed,
        ]
    else:
        stream = read(sys.stdin)  # follows ``tail -f`` line by line
    for item in stream:
        if isinstance(item, str):
            print(item, flush=True)
        elif keep(item, args):
            print(render(item), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
