"""Tests for tools/logview.py, the log reader and message tracer."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from tools import logview


def write(path: Path, *events: dict[str, object]) -> Path:
    path.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
    return path


def event(
    ts: str, name: str, level: str = "INFO", **fields: object
) -> dict[str, object]:
    return {"ts": f"2026-11-24T{ts}Z", "level": level, "event": name, **fields}


@pytest.fixture
def logs(tmp_path: Path) -> list[Path]:
    alice = write(
        tmp_path / "client-a.jsonl",
        event(
            "10:00:00.000", "message.queued", client="aaaa1111", msg="9c1e0000", seq=4
        ),
        event(
            "10:00:02.000",
            "delivery.mailbox_accepted",
            client="aaaa1111",
            msg="9c1e0000",
        ),
    )
    node = write(
        tmp_path / "node-1.jsonl",
        event("10:00:01.000", "mailbox.deposited", node="node-1", msg="9c1e0000"),
        event(
            "10:00:03.000",
            "mailbox.request_refused",
            "WARNING",
            node="node-1",
            reason="replay",
        ),
    )
    return [alice, node]


def run(capsys: pytest.CaptureFixture[str], *args: str) -> list[str]:
    assert logview.main(list(args)) == 0
    return capsys.readouterr().out.splitlines()


def test_files_are_merged_in_time_order(
    logs: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    lines = run(capsys, *map(str, logs))
    assert [line.split()[3] for line in lines] == [
        "message.queued",
        "mailbox.deposited",
        "delivery.mailbox_accepted",
        "mailbox.request_refused",
    ]
    assert lines[0].startswith("10:00:00.000 INFO aaaa1111")
    assert "seq=4" in lines[0]


def test_the_journey_of_one_message(
    logs: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    lines = run(capsys, *map(str, logs), "--msg", "9c1e")
    assert len(lines) == 3
    assert "node-1" in lines[1]


def test_filters_by_level_event_and_who(
    logs: list[Path], capsys: pytest.CaptureFixture[str]
) -> None:
    assert len(run(capsys, *map(str, logs), "--level", "WARNING")) == 1
    assert len(run(capsys, *map(str, logs), "--event", "mailbox.")) == 2
    assert len(run(capsys, *map(str, logs), "--who", "aaaa")) == 2


def test_it_reads_a_live_stream_from_stdin(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    stream = (
        json.dumps(event("10:00:00.000", "node.state_changed", node="node-2")) + "\n"
    )
    monkeypatch.setattr("sys.stdin", io.StringIO(stream + "not json\n"))

    lines = run(capsys)

    assert "node.state_changed" in lines[0]
    assert lines[1] == "not json"  # never hidden
