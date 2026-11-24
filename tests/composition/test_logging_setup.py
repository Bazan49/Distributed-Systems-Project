"""Tests for configure_logging: where the log goes, and that it can be read live."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from source.application.observability import get_logger, log_context
from source.composition.logging_setup import (
    DIR_ENV_VAR,
    LEVEL_ENV_VAR,
    configure_logging,
)


def read(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_it_writes_json_lines_to_log_dir_slash_process(tmp_path: Path) -> None:
    path = configure_logging(process="demo", log_dir=tmp_path / "logs")

    assert path == tmp_path / "logs" / "demo.jsonl"
    first = read(path)[0]
    assert first["event"] == "logging.configured"
    assert first["process"] == "demo"
    assert isinstance(first["run"], str) and len(first["run"]) == 8


def test_each_event_is_on_disk_at_once_so_it_can_be_followed_live(
    tmp_path: Path,
) -> None:
    """``tail -f`` must see an event as soon as it happens, without closing."""
    path = configure_logging(process="demo", log_dir=tmp_path)

    get_logger("x").info("message.queued", seq=1)

    assert read(path)[-1]["event"] == "message.queued"


def test_the_context_reaches_the_file(tmp_path: Path) -> None:
    path = configure_logging(process="node-2", log_dir=tmp_path, node="node-2")

    with log_context(client="3fa2c1d0"):
        get_logger("x").info("anything")

    last = read(path)[-1]
    assert (last["process"], last["node"], last["client"]) == (
        "node-2",
        "node-2",
        "3fa2c1d0",
    )


def test_runs_append_and_are_told_apart_by_their_run_id(tmp_path: Path) -> None:
    path = configure_logging(process="demo", log_dir=tmp_path)
    configure_logging(process="demo", log_dir=tmp_path)

    runs = [line["run"] for line in read(path) if line["event"] == "logging.configured"]
    assert len(runs) == 2 and runs[0] != runs[1]


def test_configuring_again_does_not_duplicate_lines(tmp_path: Path) -> None:
    configure_logging(process="demo", log_dir=tmp_path)
    path = configure_logging(process="demo", log_dir=tmp_path)

    get_logger("x").info("once")

    assert [line["event"] for line in read(path)].count("once") == 1


def test_the_level_filters_events(tmp_path: Path) -> None:
    path = configure_logging(process="demo", log_dir=tmp_path, level="WARNING")
    log = get_logger("x")

    log.info("hidden")
    log.warning("shown")

    assert [line["event"] for line in read(path)] == ["shown"]


def test_the_level_can_come_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(LEVEL_ENV_VAR, "debug")
    configure_logging(process="demo", log_dir=tmp_path)
    assert logging.getLogger().level == logging.DEBUG


def test_noisy_libraries_are_quieted(tmp_path: Path) -> None:
    configure_logging(process="demo", log_dir=tmp_path, level="DEBUG")
    assert logging.getLogger("ray").level == logging.WARNING


def test_nothing_goes_to_the_console(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(process="demo", log_dir=tmp_path)
    get_logger("x").warning("not.on.screen")

    captured = capsys.readouterr()
    assert "not.on.screen" not in captured.out + captured.err


@pytest.mark.parametrize("process", ["", "Demo", "a b", "../x", "-x"])
def test_the_process_name_must_be_safe_as_a_file_name(
    tmp_path: Path, process: str
) -> None:
    with pytest.raises(ValueError):
        configure_logging(process=process, log_dir=tmp_path)


def test_an_unknown_level_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        configure_logging(process="demo", log_dir=tmp_path, level="LOUD")


def test_the_folder_can_come_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """How a Ray launcher gives every actor the same absolute folder."""
    monkeypatch.setenv(DIR_ENV_VAR, str(tmp_path / "shared"))
    path = configure_logging(process="node-1")
    assert path == tmp_path / "shared" / "node-1.jsonl"
