"""The command line of ``python -m source.composition.cli`` (without Ray)."""

from __future__ import annotations

from pathlib import Path

import pytest

from source.composition.cli import DEFAULT_DATA_DIR, parse_args
from source.composition.client import KdfStrength
from source.domain.node_id import NodeId


def test_the_defaults_join_the_local_cluster(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("P08_LOG_DIR", raising=False)
    options = parse_args(["--device", "alice"])
    assert options.device == "alice"
    assert options.seeds == (NodeId("node-1"), NodeId("node-2"))
    assert options.namespace == "mensajeria"
    assert options.address == "auto"
    assert options.data_dir == tmp_path / DEFAULT_DATA_DIR  # absolute
    assert options.log_dir == tmp_path / "logs"
    assert options.kdf is KdfStrength.SECURE


def test_every_option_can_be_given(tmp_path: Path) -> None:
    options = parse_args(
        [
            "--device",
            "bob",
            "--seeds",
            "node-3, node-1",
            "--namespace",
            "demo",
            "--address",
            "10.0.0.5:6379",
            "--data-dir",
            str(tmp_path / "d"),
            "--log-dir",
            str(tmp_path / "l"),
            "--fast-kdf",
        ]
    )
    assert options.seeds == (NodeId("node-3"), NodeId("node-1"))
    assert (options.namespace, options.address) == ("demo", "10.0.0.5:6379")
    assert options.data_dir == tmp_path / "d" and options.log_dir == tmp_path / "l"
    assert options.kdf is KdfStrength.FAST


def test_the_log_folder_comes_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("P08_LOG_DIR", str(tmp_path / "env-logs"))
    assert parse_args(["--device", "alice"]).log_dir == tmp_path / "env-logs"


@pytest.mark.parametrize(
    "argv",
    [
        [],  # --device is required
        ["--device", "Alice"],  # not a device name
        ["--device", "alice", "--seeds", "Not A Node"],
        ["--device", "alice", "--seeds", " , "],
    ],
)
def test_a_wrong_command_line_says_what_is_wrong(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        parse_args(argv)
    assert exit_info.value.code == 2
    assert "error" in capsys.readouterr().err
