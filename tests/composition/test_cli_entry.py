"""The command line of ``python -m source.composition.cli`` (without Ray)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from source.adapters.system.sqlite.sqlite_local_store import SqliteLocalStore
from source.composition.cli import DEFAULT_RAY_ADDRESS, parse_args
from source.composition.client import ClientSettings, KdfStrength, make_store
from source.composition.launcher import DEFAULT_CONFIG, load_config
from source.domain.node_id import NodeId

REPOSITORY = Path(__file__).parents[2]


def write_config(folder: Path, **values: object) -> Path:
    """A launcher configuration file in ``folder``; ``values`` override the defaults."""
    config = {"nodes": ["node-1", "node-2", "node-3"], "seeds": ["node-1"]}
    config.update(values)
    path = folder / "cluster.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def test_the_default_configuration_is_the_repositorys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(REPOSITORY)
    monkeypatch.delenv("P08_LOG_DIR", raising=False)
    options = parse_args(["--device", "alice"])
    config = load_config(DEFAULT_CONFIG)
    assert options.device == "alice"
    assert options.seeds == config.seeds
    assert options.namespace == config.namespace
    assert options.address == DEFAULT_RAY_ADDRESS
    assert options.data_dir == config.devices_dir().resolve()
    assert options.log_dir == REPOSITORY / "logs"
    assert options.kdf is KdfStrength.SECURE


def test_everything_about_the_deployment_comes_from_the_configuration(
    tmp_path: Path,
) -> None:
    path = write_config(
        tmp_path,
        namespace="demo",
        seeds=["node-3", "node-1"],
        data_dir="state",
        log_dir="trace",
        ray_address="10.0.0.5:6379",
    )
    options = parse_args(["--device", "bob", "--config", str(path), "--fast-kdf"])
    assert options.seeds == (NodeId("node-3"), NodeId("node-1"))
    assert (options.namespace, options.address) == ("demo", "10.0.0.5:6379")
    assert options.data_dir == tmp_path / "state" / "devices"
    assert options.log_dir == tmp_path / "trace"
    assert options.kdf is KdfStrength.FAST


def test_a_device_has_one_store_whoever_starts_it(tmp_path: Path) -> None:
    """The launcher and the interface put device ``alice`` in the same file,
    so it is the same account whichever of them turns it on."""
    path = write_config(tmp_path, data_dir="state")
    from_launcher = load_config(path).devices_dir()
    from_interface = parse_args(["--device", "alice", "--config", str(path)]).data_dir

    assert from_interface == from_launcher.resolve()
    store = make_store(ClientSettings(data_dir=from_interface), "alice")
    assert isinstance(store, SqliteLocalStore)
    with store:
        assert store.path == tmp_path / "state" / "devices" / "alice.db"


def test_the_log_folder_comes_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("P08_LOG_DIR", str(tmp_path / "env-logs"))
    path = write_config(tmp_path)
    options = parse_args(["--device", "alice", "--config", str(path)])
    assert options.log_dir == tmp_path / "env-logs"


@pytest.mark.parametrize(
    "argv",
    [
        [],  # --device is required
        ["--device", "Alice"],  # not a device name
        ["--device", "alice", "--config", "no/such/file.json"],
        ["--device", "alice", "--seeds", "node-1"],  # comes from the configuration
    ],
)
def test_a_wrong_command_line_says_what_is_wrong(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        parse_args(argv)
    assert exit_info.value.code == 2
    assert "error" in capsys.readouterr().err


def test_a_broken_configuration_is_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = write_config(tmp_path, nodes=[])
    with pytest.raises(SystemExit):
        parse_args(["--device", "alice", "--config", str(path)])
    assert "--config" in capsys.readouterr().err
