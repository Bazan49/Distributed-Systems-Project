"""The launcher's configuration file, without Ray."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from source.adapters.system.ray.ray_client import device_actor_name
from source.composition.launcher import (
    DEFAULT_CLIENT_FACTORY,
    DEFAULT_NAMESPACE,
    ConfigError,
    LaunchError,
    client_actor_name,
    load_config,
    main,
    parse_config,
    resolve_factory,
    start_ray_device,
)
from source.domain.node_id import NodeId

BASE = Path("/srv/cluster")


def config(**overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {"nodes": ["node-1", "node-2", "node-3"]}
    raw.update(overrides)
    return raw


def test_a_minimal_configuration_gets_the_defaults() -> None:
    cfg = parse_config(config(), BASE)
    assert cfg.nodes == (NodeId("node-1"), NodeId("node-2"), NodeId("node-3"))
    assert cfg.seeds == cfg.nodes  # every node is an initial node
    assert cfg.clients == ()
    assert cfg.namespace == DEFAULT_NAMESPACE
    assert cfg.data_dir == BASE / "data"
    assert cfg.client_factory == DEFAULT_CLIENT_FACTORY
    assert cfg.ray_address is None
    assert cfg.pin_nodes is False


def test_a_full_configuration_is_read_as_written() -> None:
    cfg = parse_config(
        config(
            namespace="demo",
            data_dir="/var/lib/msg",
            seeds=["node-2"],
            clients=[{"id": "alice"}, {"id": "bob", "options": {"kdf": "fast"}}],
            client_factory="pkg.mod:make",
            ray_address="ray://head:10001",
        ),
        BASE,
    )
    assert cfg.seeds == (NodeId("node-2"),)
    assert [c.client_id for c in cfg.clients] == ["alice", "bob"]
    assert cfg.clients[1].options == {"kdf": "fast"}
    assert cfg.data_dir == Path("/var/lib/msg")  # absolute: kept
    assert cfg.node_dir() == Path("/var/lib/msg/nodes")
    assert cfg.client_dir("bob") == Path("/var/lib/msg/clients/bob")
    assert cfg.ray_address == "ray://head:10001"


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (["node-1"], "JSON object"),
        ({}, "nodes is required"),
        (config(nodes=[]), "at least one node"),
        (config(nodes="node-1"), "list of node ids"),
        (config(nodes=["Node 1"]), "nodes"),
        (config(nodes=["node-1", "node-1"]), "repeated"),
        (config(seeds=["node-9"]), "subset"),
        (config(seeds=[]), "subset"),
        (config(clients={"id": "a"}), "clients must be a list"),
        (config(clients=[{"id": "Alice"}]), "client id"),
        (config(clients=[{"id": "a" * 33}]), "client id"),  # not a device name
        (config(clients=[{"id": "a", "extra": 1}]), "each client"),
        (config(clients=[{"id": "a", "options": []}]), "options"),
        (config(clients=[{"id": "a"}, {"id": "a"}]), "repeated"),
        (config(namespace=""), "namespace"),
        (config(data_dir=3), "data_dir"),
        (config(client_factory="no_colon"), "client_factory"),
        (config(ray_address=""), "ray_address"),
        (config(nodo=["node-1"]), "unknown keys"),
        (config(pin_nodes="yes"), "pin_nodes"),
    ],
)
def test_invalid_configurations_say_what_is_wrong(raw: Any, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        parse_config(raw, BASE)


def test_load_config_takes_relative_paths_from_the_file(tmp_path: Path) -> None:
    path = tmp_path / "cluster.json"
    path.write_text(json.dumps(config(data_dir="state")), encoding="utf-8")
    assert load_config(path).data_dir == tmp_path / "state"


def test_load_config_reports_a_missing_or_broken_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read"):
        load_config(tmp_path / "missing.json")
    broken = tmp_path / "broken.json"
    broken.write_text("{nodes: [", encoding="utf-8")
    with pytest.raises(ConfigError, match="not valid JSON"):
        load_config(broken)


def test_the_example_configuration_of_the_repository_is_valid() -> None:
    example = Path(__file__).parents[2] / "config" / "cluster.example.json"
    cfg = load_config(example)
    assert len(cfg.nodes) == 3 and len(cfg.clients) == 2


def test_resolve_factory_finds_a_function_or_explains_why_not() -> None:
    assert resolve_factory("source.composition.launcher:start_ray_device") is (
        start_ray_device
    )
    with pytest.raises(LaunchError, match="cannot import the client factory"):
        resolve_factory("source.adapters.system.no_such_module:start")
    with pytest.raises(LaunchError, match="not a function"):
        resolve_factory("source.composition.launcher:DEFAULT_NAMESPACE")


def test_client_actors_have_the_name_of_the_real_client_actor() -> None:
    assert client_actor_name("alice") == device_actor_name("alice")
    assert client_actor_name("alice") == "client-device-alice"


def test_the_default_factory_is_the_real_client_actor() -> None:
    assert resolve_factory(DEFAULT_CLIENT_FACTORY) is start_ray_device


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"alias": "Bob"}, "unknown options"),
        ({"kdf": "slow"}, "kdf"),
        ({"tick_interval_ms": 0}, "tick_interval_ms"),
        ({"tick_interval_ms": True}, "tick_interval_ms"),
        ({"log_dir": ""}, "log_dir"),
    ],
)
def test_the_default_factory_rejects_bad_options_before_starting_anything(
    options: dict[str, Any], message: str
) -> None:
    with pytest.raises(LaunchError, match=message):
        start_ray_device("bob", "state", seeds=["node-1"], namespace="ns", **options)


def test_the_command_fails_cleanly_on_a_bad_configuration(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"nodes": []}), encoding="utf-8")
    assert main([str(path)]) == 2
    assert "ERROR" in capsys.readouterr().err
