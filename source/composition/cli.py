"""Open the command-line interface of one device, joined to a running cluster.

Usage (with the cluster running: ``ray start --head`` and the launcher)::

    python -m source.composition.cli --device alice
    python -m source.composition.cli --device bob --seeds node-1,node-2

What it does:

1. Configures this process's log: ``<log-dir>/cli-<device>.jsonl``.
2. Joins the Ray cluster (``--address``, default ``auto``) and checks that the
   service nodes answer.
3. Finds the client actor of the device (``client-device-<device>``). If the
   launcher already started it, it is used as it is, with its own store. If
   not, it is started here, with its store in ``<data-dir>/<device>.db``;
   then it belongs to this process and stops when the interface is closed
   (the device turns off; its account and history stay in the store).
4. Runs the interface (``source.adapters.cli``) until the user exits.

Exit codes: 0 normal, 2 the cluster or the nodes are not reachable.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import ray

from source.adapters.cli.app import Cli
from source.adapters.cli.console import TerminalConsole
from source.adapters.system.ray.ray_gateway import DiscoveryError, RayNodeGateway
from source.adapters.system.ray.remote_client import RemoteClient
from source.composition.client import ClientSettings, KdfStrength, valid_device_name
from source.composition.logging_setup import (
    DEFAULT_LOG_DIR,
    DIR_ENV_VAR,
    configure_logging,
)
from source.composition.ray_network import start_ray_client
from source.domain.node_id import NodeId

__all__ = ["CliOptions", "main", "parse_args"]

DEFAULT_DATA_DIR = Path("data") / "devices"


@dataclass(frozen=True)
class CliOptions:
    device: str
    seeds: tuple[NodeId, ...]
    namespace: str
    address: str
    data_dir: Path  # absolute
    log_dir: Path  # absolute
    kdf: KdfStrength


def parse_args(argv: Sequence[str] | None = None) -> CliOptions:
    """Read the command line; argparse exits with a message if it is wrong."""
    parser = argparse.ArgumentParser(
        prog="python -m source.composition.cli",
        description="Open the interface of one device of a running cluster.",
    )
    parser.add_argument("--device", required=True, help="device name, e.g. alice")
    parser.add_argument(
        "--seeds", default="node-1,node-2", help="comma-separated initial nodes"
    )
    parser.add_argument("--namespace", default="mensajeria")
    parser.add_argument("--address", default="auto", help="Ray address")
    parser.add_argument(
        "--data-dir",
        default=str(DEFAULT_DATA_DIR),
        help="folder of the device's store, if this command starts its client",
    )
    parser.add_argument(
        "--log-dir",
        default=None,
        help="folder of the logs (default: P08_LOG_DIR or logs)",
    )
    parser.add_argument(
        "--fast-kdf",
        action="store_true",
        help="cheap password protection: ONLY for tests and demos",
    )
    args = parser.parse_args(argv)

    if not valid_device_name(args.device):
        parser.error(
            "--device: 1 to 32 lowercase letters, digits, '-' or '_', "
            "starting with a letter or digit"
        )
    try:
        seeds = tuple(NodeId(s.strip()) for s in args.seeds.split(",") if s.strip())
    except ValueError as exc:
        parser.error(f"--seeds: {exc}")
    if not seeds:
        parser.error("--seeds: at least one node")
    log_dir = args.log_dir or os.environ.get(DIR_ENV_VAR) or str(DEFAULT_LOG_DIR)
    return CliOptions(
        device=args.device,
        seeds=seeds,
        namespace=args.namespace,
        address=args.address,
        data_dir=Path(args.data_dir).resolve(),
        log_dir=Path(log_dir).resolve(),
        kdf=KdfStrength.FAST if args.fast_kdf else KdfStrength.SECURE,
    )


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_args(argv)
    configure_logging(process=f"cli-{options.device}", log_dir=options.log_dir)
    try:
        ray.init(
            address=options.address,
            namespace=options.namespace,
            log_to_driver=False,
        )
    except ConnectionError:
        print(
            "No encuentro el clúster de Ray. Arráncalo antes:\n"
            "  ray start --head\n"
            "  python -m source.composition.launcher <configuración>",
            file=sys.stderr,
        )
        return 2
    try:
        try:
            RayNodeGateway.from_seeds(list(options.seeds), options.namespace)
        except DiscoveryError:
            seeds = ", ".join(s.value for s in options.seeds)
            print(
                f"Ningún nodo inicial responde ({seeds}) en el namespace "
                f"«{options.namespace}». ¿Está el lanzador en marcha?",
                file=sys.stderr,
            )
            return 2
        handle = start_ray_client(
            options.device,
            [s.value for s in options.seeds],
            namespace=options.namespace,
            settings=ClientSettings(data_dir=options.data_dir, kdf=options.kdf),
            log_dir=options.log_dir,
        )
        Cli(RemoteClient(handle), TerminalConsole(), device=options.device).run()
    finally:
        ray.shutdown()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
