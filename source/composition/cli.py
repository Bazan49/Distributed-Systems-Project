"""Open the command-line interface of one device, joined to a running cluster.

Usage (with the launcher running)::

    python -m source.composition.cli --device alice
    python -m source.composition.cli --device bob --config config/other.json

It reads the SAME configuration file as the launcher (``config/cluster.json``
by default), so both agree on the Ray namespace, the seeds, the log folder and
where each device keeps its data. Nothing of the deployment is repeated here.

What it does:

1. Configures this process's log: ``<log_dir>/cli-<device>.jsonl``.
2. Joins the Ray cluster (the configuration's ``ray_address``, else the one
   running on this machine) and checks that the service nodes answer.
3. Finds the client actor of the device (``client-device-<device>``). If the
   launcher already started it, it is used as it is. If not, it is started
   here; then it belongs to this process and stops when the interface is
   closed (the device turns off; its account and history stay in the store).
   Either way the store is ``<data_dir>/devices/<device>.db``: a device name
   is one account, whoever starts it.
4. Runs the interface (``source.adapters.cli``) until the user exits.

Exit codes: 0 normal, 2 the cluster or the nodes are not reachable.
"""

from __future__ import annotations

import argparse
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
from source.composition.launcher import DEFAULT_CONFIG, ConfigError, load_config
from source.composition.logging_setup import configure_logging
from source.composition.ray_network import start_ray_client
from source.domain.node_id import NodeId

__all__ = ["CliOptions", "main", "parse_args"]

DEFAULT_RAY_ADDRESS = "auto"
"""Without ``ray_address`` in the configuration: the cluster of this machine."""


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
        "--config",
        default=str(DEFAULT_CONFIG),
        help=f"the launcher's configuration file (default: {DEFAULT_CONFIG})",
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
        config = load_config(args.config)
    except ConfigError as exc:
        parser.error(f"--config: {exc}")
    return CliOptions(
        device=args.device,
        seeds=config.seeds,
        namespace=config.namespace,
        address=config.ray_address or DEFAULT_RAY_ADDRESS,
        data_dir=config.devices_dir().resolve(),
        log_dir=config.logs_dir(),
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
            "No encuentro el clúster de Ray. Arranca antes el sistema:\n"
            "  python -m source.composition.launcher",
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
