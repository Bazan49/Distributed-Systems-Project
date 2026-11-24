"""Shared fixtures for the tests that need a real (local) Ray cluster."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import ray


@pytest.fixture(scope="session")
def ray_cluster() -> Iterator[None]:
    """Start a local Ray cluster once for every Ray test; shut it down at the end."""
    if not ray.is_initialized():
        ray.init(
            ignore_reinit_error=True,
            include_dashboard=False,  # starts faster
            log_to_driver=False,  # keep the workers' logs out of the test output
        )
    yield
    ray.shutdown()
