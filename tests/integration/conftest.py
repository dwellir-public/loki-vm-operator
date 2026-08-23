# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import json
import logging
import os
import pathlib
import re
import subprocess
import sys
import time
from typing import Any

import pytest

jubilant = pytest.importorskip("jubilant")

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Create a temporary Juju model for running tests."""
    active_context = subprocess.run(
        ["juju", "switch"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if active_context != "localhost-localhost:admin/controller":
        raise pytest.UsageError(
            "Integration tests require active context localhost-localhost:admin/controller"
        )
    with jubilant.temp_model(controller="localhost-localhost", cloud="localhost") as juju:
        controller = _current_controller(juju)
        logger.info("Using temp model: %s (controller: %s)", juju.model, controller)
        print(f"Using temp model: {juju.model} (controller: {controller})")
        yield juju

        if request.session.testsfailed:
            logger.info("Collecting Juju logs...")
            time.sleep(0.5)  # Wait for Juju to process logs.
            log = juju.debug_log(limit=1000)
            print(log, end="", file=sys.stderr)


@pytest.fixture(scope="session")
def rule_provider_charm() -> pathlib.Path:
    """Return or build the fake rule provider used by ruler API tests."""
    configured = os.environ.get("RULE_PROVIDER_CHARM_PATH")
    if configured:
        path = pathlib.Path(configured)
        if not path.exists():
            raise FileNotFoundError(f"Rule provider charm does not exist: {path}")
        return path
    provider_dir = pathlib.Path("tests/integration/rule-provider")
    subprocess.run(["charmcraft", "pack", "--destructive-mode"], cwd=provider_dir, check=True)
    paths = list(provider_dir.glob("*.charm"))
    if len(paths) != 1:
        raise RuntimeError("Expected exactly one built rule-provider charm")
    return paths[0]


@pytest.fixture(scope="session")
def baseline_charm() -> pathlib.Path:
    """Return the baseline Loki charm used by the S3 migration test."""
    configured = os.environ.get("BASELINE_CHARM_PATH")
    if not configured:
        raise pytest.UsageError("Set BASELINE_CHARM_PATH to run the S3 migration test")
    path = pathlib.Path(configured)
    if not path.exists():
        raise FileNotFoundError(f"Baseline charm does not exist: {path}")
    return path


@pytest.fixture(scope="session")
def garage_charm() -> pathlib.Path:
    """Return the locally built Garage charm used by the S3 migration test."""
    configured = os.environ.get("GARAGE_CHARM_PATH")
    if not configured:
        raise pytest.UsageError("Set GARAGE_CHARM_PATH to run the S3 migration test")
    path = pathlib.Path(configured)
    if not path.exists():
        raise FileNotFoundError(f"Garage charm does not exist: {path}")
    return path


@pytest.fixture(scope="session")
def baseline_loki_version() -> str:
    """Return the explicitly pinned workload version expected from the baseline charm."""
    configured = os.environ.get("BASELINE_LOKI_VERSION")
    if not configured:
        raise pytest.UsageError("Set BASELINE_LOKI_VERSION to run the S3 migration test")
    if re.fullmatch(r"[0-9]+\.[0-9]+(?:\.[0-9]+){0,2}", configured) is None:
        raise pytest.UsageError("BASELINE_LOKI_VERSION must be a numeric Loki version")
    parts = tuple(int(part) for part in configured.split("."))
    if (parts + (0, 0, 0))[:3] < (3, 4, 0):
        raise pytest.UsageError("BASELINE_LOKI_VERSION must be 3.4.0 or newer")
    return configured


@pytest.fixture(scope="session")
def charm():
    """Return the path of the charm under test."""
    if "CHARM_PATH" in os.environ:
        charm_path = pathlib.Path(os.environ["CHARM_PATH"])
        if not charm_path.exists():
            raise FileNotFoundError(f"Charm does not exist: {charm_path}")
        return charm_path
    # Modify below if you're building for multiple bases or architectures.
    charm_paths = list(pathlib.Path(".").glob("*.charm"))
    if not charm_paths:
        pytest.skip("No .charm file in current directory. Build with charmcraft pack.")
    if len(charm_paths) > 1:
        path_list = ", ".join(str(path) for path in charm_paths)
        raise ValueError(f"More than one .charm file in current directory: {path_list}")
    return charm_paths[0]


def _current_controller(juju: Any) -> str:
    """Return the current Juju controller name."""
    try:
        info = json.loads(juju.cli("whoami", "--format", "json", include_model=False))
        return str(info.get("controller", "unknown"))
    except Exception:  # noqa: BLE001
        return "unknown"
