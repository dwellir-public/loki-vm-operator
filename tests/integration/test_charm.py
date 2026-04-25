# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import logging
import pathlib
from typing import Any

import pytest

jubilant = pytest.importorskip("jubilant")

logger = logging.getLogger(__name__)


def test_deploy(charm: pathlib.Path, juju: Any):
    """Deploy the charm under test."""
    juju.deploy(charm.resolve(), app="loki-vm")
    juju.wait(jubilant.all_active)


# If you implement loki.get_version in the charm source,
# remove the @pytest.mark.skip line to enable this test.
# Alternatively, remove this test if you don't need it.
@pytest.mark.skip(reason="loki.get_version is not implemented")
def test_workload_version_is_set(charm: pathlib.Path, juju: Any):
    """Check that the correct version of the workload is running."""
    version = juju.status().apps["loki-vm"].version
    assert version == "3.14"  # Replace 3.14 by the expected version of the workload.
