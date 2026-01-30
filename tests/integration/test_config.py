# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

import pytest

 
jubilant = pytest.importorskip("jubilant")


def _units_all(status: jubilant.Status, app: str, predicate) -> bool:
    units = status.get_units(app)
    return bool(units) and all(predicate(unit) for unit in units.values())


def test_config_update_and_revert(juju: jubilant.Juju, charm):
    """Update config, then apply an invalid override and revert."""
    juju.deploy(charm.resolve(), app="loki-vm")
    juju.wait(lambda status: jubilant.all_active(status, "loki-vm"))

    juju.config("loki-vm", {"retention-period": 1})
    juju.wait(lambda status: jubilant.all_active(status, "loki-vm"))

    juju.config("loki-vm", {"config-override": "not: [valid"})
    juju.wait(lambda status: _units_all(status, "loki-vm", lambda u: u.is_waiting))

    juju.config("loki-vm", {"config-override": ""})
    juju.wait(lambda status: jubilant.all_active(status, "loki-vm"))
