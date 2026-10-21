# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

"""The ruler delivers an always-firing relation rule to a related alertmanager-vm."""

import json
import pathlib
import time

import pytest

jubilant = pytest.importorskip("jubilant")

LOKI = "loki-vm"
AM = "alertmanager-vm"
PROVIDER = "rule-provider"

RULES = json.dumps(
    {
        "groups": [
            {
                "name": "alertmanager-e2e",
                "rules": [
                    {"alert": "LokiToAlertmanager", "expr": "vector(1) > 0", "for": "0s"},
                ],
            }
        ]
    }
)


def _wait_for_url(juju, *, present: bool, timeout: float = 300) -> str:
    """Poll Loki's live config until the ruler Alertmanager URL is (or is not) rendered."""
    deadline = time.time() + timeout
    while True:
        try:
            config = juju.ssh(f"{LOKI}/0", "curl -fsS http://127.0.0.1:3100/config")
        except Exception:  # noqa: BLE001 - Loki may be restarting
            config = ""
        if config and (("alertmanager_url: http://" in config) == present):
            return config
        if time.time() > deadline:
            raise AssertionError(f"alertmanager_url present={present} never observed in config")
        time.sleep(10)


def test_rule_alert_reaches_alertmanager(
    charm: pathlib.Path,
    alertmanager_charm: pathlib.Path,
    rule_provider_charm: pathlib.Path,
    juju,
):
    juju.deploy(charm.resolve(), app=LOKI)
    juju.deploy(alertmanager_charm.resolve(), app=AM)
    juju.deploy(rule_provider_charm.resolve(), app=PROVIDER, config={"alert-rules": RULES})
    juju.integrate(f"{PROVIDER}:send-loki-logs", f"{LOKI}:loki_push_api")
    juju.integrate(f"{LOKI}:alertmanager", f"{AM}:alerting")
    juju.wait(jubilant.all_active, timeout=1500)

    _wait_for_url(juju, present=True)

    deadline = time.time() + 600
    while time.time() < deadline:
        body = juju.ssh(f"{AM}/0", "curl -fsS http://127.0.0.1:9093/api/v2/alerts")
        if any(a["labels"].get("alertname") == "LokiToAlertmanager" for a in json.loads(body)):
            break
        time.sleep(15)
    else:
        raise AssertionError("LokiToAlertmanager never reached Alertmanager")

    juju.remove_relation(f"{LOKI}:alertmanager", f"{AM}:alerting")
    juju.wait(jubilant.all_active, timeout=600)
    _wait_for_url(juju, present=False)
