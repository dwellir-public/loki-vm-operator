# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import pytest
from ops import testing

from charm import LokiVmCharm


def mock_get_version():
    """Get a mock version string without executing the workload code."""
    return "1.0.0"


def test_start(monkeypatch: pytest.MonkeyPatch):
    """Test that the charm has the correct state after handling the start event."""
    # Arrange:
    ctx = testing.Context(LokiVmCharm)
    monkeypatch.setattr("charm.loki.get_version", mock_get_version)
    monkeypatch.setattr("charm.loki.ensure_data_dir", lambda _: None)
    monkeypatch.setattr("charm.loki.start", lambda: None)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    # Act:
    state_out = ctx.run(ctx.on.start(), testing.State())
    # Assert:
    assert state_out.workload_version is not None
    assert isinstance(state_out.unit_status, testing.ActiveStatus)


def test_config_override_used(monkeypatch: pytest.MonkeyPatch):
    ctx = testing.Context(LokiVmCharm)
    seen = {}

    def mock_write_config_text(config_text: str, **_):
        seen["config"] = config_text

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", mock_write_config_text)
    config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "",
        "config-override": "auth_enabled: false\nserver:\n  http_listen_port: 3100\n",
    }

    state_out = ctx.run(ctx.on.config_changed(), testing.State(config=config))

    assert "auth_enabled: false" in seen["config"]
    assert isinstance(state_out.unit_status, testing.ActiveStatus)



def test_invalid_config_keeps_last_good(monkeypatch: pytest.MonkeyPatch):
    ctx = testing.Context(LokiVmCharm)
    writes = []

    def mock_write_config_text(config_text: str, **_):
        writes.append(config_text)

    def verify_ok(**_):
        return None

    def verify_fail(**_):
        raise RuntimeError("invalid config")

    monkeypatch.setattr("charm.loki.write_config_text", mock_write_config_text)

    good_config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "",
        "config-override": "",
    }

    monkeypatch.setattr("charm.loki.verify_config", verify_ok)
    state_out = ctx.run(ctx.on.config_changed(), testing.State(config=good_config))
    assert len(writes) == 1
    assert isinstance(state_out.unit_status, testing.ActiveStatus)

    bad_config = {
        **good_config,
        "config-override": "not: [valid",
    }

    monkeypatch.setattr("charm.loki.verify_config", verify_fail)
    state_out = ctx.run(ctx.on.config_changed(), testing.State(config=bad_config))

    assert len(writes) == 1
    assert isinstance(state_out.unit_status, testing.WaitingStatus)


def test_config_drift_sets_maintenance(monkeypatch: pytest.MonkeyPatch, tmp_path):
    ctx = testing.Context(LokiVmCharm)
    config_path = tmp_path / "config.yml"
    monkeypatch.setattr("charm.DEFAULT_CONFIG_PATH", str(config_path))
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)

    def write_config_text(config_text: str, **_):
        config_path.write_text(config_text, encoding="utf-8")

    monkeypatch.setattr("charm.loki.write_config_text", write_config_text)
    config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "",
        "config-override": "auth_enabled: false\n",
    }

    state_out = ctx.run(ctx.on.config_changed(), testing.State(config=config))
    assert isinstance(state_out.unit_status, testing.ActiveStatus)

    config_path.write_text("manual: true\n", encoding="utf-8")
    state_out = ctx.run(ctx.on.update_status(), state_out)

    assert isinstance(state_out.unit_status, testing.MaintenanceStatus)
