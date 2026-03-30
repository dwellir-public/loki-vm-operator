# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import pytest
import yaml
from ops import testing

from charm import LokiVmCharm

META = {
    "name": "loki-vm",
    "provides": {
        "loki_push_api": {"interface": "loki_push_api"},
        "grafana-source": {"interface": "grafana_datasource"},
        "send-datasource": {"interface": "grafana_datasource_exchange"},
    },
    "requires": {
        "ingress": {"interface": "ingress_per_unit"},
        "s3": {"interface": "s3", "limit": 1},
    },
    "peers": {"replicas": {"interface": "loki_replica"}},
    "storage": {"loki-persisted": {"type": "filesystem"}},
}

CONFIG_SPEC = {
    "options": {
        "ingestion-rate-mb": {"type": "int", "default": 4},
        "ingestion-burst-size-mb": {"type": "int", "default": 15},
        "retention-period": {"type": "int", "default": 14},
        "reporting-enabled": {"type": "boolean", "default": True},
        "external-url": {"type": "string", "default": ""},
        "config-override": {"type": "string", "default": ""},
    }
}


def _context() -> testing.Context:
    return testing.Context(LokiVmCharm, meta=META, config=CONFIG_SPEC)


def mock_get_version():
    """Get a mock version string without executing the workload code."""
    return "1.0.0"


def _s3_relation(
    *,
    endpoint: str = "http://10.0.0.10:3900",
    path: str = "",
    remote_app_name: str = "s3-integrator",
) -> testing.Relation:
    return testing.Relation(
        "s3",
        interface="s3",
        remote_app_name=remote_app_name,
        remote_app_data={
            "endpoint": endpoint,
            "bucket": "juju-s3-rel-10",
            "access-key": "access",
            "secret-key": "very-secret",
            "region": "garage",
            "path": path,
        },
    )


@pytest.fixture(autouse=True)
def _mock_workload_calls(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("charm.loki.is_active", lambda **_: False)
    monkeypatch.setattr("charm.loki.get_version", lambda: None)
    monkeypatch.setattr("charm.loki.start", lambda: None)
    monkeypatch.setattr("charm.loki.restart", lambda: None)


def test_start(monkeypatch: pytest.MonkeyPatch):
    """Verify start sets workload version and Active status."""
    # Arrange:
    ctx = _context()
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
    """Ensure config-override bypasses generated config rendering."""
    ctx = _context()
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
    """Keep last-good config when validation fails on a new config."""
    ctx = _context()
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
    """Detect on-disk config drift and switch to Maintenance status."""
    ctx = _context()
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


def test_replicas_relation_updates_memberlist_config(monkeypatch: pytest.MonkeyPatch):
    """Render memberlist config when peer relation provides join members."""
    ctx = _context()
    seen = {}

    def mock_write_config_text(config_text: str, **_):
        seen["config"] = config_text

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", mock_write_config_text)

    relation = testing.PeerRelation(
        endpoint="replicas",
        interface="loki_replica",
        peers_data={1: {"address": "10.0.0.2"}},
    )

    config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "",
        "config-override": "",
    }

    state = testing.State(config=config, relations=[relation])
    with ctx(ctx.on.update_status(), state) as manager:
        manager.charm._configure()

    rendered = "\n".join(
        line for line in seen["config"].splitlines() if not line.startswith("#")
    )
    config_yaml = yaml.safe_load(rendered)
    assert config_yaml["common"]["ring"]["kvstore"]["store"] == "memberlist"
    assert config_yaml["memberlist"]["join_members"] == ["10.0.0.2"]


def test_external_url_updates_push_endpoint(monkeypatch: pytest.MonkeyPatch):
    """Normalize external-url and publish it via LokiPushApiProvider."""
    ctx = _context()
    captured = {}

    def mock_update_endpoint(self, url: str = "", relation=None):
        captured["url"] = url

    monkeypatch.setattr("charm.LokiPushApiProvider.update_endpoint", mock_update_endpoint)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr("charm.LokiVmCharm._is_leader", lambda *_: True)

    config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "logs.example.com",
        "config-override": "",
    }

    ctx.run(ctx.on.config_changed(), testing.State(config=config))

    assert captured["url"] == "http://logs.example.com:3100"


def test_external_url_non_leader_clears_endpoint(monkeypatch: pytest.MonkeyPatch):
    """Ensure non-leaders clear their loki_push_api endpoint when external-url is set."""
    ctx = _context()
    relation = testing.Relation(
        endpoint="loki_push_api",
        interface="loki_push_api",
        local_unit_data={"endpoint": "stale"},
    )

    monkeypatch.setattr("charm.LokiVmCharm._is_leader", lambda *_: False)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)

    config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "logs.example.com",
        "config-override": "",
    }

    state_out = ctx.run(
        ctx.on.config_changed(),
        testing.State(config=config, relations=[relation]),
    )

    relation_out = next(iter(state_out.relations))
    assert relation_out.local_unit_data.get("endpoint") is None


def test_external_url_updates_grafana_source(monkeypatch: pytest.MonkeyPatch):
    """Normalize external-url and publish it via GrafanaSourceProvider."""
    ctx = _context()
    captured = {}

    def mock_update_source(self, source_url: str = ""):
        captured["source_url"] = source_url

    monkeypatch.setattr("charm.GrafanaSourceProvider.update_source", mock_update_source)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr("charm.LokiVmCharm._is_leader", lambda *_: True)

    config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "logs.example.com",
        "config-override": "",
    }

    ctx.run(ctx.on.config_changed(), testing.State(config=config))

    assert captured["source_url"] == "http://logs.example.com:3100"


def test_ipv6_unit_address_is_bracketed_in_published_urls(monkeypatch: pytest.MonkeyPatch):
    """Wrap IPv6 literal unit addresses before publishing direct HTTP endpoints."""
    ctx = _context()
    captured = {}

    def mock_update_endpoint(self, url: str = "", relation=None):
        captured["push_url"] = url

    def mock_update_source(self, source_url: str = ""):
        captured["source_url"] = source_url

    monkeypatch.setattr("charm.LokiPushApiProvider.update_endpoint", mock_update_endpoint)
    monkeypatch.setattr("charm.GrafanaSourceProvider.update_source", mock_update_source)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr(
        "charm.LokiVmCharm._instance_addr",
        lambda *_: "2001:db8:1234:1:216:3eff:fed2:e559",
    )

    config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "",
        "config-override": "",
    }

    ctx.run(ctx.on.config_changed(), testing.State(config=config))

    assert captured["push_url"] == "http://[2001:db8:1234:1:216:3eff:fed2:e559]:3100"
    assert captured["source_url"] == "http://[2001:db8:1234:1:216:3eff:fed2:e559]:3100"


def test_external_url_non_leader_clears_grafana_source(monkeypatch: pytest.MonkeyPatch):
    """Ensure non-leaders clear their grafana-source endpoint when external-url is set."""
    ctx = _context()
    relation = testing.Relation(
        endpoint="grafana-source",
        interface="grafana_datasource",
        local_unit_data={"grafana_source_host": "stale"},
    )

    monkeypatch.setattr("charm.LokiVmCharm._is_leader", lambda *_: False)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)

    config = {
        "ingestion-rate-mb": 4,
        "ingestion-burst-size-mb": 15,
        "retention-period": 0,
        "reporting-enabled": True,
        "external-url": "logs.example.com",
        "config-override": "",
    }

    state_out = ctx.run(
        ctx.on.config_changed(),
        testing.State(config=config, relations=[relation]),
    )

    relation_out = next(iter(state_out.relations))
    assert relation_out.local_unit_data.get("grafana_source_host") is None


def test_grafana_source_event_updates_datasource_exchange(monkeypatch: pytest.MonkeyPatch):
    """Publish datasource UIDs to grafana-datasource-exchange on source changes."""
    ctx = _context()
    relation = testing.Relation(endpoint="grafana-source", interface="grafana_datasource")
    captured = {}

    def mock_get_source_uids(self):
        return {"grafana-uid-1": {"loki/0": "ds-uid-1"}}

    def mock_publish(self, datasources):
        captured["datasources"] = datasources

    monkeypatch.setattr("charm.GrafanaSourceProvider.get_source_uids", mock_get_source_uids)
    monkeypatch.setattr("charm.DatasourceExchange.publish", mock_publish)
    monkeypatch.setattr("charm.LokiVmCharm._is_leader", lambda *_: True)

    ctx.run(
        ctx.on.relation_changed(relation),
        testing.State(relations=[relation]),
    )

    assert captured["datasources"] == [
        {"type": "loki", "uid": "ds-uid-1", "grafana_uid": "grafana-uid-1"}
    ]


def test_clustered_loki_waits_for_s3(monkeypatch: pytest.MonkeyPatch):
    """A multi-unit deployment should wait for object storage before configuring."""
    ctx = _context()
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)

    state_out = ctx.run(ctx.on.config_changed(), testing.State(planned_units=3))

    assert isinstance(state_out.unit_status, testing.WaitingStatus)
    assert state_out.unit_status.message == "waiting for s3 relation for clustered Loki"


def test_s3_relation_renders_garage_backed_config(monkeypatch: pytest.MonkeyPatch):
    """A valid s3 relation should switch the rendered config to S3-backed TSDB."""
    ctx = _context()
    seen = {}

    def mock_write_config_text(config_text: str, **_):
        seen["config"] = config_text

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", mock_write_config_text)
    state_out = ctx.run(
        ctx.on.config_changed(),
        testing.State(relations=[_s3_relation()], planned_units=1),
    )

    rendered = "\n".join(
        line for line in seen["config"].splitlines() if not line.startswith("#")
    )
    config_yaml = yaml.safe_load(rendered)

    assert config_yaml["schema_config"]["configs"][0]["object_store"] == "s3"
    assert config_yaml["storage_config"]["aws"]["bucketnames"] == "juju-s3-rel-10"
    assert config_yaml["storage_config"]["aws"]["endpoint"] == "10.0.0.10:3900"
    assert config_yaml["storage_config"]["aws"]["s3forcepathstyle"] is True
    assert config_yaml["compactor"]["working_directory"].endswith("compactor")
    assert isinstance(state_out.unit_status, testing.ActiveStatus)


@pytest.mark.parametrize("provider_name", ["garage-vm", "s3-integrator"])
def test_s3_provider_parity(monkeypatch: pytest.MonkeyPatch, provider_name: str):
    """Either provider should render the same S3-backed Loki config."""
    ctx = _context()
    seen = {}

    def mock_write_config_text(config_text: str, **_):
        seen["config"] = config_text

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", mock_write_config_text)

    state_out = ctx.run(
        ctx.on.config_changed(),
        testing.State(relations=[_s3_relation(remote_app_name=provider_name)], planned_units=1),
    )

    rendered = "\n".join(
        line for line in seen["config"].splitlines() if not line.startswith("#")
    )
    config_yaml = yaml.safe_load(rendered)

    assert config_yaml["schema_config"]["configs"][0]["object_store"] == "s3"
    assert config_yaml["storage_config"]["aws"]["endpoint"] == "10.0.0.10:3900"
    assert config_yaml["storage_config"]["aws"]["bucketnames"] == "juju-s3-rel-10"
    assert isinstance(state_out.unit_status, testing.ActiveStatus)


def test_s3_relation_with_retention_uses_s3_delete_store(monkeypatch: pytest.MonkeyPatch):
    """Retention in S3 mode should use S3 for delete requests."""
    ctx = _context()
    seen = {}

    def mock_write_config_text(config_text: str, **_):
        seen["config"] = config_text

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", mock_write_config_text)
    ctx.run(
        ctx.on.config_changed(),
        testing.State(
            config={"retention-period": 7},
            relations=[_s3_relation()],
            planned_units=1,
        ),
    )

    rendered = "\n".join(
        line for line in seen["config"].splitlines() if not line.startswith("#")
    )
    config_yaml = yaml.safe_load(rendered)
    assert config_yaml["compactor"]["delete_request_store"] == "s3"


def test_incomplete_s3_relation_waits(monkeypatch: pytest.MonkeyPatch):
    """Incomplete s3 relation data should not overwrite config."""
    ctx = _context()
    relation = testing.Relation(
        "s3",
        interface="s3",
        remote_app_name="s3-integrator",
        remote_app_data={"endpoint": "http://10.0.0.10:3900"},
    )

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)

    state_out = ctx.run(ctx.on.config_changed(), testing.State(relations=[relation]))

    assert isinstance(state_out.unit_status, testing.WaitingStatus)
    assert state_out.unit_status.message == "waiting for complete s3 relation data"


def test_s3_relation_with_path_blocks(monkeypatch: pytest.MonkeyPatch):
    """A provider path is currently unsupported and should block explicitly."""
    ctx = _context()

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)

    state_out = ctx.run(
        ctx.on.config_changed(),
        testing.State(relations=[_s3_relation(path="prefix")]),
    )

    assert state_out.unit_status == testing.BlockedStatus(
        "s3 relation field 'path' is not supported"
    )
