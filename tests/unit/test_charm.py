# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import json
from types import SimpleNamespace

import pytest
import yaml
from ops import testing

from charm import InvalidConfigurationError, LokiVmCharm, StorageBackendState
from config_builder import S3StorageConfig
from rule_reconciler import CACHE_KEY

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

ACTIONS = {
    "set-config": {
        "description": "Set a full config",
        "params": {"config": {"type": "string"}},
        "required": ["config"],
    },
    "cluster-health": {"description": "Report Loki cluster health."},
}


def _context() -> testing.Context:
    return testing.Context(LokiVmCharm, meta=META, actions=ACTIONS, config=CONFIG_SPEC)


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


def _rule_relation(name: str = "source", relation_id: int = 7) -> testing.Relation:
    return testing.Relation(
        "loki_push_api",
        interface="loki_push_api",
        id=relation_id,
        remote_app_name=name,
        remote_app_data={
            "alert_rules": json.dumps(
                {
                    "groups": [
                        {
                            "name": f"{name}-group",
                            "rules": [{"alert": "Example", "expr": '{job="demo"}'}],
                        }
                    ]
                }
            )
        },
    )


@pytest.mark.parametrize(
    ("endpoint", "expected"),
    [
        ("http://10.0.0.10:3900", "10.0.0.10:3900"),
        ("https://s3.example.test", "s3.example.test:443"),
        ("s3.example.test:3900", "s3.example.test:3900"),
        ("http://[2001:db8::10]:3900", "[2001:db8::10]:3900"),
        ("https://[2001:db8::10]", "[2001:db8::10]:443"),
    ],
)
def test_normalize_s3_endpoint_preserves_valid_authority(
    endpoint: str,
    expected: str,
) -> None:
    assert LokiVmCharm._normalize_s3_endpoint(None, endpoint) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "endpoint",
    [
        "ftp://s3.example.test:3900",
        "http://s3.example.test:3900/prefix",
        "http://s3.example.test:3900?query=value",
        "http://user@s3.example.test:3900",
    ],
)
def test_normalize_s3_endpoint_rejects_unsupported_url_components(endpoint: str) -> None:
    with pytest.raises(InvalidConfigurationError):
        LokiVmCharm._normalize_s3_endpoint(None, endpoint)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("endpoint", "expected"),
    [
        ("http://s3.example.test:3900", True),
        ("https://s3.example.test:3900", False),
        ("s3.example.test:3900", True),
    ],
)
def test_s3_insecure_matches_normalization_default(endpoint: str, expected: bool) -> None:
    assert LokiVmCharm._s3_is_insecure(None, endpoint) is expected  # type: ignore[arg-type]


def test_storage_probe_treats_s3_auth_challenge_as_reachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def probe(url: str) -> tuple[bool, None]:
        seen.append(url)
        return True, None

    monkeypatch.setattr("charm.loki.check_s3_endpoint", probe)
    state = StorageBackendState(
        status=None,
        s3=S3StorageConfig(
            bucket="bucket",
            endpoint="[2001:db8::10]:3900",
            access_key_id="access",
            secret_access_key="secret",
            region="garage",
            insecure=True,
        ),
    )

    assert LokiVmCharm._storage_probe_result(None, state) == (True, None)  # type: ignore[arg-type]
    assert seen == ["http://[2001:db8::10]:3900"]


class _FakeRulerApi:
    calls: list[list[dict]] = []
    base_urls: list[str] = []

    def __init__(self, base_url: str) -> None:
        self.base_urls.append(base_url)

    def replace_namespace(self, groups: list[dict]) -> list[dict]:
        self.calls.append(groups)
        return []


@pytest.fixture(autouse=True)
def _mock_workload_calls(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("charm.loki.is_active", lambda **_: False)
    monkeypatch.setattr("charm.loki.get_version", lambda: None)
    monkeypatch.setattr("charm.loki.start", lambda: None)
    monkeypatch.setattr("charm.loki.restart", lambda: None)
    monkeypatch.setattr("charm.loki.check_ready", lambda *_, **__: (True, None), raising=False)
    monkeypatch.setattr("charm.prepare_filesystem_rule_store", lambda _: None)


def test_start(monkeypatch: pytest.MonkeyPatch):
    """Verify start sets workload version and Active status."""
    # Arrange:
    ctx = _context()
    monkeypatch.setattr("charm.loki.get_version", mock_get_version)
    monkeypatch.setattr("charm.loki.ensure_data_dir", lambda _: None)
    monkeypatch.setattr("charm.loki.start", lambda: None)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr(
        "charm.LokiVmCharm._read_config_from_disk",
        lambda self: self._render_config_text(),
    )
    # Act:
    state_out = ctx.run(ctx.on.start(), testing.State())
    # Assert:
    assert state_out.workload_version is not None
    assert state_out.unit_status == testing.ActiveStatus("ready(1/1), storage(local)")


def test_start_waits_through_not_ready_then_reports_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Start should not calculate final health until local Loki becomes ready."""
    ctx = _context()
    readiness = iter([(False, "starting"), (True, None), (True, None)])
    calls: list[str] = []
    monkeypatch.setattr("charm.loki.ensure_data_dir", lambda _: None)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)

    def check_ready(*_: object) -> tuple[bool, str | None]:
        calls.append("check")
        return next(readiness)

    monkeypatch.setattr("charm.loki.check_ready", check_ready)
    monkeypatch.setattr("charm.time.sleep", lambda _: None)

    state_out = ctx.run(ctx.on.start(), testing.State())

    assert calls == ["check", "check", "check"]
    assert state_out.unit_status == testing.ActiveStatus("ready(1/1), storage(local)")


def test_start_readiness_timeout_retains_maintenance_without_hook_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bounded readiness timeout should remain retryable via later lifecycle events."""
    ctx = _context()
    moments = iter([0.0, 121.0])
    monkeypatch.setattr("charm.loki.ensure_data_dir", lambda _: None)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr("charm.loki.check_ready", lambda *_: (False, "connection refused"))
    monkeypatch.setattr("charm.time.monotonic", lambda: next(moments))

    state_out = ctx.run(ctx.on.start(), testing.State())

    assert state_out.unit_status == testing.MaintenanceStatus("waiting for Loki readiness")


def test_single_unit_config_restart_waits_for_ready_but_unchanged_config_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a config-driven start or restart should incur the bounded readiness wait."""
    ctx = _context()
    waits: list[str] = []
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr(
        "charm.LokiVmCharm._read_config_from_disk",
        lambda self: self._render_config_text(),
    )
    monkeypatch.setattr(
        "charm.LokiVmCharm._wait_for_local_ready",
        lambda *_: waits.append("wait"),
    )

    state_out = ctx.run(ctx.on.config_changed(), testing.State())
    assert waits == ["wait"]

    state_out = ctx.run(ctx.on.config_changed(), state_out)
    assert waits == ["wait"]
    assert state_out.unit_status == testing.ActiveStatus("ready(1/1), storage(local)")


def test_single_unit_config_restart_timeout_retains_maintenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config hooks should remain successful and retryable when restarted Loki is slow."""
    ctx = _context()
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr(
        "charm.LokiVmCharm._wait_for_local_ready",
        lambda *_: (_ for _ in ()).throw(RuntimeError("timed out")),
    )

    state_out = ctx.run(ctx.on.config_changed(), testing.State())

    assert state_out.unit_status == testing.MaintenanceStatus("waiting for Loki readiness")


def test_storage_status_precedes_start_readiness_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """A blocking relation status must return before workload readiness handling."""
    ctx = _context()
    incomplete_s3 = testing.Relation(
        "s3", interface="s3", remote_app_name="garage-vm", remote_app_data={}
    )
    monkeypatch.setattr("charm.loki.ensure_data_dir", lambda _: None)
    monkeypatch.setattr(
        "charm.LokiVmCharm._wait_for_local_ready",
        lambda *_: pytest.fail("readiness wait must not replace storage status"),
    )

    state_out = ctx.run(ctx.on.start(), testing.State(relations=[incomplete_s3]))

    assert state_out.unit_status == testing.WaitingStatus("waiting for complete s3 relation data")


def test_relation_rules_are_applied_and_cached_by_leader(monkeypatch: pytest.MonkeyPatch):
    """A relation-changed event should reconcile standard alert_rules into Loki."""
    ctx = _context()
    source = _rule_relation()
    peer = testing.PeerRelation("replicas", interface="loki_replica", id=99)
    _FakeRulerApi.calls = []
    monkeypatch.setattr("charm.LokiRulerApiClient", _FakeRulerApi)

    state_out = ctx.run(
        ctx.on.relation_changed(source),
        testing.State(leader=True, relations=[source, peer]),
    )

    assert [[group["name"] for group in call] for call in _FakeRulerApi.calls][-1] == [
        "source-group"
    ]
    assert len(_FakeRulerApi.calls) == 1
    assert _FakeRulerApi.base_urls[-1] == "http://127.0.0.1:3100"
    peer_out = next(
        relation for relation in state_out.relations if relation.endpoint == "replicas"
    )
    assert peer_out.local_app_data[CACHE_KEY]


def test_non_leader_never_applies_or_caches_relation_rules(monkeypatch: pytest.MonkeyPatch):
    """Only the Juju leader may mutate shared rule state or the ruler namespace."""
    ctx = _context()
    source = _rule_relation()
    peer = testing.PeerRelation("replicas", interface="loki_replica", id=99)
    _FakeRulerApi.calls = []
    monkeypatch.setattr("charm.LokiRulerApiClient", _FakeRulerApi)

    state_out = ctx.run(
        ctx.on.relation_changed(source),
        testing.State(leader=False, relations=[source, peer]),
    )

    assert _FakeRulerApi.calls == []
    peer_out = next(
        relation for relation in state_out.relations if relation.endpoint == "replicas"
    )
    assert CACHE_KEY not in peer_out.local_app_data


def test_broken_rule_relation_withdraws_its_groups(monkeypatch: pytest.MonkeyPatch):
    """Relation-broken should exclude stale event data and clear the namespace."""
    ctx = _context()
    source = _rule_relation()
    peer = testing.PeerRelation("replicas", interface="loki_replica", id=99)
    _FakeRulerApi.calls = []
    monkeypatch.setattr("charm.LokiRulerApiClient", _FakeRulerApi)
    state = ctx.run(
        ctx.on.relation_changed(source),
        testing.State(leader=True, relations=[source, peer]),
    )
    source_out = next(
        relation for relation in state.relations if relation.endpoint == "loki_push_api"
    )

    ctx.run(ctx.on.relation_broken(source_out), state)

    assert _FakeRulerApi.calls[-1] == []


def test_relation_changed_with_omitted_rules_withdraws_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Removing alert_rules from an extant app databag must reconcile withdrawal."""
    ctx = _context()
    source = _rule_relation()
    peer = testing.PeerRelation("replicas", interface="loki_replica", id=99)
    _FakeRulerApi.calls = []
    monkeypatch.setattr("charm.LokiRulerApiClient", _FakeRulerApi)
    state = ctx.run(
        ctx.on.relation_changed(source),
        testing.State(leader=True, relations=[source, peer]),
    )
    source_without_rules = testing.Relation(
        "loki_push_api",
        interface="loki_push_api",
        id=source.id,
        remote_app_name="source",
        remote_app_data={},
    )
    peer_out = next(relation for relation in state.relations if relation.endpoint == "replicas")

    ctx.run(
        ctx.on.relation_changed(source_without_rules),
        testing.State(leader=True, relations=[source_without_rules, peer_out]),
    )

    assert _FakeRulerApi.calls[-1] == []


def test_leader_election_with_empty_state_cleans_owned_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty desired state must still delete stale charm-owned ruler state."""
    ctx = _context()
    peer = testing.PeerRelation("replicas", interface="loki_replica", id=99)
    _FakeRulerApi.calls = []
    monkeypatch.setattr("charm.LokiRulerApiClient", _FakeRulerApi)

    ctx.run(ctx.on.leader_elected(), testing.State(leader=True, relations=[peer]))

    assert _FakeRulerApi.calls == [[]]


@pytest.mark.parametrize("event_name", ["start", "upgrade", "leader", "update-status"])
def test_lifecycle_events_replay_relation_rules(
    monkeypatch: pytest.MonkeyPatch,
    event_name: str,
):
    """Lifecycle convergence should replay accepted relation rules after restarts or failover."""
    ctx = _context()
    source = _rule_relation()
    peer = testing.PeerRelation("replicas", interface="loki_replica", id=99)
    _FakeRulerApi.calls = []
    monkeypatch.setattr("charm.LokiRulerApiClient", _FakeRulerApi)
    monkeypatch.setattr("charm.loki.ensure_data_dir", lambda _: None)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr(
        "charm.loki.is_active", lambda **_: event_name in {"upgrade", "update-status"}
    )
    events = {
        "start": ctx.on.start(),
        "upgrade": ctx.on.upgrade_charm(),
        "leader": ctx.on.leader_elected(),
        "update-status": ctx.on.update_status(),
    }

    ctx.run(events[event_name], testing.State(leader=True, relations=[source, peer]))

    assert any(group["name"] == "source-group" for call in _FakeRulerApi.calls for group in call)


def test_departed_unit_keeps_remote_application_rules(monkeypatch: pytest.MonkeyPatch):
    """A unit departure must not withdraw the remote application's app-owned rules."""
    ctx = _context()
    source = _rule_relation()
    peer = testing.PeerRelation("replicas", interface="loki_replica", id=99)
    _FakeRulerApi.calls = []
    monkeypatch.setattr("charm.LokiRulerApiClient", _FakeRulerApi)

    ctx.run(
        ctx.on.relation_departed(source, departing_unit=0),
        testing.State(leader=True, relations=[source, peer]),
    )

    assert [group["name"] for group in _FakeRulerApi.calls[-1]] == ["source-group"]


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
    assert state_out.unit_status == testing.ActiveStatus("ready(1/1), storage(local)")


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


def test_update_status_surfaces_unhealthy_cluster(monkeypatch: pytest.MonkeyPatch):
    """Use a compact maintenance message when the cluster is degraded."""
    ctx = _context()
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: SimpleNamespace(healthy=False, ready_units=2, expected_units=3, members=[]),
    )
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))

    state_out = ctx.run(
        ctx.on.update_status(),
        testing.State(planned_units=3, relations=[_s3_relation()]),
    )

    assert state_out.unit_status == testing.MaintenanceStatus("ready(2/3), storage(s3(ok))")


def test_update_status_surfaces_rolling_restart_for_queued_unit(
    monkeypatch: pytest.MonkeyPatch,
):
    """A pending non-target unit should show queued rolling progress."""
    ctx = _context()
    relation = testing.PeerRelation(
        endpoint="replicas",
        interface="loki_replica",
        local_app_data={"restart-target": "loki-vm/1"},
        local_unit_data={"restart-pending": "true", "rolling-phase": "queued"},
        peers_data={1: {"address": "10.0.0.2", "restart-pending": "true"}},
    )
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: SimpleNamespace(healthy=False, ready_units=1, expected_units=2, members=[]),
    )
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))

    state_out = ctx.run(
        ctx.on.update_status(),
        testing.State(planned_units=2, relations=[relation, _s3_relation()]),
    )

    assert state_out.unit_status == testing.MaintenanceStatus(
        "rolling(queued, target=loki-vm/1), ready(1/2), storage(s3(ok))"
    )


def test_update_status_surfaces_rolling_restart_for_completed_unit(
    monkeypatch: pytest.MonkeyPatch,
):
    """A unit that has finished should still show rollout progress until peers finish."""
    ctx = _context()
    relation = testing.PeerRelation(
        endpoint="replicas",
        interface="loki_replica",
        local_app_data={"restart-target": "loki-vm/1"},
        local_unit_data={"restart-pending": "false"},
        peers_data={1: {"address": "10.0.0.2", "restart-pending": "true"}},
    )
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: SimpleNamespace(healthy=False, ready_units=1, expected_units=2, members=[]),
    )
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))

    state_out = ctx.run(
        ctx.on.update_status(),
        testing.State(planned_units=2, relations=[relation, _s3_relation()]),
    )

    assert state_out.unit_status == testing.MaintenanceStatus(
        "rolling(waiting-peers, target=loki-vm/1), ready(1/2), storage(s3(ok))"
    )


def test_cluster_health_action_reports_detailed_results(monkeypatch: pytest.MonkeyPatch):
    """Expose the same health summary plus detailed member results via action output."""
    ctx = _context()
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: SimpleNamespace(
            healthy=False,
            ready_units=2,
            expected_units=3,
            members=[
                SimpleNamespace(unit_name="loki-vm/0", address="10.0.0.1", ready=True, error=None),
                SimpleNamespace(unit_name="loki-vm/1", address="10.0.0.2", ready=True, error=None),
                SimpleNamespace(
                    unit_name="loki-vm/2",
                    address="10.0.0.3",
                    ready=False,
                    error="timed out",
                ),
            ],
        ),
    )
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))

    ctx.run(
        ctx.on.action("cluster-health"),
        testing.State(planned_units=3, relations=[_s3_relation()]),
    )

    assert ctx.action_results == {
        "healthy": False,
        "summary": "ready(2/3), storage(s3(ok))",
        "ready-units": 2,
        "expected-units": 3,
        "storage": "s3(ok)",
        "storage-error": None,
        "members": json.dumps(
            [
                {"unit": "loki-vm/0", "address": "10.0.0.1", "ready": True, "error": None},
                {"unit": "loki-vm/1", "address": "10.0.0.2", "ready": True, "error": None},
                {"unit": "loki-vm/2", "address": "10.0.0.3", "ready": False, "error": "timed out"},
            ]
        ),
    }


def test_cluster_health_action_reports_s3_probe_failures(monkeypatch: pytest.MonkeyPatch):
    """Surface S3 backend probe failures in the compact storage label."""
    ctx = _context()
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: SimpleNamespace(
            healthy=True,
            ready_units=3,
            expected_units=3,
            members=[],
        ),
    )
    monkeypatch.setattr(
        "charm.LokiVmCharm._storage_probe_result",
        lambda *_: (False, "timed out"),
    )

    ctx.run(
        ctx.on.action("cluster-health"),
        testing.State(planned_units=3, relations=[_s3_relation()]),
    )

    assert ctx.action_results is not None
    assert ctx.action_results["healthy"] is False
    assert ctx.action_results["summary"] == "ready(3/3), storage(s3(error))"
    assert ctx.action_results["storage"] == "s3(error)"
    assert ctx.action_results["storage-error"] == "timed out"


def test_cluster_health_uses_actual_unit_numbers_for_sparse_clusters(
    monkeypatch: pytest.MonkeyPatch,
):
    """Health accounting should use real unit ids, not dense 0..N numbering."""
    ctx = _context()
    relation = testing.PeerRelation(
        endpoint="replicas",
        interface="loki_replica",
        peers_data={
            3: {"address": "10.0.0.3"},
            4: {"address": "10.0.0.4"},
        },
    )

    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr("charm.loki.check_ready", lambda *_, **__: (True, None))
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))
    monkeypatch.setattr("charm.LokiVmCharm._instance_addr", lambda *_: "10.0.0.1")

    ctx.run(
        ctx.on.action("cluster-health"),
        testing.State(relations=[relation, _s3_relation()], planned_units=3),
    )

    assert ctx.action_results is not None
    assert ctx.action_results["healthy"] is True
    assert ctx.action_results["summary"] == "ready(3/3), storage(s3(ok))"
    assert json.loads(ctx.action_results["members"]) == [
        {"unit": "loki-vm/0", "address": "10.0.0.1", "ready": True, "error": None},
        {"unit": "loki-vm/3", "address": "10.0.0.3", "ready": True, "error": None},
        {"unit": "loki-vm/4", "address": "10.0.0.4", "ready": True, "error": None},
    ]


def test_clustered_non_leader_config_change_defers_restart(
    monkeypatch: pytest.MonkeyPatch,
):
    """A non-leader clustered unit should not restart itself on config change."""
    ctx = _context()
    relation = testing.PeerRelation(
        endpoint="replicas",
        interface="loki_replica",
        local_app_data={"restart-target": "loki-vm/1"},
        peers_data={1: {"address": "10.0.0.2"}},
    )
    restart_calls = []

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr("charm.loki.restart", lambda: restart_calls.append("restart"))
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: SimpleNamespace(healthy=True, ready_units=2, expected_units=2, members=[]),
    )
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))

    state_out = ctx.run(
        ctx.on.config_changed(),
        testing.State(leader=False, planned_units=2, relations=[relation, _s3_relation()]),
    )

    peer_out = next(rel for rel in state_out.relations if rel.endpoint == "replicas")
    assert peer_out.local_unit_data is not None
    assert peer_out.local_app_data is not None
    assert restart_calls == []
    assert peer_out.local_unit_data["restart-pending"] == "true"
    assert peer_out.local_app_data["restart-target"] == "loki-vm/1"


def test_target_unit_restarts_when_selected_by_leader(monkeypatch: pytest.MonkeyPatch):
    """Only the selected unit should perform the clustered restart."""
    ctx = _context()
    relation = testing.PeerRelation(
        endpoint="replicas",
        interface="loki_replica",
        local_app_data={"restart-target": "loki-vm/0"},
        local_unit_data={"restart-pending": "true"},
        peers_data={1: {"address": "10.0.0.2", "restart-pending": "false"}},
    )
    restart_calls = []
    ready_checks = []
    cluster_checks = []

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", lambda *_, **__: None)
    monkeypatch.setattr("charm.loki.restart", lambda: restart_calls.append("restart"))
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr("charm.loki.prepare_shutdown", lambda *_, **__: None, raising=False)
    monkeypatch.setattr(
        "charm.loki.check_ready",
        lambda *_, **__: (ready_checks.append("ready") or True, None),
    )
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: (
            cluster_checks.append("healthy")
            or SimpleNamespace(healthy=True, ready_units=2, expected_units=2, members=[])
        ),
    )
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))

    state_out = ctx.run(
        ctx.on.update_status(),
        testing.State(relations=[relation, _s3_relation()], planned_units=2),
    )

    peer_out = next(rel for rel in state_out.relations if rel.endpoint == "replicas")
    assert peer_out.local_unit_data is not None
    assert restart_calls == ["restart"]
    assert ready_checks == ["ready"]
    assert len(cluster_checks) >= 2
    assert peer_out.local_unit_data["restart-pending"] == "false"
    assert peer_out.local_unit_data.get("rolling-phase") is None


def test_leader_waits_for_cluster_health_before_advancing_restart_target(
    monkeypatch: pytest.MonkeyPatch,
):
    """The leader should not advance a rolling restart while health is degraded."""
    ctx = _context()
    relation = testing.PeerRelation(
        endpoint="replicas",
        interface="loki_replica",
        local_app_data={"restart-target": "loki-vm/1"},
        local_unit_data={"restart-pending": "true"},
        peers_data={1: {"address": "10.0.0.2", "restart-pending": "false"}},
    )

    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: SimpleNamespace(healthy=False, ready_units=1, expected_units=2, members=[]),
    )
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))

    state_out = ctx.run(
        ctx.on.update_status(),
        testing.State(leader=True, planned_units=2, relations=[relation, _s3_relation()]),
    )

    peer_out = next(rel for rel in state_out.relations if rel.endpoint == "replicas")
    assert peer_out.local_app_data is not None
    assert peer_out.local_app_data["restart-target"] == "loki-vm/1"


def test_replicas_coordination_change_uses_fast_path_without_config_rewrite(
    monkeypatch: pytest.MonkeyPatch,
):
    """Restart handoff on peer coordination data should not rewrite config."""
    ctx = _context()
    relation = testing.PeerRelation(
        endpoint="replicas",
        interface="loki_replica",
        local_app_data={"restart-target": "loki-vm/0"},
        local_unit_data={"restart-pending": "true"},
        peers_data={1: {"address": "10.0.0.2", "restart-pending": "false"}},
    )
    restart_calls = []
    write_calls = []
    ready_checks = []
    cluster_checks = []

    monkeypatch.setattr(
        "charm.loki.write_config_text", lambda *_, **__: write_calls.append("write")
    )
    monkeypatch.setattr("charm.loki.restart", lambda: restart_calls.append("restart"))
    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr("charm.loki.prepare_shutdown", lambda *_, **__: None, raising=False)
    monkeypatch.setattr(
        "charm.loki.check_ready",
        lambda *_, **__: (ready_checks.append("ready") or True, None),
    )
    monkeypatch.setattr(
        "charm.LokiVmCharm._cluster_health",
        lambda *_: (
            cluster_checks.append("healthy")
            or SimpleNamespace(healthy=True, ready_units=2, expected_units=2, members=[])
        ),
    )
    monkeypatch.setattr("charm.LokiVmCharm._storage_probe_result", lambda *_: (True, None))

    state_out = ctx.run(
        ctx.on.relation_changed(relation),
        testing.State(
            relations=[relation, _s3_relation()],
            planned_units=2,
            stored_states=[
                testing.StoredState(
                    name="_stored",
                    owner_path="LokiVmCharm",
                    content={
                        "peer_addresses_json": json.dumps([["loki-vm/1", "10.0.0.2"]]),
                        "config_drifted": False,
                    },
                )
            ],
        ),
    )

    peer_out = next(rel for rel in state_out.relations if rel.endpoint == "replicas")
    assert peer_out.local_unit_data is not None
    assert write_calls == []
    assert restart_calls == ["restart"]
    assert ready_checks == ["ready"]
    assert len(cluster_checks) >= 2
    assert peer_out.local_unit_data["restart-pending"] == "false"


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

    rendered = "\n".join(line for line in seen["config"].splitlines() if not line.startswith("#"))
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

    rendered = "\n".join(line for line in seen["config"].splitlines() if not line.startswith("#"))
    config_yaml = yaml.safe_load(rendered)

    assert config_yaml["schema_config"]["configs"][0]["object_store"] == "s3"
    s3 = config_yaml["storage_config"]["object_store"]["s3"]
    assert s3["bucket_name"] == "juju-s3-rel-10"
    assert s3["endpoint"] == "10.0.0.10:3900"
    assert s3["bucket_lookup_type"] == "path"
    assert "aws" not in config_yaml["storage_config"]
    assert config_yaml["compactor"]["working_directory"].endswith("compactor")
    assert isinstance(state_out.unit_status, testing.ActiveStatus)


def test_s3_relation_renders_bracketed_ipv6_for_loki_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both Thanos and legacy AWS clients require a bracketed IPv6 authority."""
    ctx = _context()
    seen: dict[str, str] = {}

    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr(
        "charm.loki.write_config_text",
        lambda config_text, **_: seen.update(config=config_text),
    )
    ctx.run(
        ctx.on.config_changed(),
        testing.State(
            relations=[_s3_relation(endpoint="http://[2001:db8::10]:3900")],
            planned_units=1,
        ),
    )

    rendered = "\n".join(line for line in seen["config"].splitlines() if not line.startswith("#"))
    config_yaml = yaml.safe_load(rendered)
    assert config_yaml["storage_config"]["object_store"]["s3"]["endpoint"] == (
        "[2001:db8::10]:3900"
    )
    assert config_yaml["ruler_storage"]["s3"]["endpoint"] == "[2001:db8::10]:3900"


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

    rendered = "\n".join(line for line in seen["config"].splitlines() if not line.startswith("#"))
    config_yaml = yaml.safe_load(rendered)

    assert config_yaml["schema_config"]["configs"][0]["object_store"] == "s3"
    s3 = config_yaml["storage_config"]["object_store"]["s3"]
    assert s3["endpoint"] == "10.0.0.10:3900"
    assert s3["bucket_name"] == "juju-s3-rel-10"
    assert s3["bucket_lookup_type"] == "path"
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

    rendered = "\n".join(line for line in seen["config"].splitlines() if not line.startswith("#"))
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
