# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

import base64
import ipaddress
import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
import yaml

jubilant = pytest.importorskip("jubilant")

NAMESPACE = "juju-loki-vm"


def _remote_relation_values(
    juju: Any,
    *,
    unit: str,
    endpoint: str,
    key: str,
) -> dict[str, str]:
    """Read provider-published values keyed by remote unit."""
    document = json.loads(juju.cli("show-unit", unit, "--format", "json"))
    values = {
        str(remote_name): str(remote["data"][key])
        for relation in document[unit].get("relation-info", [])
        if relation.get("endpoint") == endpoint
        for remote_name, remote in relation.get("related-units", {}).items()
        if key in remote.get("data", {})
    }
    if not values:
        raise AssertionError(f"Expected {endpoint}.{key} values on {unit}, found none")
    return values


def _relation_contracts(juju: Any) -> tuple[dict[str, str], dict[str, str]]:
    """Return per-unit gateway-backend and datasource endpoint contracts."""
    return (
        _remote_relation_values(
            juju,
            unit="rule-source-a/0",
            endpoint="send-loki-logs",
            key="endpoint",
        ),
        _remote_relation_values(
            juju,
            unit="rule-source-a/0",
            endpoint="receive-datasource",
            key="grafana_source_host",
        ),
    )


def test_remote_relation_values_model_the_multiunit_contract() -> None:
    class FakeJuju:
        def cli(self, *args: str) -> str:
            assert args == ("show-unit", "source/0", "--format", "json")
            return json.dumps(
                {
                    "source/0": {
                        "relation-info": [
                            {
                                "endpoint": "backend",
                                "related-units": {
                                    "loki-vm/1": {"data": {"endpoint": "http://one:3100"}},
                                    "loki-vm/2": {"data": {"endpoint": "http://two:3100"}},
                                },
                            }
                        ]
                    }
                }
            )

    assert _remote_relation_values(
        FakeJuju(), unit="source/0", endpoint="backend", key="endpoint"
    ) == {
        "loki-vm/1": "http://one:3100",
        "loki-vm/2": "http://two:3100",
    }


def _assert_surviving_relation_contracts(
    before: tuple[dict[str, str], dict[str, str]],
    after: tuple[dict[str, str], dict[str, str]],
    *,
    surviving_unit: str,
) -> None:
    """Assert each relation still publishes the surviving unit's original value."""
    for before_values, after_values in zip(before, after, strict=True):
        assert after_values == {surviving_unit: before_values[surviving_unit]}


def test_wait_for_configured_s3_endpoint_uses_requested_live_unit() -> None:
    class FakeJuju:
        def __init__(self) -> None:
            self.calls: list[tuple[str, ...]] = []

        def ssh(self, *args: str) -> str:
            self.calls.append(args)
            return """
storage_config:
  object_store:
    s3:
      endpoint: '[fd00::1]:3900'
ruler_storage:
  s3:
    endpoint: '[fd00::1]:3900'
"""

    juju = FakeJuju()
    _wait_for_configured_s3_endpoint(
        juju,
        unit="loki-vm/1",
        endpoint="[fd00::1]:3900",
        timeout=0.1,
    )

    assert juju.calls == [
        (
            "loki-vm/1",
            "sudo",
            "cat",
            "/etc/loki/config.yml",
        )
    ]


def test_wait_for_baseline_ready_uses_agent_and_live_readiness_not_workload_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checks: list[tuple[str, tuple[str, ...]]] = []

    class FakeJuju:
        def __init__(self) -> None:
            self.ssh_calls: list[tuple[str, ...]] = []

        def wait(self, ready: Any, *, timeout: float) -> None:
            assert timeout == 30
            assert ready(object())

        def ssh(self, *args: str) -> str:
            self.ssh_calls.append(args)
            return "ready\n"

    monkeypatch.setattr(
        jubilant,
        "all_active",
        lambda status, *apps: checks.append(("active", apps)) or True,
    )
    monkeypatch.setattr(
        jubilant,
        "all_agents_idle",
        lambda status, *apps: checks.append(("idle", apps)) or True,
    )
    juju = FakeJuju()

    _wait_for_baseline_ready(
        juju,
        units=("loki-vm/0", "loki-vm/1"),
        timeout=30,
    )

    assert checks == [("active", ("garage-vm",)), ("idle", ("loki-vm",))]
    assert juju.ssh_calls == [
        (unit, "curl", "-fsS", "http://127.0.0.1:3100/ready")
        for unit in ("loki-vm/0", "loki-vm/1")
    ]


def test_wait_for_leader_accepts_jubilant_titlecase_boolean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Task:
        success = True
        stdout = "True\n"

    class FakeJuju:
        def exec(self, command: str, *, unit: str, wait: int) -> Task:
            assert (command, unit, wait) == ("is-leader", "loki-vm/1", 30)
            return Task()

    monkeypatch.setattr(time, "sleep", lambda seconds: None)

    _wait_for_leader(FakeJuju(), unit="loki-vm/1", timeout=0.01)


def test_workload_version_parses_loki_cli_output() -> None:
    class FakeJuju:
        def ssh(self, *args: str) -> str:
            assert args == ("loki-vm/0", "loki", "--version")
            return "loki, version 3.4.6 (branch: HEAD, revision: abc)"

    assert _workload_version(FakeJuju(), unit="loki-vm/0") == "3.4.6"


def _garage_addresses(juju: Any) -> tuple[str, str]:
    """Return Garage's model-private IPv4 and global model IPv6 addresses."""
    addresses = [
        ipaddress.ip_address(value) for value in juju.ssh("garage-vm/0", "hostname", "-I").split()
    ]
    ipv4 = next(address for address in addresses if address.version == 4 and address.is_private)
    ipv6 = next(
        address for address in addresses if address.version == 6 and not address.is_link_local
    )
    return str(ipv4), str(ipv6)


def _workload_version(juju: Any, *, unit: str) -> str:
    """Read the installed Loki version without relying on charm status."""
    output = juju.ssh(unit, "loki", "--version")
    match = re.search(r"\bversion\s+([0-9]+(?:\.[0-9]+){1,3})\b", output)
    if match is None:
        raise AssertionError(f"Could not parse Loki version on {unit}")
    return match.group(1)


def _wait_for_baseline_ready(
    juju: Any,
    *,
    units: tuple[str, ...],
    timeout: float = 20 * 60,
) -> None:
    """Wait for baseline Loki without relying on its known S3 status false-negative."""
    juju.wait(
        lambda status: jubilant.all_active(status, "garage-vm")
        and jubilant.all_agents_idle(status, "loki-vm"),
        timeout=timeout,
    )
    pending = set(units)
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for unit in tuple(sorted(pending)):
            try:
                output = juju.ssh(unit, "curl", "-fsS", "http://127.0.0.1:3100/ready")
            except Exception:  # noqa: BLE001 - Loki readiness is transient during startup.
                continue
            if output.strip() == "ready":
                pending.remove(unit)
        if pending:
            time.sleep(5)
    if pending:
        raise AssertionError(f"Baseline Loki units did not become ready: {sorted(pending)!r}")


def _wait_for_configured_s3_endpoint(
    juju: Any,
    *,
    unit: str,
    endpoint: str,
    timeout: float = 240,
) -> None:
    """Wait until both Loki Thanos stores use the exact related S3 authority."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            rendered = juju.ssh(
                unit,
                "sudo",
                "cat",
                "/etc/loki/config.yml",
            )
            config = yaml.safe_load(rendered)
            chunk_endpoint = config["storage_config"]["object_store"]["s3"]["endpoint"]
            ruler_endpoint = config["ruler_storage"]["s3"]["endpoint"]
        except Exception:  # noqa: BLE001 - relation/config convergence is transient.
            time.sleep(5)
            continue
        if chunk_endpoint == endpoint and ruler_endpoint == endpoint:
            return
        time.sleep(5)
    raise AssertionError(f"Loki did not converge both S3 stores to {endpoint!r}")


def _post_json(juju: Any, *, unit: str, url: str, payload: str) -> None:
    """POST JSON without interpolating its content into the remote shell."""
    token = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    if re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", token) is None:
        raise ValueError("base64 payload token contains unsafe characters")
    juju.ssh(
        unit,
        f"printf %s {token} | base64 --decode | "
        "curl -fsS -H 'Content-Type: application/json' --data-binary @- "
        f"{url}",
    )


def _push_log(juju: Any, *, unit: str, marker: str) -> int:
    """Push one uniquely timestamped log line and return its timestamp."""
    timestamp = time.time_ns()
    payload = json.dumps(
        {
            "streams": [
                {
                    "stream": {"job": "task7a-s3", "phase": marker},
                    "values": [[str(timestamp), marker]],
                }
            ]
        },
        separators=(",", ":"),
    )
    _post_json(
        juju,
        unit=unit,
        url="http://127.0.0.1:3100/loki/api/v1/push",
        payload=payload,
    )
    return timestamp


def _wait_for_log(
    juju: Any,
    *,
    unit: str,
    marker: str,
    timestamp: int,
    timeout: float = 180,
) -> None:
    """Wait until an exact log marker is queryable from one Loki unit."""
    query = quote(f'{{job="task7a-s3",phase="{marker}"}}', safe="")
    url = (
        "http://127.0.0.1:3100/loki/api/v1/query_range"
        f"?query={query}&start={timestamp - 1_000_000_000}&end={timestamp + 1_000_000_000}"
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            output = juju.ssh(unit, "curl", "-fsS", url)
        except Exception:  # noqa: BLE001 - transient process/restart failures are expected live.
            time.sleep(5)
            continue
        if marker in output:
            return
        time.sleep(5)
    raise AssertionError(f"Log marker {marker!r} was not queryable from {unit}")


def _rules(group: str, alert: str, source: str) -> str:
    """Return one topology-labelled standard Loki alert-rule document."""
    return json.dumps(
        {
            "groups": [
                {
                    "name": group,
                    "rules": [
                        {
                            "alert": alert,
                            "expr": (
                                f'count_over_time({{job="task7a-s3",source="{source}"}}[1m]) > 0'
                            ),
                            "for": "0s",
                            "labels": {"severity": f"{source}-warning", "source": source},
                        }
                    ],
                }
            ]
        },
        separators=(",", ":"),
    )


def _expected_rules() -> list[tuple[str, str, dict[str, str]]]:
    return [
        ("source-a", "SourceAAlert", {"severity": "a-warning", "source": "a"}),
        ("source-b", "SourceBAlert", {"severity": "b-warning", "source": "b"}),
    ]


def _rule_tuples(document: dict[str, Any]) -> list[tuple[str, str, dict[str, str]]]:
    """Extract deterministic tuples from Loki's namespace-keyed API response."""
    groups = document.get(NAMESPACE, [])
    if not isinstance(groups, list):
        return []
    return [
        (
            str(group.get("name")),
            str(rule.get("alert") or rule.get("name")),
            {str(key): str(value) for key, value in rule.get("labels", {}).items()},
        )
        for group in groups
        for rule in group.get("rules", [])
    ]


def test_rule_tuples_reads_loki_namespace_envelope_in_order() -> None:
    """Model Loki 3.4's namespace-keyed response, including stable group order."""
    document = {
        NAMESPACE: [
            {
                "name": "source-a",
                "rules": [
                    {
                        "alert": "SourceAAlert",
                        "labels": {"severity": "a-warning", "source": "a"},
                    }
                ],
            },
            {
                "name": "source-b",
                "rules": [
                    {
                        "alert": "SourceBAlert",
                        "labels": {"severity": "b-warning", "source": "b"},
                    }
                ],
            },
        ]
    }

    assert _rule_tuples(document) == _expected_rules()


def _wait_for_namespace(
    juju: Any,
    *,
    unit: str,
    expected: list[tuple[str, str, dict[str, str]]] | None = None,
    timeout: float = 240,
) -> None:
    """Wait for the exact shared S3 namespace content and deterministic ordering."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            output = juju.ssh(
                unit,
                "curl",
                "-fsS",
                f"http://127.0.0.1:3100/loki/api/v1/rules/{NAMESPACE}",
            )
            document = yaml.safe_load(output)
        except Exception:  # noqa: BLE001 - ruler propagation is eventually consistent.
            time.sleep(5)
            continue
        if isinstance(document, dict) and _rule_tuples(document) == (
            expected or _expected_rules()
        ):
            return
        time.sleep(5)
    raise AssertionError(f"Loki on {unit} did not expose the exact shared rule namespace")


def _wait_for_leader(juju: Any, *, unit: str, timeout: float = 180) -> None:
    """Wait until Juju confirms the surviving Loki unit is application leader."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            task = juju.exec("is-leader", unit=unit, wait=30)
            if task.success and task.stdout.strip().lower() == "true":
                return
        except Exception:  # noqa: BLE001 - leadership changes are transient.
            pass
        time.sleep(5)
    raise AssertionError(f"{unit} did not become leader")


def _gracefully_stop_and_remove(juju: Any, *, unit: str) -> None:
    """Let Loki flush on SIGTERM, then remove the unit and its local storage."""
    juju.ssh(unit, "sudo", "systemctl", "stop", "loki")
    juju.remove_unit(unit, destroy_storage=True)


def test_s3_upgrade_preserves_logs_rules_and_leader_recovery(
    juju: Any,
    charm: Path,
    baseline_charm: Path,
    baseline_loki_version: str,
    garage_charm: Path,
    rule_provider_charm: Path,
) -> None:
    """Prove legacy-S3 upgrade, shared rules, and leader recovery against Garage."""
    juju.deploy(garage_charm.resolve(), app="garage-vm")
    juju.wait(jubilant.all_active, timeout=20 * 60)
    garage_ipv4, garage_ipv6 = _garage_addresses(juju)
    juju.config("garage-vm", {"external-url": garage_ipv4})
    juju.wait(jubilant.all_active, timeout=10 * 60)
    juju.deploy(baseline_charm.resolve(), app="loki-vm")
    juju.integrate("garage-vm:s3", "loki-vm:s3")
    _wait_for_baseline_ready(juju, units=("loki-vm/0",))
    observed_baseline_version = _workload_version(juju, unit="loki-vm/0")
    assert observed_baseline_version == baseline_loki_version

    pre_marker = f"task7a-pre-upgrade-{time.time_ns()}"
    pre_timestamp = _push_log(juju, unit="loki-vm/0", marker=pre_marker)
    _wait_for_log(juju, unit="loki-vm/0", marker=pre_marker, timestamp=pre_timestamp)

    # Add the replacement only after ingesting the marker. With replication factor one,
    # /1 cannot have the marker in its WAL. Removing /0 with its storage forces graceful
    # flush-on-shutdown; a subsequent /1-only query can therefore succeed only via S3.
    juju.add_unit("loki-vm")
    _wait_for_baseline_ready(juju, units=("loki-vm/0", "loki-vm/1"))
    _gracefully_stop_and_remove(juju, unit="loki-vm/0")
    _wait_for_leader(juju, unit="loki-vm/1")
    juju.wait(jubilant.all_active, timeout=20 * 60)
    _wait_for_log(juju, unit="loki-vm/1", marker=pre_marker, timestamp=pre_timestamp)

    juju.refresh("loki-vm", path=charm.resolve())
    juju.wait(jubilant.all_active, timeout=20 * 60)
    assert _workload_version(juju, unit="loki-vm/1") == observed_baseline_version
    _wait_for_log(juju, unit="loki-vm/1", marker=pre_marker, timestamp=pre_timestamp)

    fresh_marker = f"task7a-post-upgrade-{time.time_ns()}"
    fresh_timestamp = _push_log(juju, unit="loki-vm/1", marker=fresh_marker)
    _wait_for_log(juju, unit="loki-vm/1", marker=fresh_marker, timestamp=fresh_timestamp)

    juju.config("garage-vm", {"external-url": f"[{garage_ipv6}]"})
    juju.wait(jubilant.all_active, timeout=20 * 60)
    _wait_for_configured_s3_endpoint(
        juju,
        unit="loki-vm/1",
        endpoint=f"[{garage_ipv6}]:3900",
    )
    _wait_for_log(juju, unit="loki-vm/1", marker=pre_marker, timestamp=pre_timestamp)
    _wait_for_log(juju, unit="loki-vm/1", marker=fresh_marker, timestamp=fresh_timestamp)

    juju.add_unit("loki-vm")
    juju.deploy(
        rule_provider_charm.resolve(),
        app="rule-source-a",
        config={"alert-rules": _rules("source-a", "SourceAAlert", "a")},
    )
    juju.deploy(
        rule_provider_charm.resolve(),
        app="rule-source-b",
        config={"alert-rules": _rules("source-b", "SourceBAlert", "b")},
    )
    juju.integrate("rule-source-a:send-loki-logs", "loki-vm:loki_push_api")
    juju.integrate("rule-source-b:send-loki-logs", "loki-vm:loki_push_api")
    juju.integrate("rule-source-a:receive-datasource", "loki-vm:grafana-source")
    juju.wait(jubilant.all_active, timeout=20 * 60)
    relation_contracts = _relation_contracts(juju)
    _wait_for_namespace(juju, unit="loki-vm/1")
    _wait_for_namespace(juju, unit="loki-vm/2")
    assert _relation_contracts(juju) == relation_contracts
    _wait_for_log(juju, unit="loki-vm/2", marker=pre_marker, timestamp=pre_timestamp)
    _wait_for_log(juju, unit="loki-vm/2", marker=fresh_marker, timestamp=fresh_timestamp)
    _gracefully_stop_and_remove(juju, unit="loki-vm/1")
    _wait_for_leader(juju, unit="loki-vm/2")
    juju.wait(jubilant.all_active, timeout=20 * 60)
    _wait_for_namespace(juju, unit="loki-vm/2")
    _assert_surviving_relation_contracts(
        relation_contracts, _relation_contracts(juju), surviving_unit="loki-vm/2"
    )
    juju.config(
        "rule-source-a",
        {"alert-rules": _rules("source-a-failover", "SourceAFailoverAlert", "a")},
    )
    failover_rules = [
        (
            "source-a-failover",
            "SourceAFailoverAlert",
            {"severity": "a-warning", "source": "a"},
        ),
        ("source-b", "SourceBAlert", {"severity": "b-warning", "source": "b"}),
    ]
    _wait_for_namespace(juju, unit="loki-vm/2", expected=failover_rules)
    _assert_surviving_relation_contracts(
        relation_contracts, _relation_contracts(juju), surviving_unit="loki-vm/2"
    )
    _wait_for_log(juju, unit="loki-vm/2", marker=pre_marker, timestamp=pre_timestamp)
    _wait_for_log(juju, unit="loki-vm/2", marker=fresh_marker, timestamp=fresh_timestamp)
    post_failover_marker = f"task7a-post-failover-{time.time_ns()}"
    post_failover_timestamp = _push_log(
        juju,
        unit="loki-vm/2",
        marker=post_failover_marker,
    )
    _wait_for_log(
        juju,
        unit="loki-vm/2",
        marker=post_failover_marker,
        timestamp=post_failover_timestamp,
    )
