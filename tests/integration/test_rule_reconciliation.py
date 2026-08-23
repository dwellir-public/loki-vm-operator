# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

import base64
import json
import re
import shlex
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
import yaml

jubilant = pytest.importorskip("jubilant")


class _RecordingJuju:
    """Record transport calls for the non-live log-post regression."""

    def __init__(self) -> None:
        self.ssh_calls: list[tuple[str, ...]] = []
        self.cli_calls: list[tuple[str, ...]] = []

    def ssh(self, target: str, command: str, *args: str) -> str:
        self.ssh_calls.append((target, command, *args))
        return ""

    def cli(self, *args: str) -> str:
        self.cli_calls.append(args)
        return ""


class _ShowUnitJuju:
    """Return deterministic show-unit data for relation contract helper tests."""

    def cli(self, *args: str) -> str:
        assert args == ("show-unit", "consumer/0", "--format", "json")
        return json.dumps(
            {
                "consumer/0": {
                    "relation-info": [
                        {
                            "endpoint": "receive",
                            "related-units": {
                                "loki-vm/0": {"data": {"endpoint": "http://loki:3100"}}
                            },
                        }
                    ]
                }
            }
        )


def test_log_post_uses_validated_base64_without_payload_interpolation() -> None:
    """Only an ASCII base64 token, never raw JSON, enters the remote shell command."""
    juju = _RecordingJuju()
    payload = '{"streams":[{"stream":{"unsafe":"a b;$(x)"},"values":[]}]}'

    _post_json(juju, unit="loki-vm/0", url="http://127.0.0.1/push", payload=payload)

    assert len(juju.ssh_calls) == 1
    command = juju.ssh_calls[0][1]
    assert payload not in command
    token = command.split("printf %s ", 1)[1].split(" |", 1)[0]
    assert re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", token)
    assert base64.b64decode(token, validate=True).decode("utf-8") == payload
    assert "base64 --decode" in command
    assert "--data-binary @-" in command


def test_remote_relation_value_reads_the_published_unit_contract() -> None:
    """Relation checks read provider-published remote unit data, not localhost state."""
    assert (
        _remote_relation_value(
            _ShowUnitJuju(), unit="consumer/0", endpoint="receive", key="endpoint"
        )
        == "http://loki:3100"
    )


def test_namespace_drift_replay_uses_owned_api_and_update_status_hook() -> None:
    """The live replay helpers delete only owned state then trigger periodic convergence."""
    juju = _RecordingJuju()

    _delete_owned_namespace(juju)
    _trigger_update_status(juju)

    assert juju.ssh_calls == [
        (
            "loki-vm/0",
            "curl",
            "-fsS",
            "-X",
            "DELETE",
            "http://127.0.0.1:3100/loki/api/v1/rules/juju-loki-vm",
        )
    ]
    assert juju.cli_calls == [("exec", "--unit", "loki-vm/0", "hooks/update-status")]


def _remote_relation_value(juju: Any, *, unit: str, endpoint: str, key: str) -> str:
    """Read one exact value published by a remote unit over a named relation endpoint."""
    document = json.loads(juju.cli("show-unit", unit, "--format", "json"))
    relation_infos = document[unit].get("relation-info", [])
    values = [
        str(remote["data"][key])
        for relation in relation_infos
        if relation.get("endpoint") == endpoint
        for remote in relation.get("related-units", {}).values()
        if key in remote.get("data", {})
    ]
    if len(values) != 1:
        raise AssertionError(f"Expected one {endpoint}.{key} value on {unit}, found {values!r}")
    return values[0]


def _post_json(juju: Any, *, unit: str, url: str, payload: str) -> None:
    """POST JSON as a validated base64 token safe for Juju's remote shell."""
    token = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    if re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", token) is None:
        raise ValueError("base64 payload token contains unsafe characters")
    command = (
        f"printf %s {token} | base64 --decode | "
        "curl -fsS -H 'Content-Type: application/json' --data-binary @- "
        f"{shlex.quote(url)}"
    )
    juju.ssh(unit, command)


def _rules(group: str, alert: str, source: str) -> str:
    return json.dumps(
        {
            "groups": [
                {
                    "name": group,
                    "rules": [
                        {
                            "alert": alert,
                            "expr": f'count_over_time({{job="task7a",source="{source}"}}[1m]) > 0',
                            "for": "0s",
                            "labels": {"severity": f"{source}-warning", "source": source},
                        }
                    ],
                }
            ]
        },
        separators=(",", ":"),
    )


def _wait_for_rules(
    juju: Any,
    expected: list[tuple[str, str, dict[str, str]]],
    *,
    timeout: float = 180,
) -> None:
    """Wait for exact deterministic group/alert order and preserved source labels."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        output = juju.ssh(
            "loki-vm/0",
            "curl",
            "-fsS",
            "http://127.0.0.1:3100/loki/api/v1/rules",
        )
        document = yaml.safe_load(output) or {}
        actual = [
            (
                str(group.get("name")),
                str(rule.get("name") or rule.get("alert")),
                {str(key): str(value) for key, value in rule.get("labels", {}).items()},
            )
            for group in document.get("juju-loki-vm", [])
            for rule in group.get("rules", [])
        ]
        if actual == expected:
            return
        time.sleep(5)
    raise AssertionError(f"Loki did not load expected ordered rules: {expected}")


def _expected(group: str, alert: str, source: str) -> tuple[str, str, dict[str, str]]:
    """Return one complete expected live rule including its entire label mapping."""
    return (group, alert, {"severity": f"{source}-warning", "source": source})


def _assert_relation_contracts(juju: Any, expected: tuple[str, str]) -> None:
    """Assert datasource and log-gateway relation endpoints remain unchanged."""
    actual = (
        _remote_relation_value(
            juju,
            unit="rule-source-a/0",
            endpoint="send-loki-logs",
            key="endpoint",
        ),
        _remote_relation_value(
            juju,
            unit="rule-source-a/0",
            endpoint="receive-datasource",
            key="grafana_source_host",
        ),
    )
    assert actual == expected


def _delete_owned_namespace(juju: Any, *, unit: str = "loki-vm/0") -> None:
    """Delete the charm namespace so lifecycle replay must recreate it."""
    juju.ssh(
        unit,
        "curl",
        "-fsS",
        "-X",
        "DELETE",
        "http://127.0.0.1:3100/loki/api/v1/rules/juju-loki-vm",
    )


def _trigger_update_status(juju: Any, *, unit: str = "loki-vm/0") -> None:
    """Invoke the real periodic lifecycle hook to request rule replay."""
    juju.cli("exec", "--unit", unit, "hooks/update-status")


def _assert_fresh_log_round_trip(juju: Any, *, timeout: float = 120) -> None:
    timestamp = time.time_ns()
    line = f"task7a-continuity-{timestamp}"
    payload = json.dumps(
        {"streams": [{"stream": {"job": "task7a"}, "values": [[str(timestamp), line]]}]}
    )
    _post_json(
        juju,
        unit="loki-vm/0",
        url="http://127.0.0.1:3100/loki/api/v1/push",
        payload=payload,
    )
    query = quote('{job="task7a"}', safe="")
    url = (
        "http://127.0.0.1:3100/loki/api/v1/query_range"
        f"?query={query}&start={timestamp - 1_000_000_000}&end={timestamp + 1_000_000_000}"
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        output = juju.ssh("loki-vm/0", "curl", "-fsS", url)
        if line in output:
            return
        time.sleep(5)
    raise AssertionError("Fresh log was not queryable after rule reconciliation")


def test_two_relation_snapshots_reach_ruler_without_breaking_logs(
    juju: Any,
    charm: Path,
    rule_provider_charm: Path,
) -> None:
    """Load two app snapshots through the real relation, ruler API, and Loki process."""
    juju.deploy(charm.resolve(), app="loki-vm")
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
    relation_contracts = (
        _remote_relation_value(
            juju,
            unit="rule-source-a/0",
            endpoint="send-loki-logs",
            key="endpoint",
        ),
        _remote_relation_value(
            juju,
            unit="rule-source-a/0",
            endpoint="receive-datasource",
            key="grafana_source_host",
        ),
    )

    _wait_for_rules(
        juju,
        [_expected("source-a", "SourceAAlert", "a"), _expected("source-b", "SourceBAlert", "b")],
    )
    _assert_relation_contracts(juju, relation_contracts)
    _assert_fresh_log_round_trip(juju)

    juju.config("rule-source-a", {"alert-rules": "not-json"})
    _wait_for_rules(
        juju,
        [_expected("source-a", "SourceAAlert", "a"), _expected("source-b", "SourceBAlert", "b")],
    )
    _assert_relation_contracts(juju, relation_contracts)
    _assert_fresh_log_round_trip(juju)

    juju.config(
        "rule-source-b",
        {"alert-rules": _rules("source-b-updated", "SourceBUpdatedAlert", "b")},
    )
    _wait_for_rules(
        juju,
        [
            _expected("source-a", "SourceAAlert", "a"),
            _expected("source-b-updated", "SourceBUpdatedAlert", "b"),
        ],
    )
    _assert_relation_contracts(juju, relation_contracts)
    _assert_fresh_log_round_trip(juju)

    juju.config("rule-source-a", {"alert-rules": " " * (60 * 1024)})
    _wait_for_rules(
        juju,
        [
            _expected("source-a", "SourceAAlert", "a"),
            _expected("source-b-updated", "SourceBUpdatedAlert", "b"),
        ],
    )
    _assert_relation_contracts(juju, relation_contracts)
    _assert_fresh_log_round_trip(juju)

    juju.config(
        "rule-source-a",
        {"alert-rules": _rules("source-a-updated", "SourceAUpdatedAlert", "a")},
    )
    _wait_for_rules(
        juju,
        [
            _expected("source-a-updated", "SourceAUpdatedAlert", "a"),
            _expected("source-b-updated", "SourceBUpdatedAlert", "b"),
        ],
    )
    _assert_relation_contracts(juju, relation_contracts)
    _assert_fresh_log_round_trip(juju)

    juju.config("rule-source-a", {"omit-alert-rules": True})
    _wait_for_rules(juju, [_expected("source-b-updated", "SourceBUpdatedAlert", "b")])
    _assert_relation_contracts(juju, relation_contracts)
    _assert_fresh_log_round_trip(juju)

    juju.remove_relation(
        "rule-source-b:send-loki-logs",
        "loki-vm:loki_push_api",
    )
    _wait_for_rules(juju, [])
    _assert_relation_contracts(juju, relation_contracts)
    _assert_fresh_log_round_trip(juju)

    juju.config("rule-source-a", {"omit-alert-rules": False})
    _wait_for_rules(juju, [_expected("source-a-updated", "SourceAUpdatedAlert", "a")])
    juju.ssh("loki-vm/0", "sudo", "systemctl", "restart", "loki")
    _wait_for_rules(juju, [_expected("source-a-updated", "SourceAUpdatedAlert", "a")])
    _delete_owned_namespace(juju)
    _wait_for_rules(juju, [])
    _trigger_update_status(juju)
    _wait_for_rules(juju, [_expected("source-a-updated", "SourceAUpdatedAlert", "a")])
    _assert_relation_contracts(juju, relation_contracts)
    _assert_fresh_log_round_trip(juju)
