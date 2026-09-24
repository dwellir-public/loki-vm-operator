"""Measured 1,024-source envelope, independent of live Juju relation validation."""

import json
import uuid

import pytest
from charms.dwellir_observability.v0 import alert_rule_transport as transport


def _corpus(count=1024, rules=4):
    snapshots = {}
    for index in range(count):
        labels = {
            "juju_model": f"model-{index}",
            "juju_model_uuid": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"model-{index}")),
            "juju_application": f"workload-{index}",
            "juju_unit": f"workload-{index}/0",
            "juju_charm": "reference",
        }
        snapshots[index] = [
            {
                "name": f"{labels['juju_model_uuid']}-workload-{index}-rule-{number}",
                "rules": [
                    {
                        "alert": f"WorkloadFault{number}",
                        "expr": "sum(rate(errors_total{"
                        + ",".join(f'{k}="{v}"' for k, v in labels.items())
                        + "}[5m])) > 0",
                        "for": "0s",
                        "labels": dict(labels, severity="warning"),
                        "annotations": {
                            "summary": "Workload reported an error",
                            "description": (
                                "Check the workload service and its upstream dependencies before "
                                "taking corrective action. Confirm fresh samples "
                                "and the exact Juju topology."
                            ),
                            "runbook_url": "https://example.invalid/test-only/runbook",
                        },
                    }
                ],
            }
            for number in range(rules)
        ]
    return snapshots


@pytest.mark.parametrize("count,rules", [(1024, 4), (1025, 1), (2048, 1)])
def test_source_corpus_preserves_all_groups_and_topology_through_wire_and_cache(count, rules):
    snapshots = _corpus(count, rules)
    if count == 2048:
        # Isolate count scaling from the separate per-value wire-size policy.
        snapshots = {
            i: [{"name": f"g-{i}", "rules": [{"alert": "Example", "expr": "up"}]}]
            for i in range(count)
        }
    groups = [group for values in snapshots.values() for group in values]
    raw = json.dumps({"groups": groups}, sort_keys=True)
    assert len(raw.encode()) > 60 * 1024
    packed = transport.encode(raw, {transport.ENCODINGS_KEY: transport.ENCODINGS})
    assert len(packed) < 60 * 1024
    assert json.loads(transport.decode(packed))["groups"] == groups
    import rule_reconciler as module

    assert module.parse_rule_groups(packed) == groups

    expected_groups = groups

    class Client:
        def replace_namespace(self, groups, **kwargs):
            assert groups == expected_groups
            return []

    sources = [
        module.RelationRuleSource(i, json.dumps({"groups": values}))
        for i, values in snapshots.items()
    ]
    cache = []
    reconciler = module.LokiRuleReconciler(Client())
    first = reconciler.reconcile(sources, cache_value=None, persist=cache.append)
    assert first.committed
    assert len(first.accepted_groups) == count * rules
    second = reconciler.reconcile(sources, cache_value=cache[-1], persist=cache.append)
    assert second.committed
    assert len(cache[-1]) < 60 * 1024


def test_high_entropy_2048_source_document_reports_wire_capacity():
    groups = [group for values in _corpus(2048, 1).values() for group in values]
    with pytest.raises(ValueError, match="receiver capacity"):
        transport.encode(
            json.dumps({"groups": groups}, sort_keys=True),
            {transport.ENCODINGS_KEY: transport.ENCODINGS},
        )
