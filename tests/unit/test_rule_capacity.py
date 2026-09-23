"""Measured 1,024-source envelope, independent of live Juju relation validation."""

import json
import uuid

from charms.dwellir_observability.v0 import alert_rule_transport as transport


def _corpus():
    snapshots = {}
    for index in range(1024):
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
            for number in range(4)
        ]
    return snapshots


def test_1024_source_corpus_preserves_all_groups_and_topology_through_wire_and_cache():
    snapshots = _corpus()
    groups = [group for values in snapshots.values() for group in values]
    raw = json.dumps({"groups": groups}, sort_keys=True)
    assert len(raw.encode()) > 2 * 1024 * 1024
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
    assert len(first.accepted_groups) == 4096
    second = reconciler.reconcile(sources, cache_value=cache[-1], persist=cache.append)
    assert second.committed
    assert len(cache[-1]) < 60 * 1024
