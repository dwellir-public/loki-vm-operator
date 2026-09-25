"""Admission behavior at the receiver transaction boundary."""

import json
from typing import Any

import pytest

import rule_reconciler as module


@pytest.fixture
def run_receiver():
    class Client:
        def replace_namespace(self, groups, **kwargs):
            return []

    receiver = module.LokiRuleReconciler(Client())
    cache: list[Any] = [None]

    def run(payloads):
        result = receiver.reconcile(
            [module.RelationRuleSource(key, raw) for key, raw in payloads.items()],
            cache_value=cache[-1],
            persist=cache.append,
        )
        return result, module._decode_cache(cache[-1]).snapshots

    return run


def payload(key):
    return json.dumps(
        {"groups": [{"name": f"g-{key}", "rules": [{"alert": "Example", "expr": "up"}]}]}
    )


@pytest.mark.parametrize("count", [31, 32, 33, 141, 1023, 1024, 1025])
def test_source_count_never_starves_high_identifiers(run_receiver, count):
    sources: dict[int, str | None] = {
        100000 + i: payload(i) if i % 2 or i == count - 1 else "{}" for i in range(count)
    }
    result, snapshots = run_receiver(sources)
    assert result.received_sources == count
    assert 100000 + count - 1 in snapshots
    assert len(snapshots) == sum(
        bool(json.loads(raw or "{}").get("groups")) for raw in sources.values()
    )
    sources[100000 + count - 1] = None
    _, retained = run_receiver(sources)
    assert retained == snapshots
    sources[100000 + count - 1] = "malformed"
    result, retained = run_receiver(sources)
    assert retained == snapshots
    assert 100000 + count - 1 in getattr(result, "errors", getattr(result, "rejected_sources", ()))
    sources[100000 + count - 1] = '{"groups":[]}'
    _, withdrawn = run_receiver(sources)
    assert 100000 + count - 1 not in withdrawn
    _, removed = run_receiver({})
    assert not removed


def test_byte_overload_preserves_owner_then_recovers(run_receiver, monkeypatch):
    _, initial = run_receiver({99999: payload(99999)})
    size = len(json.dumps(initial[99999], ensure_ascii=False).encode())
    monkeypatch.setattr(module, "MAX_RULE_BYTES", size + 20)
    result, retained = run_receiver({1: payload(1), 99999: payload(99999)})
    assert retained == initial
    assert 1 in getattr(result, "errors", getattr(result, "rejected_sources", ()))
    _, recovered = run_receiver({1: payload(1), 99999: '{"groups":[]}'})
    assert set(recovered) == {1}


def test_large_compressed_source_is_admitted_once(run_receiver):
    from cosl import LZMABase64

    document = json.loads(payload(90000))
    document["groups"][0]["rules"][0]["annotations"] = {"description": "x" * 70000}
    result, snapshots = run_receiver({90000: LZMABase64.compress(json.dumps(document))})
    assert result.accepted_sources == 1
    assert snapshots[90000] == document["groups"]
