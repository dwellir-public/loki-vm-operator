# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

import base64
import json
import logging
import zlib
from pathlib import Path
from typing import Any

import pytest

from rule_reconciler import (
    CACHE_KEY,
    CACHE_VERSION,
    MAX_API_OPERATIONS,
    MAX_APPLY_SECONDS,
    MAX_CACHE_DECODED_BYTES,
    MAX_CACHE_NODES,
    MAX_CACHE_VALUE_BYTES,
    MAX_RELATION_VALUE_BYTES,
    MAX_SOURCE_RELATIONS,
    MAX_TOTAL_GROUPS,
    MAX_TOTAL_RULES,
    InvalidRuleCacheError,
    InvalidRuleSnapshotError,
    LokiRulerApiClient,
    LokiRuleReconciler,
    RelationRuleSource,
    _decode_cache,
    merge_rule_groups,
    parse_rule_groups,
    prepare_filesystem_rule_store,
)


def test_prepare_filesystem_rule_store_creates_auth_disabled_tenant(tmp_path: Path) -> None:
    """The filesystem ruler API requires its implicit tenant directory to exist."""
    prepare_filesystem_rule_store(tmp_path)

    assert (tmp_path / "rules" / "fake").is_dir()
    assert (tmp_path / "ruler-tmp").is_dir()


def _raw(*groups: dict[str, Any]) -> str:
    return json.dumps({"groups": list(groups)}, separators=(",", ":"))


def _group(name: str, expression: str = '{job="demo"}') -> dict[str, Any]:
    return {
        "name": name,
        "rules": [
            {
                "alert": f"{name}Alert",
                "expr": expression,
                "for": "0s",
                "labels": {"source": name},
            }
        ],
    }


def _encoded_cache(document: Any) -> str:
    raw = json.dumps(document, separators=(",", ":")).encode()
    return base64.b64encode(zlib.compress(raw, level=9)).decode("ascii")


def _cache_document(
    *,
    relations: dict[str, Any] | None = None,
    accepted: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "accepted": accepted or [],
        "relations": relations or {},
        "version": CACHE_VERSION,
    }


def test_decode_cache_rejects_encoded_value_at_juju_boundary() -> None:
    with pytest.raises(InvalidRuleCacheError):
        _decode_cache("A" * MAX_CACHE_VALUE_BYTES)


def test_decode_cache_rejects_decoded_value_over_limit_and_compressed_tail() -> None:
    oversized = base64.b64encode(
        zlib.compress(b" " * (MAX_CACHE_DECODED_BYTES + 1), level=9)
    ).decode("ascii")
    valid_with_tail = base64.b64encode(
        zlib.compress(json.dumps(_cache_document()).encode()) + b"tail"
    ).decode("ascii")

    with pytest.raises(InvalidRuleCacheError):
        _decode_cache(oversized)
    with pytest.raises(InvalidRuleCacheError):
        _decode_cache(valid_with_tail)


def test_decode_cache_rejects_excessive_depth_and_nodes() -> None:
    deep: Any = 0
    for _ in range(34):
        deep = [deep]
    too_deep = _cache_document(accepted=[{"name": "deep", "rules": [], "extra": deep}])
    too_wide = _cache_document(
        accepted=[{"name": "wide", "rules": [], "extra": [0] * (MAX_CACHE_NODES + 1)}]
    )

    with pytest.raises(InvalidRuleCacheError):
        _decode_cache(_encoded_cache(too_deep))
    with pytest.raises(InvalidRuleCacheError):
        _decode_cache(_encoded_cache(too_wide))


def test_decode_cache_rejects_more_than_32_cached_relations() -> None:
    relations = {str(index): [] for index in range(MAX_SOURCE_RELATIONS + 1)}

    with pytest.raises(InvalidRuleCacheError):
        _decode_cache(_encoded_cache(_cache_document(relations=relations)))


def test_decode_cache_normalizes_invalid_relation_snapshot_error() -> None:
    document = _cache_document(relations={"1": [{"name": "x" * 513, "rules": []}]})

    with pytest.raises(InvalidRuleCacheError):
        _decode_cache(_encoded_cache(document))


def test_decode_cache_rejects_accepted_state_inconsistent_with_relation_snapshots() -> None:
    document = _cache_document(
        relations={"1": [_group("relation")]},
        accepted=[_group("unrelated")],
    )

    with pytest.raises(InvalidRuleCacheError):
        _decode_cache(_encoded_cache(document))


def test_valid_high_unicode_snapshot_round_trips_through_cache() -> None:
    client = FakeRulerClient()
    persisted: list[str] = []
    payload = json.dumps(
        {"groups": [_group("unicode", "é" * 12_000)]},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    assert len(payload.encode("utf-8")) < MAX_RELATION_VALUE_BYTES
    reconciler = LokiRuleReconciler(client)

    first = reconciler.reconcile(
        [RelationRuleSource(1, payload)],
        cache_value=None,
        persist=persisted.append,
    )
    second = reconciler.reconcile(
        [RelationRuleSource(1, payload)],
        cache_value=persisted[-1],
        persist=persisted.append,
    )

    assert first.committed is True
    assert second.committed is True
    assert second.accepted_groups == first.accepted_groups


class FakeRulerClient:
    def __init__(self) -> None:
        self.groups: list[dict[str, Any]] = []
        self.calls: list[list[dict[str, Any]]] = []
        self.fail = False

    def replace_namespace(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
        previous = self.groups
        self.calls.append(groups)
        if self.fail:
            raise RuntimeError("ruler unavailable")
        self.groups = groups
        return previous


class FakeResponse:
    def __init__(
        self, status_code: int, text: str = "", *, chunks: list[bytes] | None = None
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.content = text.encode()
        self.chunks = chunks if chunks is not None else [self.content]
        self.closed = False

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int) -> Any:
        assert chunk_size > 0
        yield from self.chunks

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append((method, url, kwargs))
        return self.responses.pop(0)


def test_parse_preserves_rule_content_and_rejects_boundary_size() -> None:
    group = _group("example", 'sum(rate({job="demo"}[1m])) > 0')

    assert parse_rule_groups(_raw(group)) == [group]

    with pytest.raises(InvalidRuleSnapshotError):
        parse_rule_groups(" " * MAX_RELATION_VALUE_BYTES)


@pytest.mark.parametrize(
    "payload",
    [
        '{"groups":[{"name":"bad","rules":[],"value":NaN}]}',
        '{"groups":[{"name":"bad","rules":[],"value":"\\ud800"}]}',
        '{"groups":[{"name":"bad","rules":[{"alert":"MissingExpr"}]}]}',
        '{"groups":[{"name":"bad","rules":[{"alert":"A","record":"R","expr":"1"}]}]}',
        '{"groups":[{"name":"bad","rules":[{"alert":"A","expr":"1","labels":[]}]}]}',
        _raw({"name": "x" * 513, "rules": []}),
        json.dumps(
            {
                "groups": [
                    {
                        "name": "deep",
                        "rules": [],
                        "x": [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]],
                    }
                ]
            }
        ),
        json.dumps({"groups": [{"name": "wide", "rules": [], "x": [0] * 10_001}]}),
    ],
)
def test_parse_rejects_unsafe_documents(payload: str) -> None:
    with pytest.raises(InvalidRuleSnapshotError):
        parse_rule_groups(payload)


def test_reconcile_merges_two_sources_deterministically_and_persists_cache() -> None:
    client = FakeRulerClient()
    persisted: dict[str, str] = {}
    reconciler = LokiRuleReconciler(client)

    result = reconciler.reconcile(
        [
            RelationRuleSource(8, _raw(_group("z"), _group("a"))),
            RelationRuleSource(3, _raw(_group("m"))),
        ],
        cache_value=None,
        persist=lambda value: persisted.__setitem__(CACHE_KEY, value),
    )

    assert [group["name"] for group in result.accepted_groups] == ["m", "a", "z"]
    assert client.groups == result.accepted_groups
    assert persisted[CACHE_KEY]


def test_malformed_source_uses_its_lkg_while_valid_sibling_changes() -> None:
    client = FakeRulerClient()
    cache = ""

    def persist(value: str) -> None:
        nonlocal cache
        cache = value

    reconciler = LokiRuleReconciler(client)
    reconciler.reconcile(
        [RelationRuleSource(1, _raw(_group("one"))), RelationRuleSource(2, _raw(_group("two")))],
        cache_value=cache,
        persist=persist,
    )
    result = reconciler.reconcile(
        [RelationRuleSource(1, "not-json"), RelationRuleSource(2, _raw(_group("changed")))],
        cache_value=cache,
        persist=persist,
    )

    assert [group["name"] for group in result.accepted_groups] == ["one", "changed"]


def test_first_seen_malformed_source_does_not_block_valid_sibling(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakeRulerClient()
    secret_payload = "not-json-SECRET-RULE-CONTENT"
    caplog.set_level(logging.WARNING)

    result = LokiRuleReconciler(client).reconcile(
        [
            RelationRuleSource(1, secret_payload),
            RelationRuleSource(2, _raw(_group("valid"))),
        ],
        cache_value=None,
        persist=lambda _value: None,
    )

    assert [group["name"] for group in result.accepted_groups] == ["valid"]
    assert "SECRET-RULE-CONTENT" not in caplog.text


def test_duplicate_group_names_retain_previous_accepted_aggregate() -> None:
    client = FakeRulerClient()
    cache = ""

    def persist(value: str) -> None:
        nonlocal cache
        cache = value

    reconciler = LokiRuleReconciler(client)
    first = reconciler.reconcile(
        [RelationRuleSource(1, _raw(_group("accepted")))],
        cache_value=cache,
        persist=persist,
    )
    second = reconciler.reconcile(
        [
            RelationRuleSource(1, _raw(_group("duplicate"))),
            RelationRuleSource(2, _raw(_group("duplicate"))),
        ],
        cache_value=cache,
        persist=persist,
    )

    assert second.committed is False
    assert second.accepted_groups == first.accepted_groups
    assert client.groups == first.accepted_groups


@pytest.mark.parametrize("cache_value", ["not-base64", "A" * MAX_CACHE_VALUE_BYTES])
def test_invalid_cache_fails_closed_while_accepting_current_valid_source(
    cache_value: str,
) -> None:
    client = FakeRulerClient()
    persisted: list[str] = []

    result = LokiRuleReconciler(client).reconcile(
        [RelationRuleSource(9, _raw(_group("fresh")))],
        cache_value=cache_value,
        persist=persisted.append,
    )

    assert [group["name"] for group in result.accepted_groups] == ["fresh"]
    assert persisted


@pytest.mark.parametrize(
    "cache_value",
    ["not-base64", "A" * MAX_CACHE_VALUE_BYTES],
    ids=["corrupt", "over-limit"],
)
def test_invalid_cache_with_malformed_source_never_applies_partial_state(
    cache_value: str,
) -> None:
    """Unknown cached LKG must not be replaced by only reconstructable siblings."""
    client = FakeRulerClient()
    persisted: list[str] = []

    result = LokiRuleReconciler(client).reconcile(
        [
            RelationRuleSource(1, "not-json"),
            RelationRuleSource(2, _raw(_group("valid-sibling"))),
        ],
        cache_value=cache_value,
        persist=persisted.append,
    )

    assert result.committed is False
    assert client.calls == []
    assert persisted == []


@pytest.mark.parametrize("cache_value", ["not-base64", "A" * MAX_CACHE_VALUE_BYTES])
def test_invalid_cache_with_aggregate_invalid_candidate_never_replays_synthetic_empty(
    cache_value: str,
) -> None:
    """A rejected reconstruction must leave unknown live ruler state untouched."""
    client = FakeRulerClient()
    client.groups = [_group("unknown-live")]
    persisted: list[str] = []

    result = LokiRuleReconciler(client).reconcile(
        [
            RelationRuleSource(1, _raw(_group("duplicate"))),
            RelationRuleSource(2, _raw(_group("duplicate"))),
        ],
        cache_value=cache_value,
        persist=persisted.append,
    )

    assert result.committed is False
    assert client.calls == []
    assert client.groups == [_group("unknown-live")]
    assert persisted == []


@pytest.mark.parametrize("cache_value", ["not-base64", "A" * MAX_CACHE_VALUE_BYTES])
def test_invalid_cache_with_apply_failure_never_replays_synthetic_empty(
    cache_value: str,
) -> None:
    """An apply failure must not trigger rollback from an untrusted empty cache."""
    client = FakeRulerClient()
    client.groups = [_group("unknown-live")]
    client.fail = True
    persisted: list[str] = []

    result = LokiRuleReconciler(client).reconcile(
        [RelationRuleSource(1, _raw(_group("candidate")))],
        cache_value=cache_value,
        persist=persisted.append,
    )

    assert result.committed is False
    assert client.calls == [[_group("candidate")]]
    assert client.groups == [_group("unknown-live")]
    assert persisted == []


@pytest.mark.parametrize("cache_value", ["not-base64", "A" * MAX_CACHE_VALUE_BYTES])
def test_invalid_cache_with_persist_failure_restores_captured_live_namespace(
    cache_value: str,
) -> None:
    """A post-apply failure must restore actual live state, never synthetic empty state."""
    client = FakeRulerClient()
    client.groups = [_group("unknown-live")]

    def fail_persist(_value: str) -> None:
        raise RuntimeError("databag full")

    result = LokiRuleReconciler(client).reconcile(
        [RelationRuleSource(1, _raw(_group("candidate")))],
        cache_value=cache_value,
        persist=fail_persist,
    )

    assert result.committed is False
    assert client.calls == [[_group("candidate")], [_group("unknown-live")]]
    assert client.groups == [_group("unknown-live")]


def test_apply_failure_keeps_prior_accepted_cache_and_rules() -> None:
    client = FakeRulerClient()
    cache = ""

    def persist(value: str) -> None:
        nonlocal cache
        cache = value

    reconciler = LokiRuleReconciler(client)
    first = reconciler.reconcile(
        [RelationRuleSource(1, _raw(_group("accepted")))],
        cache_value=cache,
        persist=persist,
    )
    accepted_cache = cache
    client.fail = True

    second = reconciler.reconcile(
        [RelationRuleSource(1, _raw(_group("candidate")))],
        cache_value=cache,
        persist=persist,
    )

    assert second.accepted_groups == first.accepted_groups
    assert cache == accepted_cache
    assert client.groups == first.accepted_groups


def test_persist_failure_rolls_live_rules_back_to_prior_accepted_state() -> None:
    client = FakeRulerClient()
    cache = ""

    def persist(value: str) -> None:
        nonlocal cache
        cache = value

    reconciler = LokiRuleReconciler(client)
    first = reconciler.reconcile(
        [RelationRuleSource(1, _raw(_group("accepted")))],
        cache_value=cache,
        persist=persist,
    )
    accepted_cache = cache

    def fail_persist(_value: str) -> None:
        raise RuntimeError("databag full")

    second = reconciler.reconcile(
        [RelationRuleSource(1, _raw(_group("candidate")))],
        cache_value=cache,
        persist=fail_persist,
    )

    assert second.accepted_groups == first.accepted_groups
    assert cache == accepted_cache
    assert client.groups == first.accepted_groups
    assert [group["name"] for group in client.calls[-1]] == ["accepted"]


def test_only_first_32_relation_ids_are_admitted() -> None:
    client = FakeRulerClient()
    cache = ""

    def persist(value: str) -> None:
        nonlocal cache
        cache = value

    sources = [
        RelationRuleSource(relation_id, _raw(_group(str(relation_id))))
        for relation_id in range(40, 0, -1)
    ]
    result = LokiRuleReconciler(client).reconcile(sources, cache_value=cache, persist=persist)

    assert [group["name"] for group in result.accepted_groups] == [str(i) for i in range(1, 33)]


def test_cached_aggregate_can_exceed_one_relation_value() -> None:
    client = FakeRulerClient()
    cache = ""

    def persist(value: str) -> None:
        nonlocal cache
        cache = value

    large = "x" * 35_000
    reconciler = LokiRuleReconciler(client)
    reconciler.reconcile(
        [
            RelationRuleSource(1, _raw(_group("one", large))),
            RelationRuleSource(2, _raw(_group("two", large))),
        ],
        cache_value=cache,
        persist=persist,
    )
    result = reconciler.reconcile(
        [RelationRuleSource(1, "invalid"), RelationRuleSource(2, "invalid")],
        cache_value=cache,
        persist=persist,
    )

    assert [group["name"] for group in result.accepted_groups] == ["one", "two"]


def test_merge_admits_exact_total_group_and_rule_boundaries() -> None:
    groups = []
    for index in range(MAX_TOTAL_GROUPS - 1):
        groups.append({"name": f"group-{index:04d}", "rules": []})
    rules = []
    for index in range(MAX_TOTAL_RULES):
        rules.append({"alert": f"A{index}", "expr": "vector(1)"})
    groups.append(
        {
            "name": "rule-boundary",
            "rules": rules,
        }
    )

    assert len(merge_rule_groups({1: groups})) == MAX_TOTAL_GROUPS


@pytest.mark.parametrize(
    "groups",
    [
        [{"name": f"group-{index:04d}", "rules": []} for index in range(MAX_TOTAL_GROUPS + 1)],
        [
            {
                "name": "too-many-rules",
                "rules": [
                    {"alert": f"A{index}", "expr": "vector(1)"}
                    for index in range(MAX_TOTAL_RULES + 1)
                ],
            }
        ],
    ],
)
def test_merge_rejects_total_work_overflow(groups: list[dict[str, Any]]) -> None:
    with pytest.raises(InvalidRuleSnapshotError):
        merge_rule_groups({1: groups})


def test_total_group_overflow_replays_relation_lkg_and_later_retry_converges() -> None:
    client = FakeRulerClient()
    persisted: list[str] = []
    reconciler = LokiRuleReconciler(client)
    first = reconciler.reconcile(
        [RelationRuleSource(1, _raw(_group("accepted")))],
        cache_value=None,
        persist=persisted.append,
    )
    overflow = _raw(
        *({"name": f"group-{index:04d}", "rules": []} for index in range(MAX_TOTAL_GROUPS + 1))
    )

    rejected = reconciler.reconcile(
        [RelationRuleSource(1, overflow)],
        cache_value=persisted[-1],
        persist=persisted.append,
    )
    retried = reconciler.reconcile(
        [RelationRuleSource(1, _raw(_group("recovered")))],
        cache_value=persisted[-1],
        persist=persisted.append,
    )

    assert rejected.committed is False
    assert rejected.accepted_groups == first.accepted_groups
    assert retried.committed is True
    assert [group["name"] for group in retried.accepted_groups] == ["recovered"]


def test_api_client_skips_unchanged_namespace() -> None:
    group = _group("same")
    session = FakeSession([FakeResponse(200, json.dumps({"juju-loki-vm": [group]}))])

    LokiRulerApiClient("http://127.0.0.1:3100", session=session).replace_namespace([group])

    assert [request[0] for request in session.requests] == ["GET"]
    assert session.requests[0][1].endswith("/loki/api/v1/rules/juju-loki-vm")
    assert session.requests[0][2]["stream"] is True


def test_api_client_streams_only_owned_namespace_and_closes_response() -> None:
    group = _group("same")
    response = FakeResponse(200, json.dumps([group]))
    session = FakeSession([response])

    LokiRulerApiClient("http://127.0.0.1:3100", session=session).replace_namespace([group])

    assert session.requests[0][1].endswith("/loki/api/v1/rules/juju-loki-vm")
    assert response.closed is True


def test_api_client_rejects_stream_over_limit_without_buffering_whole_response() -> None:
    response = FakeResponse(
        200,
        chunks=[b"x" * LokiRulerApiClient.MAX_RESPONSE_BYTES, b"x"],
    )
    session = FakeSession([response])

    with pytest.raises(InvalidRuleSnapshotError):
        LokiRulerApiClient("http://127.0.0.1:3100", session=session).replace_namespace([])

    assert response.closed is True
    assert len(session.requests) == 1


def test_api_client_creates_namespace_when_all_rules_endpoint_is_empty() -> None:
    group = _group("new")
    session = FakeSession([FakeResponse(200, "{}"), FakeResponse(202), FakeResponse(202)])

    LokiRulerApiClient("http://127.0.0.1:3100", session=session).replace_namespace([group])

    assert [request[0] for request in session.requests] == ["GET", "DELETE", "POST"]


def test_api_client_replaces_namespace_and_uses_yaml_group_posts() -> None:
    old = _group("old")
    new = _group("new")
    session = FakeSession(
        [
            FakeResponse(200, json.dumps({"juju-loki-vm": [old]})),
            FakeResponse(202),
            FakeResponse(202),
        ]
    )

    previous = LokiRulerApiClient("http://127.0.0.1:3100", session=session).replace_namespace(
        [new]
    )

    assert previous == [old]
    assert [request[0] for request in session.requests] == ["GET", "DELETE", "POST"]
    assert session.requests[-1][2]["headers"] == {"Content-Type": "application/yaml"}
    assert "name: new" in session.requests[-1][2]["data"]


def test_api_client_rolls_back_if_candidate_post_fails() -> None:
    old = _group("old")
    new = _group("new")
    session = FakeSession(
        [
            FakeResponse(200, json.dumps({"juju-loki-vm": [old]})),
            FakeResponse(202),
            FakeResponse(500),
            FakeResponse(202),
            FakeResponse(202),
        ]
    )

    with pytest.raises(RuntimeError):
        LokiRulerApiClient("http://127.0.0.1:3100", session=session).replace_namespace([new])

    assert [request[0] for request in session.requests] == [
        "GET",
        "DELETE",
        "POST",
        "DELETE",
        "POST",
    ]
    assert "name: old" in session.requests[-1][2]["data"]


def test_api_client_exact_worst_case_rollback_stays_within_operation_budget() -> None:
    current = [{"name": f"old-{index:04d}", "rules": []} for index in range(MAX_TOTAL_GROUPS)]
    desired = [{"name": f"new-{index:04d}", "rules": []} for index in range(MAX_TOTAL_GROUPS)]
    responses = [FakeResponse(200, json.dumps(current)), FakeResponse(202)]
    responses.extend(FakeResponse(202) for _ in range(MAX_TOTAL_GROUPS - 1))
    responses.append(FakeResponse(500))
    responses.append(FakeResponse(202))
    responses.extend(FakeResponse(202) for _ in range(MAX_TOTAL_GROUPS))
    session = FakeSession(responses)

    with pytest.raises(RuntimeError):
        LokiRulerApiClient(
            "http://127.0.0.1:3100", session=session, clock=lambda: 0.0
        ).replace_namespace(desired)

    assert len(session.requests) == MAX_API_OPERATIONS
    assert not session.responses


def test_api_client_total_deadline_stops_work_and_next_lifecycle_retry_is_fresh() -> None:
    group = _group("same")
    moments = iter([0.0, 0.0, MAX_APPLY_SECONDS + 1, MAX_APPLY_SECONDS + 1])
    last = MAX_APPLY_SECONDS + 1

    def clock() -> float:
        nonlocal last
        last = next(moments, last)
        return last

    session = FakeSession(
        [
            FakeResponse(200, json.dumps([group])),
            FakeResponse(200, json.dumps([group])),
        ]
    )
    client = LokiRulerApiClient("http://127.0.0.1:3100", session=session, clock=clock)

    with pytest.raises(TimeoutError):
        client.replace_namespace([group])
    client.replace_namespace([group])

    assert len(session.requests) == 2
    assert all(request[2]["timeout"] <= MAX_APPLY_SECONDS for request in session.requests)


def test_api_client_deadline_stops_after_late_write_response() -> None:
    old = _group("old")
    new = _group("new")
    moments = iter([0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, MAX_APPLY_SECONDS + 1])
    last = MAX_APPLY_SECONDS + 1

    def clock() -> float:
        nonlocal last
        last = next(moments, last)
        return last

    session = FakeSession([FakeResponse(200, json.dumps([old])), FakeResponse(202)])

    with pytest.raises(TimeoutError):
        LokiRulerApiClient(
            "http://127.0.0.1:3100", session=session, clock=clock
        ).replace_namespace([new])

    assert [request[0] for request in session.requests] == ["GET", "DELETE"]
