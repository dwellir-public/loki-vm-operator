# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

"""Bounded, durable reconciliation of relation-owned Loki alert rules."""

from __future__ import annotations

import base64
import binascii
import copy
import json
import logging
import math
import time
import zlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

import requests
import yaml

logger = logging.getLogger(__name__)


def prepare_filesystem_rule_store(data_dir: str | Path) -> None:
    """Create the writable filesystem paths required by Loki's ruler API.

    With authentication disabled Loki uses the fixed tenant identifier ``fake``.
    Its filesystem rule store expects that tenant directory to exist before the
    first API write.
    """
    root = Path(data_dir)
    (root / "rules" / "fake").mkdir(parents=True, exist_ok=True)
    (root / "ruler-tmp").mkdir(parents=True, exist_ok=True)


MAX_RELATION_VALUE_BYTES = 60 * 1024
MAX_SOURCE_RELATIONS = 32
MAX_DOCUMENT_DEPTH = 32
MAX_DOCUMENT_NODES = 10_000
MAX_GROUP_NAME_BYTES = 512
MAX_TOTAL_GROUPS = 512
MAX_TOTAL_RULES = 10_000
MAX_CACHE_VALUE_BYTES = 60 * 1024
MAX_CACHE_DECODED_BYTES = 2 * 1024 * 1024
MAX_CACHE_NODES = MAX_SOURCE_RELATIONS * MAX_DOCUMENT_NODES + 128
CACHE_VERSION = 1
CACHE_KEY = "_loki_rule_reconciler_state_v1"
MAX_APPLY_SECONDS = 30
MAX_MUTATION_API_OPERATIONS = 1 + MAX_TOTAL_GROUPS
# One read plus a full candidate mutation and worst-case full recovery.
MAX_API_OPERATIONS = 1 + 2 * MAX_MUTATION_API_OPERATIONS


class InvalidRuleSnapshotError(ValueError):
    """Report unsafe relation rule data without retaining its content."""


class InvalidRuleCacheError(ValueError):
    """Report malformed or oversized leader-shared rule state."""


class RulerClient(Protocol):
    """Describe the Loki ruler operation required by the reconciler."""

    def replace_namespace(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Replace the namespace and return its validated pre-apply contents."""
        ...


@dataclass(frozen=True)
class RelationRuleSource:
    """Represent one remote application rule databag by stable relation ID."""

    relation_id: int
    raw_payload: str | None


@dataclass(frozen=True)
class RuleReconcileResult:
    """Expose the last accepted groups and whether a candidate was committed."""

    accepted_groups: list[dict[str, Any]]
    committed: bool


@dataclass(frozen=True)
class _RuleCache:
    """Hold relation-local LKG snapshots and the last applied aggregate."""

    snapshots: dict[int, list[dict[str, Any]]]
    accepted_groups: list[dict[str, Any]]


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build an object while rejecting ambiguous duplicate JSON keys."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InvalidRuleSnapshotError("rule data contains a duplicate object key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    """Reject non-standard JSON constants such as NaN and Infinity."""
    raise InvalidRuleSnapshotError("rule data contains a non-finite number")


def _validate_utf8(value: str) -> None:
    """Reject decoded text that cannot be represented as UTF-8."""
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise InvalidRuleSnapshotError("rule data contains invalid UTF-8 text") from exc


def _validate_tree(value: Any, *, max_nodes: int = MAX_DOCUMENT_NODES) -> None:
    """Bound JSON depth, node count, scalar types, finiteness, and UTF-8."""
    pending: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > max_nodes:
            raise InvalidRuleSnapshotError("rule data has too many values")
        if depth > MAX_DOCUMENT_DEPTH:
            raise InvalidRuleSnapshotError("rule data is nested too deeply")
        if isinstance(item, dict):
            for key in item:
                if not isinstance(key, str):
                    raise InvalidRuleSnapshotError("rule data has a non-text object key")
                _validate_utf8(key)
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        else:
            _validate_scalar(item)


def _validate_scalar(value: Any) -> None:
    """Validate one JSON scalar for finiteness, type, and UTF-8."""
    if isinstance(value, float) and not math.isfinite(value):
        raise InvalidRuleSnapshotError("rule data contains a non-finite number")
    if isinstance(value, str):
        _validate_utf8(value)
    elif not isinstance(value, (int, float, bool, type(None))):
        raise InvalidRuleSnapshotError("rule data contains an unsupported value")


def _validate_group(group: Any) -> dict[str, Any]:
    """Validate one Loki group while preserving every supplied field."""
    if not isinstance(group, dict):
        raise InvalidRuleSnapshotError("each rule group must be an object")
    name = group.get("name")
    if not isinstance(name, str):
        raise InvalidRuleSnapshotError("each rule group must have a text name")
    try:
        name_bytes = len(name.encode("utf-8"))
    except UnicodeError as exc:
        raise InvalidRuleSnapshotError("rule group name is not valid UTF-8") from exc
    if not name.strip() or not name.isprintable() or name_bytes > MAX_GROUP_NAME_BYTES:
        raise InvalidRuleSnapshotError("rule group name is invalid")
    rules = group.get("rules")
    if not isinstance(rules, list) or any(not isinstance(rule, dict) for rule in rules):
        raise InvalidRuleSnapshotError("rule group rules must be a list of objects")
    for rule in rules:
        _validate_rule(rule)
    return group


def _validate_rule(rule: Mapping[str, Any]) -> None:
    """Reject rule shapes Loki cannot apply while preserving all supplied fields."""
    rule_names = [key for key in ("alert", "record") if key in rule]
    if len(rule_names) != 1:
        raise InvalidRuleSnapshotError("each rule must define exactly one alert or record name")
    name = rule[rule_names[0]]
    expression = rule.get("expr")
    if not isinstance(name, str) or not name.strip():
        raise InvalidRuleSnapshotError("rule alert or record name must be non-empty text")
    if not isinstance(expression, str) or not expression.strip():
        raise InvalidRuleSnapshotError("each rule must have a non-empty text expression")
    for field in ("labels", "annotations"):
        values = rule.get(field)
        if values is not None and (
            not isinstance(values, dict)
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in values.items()
            )
        ):
            raise InvalidRuleSnapshotError(f"rule {field} must map text keys to text values")
    for field in ("for", "keep_firing_for"):
        if field in rule and not isinstance(rule[field], str):
            raise InvalidRuleSnapshotError(f"rule {field} must be text")


def parse_rule_groups(raw_payload: str | None) -> list[dict[str, Any]]:
    """Parse one complete bounded standard `alert_rules` relation value."""
    if raw_payload is None:
        return []
    if not isinstance(raw_payload, str):
        raise InvalidRuleSnapshotError("alert_rules must be text")
    try:
        raw_size = len(raw_payload.encode("utf-8"))
    except UnicodeError as exc:
        raise InvalidRuleSnapshotError("alert_rules is not valid UTF-8") from exc
    if raw_size >= MAX_RELATION_VALUE_BYTES:
        raise InvalidRuleSnapshotError("alert_rules exceeds the safe size limit")
    try:
        document = json.loads(
            raw_payload,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        _validate_tree(document)
    except (
        InvalidRuleSnapshotError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ) as exc:
        raise InvalidRuleSnapshotError("alert_rules is not valid bounded JSON") from exc
    if not isinstance(document, dict) or set(document) != {"groups"}:
        raise InvalidRuleSnapshotError("alert_rules must contain only groups")
    groups = document["groups"]
    if not isinstance(groups, list):
        raise InvalidRuleSnapshotError("alert_rules groups must be a list")
    return [_validate_group(group) for group in groups]


def merge_rule_groups(
    snapshots: Mapping[int, Sequence[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Merge snapshots deterministically by relation ID then group name."""
    merged: list[dict[str, Any]] = []
    names: set[str] = set()
    for relation_id in sorted(snapshots):
        for group in sorted(snapshots[relation_id], key=lambda value: str(value["name"])):
            name = str(group["name"])
            if name in names:
                raise InvalidRuleSnapshotError("rule group names must be unique in the namespace")
            names.add(name)
            merged.append(copy.deepcopy(group))
    _validate_aggregate_limits(merged)
    return merged


def _validate_aggregate_limits(groups: Sequence[Mapping[str, Any]]) -> None:
    """Bound total merge and ruler API work across all admitted sources."""
    if len(groups) > MAX_TOTAL_GROUPS:
        raise InvalidRuleSnapshotError("rule aggregate has too many groups")
    rule_count = 0
    for group in groups:
        rules = group.get("rules")
        if not isinstance(rules, list):
            raise InvalidRuleSnapshotError("rule group rules must be a list")
        rule_count += len(rules)
        if rule_count > MAX_TOTAL_RULES:
            raise InvalidRuleSnapshotError("rule aggregate has too many rules")


def _decompress_cache(encoded: str) -> bytes:
    """Decode one cache value without permitting a decompression bomb."""
    if not isinstance(encoded, str):
        raise InvalidRuleCacheError("rule cache is not text")
    try:
        if len(encoded.encode("utf-8")) >= MAX_CACHE_VALUE_BYTES:
            raise InvalidRuleCacheError("rule cache exceeds the safe size limit")
        compressed = base64.b64decode(encoded, validate=True)
        decompressor = zlib.decompressobj()
        raw = decompressor.decompress(compressed, MAX_CACHE_DECODED_BYTES + 1)
        if (
            len(raw) > MAX_CACHE_DECODED_BYTES
            or decompressor.unconsumed_tail
            or not decompressor.eof
            or decompressor.unused_data
        ):
            raise InvalidRuleCacheError("rule cache decoded content exceeds safe bounds")
        return raw
    except (binascii.Error, InvalidRuleCacheError, UnicodeError, ValueError, zlib.error) as exc:
        raise InvalidRuleCacheError("rule cache compression is invalid") from exc


def _validate_cache_accepted(
    value: Any,
    snapshots: Mapping[int, Sequence[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Validate that accepted cache state exactly represents its relation snapshots."""
    if not isinstance(value, list):
        raise InvalidRuleCacheError("rule cache accepted state is invalid")
    try:
        accepted_groups = [_validate_group(group) for group in value]
        expected_groups = merge_rule_groups(snapshots)
    except InvalidRuleSnapshotError as exc:
        raise InvalidRuleCacheError("rule cache accepted state is invalid") from exc
    if accepted_groups != expected_groups:
        raise InvalidRuleCacheError("rule cache accepted state does not match relation snapshots")
    return accepted_groups


def _validate_cached_snapshot(groups: Any) -> list[dict[str, Any]]:
    """Validate cached relation groups without expanding valid Unicode content."""
    try:
        serialized = json.dumps(
            {"groups": groups},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return parse_rule_groups(serialized)
    except (InvalidRuleSnapshotError, TypeError, UnicodeError, ValueError) as exc:
        raise InvalidRuleCacheError("rule cache relation snapshot is invalid") from exc


def _decode_cache(encoded: str | None) -> _RuleCache:
    """Decode bounded, compressed leader-shared cache state."""
    if not encoded:
        return _RuleCache(snapshots={}, accepted_groups=[])
    try:
        document = json.loads(
            _decompress_cache(encoded),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        _validate_tree(document, max_nodes=MAX_CACHE_NODES)
    except (
        InvalidRuleCacheError,
        InvalidRuleSnapshotError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ) as exc:
        raise InvalidRuleCacheError("rule cache is not valid bounded state") from exc
    if not isinstance(document, dict) or set(document) != {"accepted", "relations", "version"}:
        raise InvalidRuleCacheError("rule cache structure is invalid")
    if document["version"] != CACHE_VERSION or not isinstance(document["relations"], dict):
        raise InvalidRuleCacheError("rule cache version or relation map is invalid")
    if len(document["relations"]) > MAX_SOURCE_RELATIONS:
        raise InvalidRuleCacheError("rule cache has too many relations")
    snapshots: dict[int, list[dict[str, Any]]] = {}
    for relation_id_text, groups in document["relations"].items():
        if (
            not isinstance(relation_id_text, str)
            or not relation_id_text.isdecimal()
            or len(relation_id_text) > 20
            or str(int(relation_id_text)) != relation_id_text
        ):
            raise InvalidRuleCacheError("rule cache relation identifier is invalid")
        snapshots[int(relation_id_text)] = _validate_cached_snapshot(groups)
    accepted_groups = _validate_cache_accepted(document["accepted"], snapshots)
    return _RuleCache(snapshots=snapshots, accepted_groups=accepted_groups)


def _encode_cache(cache: _RuleCache) -> str:
    """Encode cache state within Juju value and decoded-state limits."""
    document = {
        "accepted": cache.accepted_groups,
        "relations": {str(key): value for key, value in sorted(cache.snapshots.items())},
        "version": CACHE_VERSION,
    }
    try:
        _validate_tree(document, max_nodes=MAX_CACHE_NODES)
        raw = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (InvalidRuleSnapshotError, TypeError, UnicodeError, ValueError) as exc:
        raise InvalidRuleCacheError("rule cache candidate is invalid") from exc
    if len(raw) > MAX_CACHE_DECODED_BYTES:
        raise InvalidRuleCacheError("rule cache decoded content exceeds safe bounds")
    encoded = base64.b64encode(zlib.compress(raw, level=9)).decode("ascii")
    if len(encoded.encode("utf-8")) >= MAX_CACHE_VALUE_BYTES:
        raise InvalidRuleCacheError("rule cache value exceeds the safe size limit")
    return encoded


class LokiRuleReconciler:
    """Converge bounded relation LKG state into one Loki ruler namespace."""

    def __init__(self, client: RulerClient):
        """Bind the reconciler to a replace-capable Loki ruler client."""
        self._client = client

    def reconcile(
        self,
        sources: Iterable[RelationRuleSource],
        *,
        cache_value: str | None,
        persist: Callable[[str], None],
    ) -> RuleReconcileResult:
        """Validate sources, apply a candidate, and persist only accepted state."""
        cache_valid = True
        try:
            previous = _decode_cache(cache_value)
        except InvalidRuleCacheError as exc:
            logger.warning("Ignoring invalid leader-shared Loki rule cache: %s", exc)
            previous = _RuleCache(snapshots={}, accepted_groups=[])
            cache_valid = False
        ordered = sorted(sources, key=lambda source: source.relation_id)
        if len(ordered) > MAX_SOURCE_RELATIONS:
            logger.warning(
                "Only the first %s Loki rule source relations are admitted; %s were present",
                MAX_SOURCE_RELATIONS,
                len(ordered),
            )
        admitted = ordered[:MAX_SOURCE_RELATIONS]
        admitted_ids = {source.relation_id for source in admitted}
        snapshots = {
            relation_id: groups
            for relation_id, groups in previous.snapshots.items()
            if relation_id in admitted_ids
        }
        invalid_source = False
        for source in admitted:
            try:
                snapshots[source.relation_id] = parse_rule_groups(source.raw_payload)
            except InvalidRuleSnapshotError as exc:
                invalid_source = True
                logger.warning(
                    "Retaining valid Loki rules for relation %s when available: %s",
                    source.relation_id,
                    exc,
                )
        if not cache_valid and invalid_source:
            logger.warning(
                "Cannot reconstruct complete Loki rule state from current relations; "
                "leaving the ruler namespace unchanged"
            )
            return RuleReconcileResult([], False)
        try:
            accepted_candidate = merge_rule_groups(snapshots)
            candidate = _RuleCache(snapshots=snapshots, accepted_groups=accepted_candidate)
            encoded_candidate = _encode_cache(candidate)
        except (InvalidRuleCacheError, InvalidRuleSnapshotError) as exc:
            logger.warning("Retaining the last accepted Loki rule state: %s", exc)
            self._replay_cached(cache_valid, previous.accepted_groups)
            return RuleReconcileResult(copy.deepcopy(previous.accepted_groups), False)
        try:
            live_before = self._client.replace_namespace(accepted_candidate)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to apply Loki rule candidate; retaining accepted state: %s", exc
            )
            self._replay_cached(cache_valid, previous.accepted_groups)
            return RuleReconcileResult(copy.deepcopy(previous.accepted_groups), False)
        try:
            persist(encoded_candidate)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to persist Loki rule candidate; restoring accepted state: %s", exc
            )
            self._replay(previous.accepted_groups if cache_valid else live_before)
            return RuleReconcileResult(copy.deepcopy(previous.accepted_groups), False)
        return RuleReconcileResult(copy.deepcopy(accepted_candidate), True)

    def _replay_cached(self, cache_valid: bool, accepted_groups: list[dict[str, Any]]) -> None:
        """Replay cached state only when it came from a validated cache."""
        if cache_valid:
            self._replay(accepted_groups)

    def _replay(self, accepted_groups: list[dict[str, Any]]) -> None:
        """Best-effort restore the accepted namespace after candidate failure."""
        try:
            self._client.replace_namespace(copy.deepcopy(accepted_groups))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to replay the accepted Loki rule state: %s", exc)


@dataclass
class _ApplyBudget:
    """Enforce one deterministic wall-clock and HTTP-operation apply budget."""

    deadline: float
    clock: Callable[[], float]
    operations_remaining: int

    def request_timeout(self, configured_timeout: int) -> float:
        """Reserve one operation and return its bounded remaining timeout."""
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise TimeoutError("Loki ruler apply deadline exceeded")
        if self.operations_remaining <= 0:
            raise InvalidRuleSnapshotError("Loki ruler API operation budget exceeded")
        self.operations_remaining -= 1
        return min(float(configured_timeout), remaining)

    def check_deadline(self) -> None:
        """Stop streamed response processing once the total deadline expires."""
        if self.clock() >= self.deadline:
            raise TimeoutError("Loki ruler apply deadline exceeded")


class LokiRulerApiClient:
    """Idempotently replace one charm-owned namespace through Loki's ruler API."""

    NAMESPACE = "juju-loki-vm"
    MAX_RESPONSE_BYTES = 2 * 1024 * 1024
    MAX_WRITE_RESPONSE_BYTES = 64 * 1024

    def __init__(
        self,
        base_url: str,
        *,
        session: Any | None = None,
        timeout: int = 10,
        clock: Callable[[], float] = time.monotonic,
    ):
        """Configure a local Loki endpoint and injectable HTTP session."""
        self._base_url = base_url.rstrip("/")
        self._session = session or requests.Session()
        self._timeout = timeout
        self._clock = clock

    def replace_namespace(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Replace transactionally and return validated state for caller rollback."""
        budget = _ApplyBudget(
            deadline=self._clock() + MAX_APPLY_SECONDS,
            clock=self._clock,
            operations_remaining=1 + MAX_MUTATION_API_OPERATIONS,
        )
        desired = copy.deepcopy(groups)
        _validate_tree(desired, max_nodes=MAX_CACHE_NODES)
        for group in desired:
            _validate_group(group)
        _validate_aggregate_limits(desired)
        current = self._read_namespace(budget)
        if self._canonical(current) == self._canonical(desired):
            return copy.deepcopy(current)
        try:
            self._write_namespace(desired, budget)
        except Exception:
            try:
                recovery_budget = _ApplyBudget(
                    deadline=self._clock() + MAX_APPLY_SECONDS,
                    clock=self._clock,
                    operations_remaining=MAX_MUTATION_API_OPERATIONS,
                )
                self._write_namespace(current, recovery_budget)
            except Exception as rollback_error:  # noqa: BLE001
                logger.warning("Failed to roll back the Loki ruler namespace: %s", rollback_error)
            raise
        return copy.deepcopy(current)

    @property
    def _namespace_url(self) -> str:
        """Return the URL for the single charm-owned namespace."""
        namespace = quote(self.NAMESPACE, safe="")
        return f"{self._base_url}/loki/api/v1/rules/{namespace}"

    def _read_namespace(self, budget: _ApplyBudget) -> list[dict[str, Any]]:
        """Read and validate the current namespace without logging its content."""
        response = self._session.request(
            "GET",
            self._namespace_url,
            timeout=budget.request_timeout(self._timeout),
            stream=True,
        )
        try:
            if response.status_code == 404:
                return []
            response.raise_for_status()
            body = bytearray()
            for chunk in response.iter_content(chunk_size=64 * 1024):
                budget.check_deadline()
                if not chunk:
                    continue
                if len(body) + len(chunk) > self.MAX_RESPONSE_BYTES:
                    raise InvalidRuleSnapshotError("Loki ruler response exceeds safe bounds")
                body.extend(chunk)
            budget.check_deadline()
            try:
                document = yaml.safe_load(bytes(body).decode("utf-8"))
            except (RecursionError, UnicodeError, yaml.YAMLError) as exc:
                raise InvalidRuleSnapshotError("Loki ruler response is invalid") from exc
            budget.check_deadline()
        finally:
            response.close()
        groups = self._response_groups(document)
        _validate_tree(groups, max_nodes=MAX_CACHE_NODES)
        validated = [_validate_group(group) for group in groups]
        _validate_aggregate_limits(validated)
        budget.check_deadline()
        return validated

    def _response_groups(self, document: Any) -> list[Any]:
        """Normalize Loki's supported namespace response shapes to a group list."""
        if document is None:
            return []
        if isinstance(document, dict) and self.NAMESPACE in document:
            groups = document[self.NAMESPACE]
            if isinstance(groups, list):
                return groups
            raise InvalidRuleSnapshotError("Loki ruler namespace response is invalid")
        if isinstance(document, dict) and not document:
            return []
        if isinstance(document, list):
            return document
        if isinstance(document, dict) and set(document) >= {"name", "rules"}:
            return [document]
        if isinstance(document, dict) and isinstance(document.get("groups"), list):
            return document["groups"]
        if isinstance(document, dict):
            raise InvalidRuleSnapshotError("Loki ruler namespace response is unrecognized")
        raise InvalidRuleSnapshotError("Loki ruler response structure is invalid")

    def _write_namespace(self, groups: list[dict[str, Any]], budget: _ApplyBudget) -> None:
        """Delete the namespace then create every desired group in order."""
        delete_response = self._session.request(
            "DELETE",
            self._namespace_url,
            timeout=budget.request_timeout(self._timeout),
            stream=True,
        )
        self._finish_write_response(
            delete_response,
            accepted_statuses={200, 202, 204, 404},
            budget=budget,
        )
        for group in groups:
            body = yaml.safe_dump(group, sort_keys=False)
            response = self._session.request(
                "POST",
                self._namespace_url,
                data=body,
                headers={"Content-Type": "application/yaml"},
                timeout=budget.request_timeout(self._timeout),
                stream=True,
            )
            self._finish_write_response(
                response,
                accepted_statuses={200, 202, 204},
                budget=budget,
            )

    def _finish_write_response(
        self,
        response: Any,
        *,
        accepted_statuses: set[int],
        budget: _ApplyBudget,
    ) -> None:
        """Bound and close one streamed ruler mutation response."""
        try:
            budget.check_deadline()
            received = 0
            for chunk in response.iter_content(chunk_size=16 * 1024):
                budget.check_deadline()
                if not chunk:
                    continue
                received += len(chunk)
                if received > self.MAX_WRITE_RESPONSE_BYTES:
                    raise InvalidRuleSnapshotError("Loki ruler write response exceeds safe bounds")
            budget.check_deadline()
            if response.status_code not in accepted_statuses:
                response.raise_for_status()
        finally:
            response.close()

    @staticmethod
    def _canonical(groups: list[dict[str, Any]]) -> str:
        """Return a semantic comparison form without changing API payloads."""
        return json.dumps(groups, sort_keys=True, separators=(",", ":"), allow_nan=False)
