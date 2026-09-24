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
import re
import time
import zlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

import requests
import yaml
from charms.dwellir_observability.v0 import alert_rule_transport as transport

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
MAX_DOCUMENT_DEPTH = 32
MAX_DOCUMENT_NODES = 500_000
MAX_GROUP_NAME_BYTES = 512
MAX_TOTAL_GROUPS = 8192
MAX_TOTAL_RULES = 10_000
MAX_CACHE_VALUE_BYTES = 60 * 1024
MAX_CACHE_DECODED_BYTES = 16 * 1024 * 1024
MAX_CACHE_NODES = 2_000_000 + 128
CACHE_VERSION = 2
CACHE_KEY = "_loki_rule_reconciler_state_v1"
MAX_APPLY_SECONDS = 30
MAX_MUTATION_API_OPERATIONS = 1024
# One read plus a full candidate mutation and worst-case full recovery.
MAX_API_OPERATIONS = 2 + 2 * MAX_MUTATION_API_OPERATIONS
MAX_RECONCILE_API_OPERATIONS = 2 * MAX_API_OPERATIONS
MAX_RECONCILE_SECONDS = 4 * MAX_APPLY_SECONDS


class InvalidRuleSnapshotError(ValueError):
    """Report unsafe relation rule data without retaining its content."""


class InvalidRuleCacheError(ValueError):
    """Report malformed or oversized leader-shared rule state."""


def _duration_value(value: Any) -> Any:
    """Compare equivalent Prometheus duration spellings without changing payloads."""
    if not isinstance(value, str):
        return value
    tokens = re.findall(r"([0-9]+(?:\.[0-9]+)?)(ms|s|m|h|d|w|y)", value)
    if not tokens or "".join(number + unit for number, unit in tokens) != value:
        return value
    factors = {
        "ms": 1,
        "s": 1000,
        "m": 60000,
        "h": 3600000,
        "d": 86400000,
        "w": 604800000,
        "y": 31536000000,
    }
    return str(
        sum((Decimal(number) * factors[unit] for number, unit in tokens), Decimal(0)).normalize()
    )


def _normalize_durations(value: dict[str, Any], fields: tuple[str, ...]) -> None:
    """Normalize defaults only in a copied comparison document."""
    for field in fields:
        if field in value:
            value[field] = _duration_value(value[field])
        if value.get(field) in (None, "0"):
            value.pop(field, None)


class RulerApplyPendingError(RuntimeError):
    """A bounded apply batch completed; resume from observed state next hook."""


class RulerApplyError(RuntimeError):
    """Report a failed mutation and whether captured live state was restored."""

    def __init__(self, *, restored: bool):
        super().__init__("Loki ruler namespace apply failed")
        self.restored = restored


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
    received_sources: int = 0
    accepted_sources: int = 0
    rejected_sources: tuple[int, ...] = ()
    applied_sources: int | None = None
    applied_groups: int | None = None


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
            transport.decode(raw_payload, wire_limit=MAX_RELATION_VALUE_BYTES),
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
    if document == {}:
        return []
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


def _validate_aggregate_limits(
    groups: Sequence[Mapping[str, Any]], *, generations: int = 1
) -> None:
    """Bound total merge and ruler API work across all admitted sources."""
    if len(groups) > generations * MAX_TOTAL_GROUPS:
        raise InvalidRuleSnapshotError("rule aggregate has too many groups")
    rule_count = 0
    for group in groups:
        rules = group.get("rules")
        if not isinstance(rules, list):
            raise InvalidRuleSnapshotError("rule group rules must be a list")
        rule_count += len(rules)
        if rule_count > generations * MAX_TOTAL_RULES:
            raise InvalidRuleSnapshotError("rule aggregate has too many rules")


def _decompress_cache(encoded: str) -> bytes:
    """Decode one cache value without permitting a decompression bomb."""
    if not isinstance(encoded, str):
        raise InvalidRuleCacheError("rule cache is not text")
    try:
        if len(encoded.encode("utf-8")) >= MAX_CACHE_VALUE_BYTES:
            raise InvalidRuleCacheError("rule cache exceeds the safe size limit")
        if encoded.startswith("xz:"):
            return transport.decompress(encoded[3:], maximum=MAX_CACHE_DECODED_BYTES)
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
        return parse_rule_groups(
            transport.encode(serialized, {transport.ENCODINGS_KEY: transport.ENCODINGS})
        )
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
    if document["version"] not in (1, CACHE_VERSION) or not isinstance(
        document["relations"], dict
    ):
        raise InvalidRuleCacheError("rule cache version or relation map is invalid")
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
    if document["version"] == 2 and document["accepted"] is None:
        document["accepted"] = merge_rule_groups(snapshots)
    accepted_groups = _validate_cache_accepted(document["accepted"], snapshots)
    return _RuleCache(snapshots=snapshots, accepted_groups=accepted_groups)


def _encode_cache(cache: _RuleCache) -> str:
    """Encode cache state within Juju value and decoded-state limits."""
    _validate_cache_accepted(cache.accepted_groups, cache.snapshots)
    document = {
        "accepted": None,
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
    encoded = "xz:" + transport.compress(raw)
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
        snapshots, errors = transport.admit(
            [(source.relation_id, source.raw_payload) for source in ordered],
            previous.snapshots,
            parse_rule_groups,
        )
        result = partial(
            RuleReconcileResult,
            received_sources=len(ordered),
            accepted_sources=len(snapshots),
            rejected_sources=tuple(errors),
        )
        invalid_source = bool(errors)
        if errors:
            for offset in range(0, len(errors), 16):
                logger.warning(
                    "Rule sources rejected or over capacity: %s", errors[offset : offset + 16]
                )
        if not cache_valid and (
            invalid_source or any(source.raw_payload is None for source in ordered)
        ):
            logger.warning(
                "Cannot reconstruct complete Loki rule state from current relations; "
                "leaving the ruler namespace unchanged"
            )
            return result([], False)
        try:
            accepted_candidate = merge_rule_groups(snapshots)
            candidate = _RuleCache(snapshots=snapshots, accepted_groups=accepted_candidate)
            encoded_candidate = _encode_cache(candidate)
        except (InvalidRuleCacheError, InvalidRuleSnapshotError) as exc:
            logger.warning("Retaining the last accepted Loki rule state: %s", exc)
            self._replay_cached(cache_valid, previous.accepted_groups)
            return result(copy.deepcopy(previous.accepted_groups), False)
        live_before = self._apply_candidate(
            accepted_candidate,
            cache_valid=cache_valid,
            previous_groups=previous.accepted_groups,
        )
        if live_before is None:
            return result(copy.deepcopy(previous.accepted_groups), False)
        try:
            persist(encoded_candidate)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to persist Loki rule candidate; restoring accepted state: %s", exc
            )
            self._replay(previous.accepted_groups if cache_valid else live_before)
            return result(copy.deepcopy(previous.accepted_groups), False)
        return result(
            copy.deepcopy(accepted_candidate),
            not invalid_source,
            applied_sources=len(snapshots),
            applied_groups=len(accepted_candidate),
        )

    def _apply_candidate(
        self,
        groups: list[dict[str, Any]],
        *,
        cache_valid: bool,
        previous_groups: list[dict[str, Any]],
    ) -> list[dict[str, Any]] | None:
        """Apply once and avoid redundant replay when the API already recovered."""
        try:
            return self._client.replace_namespace(groups)
        except RulerApplyPendingError:
            logger.info("Rule reconciliation pending next hook")
            return None
        except RulerApplyError as exc:
            logger.warning("Failed to apply Loki rule candidate: %s", exc)
            if not exc.restored:
                self._replay_cached(cache_valid, previous_groups)
        except InvalidRuleSnapshotError as exc:
            logger.warning("Rejected Loki ruler state without mutation: %s", exc)
        except Exception as exc:  # noqa: BLE001 - injected clients may not be transactional.
            logger.warning("Failed to apply Loki rule candidate: %s", exc)
            self._replay_cached(cache_valid, previous_groups)
        return None

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
    MAX_RESPONSE_BYTES = 16 * 1024 * 1024
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
        """Converge a bounded batch and return the previously observed groups."""
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
            self._write_namespace(desired, budget, current=current)
        except RulerApplyPendingError:
            raise
        except Exception as apply_error:
            restored = False
            try:
                recovery_budget = _ApplyBudget(
                    deadline=self._clock() + MAX_APPLY_SECONDS,
                    clock=self._clock,
                    operations_remaining=1 + MAX_MUTATION_API_OPERATIONS,
                )
                observed = self._read_namespace(recovery_budget)
                self._write_namespace(current, recovery_budget, current=observed)
                restored = True
            except Exception as rollback_error:  # noqa: BLE001
                logger.warning("Failed to roll back the Loki ruler namespace: %s", rollback_error)
            raise RulerApplyError(restored=restored) from apply_error
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
        _validate_aggregate_limits(validated, generations=2)
        budget.check_deadline()
        return validated

    def _response_groups(self, document: Any) -> list[Any]:
        """Normalize Loki's supported namespace response shapes to a group list."""
        if document is None:
            raise InvalidRuleSnapshotError("Loki ruler namespace response is empty")
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

    def _write_namespace(
        self,
        groups: list[dict[str, Any]],
        budget: _ApplyBudget,
        *,
        current: list[dict[str, Any]] | None = None,
    ) -> None:
        """Apply at most 1024 changed groups without deleting the namespace.

        The live ruler stores progress between hooks and leadership changes.
        Removals follow all successful upserts. This is deliberately not an
        atomic backend transaction; pending work remains operator-visible.
        """
        before = {group["name"]: group for group in (current or [])}
        desired = {group["name"]: group for group in groups}
        updates = [
            group
            for group in groups
            if self._canonical([before.get(group["name"], {})]) != self._canonical([group])
        ]
        removed = sorted(set(before) - set(desired))
        work: list[tuple[str, str, dict[str, Any] | None]] = [
            ("POST", g["name"], g) for g in updates
        ]
        work.extend(("DELETE", name, None) for name in removed)
        for method, name, group in work[:MAX_MUTATION_API_OPERATIONS]:
            # Stop between successful mutations before the request deadline.
            # A normal time-sliced batch must not undo its completed work.
            if budget.deadline is not None and budget.deadline - budget.clock() <= 1:
                raise RulerApplyPendingError("rule apply time slice exhausted")
            headers = {}
            url = self._namespace_url
            body = None
            if method == "POST":
                body = yaml.safe_dump(group, sort_keys=False)
                headers["Content-Type"] = "application/yaml"
            else:
                url += "/" + quote(name, safe="")
            response = self._session.request(
                method,
                url,
                data=body,
                headers=headers,
                timeout=budget.request_timeout(self._timeout),
                stream=True,
            )
            self._finish_write_response(
                response,
                accepted_statuses={200, 202, 204, 404} if method == "DELETE" else {200, 202, 204},
                budget=budget,
            )
        if len(work) > MAX_MUTATION_API_OPERATIONS:
            raise RulerApplyPendingError("more rule groups remain to apply")

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
        normalized = copy.deepcopy(groups)
        for group in normalized:
            _normalize_durations(group, ("interval", "query_offset"))
            for rule in group.get("rules", []):
                _normalize_durations(rule, ("for", "keep_firing_for"))
                for field in ("labels", "annotations"):
                    if not rule.get(field):
                        rule.pop(field, None)
        return json.dumps(normalized, sort_keys=True, separators=(",", ":"), allow_nan=False)
