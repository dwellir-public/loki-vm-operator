# Copyright 2026 Dwellir AB
# See LICENSE file for licensing details.
"""Bounded interoperability with Canonical JSON/LZMA alert-rule negotiation.

Owned by dwellir-observability-reference. This does not change the
machine_observability artifact contract. See DEVELOPING.md for provenance.
"""

from __future__ import annotations

import base64
import binascii
import json
import lzma
from collections.abc import Mapping

LIBID = "ad340c8efce34907b8119d019b0f6100"
LIBAPI = 0
LIBPATCH = 1

ENCODINGS_KEY = "alert_rules_encodings"
ENCODINGS = '["lzma", "json"]'
WIRE_LIMIT = 60 * 1024
DECODED_LIMIT = 8 * 1024 * 1024
MEMORY_LIMIT = 64 * 1024 * 1024


def encoding(remote: Mapping[str, str] | None) -> str:
    """Select the upstream preferred encoding, falling back to legacy JSON."""
    try:
        advertised = json.loads((remote or {}).get(ENCODINGS_KEY, "[]"))
    except (ValueError, TypeError):
        return "json"
    return "lzma" if isinstance(advertised, list) and "lzma" in advertised else "json"


def compress(raw: bytes) -> str:
    """Encode canonical xz/base64 using a bounded encoder dictionary."""
    return base64.b64encode(lzma.compress(raw, preset=6)).decode("ascii")


def decompress(raw: str, *, maximum: int = DECODED_LIMIT) -> bytes:
    """Decode exactly one xz stream with memory and output ceilings."""
    try:
        packed = base64.b64decode(raw, validate=True)
        decoder = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=MEMORY_LIMIT)
        result = decoder.decompress(packed, max_length=maximum + 1)
        if len(result) > maximum or not decoder.eof or decoder.unused_data:
            raise ValueError("compressed alert rules exceed bounds or contain trailing data")
        return result
    except (binascii.Error, lzma.LZMAError, UnicodeError) as exc:
        raise ValueError("invalid compressed alert rules") from exc


def decode(raw: str, *, wire_limit: int = WIRE_LIMIT) -> str:
    """Return JSON text from upstream bare or JSON-wrapped xz/base64 data.

    Semantic validation (including duplicate keys) remains with the caller.
    """
    if not isinstance(raw, str) or len(raw.encode("utf-8")) >= wire_limit:
        raise ValueError("alert rules exceed encoded size limit")
    value = raw.strip()
    if value.startswith('"'):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise ValueError("invalid wrapped alert rules") from exc
    if isinstance(value, str) and value.startswith("/Td6WFoA"):
        return decompress(value).decode("utf-8")
    return raw


def encode(raw: str, remote: Mapping[str, str] | None, *, wire_limit: int = WIRE_LIMIT) -> str:
    """Publish compressed rules only to an advertising receiver; never truncate."""
    data = raw.encode("utf-8")
    if len(data) > DECODED_LIMIT:
        raise ValueError("alert rules exceed decoded size limit")
    result = compress(data) if encoding(remote) == "lzma" else raw
    if len(result.encode("utf-8")) >= wire_limit and len(data) < wire_limit:
        result = raw
    if len(result.encode("utf-8")) >= wire_limit:
        raise ValueError("alert rules exceed receiver capacity for " + encoding(remote))
    return result


def admit(sources, previous, parser, *, maximum=1024):
    """Admit rule-bearing sources, preserving existing owners before newcomers.

    Absent data is not a deletion; an explicit empty document withdraws rules.
    Return bounded diagnostic relation IDs alongside last-known-good snapshots.
    """
    current = dict(sources)
    snapshots = {key: groups for key, groups in previous.items() if key in current and groups}
    errors = []
    sizes = {
        key: len(json.dumps(groups, ensure_ascii=False).encode())
        for key, groups in snapshots.items()
    }
    total = sum(sizes.values())
    decoded_work = 0
    for key in sorted(current, key=lambda key: (key not in snapshots, key)):
        raw = current[key]
        if raw is None:
            continue
        if decoded_work >= 2 * DECODED_LIMIT:
            errors.append(key)
            continue
        try:
            decoded_work += len(decode(raw).encode("utf-8"))
            if decoded_work > 2 * DECODED_LIMIT:
                errors.append(key)
                continue
            groups = parser(raw)
        except ValueError:
            errors.append(key)
            continue
        size = len(json.dumps(groups, ensure_ascii=False).encode()) if groups else 0
        candidate_total = total - sizes.get(key, 0) + size
        if candidate_total > DECODED_LIMIT or (
            groups and key not in snapshots and len(snapshots) >= maximum
        ):
            errors.append(key)
            continue
        if groups:
            snapshots[key] = groups
            sizes[key] = size
        else:
            snapshots.pop(key, None)
            sizes.pop(key, None)
        total = candidate_total
    return snapshots, errors
