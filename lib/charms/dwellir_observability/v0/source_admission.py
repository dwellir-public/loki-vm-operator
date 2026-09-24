# Copyright 2026 Dwellir AB
# See LICENSE file for licensing details.
"""Source admission and last-known-good retention owned by the reference charm.

This policy is separate from wire encoding and backend-specific reconciliation.
"""

import json

from .alert_rule_transport import DECODED_LIMIT, decode


def admit(sources, previous, parser):
    """Admit rule-bearing sources, preserving existing owners before newcomers.

    Absent data is not a deletion; an explicit empty document withdraws rules.
    Source count is not a capacity policy. Retained decoded bytes remain bounded.
    Decode each compressed source once before semantic validation; this avoids
    a fixed per-hook decode cutoff permanently starving higher-ID sources.
    Ruler writes are separately resumable in the backend reconciler.
    """
    current = dict(sources)
    snapshots = {key: groups for key, groups in previous.items() if key in current and groups}
    errors = []
    sizes = {
        key: len(json.dumps(groups, ensure_ascii=False).encode())
        for key, groups in snapshots.items()
    }
    total = sum(sizes.values())
    for key in sorted(current, key=lambda key: (key not in snapshots, key)):
        raw = current[key]
        if raw is None:
            continue
        try:
            groups = parser(decode(raw))
        except ValueError:
            errors.append(key)
            continue
        size = len(json.dumps(groups, ensure_ascii=False).encode()) if groups else 0
        candidate_total = total - sizes.get(key, 0) + size
        if candidate_total > DECODED_LIMIT:
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
