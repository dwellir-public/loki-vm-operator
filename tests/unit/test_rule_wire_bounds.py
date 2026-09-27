"""Exercise untrusted compression through the receiver boundary."""

import base64
import json
import lzma

import pytest
from cosl import LZMABase64

import rule_reconciler as module


@pytest.mark.parametrize("kind", ["dictionary", "truncated", "trailing", "output"])
def test_unsafe_compression_rejected_at_receiver(kind):
    raw = b'{"groups":[]}'
    if kind == "dictionary":
        packed = lzma.compress(
            raw, filters=[{"id": lzma.FILTER_LZMA2, "dict_size": 128 * 1024 * 1024}]
        )
    elif kind == "output":
        packed = lzma.compress(b" " * (8 * 1024 * 1024 + 1))
    else:
        packed = lzma.compress(raw)
        packed = packed[:-1] if kind == "truncated" else packed + b"trailing"
    with pytest.raises(ValueError):
        module.parse_rule_groups(base64.b64encode(packed).decode())


def test_large_compressed_source_uses_decoded_bound():
    groups = [
        {
            "name": "large",
            "rules": [
                {"alert": "Example", "expr": "up", "annotations": {"description": "x" * 70000}}
            ],
        }
    ]
    assert module.parse_rule_groups(LZMABase64.compress(json.dumps({"groups": groups}))) == groups
