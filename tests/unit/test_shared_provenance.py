"""Builds must use complete, immutable and unmodified owner-maintained adapters."""

import hashlib
import json
import re
from pathlib import Path


def test_shared_adapter_provenance():
    """Reject local drift without needing network access to the private owner repo."""
    root = Path(__file__).resolve().parents[2]
    pins = json.loads((root / "dependencies/shared.json").read_text())
    expected = {"alert_rule_transport.py", "source_admission.py"}
    assert len(pins) == len(expected)
    assert {Path(pin["path"]).name for pin in pins} == expected
    assert len({pin["commit"] for pin in pins}) == 1
    for pin in pins:
        name = Path(pin["path"]).name
        assert pin["repository"] == (
            "https://github.com/dwellir-public/dwellir-observability-reference"
        )
        assert re.fullmatch(r"[0-9a-f]{40}", pin["commit"])
        assert pin["upstream_path"] == "shared/charms/dwellir_observability/v0/" + name
        assert pin["path"] == "lib/charms/dwellir_observability/v0/" + name
        assert pin["license"] == "Apache-2.0"
        assert pin["scope"] == "runtime"
        assert hashlib.sha256((root / pin["path"]).read_bytes()).hexdigest() == pin["sha256"]
