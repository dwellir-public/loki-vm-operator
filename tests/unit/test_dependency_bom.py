"""Provenance must fail closed for undeclared, modified and inconsistent libraries."""

import importlib.util
import json
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("dependency_bom", ROOT / "tools/dependency_bom.py")
assert SPEC is not None and SPEC.loader is not None
bom = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bom)


@pytest.fixture
def checkout(tmp_path):
    for directory in ["lib", "dependencies"]:
        shutil.copytree(ROOT / directory, tmp_path / directory)
    return tmp_path


def test_known_provenance_passes(checkout):
    assert bom.verify(checkout)


def test_modified_library_fails(checkout):
    file = next((checkout / "lib").rglob("*.py"))
    file.write_text(file.read_text() + "\n# unreviewed edit\n")
    with pytest.raises(ValueError, match="hash mismatch"):
        bom.verify(checkout)


def test_undeclared_library_fails(checkout):
    (checkout / "lib/unknown.py").write_text("# unknown owner\n")
    with pytest.raises(ValueError, match="inventory differs"):
        bom.verify(checkout)


def test_missing_manifest_entry_fails(checkout):
    path = checkout / "dependencies/vendored.json"
    entries = json.loads(path.read_text())
    path.write_text(json.dumps(entries[:-1]))
    with pytest.raises(ValueError, match="inventory differs"):
        bom.verify(checkout)


def test_metadata_mismatch_fails(checkout):
    path = checkout / "dependencies/vendored.json"
    entries = json.loads(path.read_text())
    entries[0]["library"]["LIBPATCH"] += 1
    path.write_text(json.dumps(entries))
    with pytest.raises(ValueError, match="metadata mismatch"):
        bom.verify(checkout)


def test_packed_distribution_mismatch_fails(checkout, tmp_path):
    import zipfile

    artifact = tmp_path / "example.charm"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            "venv/lib/python3.12/site-packages/cosl-0.dist-info/METADATA",
            "Name: cosl\nVersion: 0\n",
        )
    with pytest.raises(ValueError, match="Packed distributions differ"):
        bom.verify_artifact(artifact, bom.verify(checkout), [{"name": "cosl", "version": "1"}])


def test_packed_library_tampering_fails(checkout, tmp_path):
    import zipfile

    manifest = [entry for entry in bom.verify(checkout) if entry["scope"] == "runtime"]
    artifact = tmp_path / "example.charm"
    with zipfile.ZipFile(artifact, "w") as archive:
        for entry in manifest:
            archive.writestr(
                entry["path"], (checkout / entry["path"]).read_bytes() + b"# changed\n"
            )
    with pytest.raises(ValueError, match="Packed library differs"):
        bom.verify_artifact(artifact, manifest, [])


def test_packed_runtime_source_mismatch_fails(tmp_path):
    import zipfile

    (tmp_path / "src").mkdir()
    (tmp_path / "src/charm.py").write_text("# reviewed source\n")
    artifact = tmp_path / "example.charm"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("src/charm.py", "# different source\n")
    with pytest.raises(ValueError, match="Packed runtime file differs"):
        bom.verify_artifact(artifact, [], [], root=tmp_path)


def test_inventory_needs_no_adapter_manifest(checkout):
    (checkout / "dependencies/shared.json").unlink(missing_ok=True)
    entries = bom.verify(checkout)
    assert {entry["scope"] for entry in entries} == {"runtime"}


@pytest.mark.parametrize(
    "generated", ["parts", "prime", "stage", "venv", ".venv", ".tox", "build", "dist"]
)
def test_generated_fixture_libraries_are_not_source(checkout, generated):
    module = checkout / "tests/retained/example" / generated / "lib/site-packages/generated.py"
    module.parent.mkdir(parents=True)
    module.write_text("# generated dependency, not vendored source\n")
    assert bom.verify(checkout)


def test_untracked_fixture_library_still_rejected(checkout):
    module = checkout / "tests/retained/example/lib/charms/unknown.py"
    module.parent.mkdir(parents=True)
    module.write_text("# undeclared real fixture source\n")
    with pytest.raises(ValueError, match="inventory differs"):
        bom.verify(checkout)


def test_generated_directory_name_inside_real_library_is_not_ignored(checkout):
    module = checkout / "tests/retained/example/lib/parts/unknown.py"
    module.parent.mkdir(parents=True)
    module.write_text("# real source package happens to be named parts\n")
    with pytest.raises(ValueError, match="inventory differs"):
        bom.verify(checkout)
