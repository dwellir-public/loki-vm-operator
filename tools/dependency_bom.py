"""Verify vendored provenance and produce development or packed-charm CycloneDX BOMs."""

import argparse
import ast
import email
import hashlib
import json
import os
import re
import subprocess
import zipfile
from pathlib import Path
from urllib.request import urlopen

UV_VERSION = "0.12.18"


def digest(data):
    """Hash exact input bytes."""
    return hashlib.sha256(data).hexdigest()


def normalized(name):
    """Normalize distribution names according to Python packaging rules."""
    return re.sub(r"[-_.]+", "-", name).lower()


def fixture_libraries(root):
    """Find fixture source libraries while pruning build outputs before lib roots."""
    generated = {
        "parts",
        "prime",
        "stage",
        "venv",
        ".venv",
        ".tox",
        "build",
        "dist",
        "__pycache__",
    }
    for directory, children, files in os.walk(root / "tests"):
        relative = Path(directory).relative_to(root)
        in_library = "lib" in relative.parts
        if not in_library:
            children[:] = [name for name in children if name not in generated]
        else:
            for name in files:
                if name.endswith(".py"):
                    yield str(relative / name)


def verify(root):
    """Reject undeclared modules, changed bytes and inconsistent library metadata."""
    manifest = json.loads((root / "dependencies/vendored.json").read_text())
    actual = {str(p.relative_to(root)) for p in (root / "lib").rglob("*.py")}
    actual.update(fixture_libraries(root))
    declared = {entry["path"] for entry in manifest}
    if actual != declared:
        raise ValueError(f"Vendored inventory differs: {actual ^ declared}")
    for entry in manifest:
        path = root / entry["path"]
        if digest(path.read_bytes()) != entry["sha256"]:
            raise ValueError(f"Vendored hash mismatch: {entry['path']}")
        if not re.fullmatch(r"[0-9a-f]{40}", entry["commit"]):
            raise ValueError("Upstream commit must be immutable")
        values = {}
        for node in ast.parse(path.read_text()).body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in entry.get("library", {}):
                        values[target.id] = ast.literal_eval(node.value)
        if values != entry.get("library", {}):
            raise ValueError(f"Library metadata mismatch: {entry['path']}")
    return manifest


def verify_upstream(root, manifest):
    """Compare recorded bytes with immutable upstream sources, not just local hashes."""
    for entry in manifest:
        repository = entry["repository"].removeprefix("https://github.com/")
        url = (
            f"https://raw.githubusercontent.com/{repository}/"
            f"{entry['commit']}/{entry['upstream_path']}"
        )
        # Existing immutable Git objects also work for private source history.
        local = subprocess.run(
            ["git", "show", f"{entry['commit']}:{entry['upstream_path']}"],
            cwd=root,
            capture_output=True,
            check=False,
        )
        if local.returncode == 0:
            data = local.stdout
        else:
            with urlopen(url, timeout=30) as response:
                data = response.read()
        if digest(data) != entry["sha256"]:
            raise ValueError(f"Upstream source differs: {entry['path']}")


def artifact_packages(archive):
    """Read distribution versions actually present in the built charm."""
    result = {}
    for name in archive.namelist():
        if name.endswith(".dist-info/METADATA"):
            metadata = email.message_from_bytes(archive.read(name))
            key = normalized(metadata["Name"])
            if key in result:
                raise ValueError(f"Duplicate distribution in artifact: {key}")
            result[key] = metadata["Version"]
    return result


def verify_runtime_sources(root, archive):
    """Match shipped charm code and assets against the checked-out source inputs."""
    for directory in ["src", "templates"]:
        for path in (root / directory).rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts:
                name = str(path.relative_to(root))
                if name not in archive.namelist() or archive.read(name) != path.read_bytes():
                    raise ValueError(f"Packed runtime file differs: {name}")


def verify_artifact(artifact, manifest, components, root=None):
    """Compare archive contents with the locked and vendored inputs."""
    with zipfile.ZipFile(artifact) as archive:
        if root is not None:
            verify_runtime_sources(root, archive)
        installed = artifact_packages(archive)
        expected = {normalized(c["name"]): c["version"] for c in components}
        if installed != expected:
            raise ValueError(
                f"Packed distributions differ: actual={installed}, expected={expected}"
            )
        packed = {n for n in archive.namelist() if n.startswith("lib/") and n.endswith(".py")}
        if packed != {entry["path"] for entry in manifest}:
            raise ValueError("Packed vendored inventory differs")
        for entry in manifest:
            if digest(archive.read(entry["path"])) != entry["sha256"]:
                raise ValueError(f"Packed library differs: {entry['path']}")


def link_vendored(bom, metadata, manifest):
    """Connect vendored modules to the root component in the dependency graph."""
    root_ref = metadata.get("component", {}).get("bom-ref")
    if root_ref:
        graph = bom.setdefault("dependencies", [])
        root_node = next((node for node in graph if node["ref"] == root_ref), None)
        if root_node is None:
            root_node = {"ref": root_ref, "dependsOn": []}
            graph.append(root_node)
        root_node.setdefault("dependsOn", []).extend(entry["path"] for entry in manifest)


def build_bom(root, *, artifact=None, base=None, arch=None):
    """Export locked dependencies and reconcile runtime components against the archive."""
    manifest = verify(root)
    subprocess.run(["uv", "lock", "--check"], cwd=root, check=True)
    args = [
        "uvx",
        "--from",
        f"uv=={UV_VERSION}",
        "uv",
        "export",
        "--locked",
        "--format",
        "cyclonedx1.5",
        "--no-emit-project",
    ]
    args += ["--no-dev"] if artifact else ["--all-groups"]
    if artifact:
        manifest = [entry for entry in manifest if entry["scope"] == "runtime"]
    bom = json.loads(subprocess.check_output(args, cwd=root))
    catalog = json.loads((root / "dependencies/upstreams.json").read_text())
    components = bom.setdefault("components", [])
    for component in components:
        owner = catalog.get(component["name"])
        if owner:
            component.setdefault("externalReferences", []).append(
                {"type": "vcs", "url": owner["repository"]}
            )
    if artifact:
        if not base or not arch:
            raise ValueError("Artifact BOM requires explicit base and architecture")
        verify_artifact(artifact, manifest, components, root=root)
    for entry in manifest:
        components.append(
            {
                "type": "library",
                "name": entry["path"],
                "bom-ref": entry["path"],
                "version": entry.get("commit", "sha256:" + entry["sha256"]),
                "hashes": [{"alg": "SHA-256", "content": entry["sha256"]}],
                "licenses": [{"license": {"id": entry["license"]}}],
                "properties": [{"name": "dwellir:scope", "value": entry["scope"]}]
                + [
                    {"name": "dwellir:" + key, "value": str(entry[key])}
                    for key in (
                        "upstream_owner_repository",
                        "upstream_base_commit",
                        "modifications",
                    )
                    if entry.get(key)
                ],
                "externalReferences": [
                    {
                        "type": "vcs",
                        "url": (
                            f"{entry['repository']}/blob/{entry['commit']}/"
                            f"{entry['upstream_path']}"
                            if "commit" in entry
                            else entry["repository"]
                        ),
                    }
                ],
            }
        )
    metadata = bom.setdefault("metadata", {})
    link_vendored(bom, metadata, manifest)
    props = metadata.setdefault("properties", [])
    provenance = {
        "scope": "packed charm Python and vendored runtime"
        if artifact
        else "development/test lock graph",
        "coverage-exclusions": (
            "OS packages, workload binaries and transitive build-tool environments"
        ),
        "source-commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip(),
        "source-diff-sha256": digest(subprocess.check_output(["git", "diff", "HEAD"], cwd=root)),
        "uv-export-version": UV_VERSION,
    }
    for path in [
        "uv.lock",
        "pyproject.toml",
        "charmcraft.yaml",
        "dependencies/upstreams.json",
        "dependencies/vendored.json",
        "tools/dependency_bom.py",
    ]:
        provenance[path + ":sha256"] = digest((root / path).read_bytes())
    if artifact:
        provenance.update(
            {"artifact-sha256": digest(artifact.read_bytes()), "base": base, "architecture": arch}
        )
    props.extend({"name": "dwellir:" + key, "value": value} for key, value in provenance.items())
    return bom


def main():
    """Validate source provenance or generate an explicitly scoped BOM."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--verify-upstream", action="store_true")
    parser.add_argument("--base")
    parser.add_argument("--arch")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.verify_upstream:
        verify_upstream(args.root, verify(args.root))
    if args.output:
        result = build_bom(args.root, artifact=args.artifact, base=args.base, arch=args.arch)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        verify(args.root)
    print("Dependency provenance verified")


if __name__ == "__main__":
    main()
