# Developing loki-vm

This document describes common developer workflows for the charm.

## Dependency management (uv)

The charm uses `uv` for dependency management. Update Python dependencies in
`pyproject.toml`, then refresh the lockfile:

```bash
cd /home/erik/Loki-project/loki-vm-operator
uv lock
```

If you need to install deps locally for development, you can sync all groups:

```bash
uv sync --all-groups
```

For a single development environment (unit + integration + lint), use:

```bash
uv sync --group dev
```

## Running tests

### Unit tests

Install unit-test dependencies and run the suite:

```bash
cd /home/erik/Loki-project/loki-vm-operator
uv sync --group unit
uv run pytest tests/unit
```

To focus on config builder tests:

```bash
uv run pytest -k config_builder
```

To focus on `loki.py` tests:

```bash
uv run pytest -k loki
```

Clustered restart contract:

- Multi-unit `loki-vm` config changes must preserve rolling restart behavior.
- The charm is expected to restart one clustered unit at a time after config changes.
- Progress must be gated on workload and cluster health before advancing to the next unit.
- Tests that would reintroduce concurrent clustered restarts should be treated as regressions.

### Integration tests

Integration tests use Jubilant and require:
* A Juju model
* A uilt charm locally as it looks for a charm file in the current directory 

Note: Integration tests may take a few minutes to complete.
Note: Each integration test run creates a temporary Juju model (jubilant-xxxx). The model name
and controller are printed at test start for easier tracking/cleanup.

```bash
cd /home/erik/Loki-project/loki-vm-operator
charmcraft pack
uv sync --group integration
uv run pytest tests/integration
```

Alternatively, set `CHARM_PATH` to an existing `.charm` file:

```bash
CHARM_PATH=/path/to/loki-vm_ubuntu-24.04-amd64.charm uv run pytest tests/integration
```

Note: Rebuild the charm (`charmcraft pack`) after code changes so integration tests
use the latest artifact.

If you run pytest without uv, make sure `ops` is installed and `PYTHONPATH` includes `src` and `lib`:

```bash
PYTHONPATH=src:lib pytest tests/unit
```

Note: integration tests are skipped automatically if `jubilant` is not installed.


## Dependency ownership and BOMs

`pyproject.toml` and `uv.lock` define Python dependencies. `dependencies/upstreams.json`
records upstream ownership; `dependencies/vendored.json` pins the exact source,
commit, hash, license and LIB metadata of each shipped library. Locally modified
libraries identify their downstream source and upstream owner explicitly. An
upstream catalog entry alone does not install a dependency.

Review dependency updates together with their locks, source pins and compatibility
tests. Do not replace a locally patched library without reviewing its documented
changes. Run these checks from this repository:

```bash
uv lock --check
uv run tox -e provenance
python3 tools/dependency_bom.py --verify-upstream
python3 tools/dependency_bom.py --output build/development.cdx.json
# Set CHARM_PATH and CHARM_BASE to the artifact and base actually built.
python3 tools/dependency_bom.py --artifact "$CHARM_PATH" --base "$CHARM_BASE" \
  --arch amd64 --output build/runtime.cdx.json
```

The development CycloneDX BOM describes the locked development/test dependency
graph. The runtime BOM checks installed distribution versions and vendored bytes
against the built archive and records its checksum, source revision and input
hashes. These BOMs do not cover OS packages, downloaded workload binaries or
transitive build-tool environments. CI verifies provenance and keeps generated
BOMs as artifacts rather than source files.

Rule compression uses Canonical's public `cosl` API. Rule acceptance, retention
and size policy belong to this charm; no shared Dwellir transport package or
cross-repository source synchronization is required.
