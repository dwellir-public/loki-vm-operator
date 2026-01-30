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
