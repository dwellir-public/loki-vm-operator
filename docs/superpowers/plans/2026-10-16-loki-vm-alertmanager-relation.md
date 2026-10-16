# loki-vm Alertmanager Relation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the Loki ruler send evaluated alerts to `alertmanager-vm` by adding an `alertmanager` requirer endpoint (`alertmanager_dispatch` interface) to `loki-vm` and rendering the related units' URLs into `ruler.alertmanager_url` of the generated Loki configuration.

**Architecture:** Vendor the upstream `alertmanager_dispatch` v1 consumer library (the exact file already vendored in `mimir-vm`); feed the sorted, comma-joined output of `AlertmanagerConsumer.get_cluster_info()` into the existing `ConfigBuilder(alertmanager_url=...)` argument, which already renders `ruler.alertmanager_url` and `ruler.enable_alertmanager_v2`; route relation events through the existing `_reconcile_runtime` so change detection and restarts stay as they are. No other behaviour changes.

**Tech Stack:** Python 3.10+, `ops>=3,<4`, `ops[testing]` (Scenario), `pydantic>=2`, existing `uv`/`tox`/`ruff`/`pyright` setup, `jubilant` integration harness on the local LXD controller.

**Spec:** `docs/superpowers/specs/2026-10-16-loki-vm-alertmanager-relation-design.md` (this repo). Reference implementation: `/home/ibrahim/Projects/mimir-vm-operator` branch `feat/alertmanager-relation`, `src/charm.py` (`_alertmanager_urls`, `_alertmanager_urls_skipping_gone_units`) and `tests/unit/test_charm.py` lines 182-222 and 2197-2300.

## Global Constraints

- Endpoint name `alertmanager`, interface `alertmanager_dispatch`, `optional: true`, `limit: 1` (spec §3).
- Rendered key when URLs exist: `ruler.alertmanager_url` holding the sorted, comma-joined URLs, plus `ruler.enable_alertmanager_v2: true` (already emitted by the builder); when there are no URLs the rendered config must stay byte-identical to today's (spec §5).
- `config-override` wins entirely; the relation URL is never injected into an override, and a warning is logged when both are present (spec §5).
- Vendored library byte-identical to upstream commit `0ffba9a3dff6a03134c994c493e45888681d9a94`, sha256 `82d0de7b4350d795c6ea6619ee219678b692339d1e65968e97433748bb132443`, recorded in both `dependencies/upstreams.json` and `dependencies/vendored.json` (spec §4). `tox -e provenance` must pass after every task.
- `pydantic>=2,<3` declared in `pyproject.toml` runtime dependencies with `uv.lock` refreshed (spec §4).
- Source headers on new files: `# Copyright 2026 Erik Lönroth` / `# See LICENSE file for licensing details.`
- Work on branch `feat/alertmanager-relation` off `main` (already created); commit after every task with the trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Before every commit: `uv run tox -e lint,static,unit` green (unit suite currently passes; keep it that way).
- `.charm` files for the integration test live under `/home/ibrahim/Projects/build/`, never under `/tmp` or the scratchpad (the juju snap cannot read them there).

## Review Focus

1. Removing the relation (`relation-broken`) must drop `ruler.alertmanager_url` in the same hook; a stale URL makes the ruler retry a dead endpoint forever. (Pinned in Task 3.)
2. A remote Alertmanager unit whose databag has no `url` yet (joined, not yet published) must be ignored, not rendered as an empty string, and must not restart Loki. (Pinned in Task 3.)
3. Two Alertmanager units must render in a deterministic order so a hook that lists them in a different order does not restart Loki needlessly. (Pinned in Task 3.)
4. `alertmanager-relation-departed` must not crash the hook when the departing unit's databag is already gone (observed live in `mimir-vm`). (Pinned in Task 3.)
5. With `config-override` set, the override text must be written unchanged even when an Alertmanager is related; the relation URL must not be spliced in. (Pinned in Task 3.)

---

### Task 1: Vendor the dispatch library, declare the dependency and the endpoint

**Files:**
- Create: `lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py`
- Modify: `dependencies/upstreams.json`, `dependencies/vendored.json`, `pyproject.toml`, `uv.lock`, `charmcraft.yaml`, `tests/unit/test_charm.py:16-31` (the `META` dict)
- Test: `tests/unit/test_dependency_bom.py` (existing, must keep passing), `tox -e provenance`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: importable `charms.alertmanager_k8s.v1.alertmanager_dispatch.AlertmanagerConsumer`, `_ProviderSchemaV0`, `_ProviderSchemaV1`; the `alertmanager` endpoint in charm metadata and in the unit-test `META`.

- [ ] **Step 1: Copy the library from mimir-vm and verify its digest**

```bash
mkdir -p lib/charms/alertmanager_k8s/v1
cp /home/ibrahim/Projects/mimir-vm-operator/lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py \
   lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py
sha256sum lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py
```

Expected: `82d0de7b4350d795c6ea6619ee219678b692339d1e65968e97433748bb132443`. If it differs, stop: the source copy is not the upstream file.

- [ ] **Step 2: Run the BOM verification to see it fail on the undeclared library**

Run: `uv run tox -e provenance`
Expected: FAIL (undeclared library `lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py`).

- [ ] **Step 3: Declare the library in both dependency manifests**

Append to the object in `dependencies/upstreams.json` (after the `ingress_per_unit` entry):

```json
  "alertmanager_dispatch": {
    "repository": "https://github.com/canonical/alertmanager-k8s-operator",
    "role": "runtime-vendored",
    "source_path": "lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py",
    "distribution": null
  }
```

Append to the array in `dependencies/vendored.json`:

```json
  {
    "path": "lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py",
    "sha256": "82d0de7b4350d795c6ea6619ee219678b692339d1e65968e97433748bb132443",
    "library": {
      "LIBID": "37f1ca6f8fe84e3092ebbf6dc2885310",
      "LIBAPI": 1,
      "LIBPATCH": 2
    },
    "scope": "runtime",
    "upstream_path": "lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py",
    "repository": "https://github.com/canonical/alertmanager-k8s-operator",
    "commit": "0ffba9a3dff6a03134c994c493e45888681d9a94",
    "license": "Apache-2.0"
  }
```

Keep both files valid JSON (watch the trailing commas) and two-space indented like the existing entries.

- [ ] **Step 4: Declare pydantic and refresh the lock**

In `pyproject.toml`, add to the `dependencies` list, keeping alphabetical-ish grouping with the other charmlib comments:

```toml
    "pydantic>=2,<3", # charms.alertmanager_k8s.v1.alertmanager_dispatch
```

Run: `uv lock`
Expected: `uv.lock` changes only by adding `pydantic` as a direct dependency of the project; the resolved pydantic version stays `2.12.5`. Inspect with `git diff --stat uv.lock` (a handful of lines).

- [ ] **Step 5: Declare the endpoint**

In `charmcraft.yaml`, under `requires:` after the `s3` block:

```yaml
  alertmanager:
    interface: alertmanager_dispatch
    optional: true
    limit: 1
    description: |
      Send ruler alert notifications to related Alertmanager units
      (for example alertmanager-vm). Rendered as `ruler.alertmanager_url`.
```

In `tests/unit/test_charm.py`, extend `META["requires"]`:

```python
    "requires": {
        "ingress": {"interface": "ingress_per_unit"},
        "s3": {"interface": "s3", "limit": 1},
        "alertmanager": {"interface": "alertmanager_dispatch", "limit": 1},
    },
```

- [ ] **Step 6: Verify provenance, BOM tests and the full unit suite pass**

Run: `uv run tox -e provenance && uv run tox -e lint,static,unit`
Expected: all PASS. `python3 tools/dependency_bom.py --verify-upstream` must also pass (it fetches the file from GitHub at the recorded commit; network required).

- [ ] **Step 7: Commit**

```bash
git add lib/charms/alertmanager_k8s dependencies/upstreams.json dependencies/vendored.json \
        pyproject.toml uv.lock charmcraft.yaml tests/unit/test_charm.py
git commit -m "feat: vendor alertmanager_dispatch and declare the alertmanager endpoint

Vendors canonical/alertmanager-k8s-operator lib v1 (LIBPATCH 2) at commit
0ffba9a3 with BOM entries, declares pydantic explicitly, and adds the
optional single-app alertmanager requirer endpoint.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Pin the config builder's ruler rendering

**Files:**
- Test: `tests/unit/test_config_builder.py`

**Interfaces:**
- Consumes: `ConfigBuilder(alertmanager_url: Optional[str])` in `src/config_builder.py:59` and the `_ruler` property at `src/config_builder.py:163-177` (both exist; no production change in this task).
- Produces: pinned behaviour the charm in Task 3 relies on.

- [ ] **Step 1: Write the pinning tests**

Append to `tests/unit/test_config_builder.py`:

```python
def _builder(**overrides) -> ConfigBuilder:
    kwargs = {
        "instance_addr": "10.0.0.1",
        "ingestion_rate_mb": 4,
        "ingestion_burst_size_mb": 15,
        "retention_period": 14,
        "reporting_enabled": True,
        "data_dir": "/var/lib/loki",
        "loki_version": "3.4.0",
    }
    kwargs.update(overrides)
    return ConfigBuilder(**kwargs)


def test_ruler_without_alertmanager_has_no_notification_keys() -> None:
    ruler = _builder().build()["ruler"]
    assert ruler == {"enable_api": True, "rule_path": "/var/lib/loki/ruler-tmp"}


def test_ruler_with_alertmanager_url_enables_v2_delivery() -> None:
    ruler = _builder(
        alertmanager_url="http://10.0.0.5:9093,http://[fd00::7]:9093"
    ).build()["ruler"]
    assert ruler["alertmanager_url"] == "http://10.0.0.5:9093,http://[fd00::7]:9093"
    assert ruler["enable_alertmanager_v2"] is True
    assert ruler["enable_api"] is True


def test_ruler_with_empty_alertmanager_url_renders_nothing() -> None:
    ruler = _builder(alertmanager_url="").build()["ruler"]
    assert "alertmanager_url" not in ruler
    assert "enable_alertmanager_v2" not in ruler
```

If `ConfigBuilder` is not already imported at the top of the file, add `from config_builder import ConfigBuilder` next to the existing import.

- [ ] **Step 2: Run the tests**

Run: `uv run pytest tests/unit/test_config_builder.py -v -k ruler`
Expected: all three PASS immediately. These tests pin existing behaviour; if any fails, the builder has drifted from spec §5 and the failure must be reported, not patched around.

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_config_builder.py
git commit -m "test: pin ruler alertmanager_url rendering in ConfigBuilder

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Wire the consumer into the charm

**Files:**
- Modify: `src/charm.py` (imports near line 12-20; `__init__` after `self.s3_client = S3Requirer(...)` at line 129; a new handler next to `_on_s3_changed` at line 299; `_render_config_text` at line 549-574; two new helpers after `_render_config_text`)
- Test: `tests/unit/test_charm.py`

**Interfaces:**
- Consumes: `AlertmanagerConsumer`, `_ProviderSchemaV0`, `_ProviderSchemaV1` from Task 1; `ConfigBuilder(alertmanager_url=...)` from Task 2.
- Produces: `LokiVmCharm._alertmanager_url() -> str | None`, `LokiVmCharm._alertmanager_urls() -> list[str]`, `LokiVmCharm._alertmanager_urls_skipping_gone_units() -> list[str]`, handler `LokiVmCharm._on_alertmanager_changed(event)`.

- [ ] **Step 1: Write the failing charm tests**

Add near the other helpers at the top of `tests/unit/test_charm.py` (after `_s3_relation`):

```python
def _alertmanager_relation(
    *, remote_units_data: dict[int, dict[str, str]] | None = None, relation_id: int = 42
) -> testing.Relation:
    return testing.Relation(
        "alertmanager",
        interface="alertmanager_dispatch",
        id=relation_id,
        remote_app_name="alertmanager-vm",
        remote_units_data=(
            remote_units_data
            if remote_units_data is not None
            else {0: {"url": "http://10.0.0.7:9093"}}
        ),
    )


def _mock_running_loki(monkeypatch: pytest.MonkeyPatch, seen: dict) -> None:
    """Make a single-unit Loki look installed and running; capture writes and restarts."""
    on_disk: dict[str, str | None] = {"config": None}

    def mock_write_config_text(config_text: str, **_):
        seen["config"] = config_text
        on_disk["config"] = config_text

    monkeypatch.setattr("charm.loki.is_active", lambda **_: True)
    monkeypatch.setattr("charm.loki.get_version", mock_get_version)
    monkeypatch.setattr("charm.loki.verify_config", lambda **_: None)
    monkeypatch.setattr("charm.loki.write_config_text", mock_write_config_text)
    monkeypatch.setattr(
        "charm.loki.restart", lambda: seen.setdefault("calls", []).append("restart")
    )
    monkeypatch.setattr("charm.LokiVmCharm._read_config_from_disk", lambda self: on_disk["config"])
    monkeypatch.setattr("charm.LokiVmCharm._wait_for_local_ready", lambda *_, **__: None)


def _rendered_yaml(seen: dict) -> dict:
    text = "\n".join(line for line in seen["config"].splitlines() if not line.startswith("#"))
    return yaml.safe_load(text)
```

Add `import logging` to the imports and, next to the existing `from charm import ...` line:

```python
from charms.alertmanager_k8s.v1.alertmanager_dispatch import AlertmanagerConsumer
```

Append the tests at the end of the file:

```python
def test_alertmanager_relation_renders_sorted_url_and_restarts(monkeypatch: pytest.MonkeyPatch):
    ctx = _context()
    seen: dict = {}
    _mock_running_loki(monkeypatch, seen)
    relation = _alertmanager_relation(
        remote_units_data={0: {"url": "http://10.0.0.7:9093"}, 1: {"url": "http://10.0.0.5:9093"}}
    )

    ctx.run(ctx.on.relation_changed(relation, remote_unit=1), testing.State(relations=[relation]))

    ruler = _rendered_yaml(seen)["ruler"]
    assert ruler["alertmanager_url"] == "http://10.0.0.5:9093,http://10.0.0.7:9093"
    assert ruler["enable_alertmanager_v2"] is True
    assert seen["calls"] == ["restart"]


def test_alertmanager_unit_without_url_is_ignored_and_does_not_restart(
    monkeypatch: pytest.MonkeyPatch,
):
    ctx = _context()
    seen: dict = {}
    _mock_running_loki(monkeypatch, seen)
    relation = _alertmanager_relation(remote_units_data={0: {}})

    # Baseline render (first write always restarts), then the joined hook must be a no-op.
    state = ctx.run(ctx.on.config_changed(), testing.State(relations=[relation]))
    seen["calls"] = []
    ctx.run(ctx.on.relation_joined(relation, remote_unit=0), state)

    assert "alertmanager_url" not in _rendered_yaml(seen)["ruler"]
    assert seen["calls"] == []


def test_alertmanager_relation_broken_drops_url_and_restarts(monkeypatch: pytest.MonkeyPatch):
    ctx = _context()
    seen: dict = {}
    _mock_running_loki(monkeypatch, seen)
    relation = _alertmanager_relation()

    state = ctx.run(
        ctx.on.relation_changed(relation, remote_unit=0), testing.State(relations=[relation])
    )
    assert "alertmanager_url" in _rendered_yaml(seen)["ruler"]
    seen["calls"] = []
    ctx.run(ctx.on.relation_broken(relation), state)

    assert "alertmanager_url" not in _rendered_yaml(seen)["ruler"]
    assert seen["calls"] == ["restart"]


def _raise_key_error(self) -> set[str]:
    raise KeyError("departing unit")


def test_alertmanager_urls_fall_back_when_consumer_raises_key_error(
    monkeypatch: pytest.MonkeyPatch,
):
    ctx = _context()
    seen: dict = {}
    _mock_running_loki(monkeypatch, seen)
    monkeypatch.setattr(AlertmanagerConsumer, "get_cluster_info", _raise_key_error)
    relation = _alertmanager_relation(
        remote_units_data={
            0: {"url": "http://10.0.0.5:9093"},
            1: {"public_address": "10.0.0.6:9093", "scheme": "http"},
        }
    )

    ctx.run(ctx.on.config_changed(), testing.State(relations=[relation]))

    assert _rendered_yaml(seen)["ruler"]["alertmanager_url"] == (
        "http://10.0.0.5:9093,http://10.0.0.6:9093"
    )


def test_alertmanager_relation_departed_keeps_remaining_unit_url(
    monkeypatch: pytest.MonkeyPatch,
):
    ctx = _context()
    seen: dict = {}
    _mock_running_loki(monkeypatch, seen)
    relation = _alertmanager_relation(remote_units_data={0: {"url": "http://10.0.0.7:9093"}})

    # Unit 1 departs with no databag left; without the fallback the hook raises KeyError.
    ctx.run(ctx.on.relation_departed(relation, remote_unit=1), testing.State(relations=[relation]))

    assert _rendered_yaml(seen)["ruler"]["alertmanager_url"] == "http://10.0.0.7:9093"


def test_config_override_ignores_alertmanager_relation_with_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    ctx = _context()
    seen: dict = {}
    _mock_running_loki(monkeypatch, seen)
    override = "auth_enabled: false\nserver:\n  http_listen_port: 3100\n"
    relation = _alertmanager_relation()

    with caplog.at_level(logging.WARNING, logger="charm"):
        ctx.run(
            ctx.on.relation_changed(relation, remote_unit=0),
            testing.State(config={"config-override": override}, relations=[relation]),
        )

    assert seen["config"] == override
    assert "alertmanager" in caplog.text and "config-override" in caplog.text


def test_alertmanager_relation_is_not_a_rule_source(monkeypatch: pytest.MonkeyPatch):
    ctx = _context()
    seen: dict = {}
    _mock_running_loki(monkeypatch, seen)
    _FakeRulerApi.calls = []
    monkeypatch.setattr("charm.LokiRulerApiClient", _FakeRulerApi)
    peer = testing.PeerRelation("replicas", interface="loki_replica", id=99)
    relation = _alertmanager_relation(
        remote_units_data={0: {"url": "http://10.0.0.7:9093", "alert_rules": '{"groups":[]}'}}
    )

    ctx.run(
        ctx.on.relation_changed(relation, remote_unit=0),
        testing.State(leader=True, relations=[relation, peer]),
    )

    assert all(groups == [] for groups in _FakeRulerApi.calls)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_charm.py -v -k alertmanager`
Expected: FAIL. The first test fails with `KeyError: 'alertmanager_url'` (or an `AttributeError` if `_rendered_yaml` finds no `ruler` change), the departed test errors with `KeyError`, the override test fails on the missing warning. The rule-source test may already pass; that is fine.

- [ ] **Step 3: Add the imports and the consumer**

In `src/charm.py` imports, after `import ops`:

```python
import pydantic
from charms.alertmanager_k8s.v1.alertmanager_dispatch import (
    AlertmanagerConsumer,
    _ProviderSchemaV0,  # pyright: ignore[reportPrivateUsage]
    _ProviderSchemaV1,  # pyright: ignore[reportPrivateUsage]
)
```

In `__init__`, directly after `self.s3_client = S3Requirer(self, relation_name="s3")`:

```python
        self.alertmanager_consumer = AlertmanagerConsumer(self, relation_name="alertmanager")
        framework.observe(
            self.alertmanager_consumer.on.cluster_changed,  # pyright: ignore
            self._on_alertmanager_changed,
        )
        framework.observe(self.on.alertmanager_relation_created, self._on_alertmanager_changed)
        framework.observe(self.on.alertmanager_relation_joined, self._on_alertmanager_changed)
        framework.observe(self.on.alertmanager_relation_broken, self._on_alertmanager_changed)
```

- [ ] **Step 4: Add the handler and the URL helpers**

After `_on_s3_changed`:

```python
    def _on_alertmanager_changed(self, event: ops.EventBase) -> None:
        """Handle Alertmanager relation changes and re-render the ruler notification target."""
        self._reconcile_runtime(status_message="updating alertmanager configuration")
```

In `_render_config_text`, extend the override branch and pass the URL to the builder:

```python
        override = str(self.config.get("config-override", "")).strip()
        if override:
            if not self._override_has_ruler_contract(override):
                logger.warning(
                    "config-override does not enable relation alert-rule reconciliation"
                )
            if self._alertmanager_url():
                logger.warning(
                    "config-override is set; the alertmanager relation URL is not rendered. "
                    "Set ruler.alertmanager_url in the override to deliver notifications."
                )
            return override
        source_data = self._sorted_source_data()
        builder = ConfigBuilder(
            instance_addr=self._instance_addr(),
            ingestion_rate_mb=int(self.config["ingestion-rate-mb"]),
            ingestion_burst_size_mb=int(self.config["ingestion-burst-size-mb"]),
            retention_period=int(self.config["retention-period"]),
            reporting_enabled=bool(self.config["reporting-enabled"]),
            grafana_external_url=source_data.external_url,
            datasource_uid=source_data.get_unit_uid(self.unit.name),
            data_dir=self.data_dir,
            memberlist_join_members=self._memberlist_join_members(),
            s3=self._storage_backend_state().s3,
            loki_version=loki.get_version(),
            alertmanager_url=self._alertmanager_url(),
        )
```

After `_render_config_text`, add:

```python
    def _alertmanager_url(self) -> str | None:
        """Return the comma-joined Alertmanager URLs for `ruler.alertmanager_url`, or None.

        Loki treats each comma-separated URL as a separate Alertmanager group, so every
        related unit receives every notification.
        """
        urls = self._alertmanager_urls()
        return ",".join(urls) if urls else None

    def _alertmanager_urls(self) -> list[str]:
        """Return every URL published by related Alertmanager units, sorted.

        During `alertmanager-relation-broken` ops already excludes the departing relation
        from the model, so the rendered config drops its URLs in the same hook. During
        `alertmanager-relation-departed` the vendored consumer can raise `KeyError` for the
        departing unit; that case goes through the fallback, which skips the departing unit.
        """
        try:
            return sorted(self.alertmanager_consumer.get_cluster_info())
        except KeyError:
            return self._alertmanager_urls_skipping_gone_units()

    def _alertmanager_urls_skipping_gone_units(self) -> list[str]:
        """Parse Alertmanager unit data exactly like the vendored consumer, skipping gone units.

        The vendored library must stay byte-identical to upstream for provenance checks, so
        the departing-unit tolerance lives here and mirrors its v1-then-v0 parsing.
        """
        relation = self.model.get_relation("alertmanager")
        if relation is None:
            return []
        urls: set[str] = set()
        for unit in relation.units:
            try:
                rel_data = relation.data[unit]
            except KeyError:
                logger.debug("Skipping Alertmanager unit %s with no relation data", unit.name)
                continue
            if not rel_data:
                continue
            try:  # v1
                urls.add(_ProviderSchemaV1(**rel_data).url)
            except pydantic.ValidationError as ev1:
                try:  # v0
                    data = _ProviderSchemaV0(**rel_data)
                except pydantic.ValidationError as ev0:
                    logger.warning("Relation data failed validation for v1: %s", ev1)
                    logger.warning("Relation data failed validation for v0: %s", ev0)
                else:
                    urls.add(f"{data.scheme}://{data.public_address}")
        return sorted(urls)
```

- [ ] **Step 5: Run the alertmanager tests, then the whole suite**

Run: `uv run pytest tests/unit/test_charm.py -v -k alertmanager`
Expected: 7 PASS.

Run: `uv run tox -e lint,static,unit`
Expected: PASS. If pyright complains about the private imports despite the ignore comments, keep the comments exactly as written (they match the accepted form in `mimir-vm`).

- [ ] **Step 6: Prove the departed test needs the fallback**

Temporarily replace the body of `_alertmanager_urls` with `return sorted(self.alertmanager_consumer.get_cluster_info())`, run `uv run pytest tests/unit/test_charm.py -k departed_keeps_remaining`, confirm it FAILS with `KeyError`, then restore the fallback and re-run to confirm PASS. Mention the result in the commit body.

- [ ] **Step 7: Commit**

```bash
git add src/charm.py tests/unit/test_charm.py
git commit -m "feat: render related Alertmanager units into ruler.alertmanager_url

Relation events reuse _reconcile_runtime, so change detection and the
single-unit or rolling restart paths are unchanged. relation-departed
falls back to charm-side parsing because the vendored consumer raises
KeyError for the departing unit (regression test fails without it).
config-override keeps precedence and logs a warning.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Documentation and the end-to-end test

**Files:**
- Create: `tests/integration/test_alertmanager_relation.py`
- Modify: `tests/integration/conftest.py` (append a fixture), `tox.ini:78-84` (`pass_env`), `README.md:59-75` and `README.md:537`, `docs/charm-architecture.md:55-64`

**Interfaces:**
- Consumes: the `alertmanager` endpoint (Task 1) and runtime behaviour (Task 3); existing fixtures `charm`, `rule_provider_charm`, `juju` in `tests/integration/conftest.py`; the `rule-provider` test charm's `send-loki-logs` endpoint and `alert-rules` config.
- Produces: fixture `alertmanager_charm`, env var `ALERTMANAGER_CHARM_PATH`.

- [ ] **Step 1: Add the fixture and pass the env var through tox**

Append to `tests/integration/conftest.py`:

```python
@pytest.fixture(scope="session")
def alertmanager_charm() -> pathlib.Path:
    """Return the prebuilt alertmanager-vm charm for the Alertmanager relation test."""
    configured = os.environ.get("ALERTMANAGER_CHARM_PATH")
    if not configured:
        pytest.skip("Set ALERTMANAGER_CHARM_PATH to run the Alertmanager relation test")
    path = pathlib.Path(configured)
    if not path.exists():
        raise FileNotFoundError(f"Alertmanager charm does not exist: {path}")
    return path
```

In `tox.ini` `[testenv:integration]` `pass_env`, add after `RULE_PROVIDER_CHARM_PATH`:

```ini
    ALERTMANAGER_CHARM_PATH
```

- [ ] **Step 2: Write the end-to-end test**

Create `tests/integration/test_alertmanager_relation.py`:

```python
# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

"""The ruler delivers an always-firing relation rule to a related alertmanager-vm."""

import json
import pathlib
import time

import pytest

jubilant = pytest.importorskip("jubilant")

LOKI = "loki-vm"
AM = "alertmanager-vm"
PROVIDER = "rule-provider"

RULES = json.dumps(
    {
        "groups": [
            {
                "name": "alertmanager-e2e",
                "rules": [
                    {"alert": "LokiToAlertmanager", "expr": "vector(1) > 0", "for": "0s"},
                ],
            }
        ]
    }
)


def _wait_for_url(juju, *, present: bool, timeout: float = 300) -> str:
    """Poll Loki's live config until the ruler Alertmanager URL is (or is not) rendered."""
    deadline = time.time() + timeout
    while True:
        try:
            config = juju.ssh(f"{LOKI}/0", "curl -fsS http://127.0.0.1:3100/config")
        except Exception:  # noqa: BLE001 - Loki may be restarting
            config = ""
        if config and (("alertmanager_url: http://" in config) == present):
            return config
        if time.time() > deadline:
            raise AssertionError(f"alertmanager_url present={present} never observed in config")
        time.sleep(10)


def test_rule_alert_reaches_alertmanager(
    charm: pathlib.Path,
    alertmanager_charm: pathlib.Path,
    rule_provider_charm: pathlib.Path,
    juju,
):
    juju.deploy(charm.resolve(), app=LOKI)
    juju.deploy(alertmanager_charm.resolve(), app=AM)
    juju.deploy(rule_provider_charm.resolve(), app=PROVIDER, config={"alert-rules": RULES})
    juju.integrate(f"{PROVIDER}:send-loki-logs", f"{LOKI}:loki_push_api")
    juju.integrate(f"{LOKI}:alertmanager", f"{AM}:alerting")
    juju.wait(jubilant.all_active, timeout=1500)

    _wait_for_url(juju, present=True)

    deadline = time.time() + 600
    while time.time() < deadline:
        body = juju.ssh(f"{AM}/0", "curl -fsS http://127.0.0.1:9093/api/v2/alerts")
        if any(a["labels"].get("alertname") == "LokiToAlertmanager" for a in json.loads(body)):
            break
        time.sleep(15)
    else:
        raise AssertionError("LokiToAlertmanager never reached Alertmanager")

    juju.remove_relation(f"{LOKI}:alertmanager", f"{AM}:alerting")
    juju.wait(jubilant.all_active, timeout=600)
    _wait_for_url(juju, present=False)
```

- [ ] **Step 3: Document the relation**

In `README.md` "Relations" list (line 62-66), add:

```markdown
- `alertmanager` (requires, `alertmanager_dispatch`) for ruler notifications to `alertmanager-vm`
```

After the "Alert rules on `loki_push_api`" paragraph (before "For a single local unit..." or directly after that block, keeping the section intact), add:

````markdown
### Alertmanager notifications

Relate `loki-vm:alertmanager` to `alertmanager-vm:alerting` to deliver evaluated relation
rules as notifications. The charm renders every related Alertmanager unit URL, sorted and
comma-joined, into `ruler.alertmanager_url` with `enable_alertmanager_v2: true`; Loki treats
each URL as its own Alertmanager group. The endpoint accepts one Alertmanager application
(`limit: 1`) because the upstream consumer library cannot address two applications on one
endpoint; several units of that application are fine. Adding, removing or re-addressing a
unit changes the generated config and restarts Loki (rolling restart in clustered mode).
When `config-override` is set the override is used verbatim: the relation URL is not injected
and a warning is logged, so set `ruler.alertmanager_url` in the override yourself.

```
juju integrate loki-vm:alertmanager alertmanager-vm:alerting
```
````

Change the sentence at `README.md:537` from
`fit. Notifications require separately configured Alertmanager routing.` to
`fit. Notifications are delivered over the `alertmanager` relation (see "Alertmanager notifications").`

In `docs/charm-architecture.md` "Relation Flows" list, add after the `s3` bullet:

```markdown
- `alertmanager`: consumes `alertmanager_dispatch` unit URLs and renders them, sorted and
  comma-joined, into `ruler.alertmanager_url`; changes go through the normal config
  reconcile and restart path. Ignored while `config-override` is set.
```

- [ ] **Step 4: Lint, static and unit**

Run: `uv run tox -e lint,static,unit`
Expected: PASS.

- [ ] **Step 5: Pack and run the end-to-end test on LXD**

```bash
charmcraft pack   # plain pack; --destructive-mode fails on this 26.04 host
cp loki-vm_*.charm /home/ibrahim/Projects/build/loki-vm_amd64.charm
```

Reuse the packed charms from the earlier work:
`/home/ibrahim/Projects/build/alertmanager-vm_amd64.charm` (alertmanager-vm 5daa765) and the
rule-provider test charm. The rule-provider charm here is `tests/integration/rule-provider`
(Loki's own, not Mimir's): pack it with `charmcraft pack` inside that directory and copy the
result to `/home/ibrahim/Projects/build/loki-rule-provider-test_amd64.charm`.

```bash
CHARM_PATH=/home/ibrahim/Projects/build/loki-vm_amd64.charm \
ALERTMANAGER_CHARM_PATH=/home/ibrahim/Projects/build/alertmanager-vm_amd64.charm \
RULE_PROVIDER_CHARM_PATH=/home/ibrahim/Projects/build/loki-rule-provider-test_amd64.charm \
uv run tox -e integration -- tests/integration/test_alertmanager_relation.py
```

Expected: `1 passed` (budget about 5-8 minutes after packing). Afterwards run `juju switch local:obs` and confirm no leftover `jubilant-*` models with `juju models`.

If the alert never reaches Alertmanager but the URL is rendered, inspect the ruler on the unit:
`juju ssh loki-vm/0 'curl -fsS http://127.0.0.1:3100/loki/api/v1/rules'` and
`juju ssh loki-vm/0 'curl -fsS http://127.0.0.1:3100/prometheus/api/v1/alerts'`. If Loki rejects
`vector(1) > 0`, replace the rule expression with
`sum(count_over_time({job=~".+"}[1h])) >= 0` and push one log line to
`http://<loki>:3100/loki/api/v1/push` before waiting (the existing `test_charm.py` shows the
push payload shape). Record whichever expression worked in the commit message.

- [ ] **Step 6: Commit**

```bash
git add tests/integration/test_alertmanager_relation.py tests/integration/conftest.py tox.ini \
        README.md docs/charm-architecture.md
git commit -m "docs: describe the alertmanager relation and add its integration test

e2e on LXD: deploy loki-vm, alertmanager-vm and the rule-provider test charm,
relate, assert the always-firing rule reaches Alertmanager, then assert the
URL leaves the live config after the relation is removed. <N> passed in <T>s.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review

- **Spec coverage:** §3 endpoint → Task 1; §4 library, BOM, pydantic → Task 1; §5 rendering → Task 2 (pinned) and Task 3 (charm supplies value, override warning); §6 flow, fallback, not-a-rule-source → Task 3; §7 tests → Tasks 2-4; §8 docs → Task 4.
- **Placeholder scan:** none; the only conditional step (LogQL fallback expression in Task 4 Step 5) gives the exact alternative.
- **Type consistency:** `_alertmanager_url() -> str | None` feeds `ConfigBuilder(alertmanager_url: Optional[str])`; `_alertmanager_urls() -> list[str]`; fixture `alertmanager_charm -> pathlib.Path` matches the test signature.
- **Review Focus:** items 1-5 are each pinned by a named test in Task 3.
