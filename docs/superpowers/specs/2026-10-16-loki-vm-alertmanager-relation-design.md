# loki-vm Alertmanager relation — design

Date: 2026-10-16. Status: approved for planning.

## 1. Goal

Let the Loki ruler deliver evaluated alert notifications to `alertmanager-vm` through a Juju
relation, so that alert rules received over `loki_push_api` produce pages without any
hand-edited `config-override`. This mirrors the `alertmanager` relation already implemented in
`mimir-vm` (branch `feat/alertmanager-relation`, commits 6dfaa49..b7202e3) and the provider side
in `alertmanager-vm` (`alerting` endpoint, `alertmanager_dispatch` interface).

## 2. Scope

In scope:

- A new `alertmanager` requirer endpoint on `loki-vm`.
- Rendering the related Alertmanager unit URLs into the generated Loki configuration.
- Unit tests, one jubilant end-to-end test, README and architecture documentation.

Out of scope:

- TLS to Alertmanager, Alertmanager discovery via DNS, per-tenant routing, and any change to
  `config-override` behaviour beyond documenting that the relation is ignored while an override
  is set.
- Changes to `alertmanager-vm`.

## 3. Endpoint

```yaml
requires:
  alertmanager:
    interface: alertmanager_dispatch
    optional: true
    limit: 1
    description: Send ruler alert notifications to related Alertmanager units.
```

`limit: 1` because the vendored consumer library calls `model.get_relation(name)`, which
raises `TooManyRelatedAppsError` when two applications are related on the same endpoint
(the same ruling was taken for `mimir-vm`). Multiple *units* of one Alertmanager application
are supported.

## 4. Library

Vendor `lib/charms/alertmanager_k8s/v1/alertmanager_dispatch.py` byte-for-byte from
`canonical/alertmanager-k8s-operator` at commit `0ffba9a3dff6a03134c994c493e45888681d9a94`
(LIBID `37f1ca6f8fe84e3092ebbf6dc2885310`, LIBAPI 1, LIBPATCH 2, sha256
`82d0de7b4350d795c6ea6619ee219678b692339d1e65968e97433748bb132443`). The same file is already
vendored in `mimir-vm`. Record it in `dependencies/upstreams.json` and `dependencies/vendored.json`
so `tools/dependency_bom.py --verify-upstream` and `tox -e provenance` stay green.

The library declares `PYDEPS = ["pydantic>=2"]`. `pydantic` is today only a transitive dependency
(via `cosl`); add `pydantic>=2,<3` to `pyproject.toml` runtime dependencies so the charm's own
manifest states what the vendored code needs.

## 5. Configuration rendering

`src/config_builder.py` already accepts `alertmanager_url: Optional[str]` and, when set, renders:

```yaml
ruler:
  enable_api: true
  rule_path: <data_dir>/ruler-tmp
  alertmanager_url: <value>
  enable_alertmanager_v2: true
```

No builder change is required. The charm supplies the value as the sorted, comma-joined set of
URLs published by the related Alertmanager units, or `None` when there are none. Loki treats each
comma-separated URL as a separate Alertmanager group, so every related unit receives every
notification. With no related units the rendered configuration is byte-identical to today's.

When `config-override` is set the override is the entire configuration; the relation URL is not
injected and the charm logs a warning naming the relation. Operators who use an override must set
`ruler.alertmanager_url` themselves.

## 6. Charm flow

- `AlertmanagerConsumer(self, relation_name="alertmanager")` is constructed in `__init__`.
- Its `cluster_changed` event and the raw `alertmanager-relation-created`, `-joined` and
  `-broken` events all call one handler that invokes
  `self._reconcile_runtime(status_message="updating alertmanager configuration")`, exactly as
  the `s3` handlers do. Existing change detection, single-unit restart with readiness wait, and
  the clustered rolling restart apply unchanged.
- `_alertmanager_url()` returns `",".join(sorted(urls)) or None`, where `urls` comes from
  `AlertmanagerConsumer.get_cluster_info()`.
- During `alertmanager-relation-departed` the vendored consumer can raise `KeyError` because the
  departing unit is still listed in `relation.units` while its databag is gone (observed live in
  `mimir-vm`). The charm catches `KeyError` and falls back to parsing each remaining unit's data
  with the library's own `_ProviderSchemaV1` / `_ProviderSchemaV0` models, skipping units whose
  databag read raises. Units with an empty databag (joined, nothing published yet) are ignored.
- The `alertmanager` relation is never a rule source: `_reconcile_rules` iterates only
  `loki_push_api` relations and is not modified.

## 7. Tests

Unit (`ops[testing]` Scenario, existing patterns in `tests/unit/test_charm.py`):

1. Builder renders the ruler block with and without `alertmanager_url` (config builder test).
2. `relation-changed` with two units renders the comma-joined, sorted URL and restarts Loki.
3. A unit with an empty databag is ignored and nothing restarts.
4. `relation-broken` drops `ruler.alertmanager_url` and restarts.
5. The fallback parser handles both v1 (`url`) and v0 (`public_address` + `scheme`) data when
   the consumer raises `KeyError`.
6. `relation-departed` keeps the remaining unit's URL without a hook error.
7. `config-override` set: the override text is written unchanged even with a related
   Alertmanager, and a warning is logged.

Integration (`tests/integration/test_alertmanager_relation.py`, gated on
`ALERTMANAGER_CHARM_PATH`, reusing the existing `rule-provider` test charm via
`RULE_PROVIDER_CHARM_PATH`): deploy `loki-vm`, `alertmanager-vm` and `rule-provider` with an
always-firing LogQL rule (`vector(1) > 0`); relate; wait until Loki's live `/config` shows
`alertmanager_url: http://`; assert the alert appears in Alertmanager's `/api/v2/alerts`;
remove the relation; assert the URL disappears from the live config.

## 8. Documentation

- README: add `alertmanager` to the relations list; a new "Alertmanager notifications"
  subsection under "Alert rules on `loki_push_api`" describing the relation, the restart on
  change, the `limit: 1` ruling and the `config-override` interaction; update the sentence
  "Notifications require separately configured Alertmanager routing" to point at the relation.
- `docs/charm-architecture.md`: add the relation to "Relation Flows".

## 9. Rulings carried from mimir-vm

- `limit: 1` (see §3).
- Departed-unit fallback at the charm layer importing the library's private schema classes,
  because the vendored file must stay byte-identical to upstream for provenance checks.
- Unit URLs are rendered as published (`http://<unit-address>:9093`); `web_external_url` on the
  Alertmanager side is not involved.
