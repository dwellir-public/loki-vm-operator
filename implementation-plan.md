# Loki VM Charm Implementation Plan

This document is the checklist we will tick off as we build the Loki VM charm.
It is intentionally staged from basic scaffolding to more complex features.

## Phase 0 — Baseline alignment
- [x] Update `charmcraft.yaml` for ubuntu 24.04 amd64 only (uv plugin).
- [ ] Update metadata for name/title/summary/description to match `loki-vm`.
- [x] Declare `loki-persisted` storage in charmcraft and use it for data paths.
- [ ] Add relations: `peers` (replicas), `ingress`, `loki_push_api`.
- [ ] Add config options:
  - [x] `ingestion-rate-mb`
  - [x] `ingestion-burst-size-mb`
  - [x] `retention-period`
  - [x] `reporting-enabled`
  - [x] `external-url`
  - [x] `config-override` (full-file override)

## Phase 1 — Workload install + service control
- [x] Implement `src/loki.py` install path using the official Loki apt repository.
- [x] Add systemd unit management: start/stop/restart/reload.
- [x] Implement `get_version()` (from `loki --version`).
- [x] Add smoke checks to ensure service is up after start.
- [x] Do not install or support promtail; if log shipping is needed, prefer Grafana Alloy.

## Phase 2 — Config builder + persistence
- [x] Port/adapt `ConfigBuilder` from `loki-k8s-operator/src/config_builder.py`.
- [x] Store config at a stable path (e.g. `/etc/loki/config.yml`).
- [x] Use package-standard config path `/etc/loki/config.yml` and preserve the package default.
- [x] Use `loki-persisted` (juju storage) for TSDB/chunks/rules/compactor paths, if not specified use default from apt package.
- [x] Remove BoltDB-shipper support; use TSDB-only schema/storage config.
- [x] Keep a backup copy of the rendered config on disk.
- [x] Validate config with `loki -config.file ... -verify-config` before applying.
- [x] Add a generated-config header warning; preserve package default at `/etc/loki/config.yml.package-default`.
- [x] Persist invalid configs under `/tmp/loki-config-invalid-*.yaml` for debugging.
- [x] Ensure storage permissions for the Loki user (uid/gid from passwd) before restart.

### Testing of phase 0,1,2
- [x] Deploy charm and ensure Loki is installed and running.
- [x] Verify config persistence and validation (unit tests).
- [x] Test config override and drift detection (unit tests).

## Phase 3 — Charm orchestration basics
- [x] Wire events: install, start, config-changed, storage-attached/detaching.
- [x] Wire upgrade-charm handling.
- [x] Apply config only when valid; keep last-good config on validation failure.
- [x] Set workload version from `get_version()`.
- [x] Implement status logic (Active/Maintenance/Waiting on invalid config).
- [x] Ensure `upgrade-charm` does not restart Loki or rewrite config.

## Phase 4 — Config overrides + drift detection
- [ ] `config-override` replaces generated config entirely.
- [ ] Empty `config-override` regenerates config from charm logic.
- [ ] Detect on-disk manual edits and set Maintenance + warning log.
- [ ] Clear Maintenance when on-disk config matches in-memory config.

## Phase 5 — Relations
- [ ] Implement `LokiPushApiProvider` (v1 lib, fetched via `charmcraft fetch-lib`).
- [ ] Publish push endpoint using `external-url` when set.
- [ ] Implement peer relation for 1 or 3 instance clustering.
- [ ] Implement `ingress` relation updates (ingestion endpoint).

## Phase 6 — Actions + upgrades
- [ ] Action: `loki-service` (start/stop/restart/reload).
- [ ] Action: `show-runtime-config`.
- [ ] Action: `upgrade-loki` (optional version pin; safe upgrade flow).

## Phase 7 — Tests + docs
- [x] Unit tests for `Loki` workload class.
- [x] Unit tests for config rendering + validation behavior.
- [ ] Integration tests for deploy + relations.
- [x] Update README with usage, config, actions, and relations.
- [x] Ensure unit tests run without privileged rights; document test commands in `DEVELOPING.md`.
