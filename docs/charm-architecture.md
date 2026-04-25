# Loki VM Charm Architecture

## Overview

`loki-vm` manages a Grafana Loki systemd workload on Ubuntu machines. The charm renders Loki
configuration, validates it before writing, starts and restarts the service, publishes Loki and
Grafana datasource endpoints, and manages local or S3-backed storage.

## Workload Install and Bootstrap

On install, the charm adds the Grafana APT repository, installs the `loki` package, and preserves
the package default configuration before replacing it with Juju-managed config. On start, it
ensures the configured data directory exists, renders and validates config, starts Loki, and
publishes workload version information when available.

## Runtime and Service Management

`src/charm.py` owns Juju orchestration and status handling. `src/loki.py` owns host-level workload
operations such as package install, systemd interaction, config verification, and HTTP readiness
checks. The charm only writes validated config and keeps the last good config in stored state so
invalid updates do not replace a working deployment.

## Configuration Flow

The charm renders config through `src/config_builder.py` unless `config-override` is set. Generated
config is validated with `loki -verify-config` against a temporary file before it is written to
`/etc/loki/config.yml`. When the on-disk config drifts from the last good Juju-managed config,
`update-status` surfaces that drift until the charm re-applies a valid config.

## Relation Flows

- `ingress`: publishes the externally reachable Loki base URL when available.
- `loki_push_api`: publishes the push endpoint consumed by log shippers.
- `grafana-source`: publishes Loki datasource information for Grafana.
- `send-datasource`: republishes datasource UIDs assigned by Grafana over datasource-exchange.
- `replicas`: shares peer addresses for memberlist-based clustered Loki operation.
- `s3`: provides object storage credentials required for clustered Loki and optional for single-unit
  S3-backed deployments.

## Storage and Data Paths

By default, Loki stores data under `/var/lib/loki`. If the `loki-persisted` storage attachment is
present, the charm updates the effective data directory and maintains a symlink from the package
default path so upstream Loki expectations still hold. Generated config is written to
`/etc/loki/config.yml`, with backups under the data directory.

## Scaling and Cluster Health

For single-unit deployments, the charm uses the filesystem backend unless S3 is related. For
multi-unit deployments, the charm requires S3 and renders memberlist join members from the
`replicas` peer relation. `update-status` uses compact runtime summaries such as
`ready(3/3), storage(local)` or `ready(3/3), storage(s3(ok))` and downgrades to
`MaintenanceStatus` when not all expected units are ready. When S3 is configured, the charm also
probes the related S3 endpoint before reporting `s3(ok)`. The `cluster-health` action exposes the
same summary plus per-unit readiness details.

## Upgrade Strategy

Charm upgrades do not rewrite config or restart Loki during `upgrade-charm`. Runtime reconciliation
continues on config, relation, storage, and periodic status events. In clustered mode, config
changes are guaranteed to use rolling restarts rather than concurrent restarts across units. The
charm writes validated config on all units first, then restarts one unit at a time while waiting
for `/ready` and cluster health to recover before advancing to the next unit.

## Failure Recovery and Operational Notes

If config validation fails, the charm preserves the previous working config and stores the failed
candidate in `/tmp` for inspection. Storage relation errors are surfaced directly through Juju
status. Runtime cluster degradation is surfaced through compact readiness summaries, while the
`cluster-health` action provides per-unit detail for debugging.
