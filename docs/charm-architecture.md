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
For single-unit starts and configuration-driven restarts, the charm boundedly
waits for the local readiness endpoint before calculating final health. A
timeout remains a truthful, retryable `waiting for Loki readiness` Maintenance
status instead of failing the hook. Events that do not start or restart Loki do
not incur this wait, and blocking storage/config statuses keep precedence.

## Configuration Flow

The charm renders config through `src/config_builder.py` unless `config-override` is set. Generated
config is validated with `loki -verify-config` against a temporary file before it is written to
`/etc/loki/config.yml`. When the on-disk config drifts from the last good Juju-managed config,
`update-status` surfaces that drift until the charm re-applies a valid config.
The generated ruler block always enables the ruler API and its scratch path,
independently of whether an Alertmanager URL is configured.
Single-unit rule definitions use the filesystem ruler store on
`loki-persisted`; before configuration validation the charm prepares
`rules/fake` for Loki's fixed auth-disabled tenant and the `ruler-tmp` scratch
directory. Clustered deployments use their required shared S3 backend with a
dedicated `ruler` prefix, allowing the elected leader to reconcile one durable
namespace for all ruler processes.
The global Thanos object-store switch is rendered atomically for both existing
chunk/index storage and ruler storage: local paths stay under the same data
directory, while S3 keeps the same bucket, endpoint, credentials, and path-style
lookup. This avoids selecting the new ruler client while leaving the log data
plane on an inactive legacy storage block.
This generated configuration has an explicit Loki 3.4.0 minimum because that
release introduced `use_thanos_objstore`. Charm refresh intentionally does not
upgrade the APT workload: a known older installed version retains its last-good
configuration and produces a waiting status. A custom override may support a
different Loki version, but it must explicitly preserve the ruler API/storage
contract described above or the charm rejects it before writing.

## Relation Flows

- `ingress`: publishes the externally reachable Loki base URL when available.
- `loki_push_api`: publishes the push endpoint consumed by log shippers and receives standard
  application-owned `alert_rules` snapshots.
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

## Alert-rule Reconciliation

`src/rule_reconciler.py` validates relation JSON, enforces the sub-60-KiB value limit and bounded
tree/work limits, and deterministically merges groups by relation ID and group name. It preserves
expressions, names, and labels. At most 32 source relations are admitted, and explicit aggregate
group/rule, HTTP-operation, response-stream, and total apply-deadline limits bound ruler work. The leader owns the
single `juju-loki-vm` namespace through Loki's supported ruler HTTP API; unchanged state is not
rewritten, while updates and removals use group YAML requests. Mutation responses
are streamed, bounded, and closed. A failed or expired candidate write restores
the captured pre-apply namespace under an independent bounded recovery budget,
so an exhausted candidate deadline cannot prevent rollback.

The `replicas` application databag contains a versioned, compressed, bounded cache of each
relation's last-known-good snapshot plus the last accepted aggregate. Malformed sources are
isolated, first-seen malformed sources are skipped, and valid siblings continue. Apply or cache
failure triggers best-effort replay of accepted state without logging rule content. Relation
changed, broken, and departed events converge state, as do start, upgrade, leader election, and
update-status after a workload process restart.

### Ruler API trust boundary

The generated config deliberately retains Loki's existing `auth_enabled: false`
mode. In Loki, ingestion, query, and ruler management routes share the
`0.0.0.0:3100` listener, so the ruler route does not gain separate
authentication from this feature. The deployment threat model therefore
assumes that the Juju model network and direct workload port are trusted.
Operators must not expose `/loki/api/v1/rules` or unrestricted port 3100 to
untrusted networks. External ingress should segment allowed ingestion/query
paths, while firewall policy keeps ruler management reachable only by trusted
charm and operator traffic.

## Scaling and Cluster Health

For single-unit deployments, the charm uses the filesystem backend unless S3 is related. For
multi-unit deployments, the charm requires S3 and renders memberlist join members from the
`replicas` peer relation. `update-status` uses compact runtime summaries such as
`ready(3/3), storage(local)` or `ready(3/3), storage(s3(ok))` and downgrades to
`MaintenanceStatus` when not all expected units are ready. When S3 is configured, the charm also
probes the related S3 endpoint before reporting `s3(ok)`. The `cluster-health` action exposes the
same summary plus per-unit readiness details.

## Upgrade Strategy

Charm upgrades reconcile generated config so newly required ruler settings are applied, then
replay accepted rule state. Runtime reconciliation also continues on config, relation, storage,
and periodic status events. In clustered mode, config
changes are guaranteed to use rolling restarts rather than concurrent restarts across units. The
charm writes validated config on all units first, then restarts one unit at a time while waiting
for `/ready` and cluster health to recover before advancing to the next unit.

## Failure Recovery and Operational Notes

If config validation fails, the charm preserves the previous working config and stores the failed
candidate in `/tmp` for inspection. Storage relation errors are surfaced directly through Juju
status. Runtime cluster degradation is surfaced through compact readiness summaries, while the
`cluster-health` action provides per-unit detail for debugging.
