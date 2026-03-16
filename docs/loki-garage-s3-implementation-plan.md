# Loki Garage S3 Integration Plan

## Goal

Add support for using `garage-vm` as the object-storage backend for `loki-vm`
through the existing `s3` relation.

The target is to keep `loki-vm` as a machine charm running the single-binary
Loki process while allowing its persisted log data to live in Garage-backed
object storage instead of only on the local filesystem.

## Why this is needed

Today `loki-vm` is filesystem-only:

- `schema_config.configs[*].object_store` is always `filesystem`
- `storage_config.filesystem.directory` is always used for chunks
- `compactor.delete_request_store` is `filesystem` when retention is enabled

That is acceptable for local development and single-node testing, but it is not
the right storage model for multi-unit Loki ingest or longer-lived clusters.

Upstream Loki guidance is:

- TSDB single-store is the recommended storage mode for Loki 2.8+
- scalable deployments require object storage
- single-binary can use filesystem, but object storage is recommended for
  production-style deployments

Sources:

- https://grafana.com/docs/loki/latest/configure/storage/
- https://grafana.com/docs/loki/latest/setup/install/helm/configure-storage/
- https://grafana.com/docs/loki/latest/configure/examples/configuration-examples/
- https://grafana.com/docs/loki/latest/operations/storage/retention/

## Existing integration contract

`garage-vm` already provides the fields `loki-vm` needs:

- `endpoint`
- `bucket`
- `access-key`
- `secret-key-secret-id`
- `region`
- `tls`
- `insecure`

Current provider behavior:

- one Garage bucket per `s3` relation
- one Garage access key per `s3` relation
- secret key delivered through a Juju secret

This contract is documented in:

- [README.md](/home/erik/Loki-project/garage-vm-operator/README.md)

## Release-1 design decisions

- Use the existing `garage-vm:s3` relation as-is. Do not require a new storage
  interface.
- Use Loki TSDB single-store with `object_store: s3`.
- Use one Garage bucket per `s3` relation. Do not require multiple buckets.
- Keep local Juju storage for:
  - WAL
  - TSDB active index directory
  - TSDB cache
  - compactor working directory
  - local ruler/rules files
- Keep filesystem mode as the fallback when no `s3` relation exists.
- Do not require `s3` for single-node local testing.
- Treat `s3` as the expected backend for multi-unit clustered Loki ingestion.

## Expected runtime behavior

### Without `s3`

- `loki-vm` keeps the current filesystem-backed config
- single-unit testing remains simple
- multi-unit Loki is still technically deployable, but shared durable storage is
  absent and the charm should document that clearly

### With `s3`

- `loki-vm` renders Loki config for TSDB single-store on S3
- chunks and shipped TSDB index data are stored in Garage
- local disk still holds WAL, active TSDB index files, cache, compactor working
  files, and ruler local state
- the charm should wait until valid `s3` relation data and secret content are
  available before claiming Loki is fully configured

## Config mapping plan

When `s3` relation data is present, render Loki approximately as:

```yaml
schema_config:
  configs:
    - from: <date>
      store: tsdb
      object_store: s3
      schema: v13
      index:
        prefix: index_
        period: 24h

storage_config:
  tsdb_shipper:
    active_index_directory: <local path>
    cache_location: <local path>
  aws:
    s3: s3://<access-key>:<secret-key>@<endpoint-host>/<bucket>
    region: <region>
    s3forcepathstyle: true
    insecure: <true when Garage says insecure>

compactor:
  working_directory: <local path>
  retention_enabled: <true|false>
  delete_request_store: s3
```

Notes:

- Garage is S3-compatible, so Loki should use the `aws` storage config block,
  the same pattern Loki documents for MinIO and other S3-compatible endpoints.
- `s3forcepathstyle: true` should be the default for Garage compatibility.
- The endpoint must be normalized carefully, including host, port, and scheme.
- Secret material must come from the Juju secret referenced by
  `secret-key-secret-id`, not from plain relation data.

## Charm changes required

### Metadata and relation wiring

- Add `requires.s3` to [charmcraft.yaml](/home/erik/Loki-project/loki-vm-operator/charmcraft.yaml)
- Observe `s3-relation-created`, `s3-relation-changed`, and
  `s3-relation-broken`
- Reconcile config when Juju secret contents change

### Relation parsing

- Add a small `s3` helper/model in `src/` similar to the existing Mimir and
  Garage handling patterns
- Parse:
  - endpoint
  - bucket
  - access key
  - secret ID
  - region
  - tls
  - insecure
- Resolve the Juju secret to get the secret key
- Normalize endpoint details for Loki’s `aws` config format

### Config builder changes

- Extend [config_builder.py](/home/erik/Loki-project/loki-vm-operator/src/config_builder.py)
  with an explicit storage mode
- Keep filesystem rendering as one branch
- Add S3 rendering as the other branch
- Switch:
  - `schema_config.configs[*].object_store`
  - `storage_config`
  - `common.storage`
  - `compactor.delete_request_store`
- Preserve existing local directories for WAL, cache, compactor, and rules

### Status and safety

- Block or wait if the `s3` relation is present but incomplete
- Block or wait if the Juju secret cannot be resolved
- Keep last-known-good config behavior when relation data is invalid
- Expose a clear status such as:
  - `waiting for S3 credentials`
  - `waiting for S3 endpoint data`
  - `Loki configured with Garage S3 backend`

### Documentation

- Update the main README with:
  - single-node filesystem deployment flow
  - clustered deployment flow with Garage relation
  - expected storage split between local disk and object store
  - retention caveats for object storage

## Proposed delivery phases

- [x] Phase 0: Add `docs/` planning and record design decisions
- [x] Phase 1: Add `s3` relation metadata and relation/secret parsing helpers
- [x] Phase 2: Add config-builder support for filesystem vs Garage-backed S3
- [x] Phase 3: Add charm reconciliation, statuses, and last-known-good handling
- [x] Phase 4: Add unit tests for relation parsing, secret resolution, and
      rendered Loki config
- [x] Phase 5: Redeploy `loki-vm` against a live `garage-vm` cluster and
      validate end-to-end ingestion/query
- [x] Phase 6: Update README with deployment and operational guidance

## Validation plan

### Unit validation

- rendered config stays filesystem-backed without `s3`
- rendered config switches to `object_store: s3` with valid relation data
- `storage_config.aws` is rendered correctly for Garage endpoint + bucket
- `delete_request_store` becomes `s3` when retention is enabled in S3 mode
- invalid or missing secret data does not overwrite the last-known-good config

### Live validation

1. Deploy `garage-vm` with 3 units
2. Deploy `loki-vm` with 3 units and local storage attached
3. Integrate `loki-vm:s3` with `garage-vm:s3`
4. Confirm Loki starts with the S3-backed config on all units
5. Push logs through Alloy or a manual push client
6. Query logs from a different Loki unit and from Grafana
7. Restart one Loki unit and confirm previously ingested data is still
   queryable
8. Confirm Garage bucket contents grow as Loki ingests data

## Open questions

- Should `loki-vm` remain permissive in clustered mode without `s3`, or should
  it explicitly block multi-unit ingest until object storage exists?
- Should the charm expose a config knob to force filesystem-only mode even when
  an `s3` relation is present, or should the relation always win?
- Should we add an action such as `show-storage-mode` so operators can confirm
  whether Loki is currently running in filesystem or Garage-backed mode?

## Recommended answers for now

- Do not block single-unit deployments without `s3`
- For multi-unit deployments, prefer a waiting/blocked status if `s3` is
  absent, because shared durable storage is the expected model
- Let relation-driven S3 mode take precedence over filesystem defaults
- Add a small status or action later if operators need explicit visibility
