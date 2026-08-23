# loki-vm

Charmhub package name: loki-vm
More information: https://charmhub.io/loki-vm

Machine charm that installs and manages Grafana Loki on Ubuntu. It targets VM deployments
and provides a base for adding relations, configuration, and operational actions.

## Usage

Build the charm locally:

```
charmcraft pack
```

Deploy a built charm:

```
juju deploy ./loki-vm_ubuntu-24.04-amd64.charm
```

## Configuration

Configuration options are defined in `charmcraft.yaml`. This charm is still
under active development and additional options will be added as features land.

Loki configuration is rendered to `/etc/loki/config.yml` to match the package
default. The original package file is preserved as
`/etc/loki/config.yml.package-default` for reference and rollback.

## Actions

Planned actions include:
- `loki-service` (start/stop/restart/reload)
- `show-runtime-config`
- `upgrade-loki`

## Testing log ingest/query

Basic smoke test for push + query (run on the unit or via `juju exec`):

```bash
juju exec --unit loki-vm/0 -- curl -sS http://127.0.0.1:3100/ready
```

If not ready yet, wait a few seconds and try again. Then push a test log line:

```bash
juju exec --unit loki-vm/0 -- bash -lc 'ts=$(date +%s%N); payload=$(printf "{\"streams\":[{\"stream\":{\"job\":\"manual-test\",\"app\":\"loki-vm\"},\"values\":[[\"%s\",\"hello from loki-vm test\"]]}]}" "$ts"); curl -sS -H "Content-Type: application/json" -X POST http://127.0.0.1:3100/loki/api/v1/push -d "$payload"'
```

Query over a time range (last 5 minutes):

```bash
juju exec --unit loki-vm/0 -- bash -lc 'end=$(date +%s%N); start=$((end-5*60*1000000000)); curl -sS -G http://127.0.0.1:3100/loki/api/v1/query_range --data-urlencode "query={job=\"manual-test\",app=\"loki-vm\"}" --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode "step=1s"'
```

## Relations

Supported relations:
- `replicas` (peers) for clustering
- `ingress` (ingress_per_unit)
- `s3` (requires) for upstream S3 provider contracts
- `loki_push_api` (provides Loki push endpoint)

### Alert rules on `loki_push_api`

The same standard `loki_push_api` relation accepts application-owned
`alert_rules` JSON from Alloy or another compatible producer. The leader
validates each remote application independently and merges groups in stable
relation-ID/group-name order without changing expressions, names, or labels.
The merged groups are written to Loki's ruler API in the charm-owned
`juju-loki-vm` namespace; they are separate from the log ingestion path and do
not require Alertmanager to be configured.
For a single local unit, the ruler store is filesystem-backed on the attached
`loki-persisted` volume. With Loki authentication disabled, the charm prepares
the implicit `fake` tenant directory before the first API write. Clustered Loki
already requires S3; the ruler uses that same shared backend under its own
`ruler` prefix so leadership changes do not leave unit-local rule copies.
Both chunks and rules use Loki's Thanos object-store client with the existing
chunk directory or S3 bucket unchanged; path-style S3 lookup remains enabled
for Garage and other compatible providers.
Generated configuration requires Loki 3.4.0 or newer because Grafana introduced
the Thanos object-store client and `use_thanos_objstore` in Loki 3.4. The charm
does not upgrade the workload during `upgrade-charm`; an older retained package
is reported as waiting until the operator upgrades Loki or supplies a compatible
`config-override`. See Grafana's
[storage-client migration guide](https://grafana.com/docs/loki/latest/setup/migrate/migrate-storage-clients/).

When `config-override` is used, it must retain `auth_enabled: false`, Loki's
HTTP listener on port 3100, `ruler.enable_api: true`, a non-empty
`ruler.rule_path`, and a `ruler_storage.backend`. The charm reports a clear
waiting status instead of attempting rule reconciliation against an override
that omits this contract.

Each relation document must be strictly below 60 KiB. The charm also bounds
JSON depth, node count, group-name bytes, cache size, the number of admitted
source relations, and aggregate group/rule and ruler-API work. One apply has a
fixed total deadline; if a timed-out write has mutated Loki, restoration of the
captured namespace receives a separate bounded recovery deadline. Mutation
responses are streamed, size-limited, and always closed. A malformed or over-limit update retains that relation's
last-known-good snapshot while valid sibling relations continue. Valid omission
or relation removal withdraws owned groups.

The `replicas` application databag stores a bounded compressed cache containing
per-relation snapshots and the last accepted rendered state. This supports
replay after leadership changes, charm upgrades, service restarts, and periodic
reconciliation. Candidate persistence or ruler API failure retains and
replays the prior accepted state. Rule bodies are never written to charm logs.

Loki runs with `auth_enabled: false`, and its ingestion, query, and ruler APIs
share the listener on `0.0.0.0:3100`. The charm therefore assumes a trusted
Juju model network: the ruler API is not authenticated independently. Do not
expose Loki's management routes directly to untrusted networks. When publishing
ingestion or query endpoints externally, use ingress/firewall segmentation that
does not publish `/loki/api/v1/rules` and restrict direct port 3100 access to
trusted charm and operator traffic.

After a single-unit start or configuration-driven restart, the charm waits for
the local `/ready` endpoint before publishing final runtime health. The wait is
bounded: if Loki is still starting, the hook completes successfully with
`waiting for Loki readiness` Maintenance status, and later lifecycle events
retry health and rule convergence. Clustered deployments continue to use the
separate rolling-restart coordination and readiness checks.

Inspect loaded alert rules directly:

```bash
juju exec --unit loki-vm/0 -- \
  curl -fsS 'http://127.0.0.1:3100/prometheus/api/v1/rules?type=alert'
```

### S3 migration integration inputs

The live S3 upgrade test is selected only with explicit local artifacts and a
pinned baseline workload version:

```bash
CHARM_PATH=./loki-vm_amd64.charm \
BASELINE_CHARM_PATH=/path/to/baseline-loki.charm \
BASELINE_LOKI_VERSION=3.4.6 \
GARAGE_CHARM_PATH=/path/to/garage.charm \
uv run tox -e integration -- tests/integration/test_s3_rule_migration.py::test_s3_upgrade_preserves_logs_rules_and_leader_recovery
```

The test fails explicitly when a required variable or artifact is missing. It
asserts that the installed Loki version matches the pin before refresh and is
unchanged afterward, because charm refresh does not upgrade the workload package.

## 3-node cluster behavior

When deployed with three units, `loki-vm` forms a memberlist cluster. Each unit
advertises its address to peers and the ring uses `memberlist.join_members` to
discover the other units. This assumes a shared backend storage (S3/MinIO) if you
intend to ingest to multiple units.

### Object storage behavior

`loki-vm` now supports the Canonical `s3` integration pattern based on
`object-storage-charmlib`.

- Without an `s3` relation, single-unit Loki stays on the local filesystem.
- With an `s3` relation, Loki switches to TSDB single-store on S3.
- Multi-unit clustered Loki now waits for `s3` before claiming it is fully
  configured.
- Validated providers are:
  - `garage-vm:s3`
  - `s3-integrator:s3-credentials` track `2`

In other words, `loki-vm` is compatible with the same
`object-storage-charmlib` relation contract used by Canonical's
`s3-integrator` track `2`. The charm does not require a Garage-specific schema
or any custom S3 shim.

Local disk is still used for:

- WAL
- active TSDB index files
- TSDB cache
- compactor working files
- local rules state
- ruler API scratch state under the effective Loki data directory

Garage object storage is used for:

- shipped TSDB blocks
- shipped TSDB index data
- retention delete-request state when retention is enabled

Example clustered deployment with Garage:

```bash
juju deploy garage-vm garage-vm --num-units 3 --config replication-mode=3
juju deploy loki-vm loki-vm --num-units 3 --storage loki-persisted=rootfs,2G --config retention-period=30
juju integrate loki-vm:s3 garage-vm:s3
```

Example deployment with Canonical `s3-integrator` track `2`:

```bash
juju deploy s3-integrator --channel=2/edge
juju deploy loki-vm loki-vm --num-units 1 --storage loki-persisted=rootfs,2G
juju config s3-integrator endpoint=http://10.232.126.109 region=us-east-1 bucket=mybucket s3-uri-style=path
juju add-secret s3-creds access-key=<ACCESS_KEY> secret-key=<SECRET_KEY>
juju grant-secret s3-creds s3-integrator
juju config s3-integrator credentials=secret:<secret-id>
juju integrate s3-integrator:s3-credentials loki-vm:s3
```

For plain HTTP S3 endpoints such as MicroCeph RGW in local test environments,
keep the endpoint scheme as `http://...`; `loki-vm` will normalize the endpoint
and render `insecure: true` automatically.

### Example provider switch: `garage-vm` to Ceph RGW via `s3-integrator`

The validated operator pattern is:

- create one RW S3 account on the external provider
- create one bucket per workload
- deploy one `s3-integrator` app per bucket in the same Juju model as the consumer
- switch relations one consumer at a time

Concrete example using:

- Ceph RGW running on the `admin/lxd-hosts` model hosts `192.168.243.250-252`
- `loki-vm` running in model `erik-lonroth@external/observability1`
- bucket `observability1-loki`
- shared RW account also used by Mimir

Create the RGW user and bucket on `singapore-admin1`:

```bash
ssh dwellir@192.168.243.250
sudo ./cephadm shell -- radosgw-admin user create --uid observability1-s3 --display-name "observability1 S3"
sudo ./cephadm shell -- radosgw-admin user info --uid observability1-s3
export AWS_ACCESS_KEY_ID='<ACCESS_KEY>'
export AWS_SECRET_ACCESS_KEY='<SECRET_KEY>'
export AWS_DEFAULT_REGION='us-east-1'
export S3_ENDPOINT='http://192.168.243.250:8081'
aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket observability1-loki
```

Deploy and configure the Loki-specific `s3-integrator` in the consumer model:

```bash
juju deploy s3-integrator loki-s3 --channel 2/edge
juju add-secret ceph-rgw-observability1 access-key='<ACCESS_KEY>' secret-key='<SECRET_KEY>'
juju grant-secret ceph-rgw-observability1 loki-s3
juju config loki-s3 \
  endpoint='http://192.168.243.250:8081' \
  bucket='observability1-loki' \
  credentials='secret:<secret-id>' \
  s3-uri-style='path' \
  s3-api-version='4' \
  region='us-east-1'
```

Cut over Loki from `garage-vm` to the new provider or just relate if you dont have a previous s3:

```bash
juju remove-relation garage-vm:s3 loki-vm:s3
juju integrate loki-s3:s3-credentials loki-vm:s3
```

Verify:

```bash
juju status loki-s3 loki-vm
juju ssh loki-vm/0 'sudo grep -n "observability1-loki\|192.168.243.250:8081" /etc/loki/config.yml'
```

Then run a normal Loki push/query smoke test through `loki-loadbalancer-vm`.

Operational notes:

- deploy `s3-integrator` in the same model as `loki-vm`; the external Ceph cluster does not need to be in the same Juju model
- use `s3-uri-style=path` for IP-based RGW endpoints
- for production, prefer a load balancer or VIP in front of the three RGW daemons instead of pointing at a single node IP
- switching providers can cause transient WAL flush errors while the old credentials are being revoked; this is expected during the cutover window

Set `retention-period` deliberately for any deployment that is meant to run for
longer than short-lived testing. The charm now defaults to `14` days as a guard
against unbounded growth, but operators should still choose a value that fits
their expected workload and available storage. A finite value such as `14`,
`30`, or `60` protects against eventually depleting available storage in
Garage-backed object storage.

### How ingestion works

For clustered `loki-vm`, it is useful to separate Loki replication from Garage
replication:

1. A client such as Alloy pushes logs to the published Loki write endpoint.
2. The receiving Loki unit acts as distributor and routes the stream according
   to the Loki ring.
3. In the current charm, Loki runs with `common.replication_factor: 1`, so a
   given write is ingested once by the owning ingester rather than being copied
   to all Loki units.
4. Loki then persists durable data through its configured storage backend.
5. When `loki-vm` is related to `garage-vm:s3`, that durable data is stored in
   Garage-backed object storage.

This means:

- `garage-vm replication-mode=3` replicates object data across the three Garage
  nodes.
- it does **not** mean Loki writes every log line to all three Loki units.
- in the current implementation, durable backend storage is replicated by
  Garage, while Loki ingestion itself is not triplicated across all ingesters.

So the effective model is:

- one logical Loki ingest per stream write, according to the Loki ring, and
- replicated object storage durability underneath, provided by Garage.

### Rough storage sizing

For `loki-vm` related to `garage-vm:s3`, long-term retained data is primarily
stored in Garage object storage. Local Loki disk is still needed for WAL, cache,
and compactor working files, but object storage is the main capacity driver.

Use this rough equation for logical retained Loki data:

```text
S_logical = N * E * B * 86400 * R * ((1 + I) / C)
```

Where:

- `N` = number of servers
- `E` = average log entries per second per server
- `B` = average bytes per log entry
- `R` = retention in days
- `I` = metadata/index overhead fraction
- `C` = raw-to-stored compression ratio

Then estimate physical Garage storage consumed as:

```text
S_physical = S_logical * G
```

Where:

- `G` = Garage replication factor
- for `garage-vm --config replication-mode=3`, use `G = 3`

Useful rule-of-thumb values for syslog:

- `B`: `200` to `350` bytes per line
- `C`: `4` to `8`
- `I`: `0.05` to `0.20`

Worked example for `10` servers sending a normal syslog workload:

- `N = 10`
- `E = 10` lines/sec/server
- `B = 250`
- `R = 30` days
- `I = 0.10`
- `C = 5`
- `G = 3`

Raw ingest per day:

```text
GiB/day_raw = (N * E * B * 86400) / 1024^3
            ≈ 2.01 GiB/day
```

Logical retained Loki data:

```text
GiB_logical_total = 2.01 * 30 * (1.10 / 5)
                  ≈ 13.3 GiB
```

Physical Garage storage with replication `3`:

```text
GiB_garage_physical = 13.3 * 3
                    ≈ 39.9 GiB
```

So a reasonable first estimate for that workload is:

- about `13 GiB` of logical Loki retained data, and
- about `40 GiB` of physical Garage storage consumed

If the same `10` servers are noisier, for example `50` lines/sec/server at
`300` bytes/line, the same equation yields roughly:

- `12.1 GiB/day` raw ingest
- `79.9 GiB` logical retained data at `30` days
- `240 GiB` physical Garage storage at replication `3`

Practical recommendation:

- set `retention-period` explicitly, for example `14`, `30`, or `60`
- the default `14` days is only a guardrail, not a sizing recommendation for
  every environment
- do not set `retention-period=0` unless unbounded growth is intentional
- size Garage storage for retained data multiplied by the Garage replication
  factor

Example with explicit retention:

```bash
juju config loki-vm retention-period=30
```

This is primarily a safety control to avoid exhausting available disk/object
storage over time.

### Ingestion endpoint publishing

The charm publishes a Loki push API endpoint via the `loki_push_api` relation.
Behavior depends on whether `external-url` is set:

- **`external-url` set (recommended for clusters):**
  - Only the **leader** unit publishes the endpoint.
  - The published endpoint is exactly the `external-url` base with
    `/loki/api/v1/push` appended.
  - Use this when you have a load balancer, ingress, or proxy in front of the
    Loki cluster.

- **`external-url` unset:**
  - Each unit publishes its own unit address.
  - Clients (e.g., Alloy) will fan‑out writes to all endpoints, which can
    duplicate logs unless you are intentionally doing multi‑write.

## External URL configuration (details)

`external-url` controls the base address advertised to clients. It should be a
scheme + host + optional port (no `/loki/api/v1/push` suffix).

Example with a load balancer:

```bash
juju config loki-vm external-url="https://logs.example.com"
```

Example with a direct unit address:

```bash
juju config loki-vm external-url="http://10.0.0.10:3100"
```

The charm appends `/loki/api/v1/push` internally when publishing relation data.

## Cross-model logging (single unit)

To allow a client charm in another model to push logs to a single-unit Loki VM,
offer the `loki_push_api` relation from the Loki model and consume it in the
client model.

On the Loki model (where `loki-vm/0` is deployed):

```bash
juju offer loki-vm:loki_push_api
```

On the client model:

```bash
juju consume <controller>:<loki-model>.loki-vm
juju relate <client-app>:logging loki-vm:loki_push_api
```

If the client expects a different relation name, adjust the left-hand side
accordingly.

`external-url` controls the base address advertised to clients. It should be a
scheme + host + optional port (no `/loki/api/v1/push` suffix), for example:

```bash
juju config loki-vm external-url="http://10.0.0.10:3100"
```

If you are using ingress or a proxy, set `external-url` to the externally
reachable base URL instead, for example:

```bash
juju config loki-vm external-url="https://logs.example.com"
```

The charm will append `/loki/api/v1/push` internally when publishing the
relation endpoint.

## Other resources

<!-- If your charm is documented somewhere else other than Charmhub, provide a link separately. -->

- [Developing](DEVELOPING.md)
- [Contributing](CONTRIBUTING.md) <!-- or link to other contribution documentation -->

- See the [Juju documentation](https://documentation.ubuntu.com/juju/3.6/howto/manage-charms/) for more information about developing and improving charms.
