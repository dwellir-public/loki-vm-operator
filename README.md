<!--
Avoid using this README file for information that is maintained or published elsewhere, e.g.:

* charmcraft.yaml > published on Charmhub
* documentation > published on (or linked to from) Charmhub
* detailed contribution guide > documentation or CONTRIBUTING.md

Use links instead.
-->

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
- `loki_push_api` (provides Loki push endpoint)

## 3-node cluster behavior

When deployed with three units, `loki-vm` forms a memberlist cluster. Each unit
advertises its address to peers and the ring uses `memberlist.join_members` to
discover the other units. This assumes a shared backend storage (S3/MinIO) if you
intend to ingest to multiple units.

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
