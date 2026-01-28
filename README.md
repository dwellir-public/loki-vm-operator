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

Planned relations include:
- `peers` (replicas)
- `ingress`
- `loki_push_api`

## Other resources

<!-- If your charm is documented somewhere else other than Charmhub, provide a link separately. -->

- [Developing](DEVELOPING.md)
- [Contributing](CONTRIBUTING.md) <!-- or link to other contribution documentation -->

- See the [Juju documentation](https://documentation.ubuntu.com/juju/3.6/howto/manage-charms/) for more information about developing and improving charms.
