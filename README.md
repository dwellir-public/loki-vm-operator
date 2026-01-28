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
