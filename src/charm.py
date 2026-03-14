#!/usr/bin/env python3
# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

"""Charm the application."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import ops
import yaml
from charms.grafana_k8s.v0.grafana_source import GrafanaSourceData, GrafanaSourceProvider
from charms.loki_k8s.v1.loki_push_api import LokiPushApiProvider
from charms.traefik_k8s.v1.ingress_per_unit import IngressPerUnitRequirer
from cosl.interfaces.datasource_exchange import DatasourceDict, DatasourceExchange

# A standalone module for workload-specific logic (no charming concerns):
import loki
from config_builder import (
    DEFAULT_CONFIG_BACKUP_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_DIR,
    DEFAULT_PACKAGE_CONFIG_BACKUP_PATH,
    ConfigBuilder,
)

logger = logging.getLogger(__name__)


class LokiVmCharm(ops.CharmBase):
    """Charm the application."""

    _stored = ops.StoredState()

    def __init__(self, framework: ops.Framework):
        """Initialize the charm, relations, and event observers."""
        super().__init__(framework)
        self._stored.set_default(
            data_dir=DEFAULT_DATA_DIR,
            last_good_config="",
            last_failed_config_path="",
            config_drifted=False,
        )
        self.ingress = IngressPerUnitRequirer(
            self,
            relation_name="ingress",
            port=3100,
            scheme=lambda: "http",
            strip_prefix=True,
        )
        self.loki_provider = LokiPushApiProvider(
            self,
            relation_name="loki_push_api",
            port=3100,
            scheme="http",
            path="loki/api/v1/push",
        )
        self.grafana_source_provider = GrafanaSourceProvider(
            charm=self,
            relation_name="grafana-source",
            source_type="loki",
            source_url=self._external_url_base(),
        )
        self.datasource_exchange = DatasourceExchange(
            self,
            provider_endpoint="send-datasource",
            requirer_endpoint=None,
        )
        framework.observe(self.on.install, self._on_install)
        framework.observe(self.on.start, self._on_start)
        framework.observe(self.on.config_changed, self._on_config_changed)
        framework.observe(self.on.upgrade_charm, self._on_upgrade_charm)
        framework.observe(self.on.update_status, self._on_update_status)
        framework.observe(self.ingress.on.ready_for_unit, self._on_ingress_changed)
        framework.observe(self.ingress.on.revoked_for_unit, self._on_ingress_changed)
        framework.observe(
            self.on.send_datasource_relation_changed,
            self._on_grafana_source_changed,
        )
        framework.observe(
            self.on.send_datasource_relation_departed,
            self._on_grafana_source_changed,
        )
        framework.observe(
            self.on.grafana_source_relation_joined,
            self._on_grafana_source_changed,
        )
        framework.observe(
            self.on.grafana_source_relation_changed,
            self._on_grafana_source_changed,
        )
        framework.observe(
            self.on.grafana_source_relation_created,
            self._on_grafana_source_changed,
        )
        framework.observe(
            self.on.grafana_source_relation_departed,
            self._on_grafana_source_changed,
        )
        framework.observe(
            self.on.loki_persisted_storage_attached, self._on_loki_persisted_storage_attached
        )
        framework.observe(
            self.on.loki_persisted_storage_detaching, self._on_loki_persisted_storage_detaching
        )
        framework.observe(self.on.replicas_relation_joined, self._on_replicas_changed)
        framework.observe(self.on.replicas_relation_changed, self._on_replicas_changed)
        framework.observe(self.on.replicas_relation_departed, self._on_replicas_changed)

    def _on_install(self, event: ops.InstallEvent):
        """Install the workload and preserve the package default config."""
        loki.install()
        loki.preserve_default_config(
            config_path=Path(DEFAULT_CONFIG_PATH),
            preserved_path=Path(DEFAULT_PACKAGE_CONFIG_BACKUP_PATH),
        )

    def _on_start(self, event: ops.StartEvent):
        """Start Loki, apply config, and set workload/version status."""
        self.unit.status = ops.MaintenanceStatus("starting workload")
        loki.ensure_data_dir(self.data_dir)
        config_ok = self._configure()
        loki.start()
        version = loki.get_version()
        if version is not None:
            self.unit.set_workload_version(version)
        self._refresh_loki_provider_endpoint()
        self._refresh_grafana_source_endpoint()
        self._update_datasource_exchange()
        if config_ok:
            self.unit.status = ops.ActiveStatus()
        else:
            self.unit.status = self._invalid_config_status()

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Re-render and apply configuration when charm config changes."""
        self.unit.status = ops.MaintenanceStatus("configuring Loki")
        self._refresh_loki_provider_endpoint()
        self._refresh_grafana_source_endpoint()
        if self._configure():
            self.unit.status = ops.ActiveStatus("Loki configuration updated and validated.")
        else:
            self.unit.status = self._invalid_config_status()

    def _on_upgrade_charm(self, event: ops.UpgradeCharmEvent) -> None:
        """Handle charm upgrade without restarting or rewriting configuration."""
        logger.info("Upgrade-charm event: skipping config rewrite and restart.")

    def _on_update_status(self, event: ops.UpdateStatusEvent) -> None:
        """Handle periodic status updates (detect drift and workload health)."""
        if not loki.is_active():
            self.unit.status = ops.MaintenanceStatus("Loki service not running")
            return
        self._reconcile_config_drift_status()
        if self._is_service_down_status() and not self._stored.config_drifted:
            self.unit.status = ops.ActiveStatus("Loki configuration updated and validated.")

    def _on_ingress_changed(self, event: ops.EventBase) -> None:
        """Handle ingress updates by refreshing the published endpoint."""
        self._refresh_loki_provider_endpoint()
        self._refresh_grafana_source_endpoint()

    def _on_grafana_source_changed(self, event: ops.EventBase) -> None:
        """Handle Grafana source relation changes and update datasource exchange."""
        self._update_datasource_exchange()

    def _on_replicas_changed(self, event: ops.RelationEvent) -> None:
        """Handle peer relation changes and reconfigure memberlist joins."""
        if event.relation:
            event.relation.data[self.unit]["address"] = self._instance_addr()
        self.unit.status = ops.MaintenanceStatus("updating cluster configuration")
        if self._configure():
            self.unit.status = ops.ActiveStatus("Loki configuration updated and validated.")
        else:
            self.unit.status = self._invalid_config_status()

    def _on_loki_persisted_storage_attached(self, event: ops.StorageAttachedEvent) -> None:
        """Update data dir when loki-persisted storage is attached."""
        storage_path = event.storage.location
        if storage_path is None:
            logger.warning("loki-persisted storage attached without a location")
            return
        Path(storage_path).mkdir(parents=True, exist_ok=True)
        self._stored.data_dir = str(storage_path)
        loki.ensure_data_dir(self._stored.data_dir)

    def _on_loki_persisted_storage_detaching(self, event: ops.StorageDetachingEvent) -> None:
        """Restore default data dir when loki-persisted storage detaches."""
        self._stored.data_dir = DEFAULT_DATA_DIR
        logger.warning(
            "loki-persisted storage detaching; data dir will be reset to %s",
            self._stored.data_dir,
        )
        loki.ensure_data_dir(self._stored.data_dir)

    @property
    def data_dir(self) -> str:
        """Return the data directory for Loki (storage if attached, else default)."""
        return str(self._stored.data_dir)

    def _configure(self) -> bool:
        """Render, validate, and persist Loki configuration.

        Flow:
        - Ensure Loki can write to the data directory.
        - Render config from `config-override` or ConfigBuilder output.
        - Verify config with `loki -verify-config` on a temp file.
        - If invalid, keep the last-good config and record the failed config path.
        - If valid, write the config + backup and update last-good config state.
        - Clear drift status after a successful apply.
        """
        loki.ensure_data_dir_permissions(self.data_dir)
        config_text = self._render_config_text()
        if not config_text:
            return True
        if not self._validate_config_text(config_text):
            logger.warning("Configuration validation failed; keeping previous config.")
            failed_path = self._persist_failed_config(config_text)
            if failed_path:
                self._stored.last_failed_config_path = str(failed_path)
            if self._stored.last_good_config:
                self.unit.status = self._invalid_config_status()
            return False
        loki.write_config_text(
            config_text,
            config_path=Path(DEFAULT_CONFIG_PATH),
            backup_path=Path(DEFAULT_CONFIG_BACKUP_PATH),
        )
        # Store the last good config
        self._stored.last_good_config = config_text
        if self._stored.config_drifted:
            logger.info("Loki config drift resolved by applying charm configuration.")
        self._stored.config_drifted = False
        return True

    def _persist_failed_config(self, config_text: str) -> Path | None:
        """Persist a failed config to /tmp for debugging."""
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                delete=False,
                prefix="loki-config-invalid-",
                suffix=".yaml",
                dir="/tmp",
            ) as tmp:
                tmp.write(config_text)
                failed_path = Path(tmp.name)
            logger.warning("Invalid Loki config kept at %s", failed_path)
            return failed_path
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to persist invalid config: %s", exc)
            return None

    def _invalid_config_status(self) -> ops.WaitingStatus:
        """Return a WaitingStatus message for invalid Loki config."""
        if self._stored.last_failed_config_path:
            message = (
                "Invalid Loki config; check logs and "
                f"{self._stored.last_failed_config_path}"
            )
        else:
            message = "Invalid Loki config; check logs to fix configuration"
        return ops.WaitingStatus(message)

    def _render_config_text(self) -> str:
        """Return the Loki config as a YAML string."""
        override = str(self.config.get("config-override", "")).strip()
        if override:
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
        )
        rendered = yaml.safe_dump(builder.build(), sort_keys=False)
        return f"{loki.GENERATED_CONFIG_HEADER}{rendered}"

    def _validate_config_text(self, config_text: str) -> bool:
        """Validate Loki config by writing to a temp file and running verify."""
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
                tmp.write(config_text)
                tmp_path = Path(tmp.name)
            loki.verify_config(config_path=tmp_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Loki config verification failed: %s", exc)
            return False
        finally:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()
        return True

    def _reconcile_config_drift_status(self) -> None:
        """Detect config drift and update unit status if appropriate."""
        drifted = self._has_config_drift()
        if drifted != self._stored.config_drifted:
            if drifted:
                logger.warning(
                    "Detected manual Loki config change at %s", DEFAULT_CONFIG_PATH
                )
            else:
                logger.info("Loki config drift cleared.")
        self._stored.config_drifted = drifted
        if drifted:
            if isinstance(self.unit.status, (ops.ActiveStatus, ops.MaintenanceStatus)):
                self.unit.status = ops.MaintenanceStatus(self._config_drift_message())
            return
        if self._is_drift_status():
            self.unit.status = ops.MaintenanceStatus("Loki configuration drift cleared.")

    def _has_config_drift(self) -> bool:
        """Return True when on-disk config differs from the last good config."""
        if not self._stored.last_good_config:
            return False
        on_disk = self._read_config_from_disk()
        if on_disk is None:
            return True
        return on_disk != self._stored.last_good_config

    def _read_config_from_disk(self) -> str | None:
        """Read the on-disk config text, returning None if unreadable."""
        path = Path(DEFAULT_CONFIG_PATH)
        try:
            return path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as exc:
            logger.warning("Failed to read Loki config from disk: %s", exc)
            return None

    def _config_drift_message(self) -> str:
        """Return the Maintenance status message for config drift."""
        return f"Manual Loki config change detected at {DEFAULT_CONFIG_PATH}"

    def _is_drift_status(self) -> bool:
        """Return True if the unit is in the drift Maintenance status."""
        return (
            isinstance(self.unit.status, ops.MaintenanceStatus)
            and self.unit.status.message == self._config_drift_message()
        )

    def _is_service_down_status(self) -> bool:
        """Return True if the unit is in the service-not-running status."""
        return (
            isinstance(self.unit.status, ops.MaintenanceStatus)
            and self.unit.status.message == "Loki service not running"
        )

    def _instance_addr(self) -> str:
        """Return the best-available address for the Loki ring and endpoints."""
        if getattr(self.unit, "private_address", None):
            return str(self.unit.private_address)
        if getattr(self.unit, "public_address", None):
            return str(self.unit.public_address)
        if getattr(self.unit, "address", None):
            return str(self.unit.address)
        for endpoint in ("replicas", "loki_push_api", "ingress"):
            binding = self.model.get_binding(endpoint)
            if binding and binding.network.bind_address:
                return str(binding.network.bind_address)
            if binding and binding.network.ingress_address:
                return str(binding.network.ingress_address)
        return "127.0.0.1"

    def _memberlist_join_members(self) -> list[str] | None:
        """Return join member addresses from the replicas relation."""
        relation = self.model.get_relation("replicas")
        if relation is None:
            return None
        members = []
        for unit in relation.units:
            addr = relation.data[unit].get("address")
            if addr and addr != self._instance_addr():
                members.append(addr)
        return members

    def _refresh_loki_provider_endpoint(self) -> None:
        """Publish Loki push API endpoint to relation data."""
        if self._external_url_configured() and not self._is_leader():
            self._clear_loki_provider_endpoint()
            return
        url = self._external_url_base()
        if url:
            self.loki_provider.update_endpoint(url=url)

    def _refresh_grafana_source_endpoint(self) -> None:
        """Publish Grafana datasource endpoint to relation data."""
        if self._external_url_configured() and not self._is_leader():
            self._clear_grafana_source_endpoint()
            return
        self.grafana_source_provider.update_source(source_url=self._external_url_base())

    def _external_url_base(self) -> str:
        """Return base URL (scheme://host:port[/path]) for Loki ingress."""
        external = str(self.config.get("external-url", "")).strip()
        if external:
            return self._normalize_external_url(external)
        if ingress_url := self._ingress_url():
            return ingress_url.rstrip("/")
        addr = self._instance_addr()
        return f"http://{addr}:3100"

    def _external_url_configured(self) -> bool:
        """Return True when external-url is set."""
        return bool(str(self.config.get("external-url", "")).strip())

    def _is_leader(self) -> bool:
        """Return True when this unit is the leader."""
        return self.unit.is_leader()

    def _clear_loki_provider_endpoint(self) -> None:
        """Clear the published endpoint on this unit's relation data."""
        for relation in self.model.relations.get("loki_push_api", []):
            relation.data[self.unit].pop("endpoint", None)

    def _clear_grafana_source_endpoint(self) -> None:
        """Clear this unit's published Grafana datasource endpoint."""
        for relation in self.model.relations.get("grafana-source", []):
            relation.data[self.unit].pop("grafana_source_host", None)

    def _normalize_external_url(self, url: str) -> str:
        """Normalize external-url into a scheme://host:port[/path] base URL."""
        parsed = urlparse(url if "://" in url else f"http://{url}")
        scheme = parsed.scheme or "http"
        netloc = parsed.netloc or parsed.path
        path = parsed.path if parsed.netloc else ""
        if ":" not in netloc:
            netloc = f"{netloc}:3100"
        return f"{scheme}://{netloc}{path}".rstrip("/")

    def _ingress_url(self) -> str | None:
        """Return the ingress URL for this unit when available."""
        return self.ingress.url if self.ingress else None

    def _sorted_source_data(self) -> GrafanaSourceData:
        """Return the first Grafana source data entry in sorted UID order."""
        nested_data = self.grafana_source_provider.get_source_data()
        return nested_data[sorted(nested_data)[0]] if nested_data else GrafanaSourceData({}, None)

    def _update_datasource_exchange(self) -> None:
        """Publish datasource UID mappings over grafana-datasource-exchange."""
        if not self._is_leader():
            return
        grafana_uids_to_units_to_uids = self.grafana_source_provider.get_source_uids()
        raw_datasources: list[DatasourceDict] = []
        for grafana_uid, ds_uids in grafana_uids_to_units_to_uids.items():
            for _, ds_uid in ds_uids.items():
                raw_datasources.append(
                    {"type": "loki", "uid": ds_uid, "grafana_uid": grafana_uid}
                )
        self.datasource_exchange.publish(datasources=raw_datasources)


if __name__ == "__main__":  # pragma: nocover
    ops.main(LokiVmCharm)
