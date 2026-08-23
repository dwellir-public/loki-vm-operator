#!/usr/bin/env python3
# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

"""Charm the application."""

from __future__ import annotations

import json
import logging
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import ops
import yaml
from charms.grafana_k8s.v0.grafana_source import GrafanaSourceData, GrafanaSourceProvider
from charms.loki_k8s.v1.loki_push_api import LokiPushApiProvider
from charms.traefik_k8s.v1.ingress_per_unit import IngressPerUnitRequirer
from cosl.interfaces.datasource_exchange import DatasourceDict, DatasourceExchange
from object_storage import S3Requirer

# A standalone module for workload-specific logic (no charming concerns):
import loki
from config_builder import (
    DEFAULT_CONFIG_BACKUP_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_DIR,
    DEFAULT_PACKAGE_CONFIG_BACKUP_PATH,
    ConfigBuilder,
    S3StorageConfig,
)
from rule_reconciler import (
    CACHE_KEY as RULE_CACHE_KEY,
)
from rule_reconciler import (
    LokiRulerApiClient,
    LokiRuleReconciler,
    RelationRuleSource,
    prepare_filesystem_rule_store,
)

logger = logging.getLogger(__name__)

RESTART_PENDING_KEY = "restart-pending"
RESTART_TARGET_KEY = "restart-target"
ROLLING_PHASE_KEY = "rolling-phase"


class InvalidConfigurationError(RuntimeError):
    """Raised when charm configuration or relation data is invalid."""


@dataclass(frozen=True)
class StorageBackendState:
    """Resolved storage backend state for the current unit."""

    status: ops.StatusBase | None
    s3: S3StorageConfig | None


@dataclass(frozen=True)
class ClusterMemberHealth:
    """Readiness status for one Loki unit in the cluster."""

    unit_name: str
    address: str | None
    ready: bool
    error: str | None = None


@dataclass(frozen=True)
class ClusterHealth:
    """Aggregated readiness status for the Loki deployment."""

    expected_units: int
    ready_units: int
    members: list[ClusterMemberHealth]

    @property
    def healthy(self) -> bool:
        """Return whether every expected unit is ready."""
        return self.ready_units == self.expected_units


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
            configuration_error="",
            config_drifted=False,
            peer_addresses_json="",
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
        self.s3_client = S3Requirer(self, relation_name="s3")
        framework.observe(self.on.install, self._on_install)
        framework.observe(self.on.start, self._on_start)
        framework.observe(self.on.config_changed, self._on_config_changed)
        framework.observe(self.on.upgrade_charm, self._on_upgrade_charm)
        framework.observe(self.on.update_status, self._on_update_status)
        framework.observe(self.on.leader_elected, self._on_leader_elected)
        framework.observe(self.on.cluster_health_action, self._on_cluster_health_action)
        framework.observe(self.ingress.on.ready_for_unit, self._on_ingress_changed)
        framework.observe(self.ingress.on.revoked_for_unit, self._on_ingress_changed)
        framework.observe(
            self.s3_client.on.storage_connection_info_changed,
            self._on_s3_changed,
        )
        framework.observe(
            self.s3_client.on.storage_connection_info_gone,
            self._on_s3_changed,
        )
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
        framework.observe(self.on.s3_relation_created, self._on_s3_changed)
        framework.observe(self.on.s3_relation_joined, self._on_s3_changed)
        framework.observe(self.on.s3_relation_changed, self._on_s3_changed)
        framework.observe(self.on.s3_relation_broken, self._on_s3_changed)
        framework.observe(self.on.replicas_relation_joined, self._on_replicas_changed)
        framework.observe(self.on.replicas_relation_changed, self._on_replicas_changed)
        framework.observe(self.on.replicas_relation_departed, self._on_replicas_changed)
        framework.observe(
            self.on.loki_push_api_relation_changed,
            self._on_loki_alert_rules_changed,
        )
        framework.observe(
            self.on.loki_push_api_relation_broken,
            self._on_loki_push_api_relation_broken,
        )
        framework.observe(
            self.on.loki_push_api_relation_departed,
            self._on_loki_push_api_relation_departed,
        )

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
        self._refresh_loki_provider_endpoint()
        self._refresh_grafana_source_endpoint()
        storage_state = self._storage_backend_state()
        if storage_state.status is not None:
            self.unit.status = storage_state.status
            return
        config_ok, _ = self._configure()
        if not config_ok:
            self.unit.status = self._invalid_config_status()
            return
        loki.start()
        if not self._wait_for_single_unit_ready():
            return
        self._reconcile_rules()
        version = loki.get_version()
        if version is not None:
            self.unit.set_workload_version(version)
        self._update_datasource_exchange()
        self.unit.status = self._runtime_health_status()

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Re-render and apply configuration when charm config changes."""
        self._reconcile_runtime(status_message="configuring Loki")

    def _on_upgrade_charm(self, event: ops.UpgradeCharmEvent) -> None:
        """Apply required config changes and replay accepted rules on upgrade."""
        self._reconcile_runtime(status_message="upgrading Loki charm")

    def _on_update_status(self, event: ops.UpdateStatusEvent) -> None:
        """Handle periodic status updates (detect drift and workload health)."""
        storage_state = self._storage_backend_state()
        if storage_state.status is not None:
            self.unit.status = storage_state.status
            return
        if not loki.is_active():
            self.unit.status = ops.MaintenanceStatus("Loki service not running")
            return
        self._reconcile_config_drift_status()
        if not self._stored.config_drifted:
            self._reconcile_rolling_restart()
            self.unit.status = self._runtime_health_status()
            self._reconcile_rules()

    def _on_leader_elected(self, event: ops.LeaderElectedEvent) -> None:
        """Replay leader-shared accepted rules after leadership changes."""
        self._reconcile_rules()

    def _on_loki_alert_rules_changed(self, event: ops.EventBase) -> None:
        """Reconcile standard provider alert-rule changes into Loki's ruler."""
        self._reconcile_rules()

    def _on_loki_push_api_relation_broken(self, event: ops.RelationBrokenEvent) -> None:
        """Withdraw the broken relation even while Juju still exposes stale data."""
        self._reconcile_rules(excluded_relation_id=event.relation.id)

    def _on_loki_push_api_relation_departed(self, event: ops.RelationDepartedEvent) -> None:
        """Re-evaluate app-owned rules after a remote unit departs."""
        self._reconcile_rules()

    def _on_ingress_changed(self, event: ops.EventBase) -> None:
        """Handle ingress updates by refreshing the published endpoint."""
        self._refresh_loki_provider_endpoint()
        self._refresh_grafana_source_endpoint()

    def _on_grafana_source_changed(self, event: ops.EventBase) -> None:
        """Re-publish datasource-exchange payload when Grafana-source data changes.

        This keeps the `send-datasource` relation in sync with datasource UIDs
        assigned by related Grafana applications.
        """
        self._update_datasource_exchange()

    def _on_replicas_changed(self, event: ops.RelationEvent) -> None:
        """Handle peer relation changes and reconfigure memberlist joins."""
        if event.relation:
            event.relation.data[self.unit]["address"] = self._instance_addr()
        current_addresses = self._peer_addresses_json()
        if current_addresses != self._stored.peer_addresses_json:
            self._stored.peer_addresses_json = current_addresses
            self._reconcile_runtime(status_message="updating cluster configuration")
            return
        self._fast_reconcile_rolling_restart()

    def _on_s3_changed(self, event: ops.EventBase) -> None:
        """Handle S3 relation changes and reconfigure storage."""
        self._reconcile_runtime(status_message="updating storage configuration")

    def _on_cluster_health_action(self, event: ops.ActionEvent) -> None:
        """Report compact cluster health plus detailed member readiness."""
        storage_state = self._storage_backend_state()
        cluster_health = self._cluster_health()
        storage_ok, storage_error = self._storage_probe_result(storage_state)
        event.set_results(
            {
                "healthy": storage_state.status is None and storage_ok and cluster_health.healthy,
                "summary": self._health_summary(cluster_health),
                "ready-units": cluster_health.ready_units,
                "expected-units": cluster_health.expected_units,
                "storage": self._storage_status_label(storage_state),
                "storage-error": storage_error,
                "members": json.dumps(
                    [
                        {
                            "unit": member.unit_name,
                            "address": member.address,
                            "ready": member.ready,
                            "error": member.error,
                        }
                        for member in cluster_health.members
                    ]
                ),
            }
        )

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

    def _reconcile_runtime(self, *, status_message: str) -> None:
        """Re-render config, publish endpoints, and restart when needed."""
        self.unit.status = ops.MaintenanceStatus(status_message)
        self._refresh_loki_provider_endpoint()
        self._refresh_grafana_source_endpoint()
        storage_state = self._storage_backend_state()
        if storage_state.status is not None:
            self.unit.status = storage_state.status
            return
        config_ok, config_changed = self._configure()
        if not config_ok:
            self.unit.status = self._invalid_config_status()
            return
        if config_changed:
            if self._is_clustered_mode():
                self._set_restart_pending(True)
            else:
                restarted = self._restart_if_running()
                if restarted and not self._wait_for_single_unit_ready():
                    return
        self._reconcile_rolling_restart()
        self._set_workload_version()
        self._update_datasource_exchange()
        self.unit.status = self._runtime_health_status()
        self._reconcile_rules()

    def _reconcile_rules(self, *, excluded_relation_id: int | None = None) -> None:
        """Apply bounded relation rules and persist accepted state in peer app data."""
        if not self._is_leader():
            return
        peer = self._peer_relation()
        if peer is None:
            logger.warning("Cannot reconcile Loki rules until the replicas relation is available")
            return
        sources = [
            RelationRuleSource(
                relation.id,
                relation.data[relation.app].get("alert_rules"),
            )
            for relation in self.model.relations.get("loki_push_api", [])
            if relation.id != excluded_relation_id and relation.app is not None
        ]
        cache_value = peer.data[self.app].get(RULE_CACHE_KEY)
        client = LokiRulerApiClient("http://127.0.0.1:3100")
        reconciler = LokiRuleReconciler(client)
        reconciler.reconcile(
            sources,
            cache_value=cache_value,
            persist=lambda value: peer.data[self.app].__setitem__(RULE_CACHE_KEY, value),
        )

    def _fast_reconcile_rolling_restart(self) -> None:
        """Advance rolling restart state without rewriting config."""
        storage_state = self._storage_backend_state()
        if storage_state.status is not None:
            self.unit.status = storage_state.status
            return
        if not loki.is_active():
            self.unit.status = ops.MaintenanceStatus("Loki service not running")
            return
        if self._stored.config_drifted:
            return
        self._reconcile_rolling_restart()
        self.unit.status = self._runtime_health_status()

    def _configure(self) -> tuple[bool, bool]:
        """Render, validate, and persist Loki configuration.

        Flow:
        - Ensure Loki can write to the data directory.
        - Render config from `config-override` or ConfigBuilder output.
        - Verify config with `loki -verify-config` on a temp file.
        - If invalid, keep the last-good config and record the failed config path.
        - If valid, write the config + backup and update last-good config state.
        - Clear drift status after a successful apply.
        """
        prepare_filesystem_rule_store(self.data_dir)
        loki.ensure_data_dir_permissions(self.data_dir)
        self._stored.configuration_error = ""
        try:
            config_text = self._render_config_text()
        except (InvalidConfigurationError, ValueError) as exc:
            logger.warning("Cannot render managed Loki configuration: %s", exc)
            self._stored.configuration_error = str(exc)
            return False, False
        if not config_text:
            return True, False
        if not self._validate_config_text(config_text):
            logger.warning("Configuration validation failed; keeping previous config.")
            failed_path = self._persist_failed_config(config_text)
            if failed_path:
                self._stored.last_failed_config_path = str(failed_path)
            if self._stored.last_good_config:
                self.unit.status = self._invalid_config_status()
            return False, False
        on_disk = self._read_config_from_disk()
        config_changed = (
            self._stored.last_good_config != config_text
            or on_disk != config_text
            or self._stored.config_drifted
        )
        if not config_changed:
            return True, False
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
        self._stored.configuration_error = ""
        return True, True

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
        if self._stored.configuration_error:
            return ops.WaitingStatus(f"{self._stored.configuration_error}; check logs")
        if self._stored.last_failed_config_path:
            message = f"Invalid Loki config; check logs and {self._stored.last_failed_config_path}"
        else:
            message = "Invalid Loki config; check logs to fix configuration"
        return ops.WaitingStatus(message)

    def _render_config_text(self) -> str:
        """Return the Loki config as a YAML string."""
        override = str(self.config.get("config-override", "")).strip()
        if override:
            self._validate_override_ruler_contract(override)
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
            s3=self._storage_backend_state().s3,
            loki_version=loki.get_version(),
        )
        rendered = yaml.safe_dump(builder.build(), sort_keys=False)
        return f"{loki.GENERATED_CONFIG_HEADER}{rendered}"

    @staticmethod
    def _validate_override_ruler_contract(override: str) -> None:
        """Require override config to retain the local ruler contract used by the charm."""
        try:
            document = yaml.safe_load(override)
        except yaml.YAMLError as exc:
            raise InvalidConfigurationError("config-override must be valid YAML") from exc
        if not isinstance(document, dict):
            raise InvalidConfigurationError("config-override must be a YAML mapping")
        ruler = document.get("ruler")
        ruler_storage = document.get("ruler_storage")
        server = document.get("server")
        capable = (
            document.get("auth_enabled") is False
            and isinstance(server, dict)
            and server.get("http_listen_port", 3100) == 3100
            and isinstance(ruler, dict)
            and ruler.get("enable_api") is True
            and bool(ruler.get("rule_path"))
            and isinstance(ruler_storage, dict)
            and bool(ruler_storage.get("backend"))
        )
        if not capable:
            raise InvalidConfigurationError(
                "config-override must enable the charm-managed ruler API"
            )

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
                logger.warning("Detected manual Loki config change at %s", DEFAULT_CONFIG_PATH)
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

    def _runtime_health_status(self) -> ops.StatusBase:
        """Return compact runtime health for ready or degraded clusters."""
        cluster_health = self._cluster_health()
        if rolling_status := self._rolling_restart_status(cluster_health):
            return rolling_status
        summary = self._health_summary(cluster_health)
        if cluster_health.healthy:
            return ops.ActiveStatus(summary)
        return ops.MaintenanceStatus(summary)

    def _health_summary(self, cluster_health: ClusterHealth) -> str:
        """Render the compact runtime health summary."""
        return (
            f"ready({cluster_health.ready_units}/{cluster_health.expected_units}), "
            f"storage({self._storage_status_label(self._storage_backend_state())})"
        )

    def _storage_status_label(self, storage_state: StorageBackendState) -> str:
        """Return a short storage health label."""
        if storage_state.s3 is None and storage_state.status is None:
            return "local"
        if storage_state.s3 is not None and storage_state.status is None:
            storage_ok, _ = self._storage_probe_result(storage_state)
            return "s3(ok)" if storage_ok else "s3(error)"
        if isinstance(storage_state.status, ops.WaitingStatus):
            return "waiting"
        if isinstance(storage_state.status, ops.BlockedStatus):
            return "error"
        return "unknown"

    def _storage_probe_result(self, storage_state: StorageBackendState) -> tuple[bool, str | None]:
        """Probe configured S3 endpoint reachability when one is present.

        Loki readiness provides the authenticated bucket/configuration check. This
        unauthenticated endpoint probe deliberately treats S3's expected HTTP 403
        authentication challenge as proof that the configured service is reachable.
        """
        if storage_state.s3 is None:
            return True, None
        scheme = "http" if storage_state.s3.insecure else "https"
        return loki.check_s3_endpoint(f"{scheme}://{storage_state.s3.endpoint}")

    def _rolling_phase(self, unit_name: str | None = None) -> str:
        """Return the stored rolling-restart phase for a unit."""
        relation = self._peer_relation()
        if relation is None:
            return ""
        name = unit_name or self.unit.name
        if name == self.unit.name:
            return relation.data[self.unit].get(ROLLING_PHASE_KEY, "")
        for unit in relation.units:
            if unit.name == name:
                return relation.data[unit].get(ROLLING_PHASE_KEY, "")
        return ""

    def _set_rolling_phase(self, phase: str) -> None:
        """Persist the current rolling-restart phase for this unit."""
        relation = self._peer_relation()
        if relation is None:
            return
        if phase:
            relation.data[self.unit][ROLLING_PHASE_KEY] = phase
            return
        relation.data[self.unit].pop(ROLLING_PHASE_KEY, None)

    def _rolling_restart_status(self, cluster_health: ClusterHealth) -> ops.StatusBase | None:
        """Return per-unit rolling-restart progress while a rollout is active."""
        if not self._is_clustered_mode():
            return None
        target = self._restart_target()
        pending_units = self._pending_restart_units()
        if not target and not pending_units:
            return None
        phase = self._rolling_phase()
        if target == self.unit.name and self._restart_pending(self.unit.name):
            phase = phase or "restarting-self"
        elif self._restart_pending(self.unit.name):
            phase = "queued"
        elif target:
            phase = "waiting-peers"
        else:
            phase = "completed"
        target_label = "self" if target == self.unit.name else (target or "-")
        return ops.MaintenanceStatus(
            f"rolling({phase}, target={target_label}), {self._health_summary(cluster_health)}"
        )

    def _is_clustered_mode(self) -> bool:
        """Return whether restarts should be coordinated via the peer relation."""
        return self.app.planned_units() > 1 and self._peer_relation() is not None

    def _peer_relation(self) -> ops.Relation | None:
        """Return the peer relation used for clustered restart coordination."""
        return self.model.get_relation("replicas")

    def _set_restart_pending(self, pending: bool) -> None:
        """Persist whether this unit still requires a rolling restart."""
        relation = self._peer_relation()
        if relation is None:
            return
        relation.data[self.unit][RESTART_PENDING_KEY] = "true" if pending else "false"
        if pending:
            relation.data[self.unit].setdefault(ROLLING_PHASE_KEY, "queued")
        else:
            self._set_rolling_phase("")

    def _restart_pending(self, unit_name: str) -> bool:
        """Return whether the named unit still requires a coordinated restart."""
        relation = self._peer_relation()
        if relation is None:
            return False
        if unit_name == self.unit.name:
            return relation.data[self.unit].get(RESTART_PENDING_KEY) == "true"
        for unit in relation.units:
            if unit.name == unit_name:
                return relation.data[unit].get(RESTART_PENDING_KEY) == "true"
        return False

    def _pending_restart_units(self) -> list[str]:
        """Return sorted unit names still pending restart."""
        relation = self._peer_relation()
        if relation is None:
            return []
        pending_units: list[str] = []
        if relation.data[self.unit].get(RESTART_PENDING_KEY) == "true":
            pending_units.append(self.unit.name)
        for unit in relation.units:
            if relation.data[unit].get(RESTART_PENDING_KEY) == "true":
                pending_units.append(unit.name)
        return sorted(set(pending_units), key=lambda name: (name == self.unit.name, name))

    def _restart_target(self) -> str:
        """Return the unit name currently selected for rolling restart."""
        relation = self._peer_relation()
        if relation is None:
            return ""
        return relation.data[self.app].get(RESTART_TARGET_KEY, "")

    def _set_restart_target(self, unit_name: str) -> None:
        """Persist the rolling-restart target in peer app data."""
        relation = self._peer_relation()
        if relation is None or not self._is_leader():
            return
        relation.data[self.app][RESTART_TARGET_KEY] = unit_name

    def _reconcile_rolling_restart(self) -> None:
        """Advance the rolling restart and execute the selected unit restart."""
        if not self._is_clustered_mode():
            return
        if self._is_leader():
            self._advance_rolling_restart()
        if self._restart_target() == self.unit.name and self._restart_pending(self.unit.name):
            self._perform_target_restart()

    def _advance_rolling_restart(self) -> None:
        """Select the next restart target when cluster health allows it."""
        target = self._restart_target()
        cluster_health = self._cluster_health()
        pending_units = self._pending_restart_units()

        if target:
            if self._restart_pending(target):
                return
            if not cluster_health.healthy:
                return
            self._set_restart_target("")
            target = ""

        if not cluster_health.healthy or target or not pending_units:
            return

        self._set_restart_target(pending_units[0])

    def _perform_target_restart(self) -> None:
        """Gracefully restart the selected unit and clear its pending flag."""
        self._set_rolling_phase("restarting-self")
        self.unit.status = self._runtime_health_status()
        try:
            loki.prepare_shutdown(self._unit_base_url(self._instance_addr()))
        except Exception as exc:  # noqa: BLE001
            logger.info("Skipping graceful shutdown preparation: %s", exc)
        self._restart_if_running()
        self._set_rolling_phase("waiting-ready")
        self.unit.status = self._runtime_health_status()
        self._wait_for_local_ready()
        self._set_rolling_phase("waiting-cluster")
        self.unit.status = self._runtime_health_status()
        self._wait_for_cluster_healthy()
        self._set_restart_pending(False)

    def _wait_for_local_ready(self, *, timeout: int = 120) -> None:
        """Wait until the local Loki unit reports ready after a restart."""
        deadline = time.monotonic() + timeout
        base_url = self._unit_base_url(self._instance_addr())
        while time.monotonic() < deadline:
            ready, _ = loki.check_ready(base_url)
            if ready:
                return
            time.sleep(1)
        raise RuntimeError(f"Timed out waiting for Loki readiness on {self.unit.name}")

    def _wait_for_single_unit_ready(self) -> bool:
        """Wait after a local start without turning a readiness timeout into a hook error."""
        try:
            self._wait_for_local_ready()
        except RuntimeError as exc:
            logger.warning("Loki did not become ready before the hook deadline: %s", exc)
            self.unit.status = ops.MaintenanceStatus("waiting for Loki readiness")
            return False
        return True

    def _wait_for_cluster_healthy(self, *, timeout: int = 120) -> None:
        """Wait until the Loki cluster is healthy after a rolling restart step."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._cluster_health().healthy:
                return
            time.sleep(1)
        raise RuntimeError(f"Timed out waiting for Loki cluster recovery on {self.unit.name}")

    def _cluster_health(self) -> ClusterHealth:
        """Probe local and peer readiness endpoints for cluster health."""
        members: list[ClusterMemberHealth] = []
        relation = self.model.get_relation("replicas")
        remote_addresses = (
            {unit.name: relation.data[unit].get("address") for unit in relation.units}
            if relation is not None
            else {}
        )
        known_units = {self.unit.name, *remote_addresses.keys()}
        ordered_units = sorted(
            known_units,
            key=lambda unit_name: int(unit_name.rsplit("/", 1)[1]),
        )
        expected_units = len(ordered_units)
        for unit_name in ordered_units:
            address = (
                self._instance_addr()
                if unit_name == self.unit.name
                else remote_addresses.get(unit_name)
            )
            if not address:
                members.append(
                    ClusterMemberHealth(
                        unit_name=unit_name,
                        address=None,
                        ready=False,
                        error="missing address",
                    )
                )
                continue
            ready, error = loki.check_ready(self._unit_base_url(address))
            members.append(
                ClusterMemberHealth(
                    unit_name=unit_name,
                    address=address,
                    ready=ready,
                    error=error,
                )
            )
        return ClusterHealth(
            expected_units=expected_units,
            ready_units=sum(member.ready for member in members),
            members=members,
        )

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
        private_address = getattr(self.unit, "private_address", None)
        if private_address:
            return str(private_address)
        public_address = getattr(self.unit, "public_address", None)
        if public_address:
            return str(public_address)
        address = getattr(self.unit, "address", None)
        if address:
            return str(address)
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

    def _peer_addresses_json(self) -> str:
        """Return a stable JSON snapshot of peer addresses for config drift checks."""
        relation = self._peer_relation()
        if relation is None:
            return "[]"
        addresses = sorted(
            (unit.name, relation.data[unit].get("address", "")) for unit in relation.units
        )
        return json.dumps(addresses)

    def _storage_backend_state(self) -> StorageBackendState:
        """Return the resolved storage backend and any blocking/waiting status."""
        relation = self.model.get_relation("s3")
        if relation is None or relation.app is None:
            if self.app.planned_units() > 1:
                return StorageBackendState(
                    status=ops.WaitingStatus("waiting for s3 relation for clustered Loki"),
                    s3=None,
                )
            return StorageBackendState(status=None, s3=None)

        try:
            s3 = self._s3_relation_details()
        except InvalidConfigurationError as exc:
            return StorageBackendState(status=ops.BlockedStatus(str(exc)), s3=None)

        if s3 is None:
            return StorageBackendState(
                status=ops.WaitingStatus("waiting for complete s3 relation data"),
                s3=None,
            )
        return StorageBackendState(status=None, s3=s3)

    def _s3_relation_details(self) -> S3StorageConfig | None:
        """Return relation-provided S3 configuration when available."""
        relation = self.model.get_relation("s3")
        if relation is None:
            return None
        app_data = self.s3_client.get_storage_connection_info(relation)
        required = ("endpoint", "bucket", "access-key", "secret-key", "region")
        if any(not str(app_data.get(field, "")).strip() for field in required):
            return None
        path = str(app_data.get("path", "")).strip()
        if path:
            raise InvalidConfigurationError("s3 relation field 'path' is not supported")
        bucket = str(app_data.get("bucket", "")).strip()
        endpoint = str(app_data.get("endpoint", "")).strip()
        access_key = str(app_data.get("access-key", "")).strip()
        secret_key = str(app_data.get("secret-key", "")).strip()
        region = str(app_data.get("region", "")).strip()

        if not all((bucket, endpoint, access_key, secret_key, region)):
            return None

        return S3StorageConfig(
            bucket=bucket,
            endpoint=self._normalize_s3_endpoint(endpoint),
            access_key_id=access_key,
            secret_access_key=secret_key,
            region=region,
            insecure=self._s3_is_insecure(endpoint),
        )

    def _normalize_s3_endpoint(self, value: str) -> str:
        """Convert relation endpoint values into Loki's expected host:port format."""
        endpoint = value.strip().rstrip("/")
        try:
            parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
        except ValueError as exc:
            raise InvalidConfigurationError(f"Invalid s3 endpoint {value!r}") from exc
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.params
            or parsed.query
            or parsed.fragment
        ):
            raise InvalidConfigurationError(f"Invalid s3 endpoint {value!r}")
        host = parsed.hostname
        if ":" in host:
            host = f"[{host}]"
        return f"{host}:{port}"

    def _s3_is_insecure(self, endpoint: str) -> bool:
        """Infer whether the S3 endpoint should use HTTP."""
        normalized = endpoint.strip().lower()
        return "://" not in normalized or normalized.startswith("http://")

    def _parse_bool(self, value: str, *, field_name: str) -> bool:
        """Parse a boolean relation field."""
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
        raise InvalidConfigurationError(
            f"Invalid s3 relation field {field_name!r}: expected boolean, got {value!r}"
        )

    def _set_workload_version(self) -> None:
        """Publish workload version when available."""
        version = loki.get_version()
        if version is not None:
            self.unit.set_workload_version(version)

    def _restart_if_running(self) -> bool:
        """Restart Loki after a config change when the workload is already installed."""
        if loki.is_active():
            loki.restart()
            return True
        if loki.get_version() is not None:
            loki.start()
            return True
        return False

    def _refresh_loki_provider_endpoint(self) -> None:
        """Publish Loki push API endpoint to relation data."""
        if self._external_url_configured() and not self._is_leader():
            self._clear_loki_provider_endpoint()
            return
        url = self._external_url_base()
        if url:
            self.loki_provider.update_endpoint(url=url)

    def _refresh_grafana_source_endpoint(self) -> None:
        """Refresh the endpoint published on `grafana-source` relations.

        Leaders publish the effective Loki URL. Non-leaders clear stale unit
        data when `external-url` is configured, mirroring the `loki_push_api`
        publication behavior.
        """
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
        addr = self._format_host_for_url(self._instance_addr())
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
        """Remove this unit's datasource host entry from `grafana-source` data."""
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

    def _format_host_for_url(self, host: str) -> str:
        """Return a host suitable for inclusion in an HTTP URL."""
        if ":" in host and not host.startswith("["):
            return f"[{host}]"
        return host

    def _ingress_url(self) -> str | None:
        """Return the ingress URL for this unit when available."""
        return self.ingress.url if self.ingress else None

    def _unit_base_url(self, host: str) -> str:
        """Return the direct base URL for a Loki unit address."""
        return f"http://{self._format_host_for_url(host)}:3100"

    def _sorted_source_data(self) -> GrafanaSourceData:
        """Return deterministic Grafana source metadata for ruler link fields.

        When multiple Grafana apps are related, this picks the first entry in
        sorted `grafana_uid` order to keep rendered config stable.
        """
        nested_data = self.grafana_source_provider.get_source_data()
        return nested_data[sorted(nested_data)[0]] if nested_data else GrafanaSourceData({}, None)

    def _update_datasource_exchange(self) -> None:
        """Publish Loki datasource UID mappings over `send-datasource`.

        Leader-only operation: for each related Grafana instance, convert the
        unit UID mapping into exchange payload entries with type `loki`.
        """
        if not self._is_leader():
            return
        grafana_uids_to_units_to_uids = self.grafana_source_provider.get_source_uids()
        raw_datasources: list[DatasourceDict] = []
        for grafana_uid, ds_uids in grafana_uids_to_units_to_uids.items():
            for _, ds_uid in ds_uids.items():
                raw_datasources.append({"type": "loki", "uid": ds_uid, "grafana_uid": grafana_uid})
        self.datasource_exchange.publish(datasources=raw_datasources)


if __name__ == "__main__":  # pragma: nocover
    ops.main(LokiVmCharm)
