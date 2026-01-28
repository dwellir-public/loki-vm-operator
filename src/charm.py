#!/usr/bin/env python3
# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

"""Charm the application."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import ops
import yaml

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
        super().__init__(framework)
        self._stored.set_default(
            data_dir=DEFAULT_DATA_DIR,
            last_good_config="",
            last_failed_config_path="",
        )
        framework.observe(self.on.install, self._on_install)
        framework.observe(self.on.start, self._on_start)
        framework.observe(self.on.config_changed, self._on_config_changed)
        framework.observe(self.on.upgrade_charm, self._on_upgrade_charm)
        framework.observe(
            self.on.loki_persisted_storage_attached, self._on_loki_persisted_storage_attached
        )
        framework.observe(
            self.on.loki_persisted_storage_detaching, self._on_loki_persisted_storage_detaching
        )

    def _on_install(self, event: ops.InstallEvent):
        """
        Install the workload on the machine. 
        Preserve the default config file.
        """
        loki.install()
        loki.preserve_default_config(
            config_path=Path(DEFAULT_CONFIG_PATH),
            preserved_path=Path(DEFAULT_PACKAGE_CONFIG_BACKUP_PATH),
        )

    def _on_start(self, event: ops.StartEvent):
        """Handle start event."""
        self.unit.status = ops.MaintenanceStatus("starting workload")
        loki.ensure_data_dir(self.data_dir)
        config_ok = self._configure()
        loki.start()
        version = loki.get_version()
        if version is not None:
            self.unit.set_workload_version(version)
        if config_ok:
            self.unit.status = ops.ActiveStatus()
        else:
            self.unit.status = self._invalid_config_status()

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        """Handle config changes."""
        self.unit.status = ops.MaintenanceStatus("configuring Loki")
        if self._configure():
            self.unit.status = ops.ActiveStatus("Loki configuration updated and validated.")
        else:
            self.unit.status = self._invalid_config_status()

    def _on_upgrade_charm(self, event: ops.UpgradeCharmEvent) -> None:
        """Handle charm upgrade without restarting or rewriting configuration."""
        logger.info("Upgrade-charm event: skipping config rewrite and restart.")

    def _on_loki_persisted_storage_attached(self, event: ops.StorageAttachedEvent) -> None:
        """Handle loki-persisted storage attachment."""
        storage_path = event.storage.location
        if storage_path is None:
            logger.warning("loki-persisted storage attached without a location")
            return
        Path(storage_path).mkdir(parents=True, exist_ok=True)
        self._stored.data_dir = str(storage_path)
        loki.ensure_data_dir(self._stored.data_dir)

    def _on_loki_persisted_storage_detaching(self, event: ops.StorageDetachingEvent) -> None:
        """Handle loki-persisted storage detaching."""
        self._stored.data_dir = DEFAULT_DATA_DIR
        logger.warning("loki-persisted storage detaching; data dir will be reset to %s", self._stored.data_dir)
        loki.ensure_data_dir(self._stored.data_dir)

    @property
    def data_dir(self) -> str:
        """Return the data directory for Loki (storage if attached, else default)."""
        return str(self._stored.data_dir)

    def _configure(self) -> bool:
        """
        * Ensure data dir permissions are set.
        * Render config
        * Validate and persist Loki configuration to DEFAULT_CONFIG_PATH.
        * Store the last good config in _stored.last_good_config.
        * Logs invalid config status if validation fails.
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
        builder = ConfigBuilder(
            instance_addr=self._instance_addr(),
            ingestion_rate_mb=int(self.config["ingestion-rate-mb"]),
            ingestion_burst_size_mb=int(self.config["ingestion-burst-size-mb"]),
            retention_period=int(self.config["retention-period"]),
            reporting_enabled=bool(self.config["reporting-enabled"]),
            data_dir=self.data_dir,
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

    def _instance_addr(self) -> str:
        """Return the best-available address for the Loki ring."""
        if getattr(self.unit, "private_address", None):
            return str(self.unit.private_address)
        if getattr(self.unit, "public_address", None):
            return str(self.unit.public_address)
        return "127.0.0.1"


if __name__ == "__main__":  # pragma: nocover
    ops.main(LokiVmCharm)
