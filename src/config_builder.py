#!/usr/bin/env python3
# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.
"""Config builder for Loki VM charm."""

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

HTTP_LISTEN_PORT = 3100

DEFAULT_CONFIG_DIR = "/etc/loki"
DEFAULT_CONFIG_PATH = os.path.join(DEFAULT_CONFIG_DIR, "config.yml")
DEFAULT_PACKAGE_CONFIG_BACKUP_PATH = os.path.join(
    DEFAULT_CONFIG_DIR, "config.yml.package-default"
)

# Where Loki stores its data per default
DEFAULT_DATA_DIR = "/var/lib/loki"

# Backup path for the config
DEFAULT_CONFIG_BACKUP_PATH = os.path.join(DEFAULT_DATA_DIR, "config.yml.bak")


@dataclass(frozen=True)
class S3StorageConfig:
    """S3 configuration used for Loki TSDB single-store."""

    bucket: str
    endpoint: str
    access_key_id: str
    secret_access_key: str
    region: str
    insecure: bool


class ConfigBuilder:
    """Loki configuration builder class.

    Configuration is required for Loki to start, including: storage paths, schema,
    ring.

    Reference: https://grafana.com/docs/loki/latest/configuration/
    """

    _target: str = "all"
    _auth_enabled: bool = False

    def __init__(
        self,
        *,
        instance_addr: str,
        ingestion_rate_mb: int,
        ingestion_burst_size_mb: int,
        retention_period: int,
        reporting_enabled: bool,
        data_dir: str = DEFAULT_DATA_DIR,
        http_tls: bool = False,
        tsdb_versions_migration_dates: Optional[List[Dict[str, str]]] = None,
        alertmanager_url: Optional[str] = None,
        grafana_external_url: Optional[str] = None,
        datasource_uid: Optional[str] = None,
        memberlist_join_members: Optional[List[str]] = None,
        s3: Optional[S3StorageConfig] = None,
    ):
        """Init method."""
        self.instance_addr = instance_addr
        self.ingestion_rate_mb = ingestion_rate_mb
        self.ingestion_burst_size_mb = ingestion_burst_size_mb
        self.retention_period = retention_period
        self.reporting_enabled = reporting_enabled
        self.http_tls = http_tls
        self.tsdb_versions_migration_dates = tsdb_versions_migration_dates or []
        self.alertmanager_url = alertmanager_url
        self.grafana_external_url = grafana_external_url
        self.datasource_uid = datasource_uid
        self.memberlist_join_members = memberlist_join_members
        self.s3 = s3

        self.data_dir = data_dir
        self.chunks_dir = os.path.join(self.data_dir, "chunks")
        self.rules_dir = os.path.join(self.data_dir, "rules")
        self.compactor_dir = os.path.join(self.data_dir, "compactor")
        self.tsdb_dir = os.path.join(self.data_dir, "tsdb-index")
        self.tsdb_cache_dir = os.path.join(self.data_dir, "tsdb-cache")

    def build(self) -> dict:
        """Build Loki config dictionary."""
        loki_config = {
            "target": self._target,
            "auth_enabled": self._auth_enabled,
            "common": self._common,
            "ingester": self._ingester,
            "schema_config": self._schema_config,
            "server": self._server,
            "storage_config": self._storage_config,
            "limits_config": self._limits_config,
            "query_range": self._query_range,
            "chunk_store_config": self._chunk_store_config,
            "frontend": self._frontend,
            "querier": self._querier,
            "compactor": self._compactor,
        }

        if memberlist := self._memberlist:
            loki_config["memberlist"] = memberlist

        if ruler_config := self._ruler:
            loki_config["ruler"] = ruler_config

        # Overwrite the default only if reporting is not enabled
        if not self.reporting_enabled:
            loki_config["analytics"] = self._analytics

        return loki_config

    @property
    def _common(self) -> dict:
        kvstore = {"store": "inmemory"}
        if self.memberlist_join_members is not None:
            kvstore = {"store": "memberlist"}
        common = {
            "path_prefix": self.data_dir,
            "replication_factor": 1,
            "ring": {"instance_addr": self.instance_addr, "kvstore": kvstore},
        }
        if self.s3 is None:
            common["storage"] = {
                "filesystem": {
                    "chunks_directory": self.chunks_dir,
                    "rules_directory": self.rules_dir,
                }
            },
        return common

    @property
    def _ingester(self) -> dict:
        return {
            "wal": {
                "dir": os.path.join(self.chunks_dir, "wal"),
                "enabled": True,
                "flush_on_shutdown": True,
            }
        }

    @property
    def _ruler(self) -> Optional[dict]:
        if not self.alertmanager_url:
            return None
        ruler_config = {
            "alertmanager_url": self.alertmanager_url,
            "enable_alertmanager_v2": True,
        }
        if self.datasource_uid:
            ruler_config["datasource_uid"] = self.datasource_uid
        if self.grafana_external_url:
            ruler_config["external_url"] = self.grafana_external_url
        return ruler_config

    @property
    def _schema_config(self) -> dict:
        configs = [
            {
                "from": "2020-10-24",
                "index": {"period": "24h", "prefix": "index_"},
                "object_store": "s3" if self.s3 else "filesystem",
                "schema": "v13",
                "store": "tsdb",
            }
        ]

        for migration in self.tsdb_versions_migration_dates:
            if migration.get("date"):
                configs.append(
                    {
                        "from": migration["date"],
                        "index": {"period": "24h", "prefix": "index_"},
                        "object_store": "s3" if self.s3 else "filesystem",
                        "schema": migration["version"],
                        "store": "tsdb",
                    }
                )

        return {"configs": configs}

    @property
    def _server(self) -> dict:
        _server = {
            "http_listen_address": "0.0.0.0",
            "http_listen_port": HTTP_LISTEN_PORT,
        }

        if self.http_tls:
            _server["http_tls_config"] = {
                "cert_file": os.path.join(DEFAULT_CONFIG_DIR, "loki.cert.pem"),
                "key_file": os.path.join(DEFAULT_CONFIG_DIR, "loki.key.pem"),
            }

        return _server

    @property
    def _storage_config(self) -> dict:
        storage_config: dict[str, object] = {
            "tsdb_shipper": {
                "active_index_directory": self.tsdb_dir,
                "cache_location": self.tsdb_cache_dir,
            }
        }
        if self.s3 is None:
            storage_config["filesystem"] = {"directory": self.chunks_dir}
            return storage_config
        storage_config["aws"] = {
            "bucketnames": self.s3.bucket,
            "endpoint": self.s3.endpoint,
            "region": self.s3.region,
            "access_key_id": self.s3.access_key_id,
            "secret_access_key": self.s3.secret_access_key,
            "insecure": self.s3.insecure,
            "s3forcepathstyle": True,
        }
        return storage_config

    @property
    def _limits_config(self) -> dict:
        return {
            "allow_structured_metadata": True,
            "ingestion_rate_mb": float(self.ingestion_rate_mb),
            "ingestion_burst_size_mb": float(self.ingestion_burst_size_mb),
            "per_stream_rate_limit": f"{self.ingestion_rate_mb}MB",
            "per_stream_rate_limit_burst": f"{self.ingestion_burst_size_mb}MB",
            "split_queries_by_interval": "0",
            "retention_period": f"{self.retention_period}d",
        }

    @property
    def _query_range(self) -> dict:
        return {
            "parallelise_shardable_queries": False,
            "results_cache": {
                "cache": {
                    "embedded_cache": {
                        "enabled": True,
                    }
                }
            },
        }

    @property
    def _chunk_store_config(self) -> dict:
        return {
            "chunk_cache_config": {
                "embedded_cache": {
                    "enabled": True,
                }
            }
        }

    @property
    def _frontend(self) -> dict:
        return {
            "max_outstanding_per_tenant": 8192,
            "compress_responses": True,
        }

    @property
    def _querier(self) -> dict:
        return {
            "max_concurrent": 20,
        }

    @property
    def _compactor(self) -> dict:
        # Loki now requires compactor.delete_request_store when retention is enabled.
        # See https://grafana.com/docs/loki/latest/configuration/#compactor
        retention_enabled = self.retention_period != 0
        compactor = {
            "retention_enabled": retention_enabled,
            "working_directory": self.compactor_dir,
        }
        if retention_enabled:
            compactor["delete_request_store"] = "s3" if self.s3 else "filesystem"
        return compactor

    @property
    def _analytics(self) -> dict:
        return {
            "reporting_enabled": self.reporting_enabled,
        }

    @property
    def _memberlist(self) -> Optional[dict]:
        if self.memberlist_join_members is None:
            return None
        config = {}
        if self.memberlist_join_members:
            config["join_members"] = self.memberlist_join_members
        return config
