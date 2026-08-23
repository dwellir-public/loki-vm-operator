# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

import pytest

from config_builder import DEFAULT_DATA_DIR, ConfigBuilder, S3StorageConfig


@pytest.mark.parametrize("version", ["2.9.10", "3.3.2"])
def test_generated_managed_ruler_config_rejects_pre_3_4_loki(version: str) -> None:
    builder = ConfigBuilder(
        instance_addr="127.0.0.1",
        ingestion_rate_mb=4,
        ingestion_burst_size_mb=15,
        retention_period=0,
        reporting_enabled=True,
        loki_version=version,
    )

    with pytest.raises(ValueError, match="Loki >= 3.4.0"):
        builder.build()


@pytest.mark.parametrize("version", ["3.4.0", "3.5.3", None])
def test_generated_managed_ruler_config_accepts_supported_or_unknown_loki(
    version: str | None,
) -> None:
    builder = ConfigBuilder(
        instance_addr="127.0.0.1",
        ingestion_rate_mb=4,
        ingestion_burst_size_mb=15,
        retention_period=0,
        reporting_enabled=True,
        loki_version=version,
    )

    assert builder.build()["storage_config"]["use_thanos_objstore"] is True


def test_config_builder_basic_fields():
    builder = ConfigBuilder(
        instance_addr="127.0.0.1",
        ingestion_rate_mb=4,
        ingestion_burst_size_mb=15,
        retention_period=0,
        reporting_enabled=True,
    )
    config = builder.build()

    assert config["auth_enabled"] is False
    assert config["common"]["path_prefix"] == DEFAULT_DATA_DIR
    assert config["common"]["storage"]["filesystem"]["chunks_directory"].endswith("chunks")
    assert config["common"]["storage"]["filesystem"]["rules_directory"].endswith("rules")
    assert config["ruler_storage"] == {
        "backend": "filesystem",
        "filesystem": {"dir": "/var/lib/loki/rules"},
    }
    assert config["storage_config"]["object_store"] == {
        "filesystem": {"dir": "/var/lib/loki/chunks"}
    }
    assert "filesystem" not in config["storage_config"]
    assert "aws" not in config["storage_config"]
    assert config["storage_config"]["use_thanos_objstore"] is True
    assert config["limits_config"]["volume_enabled"] is True
    assert config["ruler"]["enable_api"] is True
    assert config["ruler"]["rule_path"].endswith("ruler-tmp")


def test_config_builder_tsdb_schema_only():
    builder = ConfigBuilder(
        instance_addr="127.0.0.1",
        ingestion_rate_mb=4,
        ingestion_burst_size_mb=15,
        retention_period=0,
        reporting_enabled=True,
    )
    config = builder.build()
    schema_config = config["schema_config"]["configs"]

    assert schema_config[0]["store"] == "tsdb"
    assert schema_config[0]["schema"] == "v13"


def test_config_builder_retention_and_analytics():
    builder = ConfigBuilder(
        instance_addr="127.0.0.1",
        ingestion_rate_mb=4,
        ingestion_burst_size_mb=15,
        retention_period=7,
        reporting_enabled=False,
    )
    config = builder.build()

    assert config["compactor"]["retention_enabled"] is True
    assert config["compactor"]["delete_request_store"] == "filesystem"
    assert config["limits_config"]["retention_period"] == "7d"
    assert "analytics" in config


def test_config_builder_s3_storage_mode():
    builder = ConfigBuilder(
        instance_addr="127.0.0.1",
        ingestion_rate_mb=4,
        ingestion_burst_size_mb=15,
        retention_period=7,
        reporting_enabled=True,
        s3=S3StorageConfig(
            bucket="juju-s3-rel-10",
            endpoint="10.0.0.10:3900",
            access_key_id="access",
            secret_access_key="secret",
            region="garage",
            insecure=True,
        ),
    )
    config = builder.build()

    assert "storage" not in config["common"]
    assert config["schema_config"]["configs"][0]["object_store"] == "s3"
    assert config["storage_config"]["object_store"] == {
        "s3": {
            "bucket_name": "juju-s3-rel-10",
            "endpoint": "10.0.0.10:3900",
            "access_key_id": "access",
            "secret_access_key": "secret",
            "region": "garage",
            "insecure": True,
            "bucket_lookup_type": "path",
        }
    }
    assert "filesystem" not in config["storage_config"]
    assert "aws" not in config["storage_config"]
    assert config["ruler_storage"] == {
        "backend": "s3",
        "storage_prefix": "ruler",
        "s3": {
            "bucket_name": "juju-s3-rel-10",
            "endpoint": "10.0.0.10:3900",
            "access_key_id": "access",
            "secret_access_key": "secret",
            "region": "garage",
            "insecure": True,
            "bucket_lookup_type": "path",
        },
    }
    assert config["compactor"]["delete_request_store"] == "s3"
