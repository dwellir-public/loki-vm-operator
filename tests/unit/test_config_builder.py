# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

from config_builder import ConfigBuilder, DEFAULT_DATA_DIR


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
    assert config["storage_config"]["filesystem"]["directory"].endswith("chunks")


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
