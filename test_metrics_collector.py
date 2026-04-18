"""
tests/test_metrics_collector.py
--------------------------------
Unit tests for SnowflakeMetricsCollector using mocked Snowflake connection.
Run with: pytest tests/test_metrics_collector.py -v
"""

import sys
import os
import time
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

MOCK_CONFIG = {
    "snowflake": {
        "account": "test.us-east-1",
        "user": "test_user",
        "password": "test_pass",
        "warehouse": "TEST_WH",
        "database": "TEST_DB",
        "schema": "PUBLIC",
        "role": "TEST_ROLE",
    },
    "aws": {
        "region": "us-east-1",
        "s3_bucket": "test-bucket",
        "sns_topic_arn": "arn:aws:sns:us-east-1:123:test",
        "cloudwatch_namespace": "LakehouseSRE",
    },
    "iceberg": {
        "catalog_name": "test_catalog",
        "warehouse_path": "s3://test-bucket/iceberg",
        "tables": [],
    },
    "prometheus": {"port": 8001},
    "slo": {
        "pipeline_latency_p95_seconds": 300,
        "pipeline_latency_alert_seconds": 240,
        "data_freshness_max_minutes": 30,
        "error_rate_threshold_pct": 1.0,
        "row_count_anomaly_pct": 20.0,
        "burn_rate_alert_multiplier": 14.4,
    },
    "monitoring": {
        "collection_interval_seconds": 60,
        "pipelines_to_monitor": ["RAW_INGEST_PIPELINE", "CURATED_TRANSFORM_PIPELINE"],
    },
    "otel": {
        "service_name": "test-service",
        "otlp_endpoint": "http://localhost:4317",
        "trace_sample_rate": 1.0,
    },
}


@pytest.fixture
def mock_config_file(tmp_path):
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(MOCK_CONFIG, f)
    return str(config_path)


@pytest.fixture
def mock_snowflake_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = []
    return conn, cursor


# ─────────────────────────────────────────────────────────────────────────────
# Connection Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSnowflakeConnection:

    @patch("metrics_collector.snowflake.connector.connect")
    def test_connect_calls_snowflake_with_correct_params(
        self, mock_connect, mock_config_file
    ):
        from metrics_collector import SnowflakeMetricsCollector
        collector = SnowflakeMetricsCollector(mock_config_file)
        collector.connect()

        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs["warehouse"] == "TEST_WH"
        assert call_kwargs["database"] == "TEST_DB"

    @patch("metrics_collector.snowflake.connector.connect")
    def test_connect_retries_on_failure(self, mock_connect, mock_config_file):
        from metrics_collector import SnowflakeMetricsCollector
        import snowflake.connector

        mock_connect.side_effect = [
            snowflake.connector.errors.OperationalError("timeout"),
            snowflake.connector.errors.OperationalError("timeout"),
            MagicMock(),  # 3rd attempt succeeds
        ]

        collector = SnowflakeMetricsCollector(mock_config_file)
        collector.connect()
        assert mock_connect.call_count == 3

    @patch("metrics_collector.snowflake.connector.connect")
    def test_connect_raises_after_max_retries(self, mock_connect, mock_config_file):
        from metrics_collector import SnowflakeMetricsCollector
        import snowflake.connector

        mock_connect.side_effect = snowflake.connector.errors.OperationalError("fail")

        collector = SnowflakeMetricsCollector(mock_config_file)
        with pytest.raises(snowflake.connector.errors.OperationalError):
            collector.connect()

        assert mock_connect.call_count == 3


# ─────────────────────────────────────────────────────────────────────────────
# Metric Collection Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMetricCollection:

    def _make_collector(self, mock_config_file, conn):
        from metrics_collector import SnowflakeMetricsCollector
        collector = SnowflakeMetricsCollector(mock_config_file)
        collector._conn = conn
        return collector

    def test_collect_pipeline_metrics_processes_success_row(
        self, mock_config_file, mock_snowflake_conn
    ):
        conn, cursor = mock_snowflake_conn
        cursor.fetchall.return_value = [
            {
                "PIPELINE_NAME": "RAW_INGEST_PIPELINE",
                "STATUS": "SUCCESS",
                "DURATION_SECONDS": "120.5",
                "ROWS_PRODUCED": "50000",
                "CREDITS": "0.05",
                "ERROR_CODE": None,
                "ERROR_MESSAGE": None,
                "WAREHOUSE": "TEST_WH",
            }
        ]
        collector = self._make_collector(mock_config_file, conn)
        # Should not raise
        collector.collect_pipeline_execution_metrics()

    def test_collect_pipeline_metrics_handles_failed_run(
        self, mock_config_file, mock_snowflake_conn
    ):
        conn, cursor = mock_snowflake_conn
        cursor.fetchall.return_value = [
            {
                "PIPELINE_NAME": "RAW_INGEST_PIPELINE",
                "STATUS": "FAILED",
                "DURATION_SECONDS": "30.0",
                "ROWS_PRODUCED": "0",
                "CREDITS": "0.01",
                "ERROR_CODE": "000904",
                "ERROR_MESSAGE": "invalid identifier",
                "WAREHOUSE": "TEST_WH",
            }
        ]
        collector = self._make_collector(mock_config_file, conn)
        # Should not raise and should record error counter
        collector.collect_pipeline_execution_metrics()

    def test_collect_all_does_not_raise_on_empty_results(
        self, mock_config_file, mock_snowflake_conn
    ):
        conn, cursor = mock_snowflake_conn
        cursor.fetchall.return_value = []
        collector = self._make_collector(mock_config_file, conn)
        collector.collect_all()  # Should complete without exception

    def test_pipeline_inference_from_table_name(
        self, mock_config_file, mock_snowflake_conn
    ):
        conn, _ = mock_snowflake_conn
        collector = self._make_collector(mock_config_file, conn)

        assert collector._infer_pipeline_from_table("RAW.EVENTS") == "RAW_INGEST_PIPELINE"
        assert collector._infer_pipeline_from_table("CURATED.METRICS") == "CURATED_TRANSFORM_PIPELINE"
        assert collector._infer_pipeline_from_table("BUSINESS.LOANS") == "BUSINESS_LAYER_PIPELINE"
        assert collector._infer_pipeline_from_table("ANALYTICS.KPI") == "ANALYTICS_AGGREGATION_PIPELINE"
        assert collector._infer_pipeline_from_table("OTHER.TABLE") == "UNKNOWN_PIPELINE"

    def test_collect_slo_compliance_handles_division_by_zero(
        self, mock_config_file, mock_snowflake_conn
    ):
        conn, cursor = mock_snowflake_conn
        cursor.fetchall.return_value = [
            {
                "PIPELINE_NAME": "RAW_INGEST_PIPELINE",
                "TOTAL_RUNS": "0",
                "FAILED_RUNS": "0",
            }
        ]
        collector = self._make_collector(mock_config_file, conn)
        # total_runs = 0, should not divide by zero
        collector.compute_slo_compliance()

    @patch("metrics_collector.snowflake.connector.connect")
    def test_query_failure_forces_reconnect(
        self, mock_connect, mock_config_file, mock_snowflake_conn
    ):
        import snowflake.connector
        conn, cursor = mock_snowflake_conn
        cursor.execute.side_effect = snowflake.connector.errors.OperationalError("lost connection")

        collector = self._make_collector(mock_config_file, conn)

        with pytest.raises(snowflake.connector.errors.OperationalError):
            collector._execute("SELECT 1")

        # Connection should be cleared to force reconnect
        assert collector._conn is None
