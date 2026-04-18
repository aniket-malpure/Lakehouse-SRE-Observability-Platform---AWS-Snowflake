"""
metrics_collector.py
--------------------
Collects Snowflake pipeline health metrics and exposes them as
Prometheus metrics. Tracks query latency, pipeline execution times,
row counts, credit consumption, and error rates.

Metrics exposed:
  - snowflake_pipeline_duration_seconds (Histogram)
  - snowflake_pipeline_rows_processed (Gauge)
  - snowflake_pipeline_credits_used (Gauge)
  - snowflake_pipeline_errors_total (Counter)
  - snowflake_data_freshness_minutes (Gauge)
  - snowflake_query_latency_seconds (Histogram)
"""

import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import snowflake.connector
import yaml
from dotenv import load_dotenv
from loguru import logger
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
    REGISTRY,
)

load_dotenv("config/.env")


# ─────────────────────────────────────────────────────────────────────────────
# Prometheus Metric Definitions
# ─────────────────────────────────────────────────────────────────────────────

PIPELINE_DURATION = Histogram(
    "snowflake_pipeline_duration_seconds",
    "End-to-end pipeline execution duration in seconds",
    ["pipeline_name", "status"],
    buckets=[30, 60, 120, 180, 240, 300, 600, 900],
)

PIPELINE_ROWS = Gauge(
    "snowflake_pipeline_rows_processed",
    "Number of rows processed in the last pipeline run",
    ["pipeline_name"],
)

PIPELINE_CREDITS = Gauge(
    "snowflake_pipeline_credits_used",
    "Snowflake credits consumed per pipeline run",
    ["pipeline_name", "warehouse"],
)

PIPELINE_ERRORS = Counter(
    "snowflake_pipeline_errors_total",
    "Total number of pipeline execution errors",
    ["pipeline_name", "error_type"],
)

DATA_FRESHNESS = Gauge(
    "snowflake_data_freshness_minutes",
    "Minutes since the last successful data load for each pipeline",
    ["pipeline_name", "target_table"],
)

QUERY_LATENCY = Histogram(
    "snowflake_query_latency_seconds",
    "Snowflake query execution latency in seconds",
    ["query_type", "warehouse"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)

WAREHOUSE_UTILIZATION = Gauge(
    "snowflake_warehouse_utilization_pct",
    "Percentage of warehouse capacity currently utilized",
    ["warehouse_name"],
)

SLO_COMPLIANCE = Gauge(
    "snowflake_slo_compliance_ratio",
    "Current SLO compliance ratio (1.0 = fully compliant)",
    ["slo_name"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Snowflake Connector
# ─────────────────────────────────────────────────────────────────────────────

class SnowflakeMetricsCollector:
    """
    Connects to Snowflake and periodically collects pipeline health metrics,
    exposing them to Prometheus for scraping.
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.sf_config = self.config["snowflake"]
        self.pipelines = self.config["monitoring"]["pipelines_to_monitor"]
        self.slo_config = self.config["slo"]
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    # ── Connection Management ─────────────────────────────────────────────────

    def connect(self) -> None:
        """Establish Snowflake connection with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._conn = snowflake.connector.connect(
                    account=os.path.expandvars(self.sf_config["account"]),
                    user=os.path.expandvars(self.sf_config["user"]),
                    password=os.path.expandvars(self.sf_config["password"]),
                    warehouse=self.sf_config["warehouse"],
                    database=self.sf_config["database"],
                    schema=self.sf_config["schema"],
                    role=self.sf_config["role"],
                    network_timeout=30,
                )
                logger.info("Snowflake connection established.")
                return
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                else:
                    raise

    def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed.")

    def _execute(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute a Snowflake query and return rows as list of dicts."""
        if not self._conn:
            self.connect()

        start = time.perf_counter()
        try:
            cursor = self._conn.cursor(snowflake.connector.DictCursor)
            cursor.execute(query, params)
            rows = cursor.fetchall()
            elapsed = time.perf_counter() - start

            # Record query latency to Prometheus
            QUERY_LATENCY.labels(
                query_type="monitor_query",
                warehouse=self.sf_config["warehouse"],
            ).observe(elapsed)

            return rows
        except snowflake.connector.errors.OperationalError as e:
            logger.error(f"Query failed: {e}")
            self._conn = None  # force reconnect next time
            raise

    # ── Metric Collection Methods ─────────────────────────────────────────────

    def collect_pipeline_execution_metrics(self) -> None:
        """
        Queries QUERY_HISTORY to get execution stats for monitored pipelines.
        Maps to: PIPELINE_DURATION, PIPELINE_ROWS, PIPELINE_CREDITS, PIPELINE_ERRORS
        """
        query = """
            SELECT
                qh.QUERY_TAG                          AS pipeline_name,
                qh.EXECUTION_STATUS                   AS status,
                qh.TOTAL_ELAPSED_TIME / 1000.0        AS duration_seconds,
                qh.ROWS_PRODUCED                      AS rows_produced,
                qh.CREDITS_USED_CLOUD_SERVICES        AS credits,
                qh.ERROR_CODE                         AS error_code,
                qh.ERROR_MESSAGE                      AS error_message,
                qh.WAREHOUSE_NAME                     AS warehouse
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
            WHERE qh.START_TIME >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
              AND qh.QUERY_TAG IN ({placeholders})
            ORDER BY qh.START_TIME DESC
        """.format(
            placeholders=", ".join(["%s"] * len(self.pipelines))
        )

        try:
            rows = self._execute(query, tuple(self.pipelines))
        except Exception as e:
            logger.error(f"Failed to collect pipeline metrics: {e}")
            for pipeline in self.pipelines:
                PIPELINE_ERRORS.labels(
                    pipeline_name=pipeline, error_type="collection_failure"
                ).inc()
            return

        for row in rows:
            pipeline = row["PIPELINE_NAME"] or "unknown"
            status = row["STATUS"] or "unknown"
            duration = float(row["DURATION_SECONDS"] or 0)
            rows_produced = int(row["ROWS_PRODUCED"] or 0)
            credits = float(row["CREDITS"] or 0)
            warehouse = row["WAREHOUSE"] or "unknown"

            PIPELINE_DURATION.labels(
                pipeline_name=pipeline, status=status
            ).observe(duration)

            PIPELINE_ROWS.labels(pipeline_name=pipeline).set(rows_produced)

            PIPELINE_CREDITS.labels(
                pipeline_name=pipeline, warehouse=warehouse
            ).set(credits)

            if status == "FAILED":
                error_type = row["ERROR_CODE"] or "UNKNOWN_ERROR"
                PIPELINE_ERRORS.labels(
                    pipeline_name=pipeline, error_type=error_type
                ).inc()
                logger.warning(
                    f"Pipeline {pipeline} FAILED: {row['ERROR_MESSAGE']}"
                )

        logger.debug(f"Collected execution metrics for {len(rows)} pipeline runs.")

    def collect_data_freshness_metrics(self) -> None:
        """
        Checks how many minutes ago each target table was last updated.
        Staleness > SLO threshold triggers a Prometheus gauge alert.
        """
        query = """
            SELECT
                TABLE_SCHEMA || '.' || TABLE_NAME  AS target_table,
                DATEDIFF('minute', LAST_ALTERED, CURRENT_TIMESTAMP()) AS staleness_minutes
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SNOWFLAKE')
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY staleness_minutes DESC
        """

        try:
            rows = self._execute(query)
        except Exception as e:
            logger.error(f"Failed to collect freshness metrics: {e}")
            return

        freshness_threshold = self.slo_config["data_freshness_max_minutes"]

        for row in rows:
            table = row["TARGET_TABLE"]
            staleness = int(row["STALENESS_MINUTES"] or 0)

            # Tag pipeline name based on table prefix convention
            pipeline = self._infer_pipeline_from_table(table)

            DATA_FRESHNESS.labels(
                pipeline_name=pipeline, target_table=table
            ).set(staleness)

            if staleness > freshness_threshold:
                logger.warning(
                    f"Data freshness SLO breach: {table} is {staleness}m stale "
                    f"(threshold: {freshness_threshold}m)"
                )

    def collect_warehouse_utilization(self) -> None:
        """
        Queries WAREHOUSE_METERING_HISTORY to compute utilization %.
        """
        query = """
            SELECT
                WAREHOUSE_NAME,
                AVG(AVG_RUNNING) / NULLIF(AVG(AVG_RUNNING) + AVG(AVG_QUEUED_LOAD), 0) * 100
                    AS utilization_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
            WHERE START_TIME >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
        """

        try:
            rows = self._execute(query)
        except Exception as e:
            logger.error(f"Failed to collect warehouse utilization: {e}")
            return

        for row in rows:
            warehouse = row["WAREHOUSE_NAME"]
            utilization = float(row["UTILIZATION_PCT"] or 0)
            WAREHOUSE_UTILIZATION.labels(warehouse_name=warehouse).set(utilization)

        logger.debug(f"Warehouse utilization collected for {len(rows)} warehouses.")

    def compute_slo_compliance(self) -> None:
        """
        Computes rolling SLO compliance ratios for error rate and latency SLOs.
        Writes compliance ratio to Prometheus (1.0 = 100% compliant).
        """
        # Error rate SLO: what % of pipeline runs succeeded in last hour?
        error_rate_query = """
            SELECT
                QUERY_TAG                       AS pipeline_name,
                COUNT(*)                        AS total_runs,
                SUM(CASE WHEN EXECUTION_STATUS = 'FAILED' THEN 1 ELSE 0 END)
                                                AS failed_runs
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
              AND QUERY_TAG IN ({placeholders})
            GROUP BY QUERY_TAG
        """.format(
            placeholders=", ".join(["%s"] * len(self.pipelines))
        )

        try:
            rows = self._execute(error_rate_query, tuple(self.pipelines))
        except Exception as e:
            logger.error(f"Failed to compute SLO compliance: {e}")
            return

        error_threshold = self.slo_config["error_rate_threshold_pct"] / 100.0

        for row in rows:
            pipeline = row["PIPELINE_NAME"] or "unknown"
            total = int(row["TOTAL_RUNS"] or 1)
            failed = int(row["FAILED_RUNS"] or 0)

            error_rate = failed / total
            compliant = 1.0 if error_rate <= error_threshold else max(
                0.0, 1.0 - (error_rate - error_threshold)
            )

            SLO_COMPLIANCE.labels(
                slo_name=f"{pipeline}_error_rate"
            ).set(compliant)

        logger.debug("SLO compliance ratios updated.")

    # ── Utility ───────────────────────────────────────────────────────────────

    def _infer_pipeline_from_table(self, table_name: str) -> str:
        """Infer owning pipeline from table naming convention."""
        table_upper = table_name.upper()
        if "RAW" in table_upper:
            return "RAW_INGEST_PIPELINE"
        elif "CURATED" in table_upper:
            return "CURATED_TRANSFORM_PIPELINE"
        elif "BUSINESS" in table_upper:
            return "BUSINESS_LAYER_PIPELINE"
        elif "ANALYTICS" in table_upper:
            return "ANALYTICS_AGGREGATION_PIPELINE"
        return "UNKNOWN_PIPELINE"

    def collect_all(self) -> None:
        """Run all metric collectors in one pass."""
        logger.info("Starting metrics collection cycle...")
        start = time.perf_counter()

        self.collect_pipeline_execution_metrics()
        self.collect_data_freshness_metrics()
        self.collect_warehouse_utilization()
        self.compute_slo_compliance()

        elapsed = time.perf_counter() - start
        logger.info(f"Metrics collection cycle complete in {elapsed:.2f}s")


# ─────────────────────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    with open("config/config.yaml") as f:
        config = yaml.safe_load(f)

    prom_port = config["prometheus"]["port"]
    interval = config["monitoring"]["collection_interval_seconds"]

    logger.info(f"Starting Prometheus metrics server on port {prom_port}")
    start_http_server(prom_port)

    collector = SnowflakeMetricsCollector()
    collector.connect()

    try:
        while True:
            collector.collect_all()
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Shutting down metrics collector.")
    finally:
        collector.disconnect()


if __name__ == "__main__":
    main()
