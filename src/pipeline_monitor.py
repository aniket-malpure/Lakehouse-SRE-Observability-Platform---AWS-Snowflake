"""
pipeline_monitor.py
-------------------
Main orchestrator for the Lakehouse SRE Observability Platform.
Runs the full monitoring loop:

  1. Collect Snowflake pipeline metrics → Prometheus
  2. Compute SLO burn rates
  3. Write metrics + SLO records to Iceberg tables on S3
  4. Trigger CloudWatch metric push for alarm evaluation
  5. Repeat every N seconds (configurable)

Shutdown: Ctrl+C or SIGTERM — cleans up connections gracefully.
"""

import os
import signal
import time

import boto3
import yaml
from dotenv import load_dotenv
from loguru import logger
from prometheus_client import start_http_server

from metrics_collector import SnowflakeMetricsCollector
from otel_instrumentor import PipelineSpan, tracer
from iceberg_writer import IcebergLakehouseWriter
from slo_tracker import SLOTracker

load_dotenv("config/.env")


# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Metric Publisher
# ─────────────────────────────────────────────────────────────────────────────

class CloudWatchPublisher:
    """
    Pushes custom SLO metrics to CloudWatch for alarm evaluation.
    CloudWatch alarms trigger the Lambda alerting function.
    """

    def __init__(self, config: dict):
        aws_cfg = config["aws"]
        self.namespace = aws_cfg["cloudwatch_namespace"]
        self.region = aws_cfg["region"]
        self.client = boto3.client("cloudwatch", region_name=self.region)

    def publish_slo_status(self, slo_statuses: list) -> None:
        """Batch publish SLO compliance ratios to CloudWatch."""
        metric_data = []

        for status in slo_statuses:
            # Compliance ratio (1.0 = good, <1.0 = degraded)
            metric_data.append({
                "MetricName": "SLOComplianceRatio",
                "Dimensions": [
                    {"Name": "PipelineName", "Value": status.pipeline_name},
                    {"Name": "SLOName",      "Value": status.slo_name},
                ],
                "Value": status.compliance_ratio,
                "Unit": "None",
            })

            # Burn rate
            metric_data.append({
                "MetricName": "SLOBurnRate",
                "Dimensions": [
                    {"Name": "PipelineName", "Value": status.pipeline_name},
                    {"Name": "SLOName",      "Value": status.slo_name},
                ],
                "Value": status.burn_rate,
                "Unit": "None",
            })

        # CloudWatch allows max 20 metrics per put_metric_data call
        for i in range(0, len(metric_data), 20):
            batch = metric_data[i:i + 20]
            try:
                self.client.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch,
                )
            except Exception as e:
                logger.error(f"Failed to publish CloudWatch metrics (batch {i}): {e}")

        logger.debug(f"Published {len(metric_data)} metrics to CloudWatch.")

    def publish_pipeline_health(self, pipeline_name: str, duration_s: float, status: str) -> None:
        """Publish individual pipeline run health for CloudWatch alarms."""
        try:
            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[
                    {
                        "MetricName": "PipelineDurationSeconds",
                        "Dimensions": [
                            {"Name": "PipelineName", "Value": pipeline_name},
                            {"Name": "Status",       "Value": status},
                        ],
                        "Value": duration_s,
                        "Unit": "Seconds",
                    },
                ],
            )
        except Exception as e:
            logger.warning(f"Failed to publish pipeline health metric: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Monitoring Loop
# ─────────────────────────────────────────────────────────────────────────────

class LakehouseSREMonitor:

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.interval = self.config["monitoring"]["collection_interval_seconds"]
        self._running = False

        # Components
        self.snowflake_collector = SnowflakeMetricsCollector(config_path)
        self.iceberg_writer = IcebergLakehouseWriter(config_path)
        self.slo_tracker = SLOTracker(self.config)
        self.cw_publisher = CloudWatchPublisher(self.config)

    def start(self) -> None:
        """Start Prometheus HTTP server and the monitoring loop."""
        prom_port = self.config["prometheus"]["port"]
        start_http_server(prom_port)
        logger.info(f"Prometheus metrics available at http://localhost:{prom_port}/metrics")

        self.snowflake_collector.connect()
        self._running = True

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        logger.info("Lakehouse SRE Monitor started. Press Ctrl+C to stop.")
        self._loop()

    def _loop(self) -> None:
        while self._running:
            cycle_start = time.perf_counter()

            with PipelineSpan("SRE_MONITOR", "full_collection_cycle") as span:
                try:
                    self._run_collection_cycle(span)
                except Exception as e:
                    logger.error(f"Collection cycle failed: {e}")
                    span.set_custom_attribute("error", str(e))

            elapsed = time.perf_counter() - cycle_start
            sleep_time = max(0, self.interval - elapsed)

            logger.info(f"Cycle complete in {elapsed:.1f}s. Next run in {sleep_time:.0f}s.")
            time.sleep(sleep_time)

    def _run_collection_cycle(self, parent_span: PipelineSpan) -> None:
        """One full collection + write cycle."""

        # 1. Collect Snowflake metrics (Prometheus gauges + counters updated inside)
        with PipelineSpan("SRE_MONITOR", "snowflake_collection") as step:
            self.snowflake_collector.collect_all()

        # 2. Simulate reading back fresh metrics for SLO evaluation
        #    (in production these would come from the Prometheus /metrics scrape
        #     or directly from the Snowflake query results above)
        pipeline_runs = self._get_recent_pipeline_runs()

        # 3. Feed readings into SLO tracker
        with PipelineSpan("SRE_MONITOR", "slo_computation") as step:
            for run in pipeline_runs:
                self.slo_tracker.record_error_rate(
                    run["pipeline_name"],
                    run.get("error_rate", 0.0),
                )
                self.slo_tracker.record_pipeline_latency(
                    run["pipeline_name"],
                    run.get("duration_seconds", 0.0),
                )
                self.slo_tracker.record_data_freshness(
                    run["pipeline_name"],
                    run.get("staleness_minutes", 0.0),
                )

            slo_statuses = self.slo_tracker.compute_all_burn_rates()

        # 4. Write to Iceberg
        with PipelineSpan("SRE_MONITOR", "iceberg_write") as step:
            rows_written = self.iceberg_writer.write_pipeline_metrics(pipeline_runs)
            slo_dicts = [s.to_dict() for s in slo_statuses]
            slo_rows = self.iceberg_writer.write_slo_burn_rate(slo_dicts)

            step.set_row_count(rows_in=len(pipeline_runs), rows_out=rows_written + slo_rows)

        # 5. Push to CloudWatch for alarm evaluation
        with PipelineSpan("SRE_MONITOR", "cloudwatch_publish") as step:
            self.cw_publisher.publish_slo_status(slo_statuses)
            for run in pipeline_runs:
                self.cw_publisher.publish_pipeline_health(
                    run["pipeline_name"],
                    run.get("duration_seconds", 0.0),
                    run.get("status", "UNKNOWN"),
                )

    def _get_recent_pipeline_runs(self) -> list:
        """
        In production: query Snowflake QUERY_HISTORY or read from the
        already-collected Prometheus metrics buffer. Returns a list of
        dicts with fields expected by SLOTracker.
        """
        # Placeholder — real implementation queries the collector's last result set
        return [
            {
                "pipeline_name": p,
                "status": "SUCCESS",
                "duration_seconds": 120.0,
                "rows_produced": 10000,
                "credits_used": 0.05,
                "error_rate": 0.0,
                "staleness_minutes": 5.0,
                "warehouse": self.config["snowflake"]["warehouse"],
            }
            for p in self.config["monitoring"]["pipelines_to_monitor"]
        ]

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self._running = False
        self.snowflake_collector.disconnect()


# ─────────────────────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    monitor = LakehouseSREMonitor()
    monitor.start()
