"""
iceberg_writer.py
-----------------
Writes pipeline metrics and SLO burn-rate data into Apache Iceberg tables
on AWS S3. Leverages Iceberg's ACID transactions, schema evolution,
and time-travel for auditable, reliable data lake writes.

Tables:
  raw.pipeline_metrics      — raw per-run metrics from Snowflake
  curated.slo_burn_rate     — computed SLI/SLO burn rate per pipeline

Why Iceberg:
  - ACID guarantees prevent partial writes from corrupting metrics history
  - Time-travel lets SRE team replay historical SLO states during postmortems
  - Schema evolution allows adding new metric columns without breaking readers
  - Partition pruning on run_date ensures query performance at scale
"""

import os
from datetime import datetime, timezone
from typing import List, Dict, Optional

import pyarrow as pa
import yaml
from dotenv import load_dotenv
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

load_dotenv("config/.env")


# ─────────────────────────────────────────────────────────────────────────────
# Schema Definitions
# ─────────────────────────────────────────────────────────────────────────────

PIPELINE_METRICS_SCHEMA = Schema(
    NestedField(1,  "run_id",           StringType(),    required=True),
    NestedField(2,  "pipeline_name",    StringType(),    required=True),
    NestedField(3,  "run_timestamp",    TimestampType(), required=True),
    NestedField(4,  "status",           StringType(),    required=True),
    NestedField(5,  "duration_seconds", DoubleType(),    required=False),
    NestedField(6,  "rows_produced",    LongType(),      required=False),
    NestedField(7,  "credits_used",     DoubleType(),    required=False),
    NestedField(8,  "warehouse",        StringType(),    required=False),
    NestedField(9,  "error_code",       StringType(),    required=False),
    NestedField(10, "error_message",    StringType(),    required=False),
    NestedField(11, "run_date",         StringType(),    required=True),  # partition key
)

SLO_BURN_RATE_SCHEMA = Schema(
    NestedField(1,  "slo_id",           StringType(),    required=True),
    NestedField(2,  "pipeline_name",    StringType(),    required=True),
    NestedField(3,  "slo_name",         StringType(),    required=True),
    NestedField(4,  "window_timestamp", TimestampType(), required=True),
    NestedField(5,  "burn_rate",        DoubleType(),    required=True),
    NestedField(6,  "compliance_ratio", DoubleType(),    required=True),
    NestedField(7,  "budget_remaining", DoubleType(),    required=False),
    NestedField(8,  "is_breached",      BooleanType(),   required=True),
    NestedField(9,  "window_date",      StringType(),    required=True),  # partition key
)

# Partition by run_date (day) to enable efficient time-range scans
DAILY_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=11, field_id=1000, transform=DayTransform(), name="run_date_day")
)


# ─────────────────────────────────────────────────────────────────────────────
# Iceberg Catalog + Table Management
# ─────────────────────────────────────────────────────────────────────────────

class IcebergLakehouseWriter:
    """
    Manages Iceberg catalog connection, table creation, and append writes
    for pipeline metrics and SLO burn-rate data.
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        iceberg_cfg = self.config["iceberg"]
        aws_cfg = self.config["aws"]

        self.warehouse_path = os.path.expandvars(iceberg_cfg["warehouse_path"])
        self.catalog_name = iceberg_cfg["catalog_name"]
        self.s3_bucket = os.path.expandvars(aws_cfg["s3_bucket"])
        self.region = aws_cfg["region"]

        self.catalog = self._init_catalog()
        self._ensure_tables()

    # ── Catalog ───────────────────────────────────────────────────────────────

    def _init_catalog(self):
        """Initialize PyIceberg catalog with S3 / AWS Glue backend."""
        catalog = load_catalog(
            self.catalog_name,
            **{
                "type": "glue",
                "s3.endpoint": f"https://s3.{self.region}.amazonaws.com",
                "s3.region": self.region,
                "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
                "warehouse": self.warehouse_path,
            }
        )
        logger.info(f"Iceberg catalog '{self.catalog_name}' initialized.")
        return catalog

    def _ensure_namespace(self, namespace: str) -> None:
        """Create namespace (database) if it doesn't exist."""
        try:
            self.catalog.create_namespace(namespace)
            logger.info(f"Created Iceberg namespace: {namespace}")
        except Exception:
            pass  # already exists

    def _ensure_tables(self) -> None:
        """Create Iceberg tables if they don't already exist."""
        for table_cfg in self.config["iceberg"]["tables"]:
            ns = table_cfg["namespace"]
            name = table_cfg["name"]
            full_name = f"{ns}.{name}"

            self._ensure_namespace(ns)

            try:
                self.catalog.load_table(full_name)
                logger.debug(f"Iceberg table exists: {full_name}")
            except NoSuchTableError:
                schema = (
                    PIPELINE_METRICS_SCHEMA
                    if name == "pipeline_metrics"
                    else SLO_BURN_RATE_SCHEMA
                )
                self.catalog.create_table(
                    identifier=full_name,
                    schema=schema,
                    partition_spec=DAILY_PARTITION_SPEC,
                    properties={
                        "write.format.default": "parquet",
                        "write.parquet.compression-codec": "snappy",
                        "history.expire.max-snapshot-age-ms": str(
                            30 * 24 * 60 * 60 * 1000  # 30 days
                        ),
                    },
                )
                logger.info(f"Created Iceberg table: {full_name}")

    # ── Write Methods ─────────────────────────────────────────────────────────

    def write_pipeline_metrics(self, metrics: List[Dict]) -> int:
        """
        Appends pipeline run metrics to raw.pipeline_metrics Iceberg table.

        Args:
            metrics: List of dicts with pipeline execution data from Snowflake

        Returns:
            Number of rows written
        """
        if not metrics:
            logger.debug("No pipeline metrics to write.")
            return 0

        now = datetime.now(timezone.utc)
        run_date = now.strftime("%Y-%m-%d")

        rows = []
        for m in metrics:
            rows.append({
                "run_id": f"{m.get('pipeline_name', 'unknown')}_{int(now.timestamp())}",
                "pipeline_name": m.get("pipeline_name", "unknown"),
                "run_timestamp": now,
                "status": m.get("status", "UNKNOWN"),
                "duration_seconds": float(m.get("duration_seconds") or 0),
                "rows_produced": int(m.get("rows_produced") or 0),
                "credits_used": float(m.get("credits_used") or 0),
                "warehouse": m.get("warehouse", "unknown"),
                "error_code": m.get("error_code"),
                "error_message": m.get("error_message"),
                "run_date": run_date,
            })

        arrow_table = pa.Table.from_pylist(rows, schema=self._get_arrow_schema("pipeline_metrics"))

        table = self.catalog.load_table("raw.pipeline_metrics")
        table.append(arrow_table)

        logger.info(f"Wrote {len(rows)} rows to raw.pipeline_metrics (partition: {run_date})")
        return len(rows)

    def write_slo_burn_rate(self, slo_records: List[Dict]) -> int:
        """
        Appends SLO burn-rate snapshots to curated.slo_burn_rate Iceberg table.

        Args:
            slo_records: List of burn-rate dicts computed by slo_tracker.py

        Returns:
            Number of rows written
        """
        if not slo_records:
            return 0

        now = datetime.now(timezone.utc)
        window_date = now.strftime("%Y-%m-%d")

        rows = []
        for s in slo_records:
            rows.append({
                "slo_id": f"{s['pipeline_name']}_{s['slo_name']}_{int(now.timestamp())}",
                "pipeline_name": s["pipeline_name"],
                "slo_name": s["slo_name"],
                "window_timestamp": now,
                "burn_rate": float(s.get("burn_rate", 0)),
                "compliance_ratio": float(s.get("compliance_ratio", 1.0)),
                "budget_remaining": float(s.get("budget_remaining", 1.0)),
                "is_breached": bool(s.get("is_breached", False)),
                "window_date": window_date,
            })

        arrow_table = pa.Table.from_pylist(rows, schema=self._get_arrow_schema("slo_burn_rate"))

        table = self.catalog.load_table("curated.slo_burn_rate")
        table.append(arrow_table)

        logger.info(f"Wrote {len(rows)} SLO records to curated.slo_burn_rate")
        return len(rows)

    # ── Time-Travel / Audit ───────────────────────────────────────────────────

    def read_pipeline_metrics_at(self, snapshot_id: Optional[int] = None) -> pa.Table:
        """
        Read pipeline_metrics at a specific Iceberg snapshot (time-travel).
        Useful during postmortems to replay historical SLO state.

        Args:
            snapshot_id: Iceberg snapshot ID (None = current)
        """
        table = self.catalog.load_table("raw.pipeline_metrics")

        if snapshot_id:
            scan = table.scan(snapshot_id=snapshot_id)
            logger.info(f"Time-travel read at snapshot {snapshot_id}")
        else:
            scan = table.scan()

        return scan.to_arrow()

    def list_snapshots(self) -> List[Dict]:
        """List all available Iceberg snapshots for postmortem replay."""
        table = self.catalog.load_table("raw.pipeline_metrics")
        snapshots = []
        for snap in table.snapshots():
            snapshots.append({
                "snapshot_id": snap.snapshot_id,
                "timestamp_ms": snap.timestamp_ms,
                "timestamp": datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc),
                "operation": snap.summary.get("operation", "unknown"),
            })
        return sorted(snapshots, key=lambda x: x["timestamp_ms"], reverse=True)

    # ── Schema Utils ──────────────────────────────────────────────────────────

    def _get_arrow_schema(self, table_name: str) -> pa.Schema:
        if table_name == "pipeline_metrics":
            return pa.schema([
                pa.field("run_id", pa.string()),
                pa.field("pipeline_name", pa.string()),
                pa.field("run_timestamp", pa.timestamp("us", tz="UTC")),
                pa.field("status", pa.string()),
                pa.field("duration_seconds", pa.float64()),
                pa.field("rows_produced", pa.int64()),
                pa.field("credits_used", pa.float64()),
                pa.field("warehouse", pa.string()),
                pa.field("error_code", pa.string()),
                pa.field("error_message", pa.string()),
                pa.field("run_date", pa.string()),
            ])
        else:  # slo_burn_rate
            return pa.schema([
                pa.field("slo_id", pa.string()),
                pa.field("pipeline_name", pa.string()),
                pa.field("slo_name", pa.string()),
                pa.field("window_timestamp", pa.timestamp("us", tz="UTC")),
                pa.field("burn_rate", pa.float64()),
                pa.field("compliance_ratio", pa.float64()),
                pa.field("budget_remaining", pa.float64()),
                pa.field("is_breached", pa.bool_()),
                pa.field("window_date", pa.string()),
            ])
