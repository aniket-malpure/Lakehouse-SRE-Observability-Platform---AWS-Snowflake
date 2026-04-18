"""
infrastructure/cloudwatch_setup.py
-----------------------------------
Provisions CloudWatch Alarms for all pipeline SLOs using boto3.
Creates multi-window burn rate alarms for each pipeline + SLO combination.

Alarm Strategy:
  - 1h burn rate > 14.4x  → P1 (fast burn — page immediately)
  - 6h burn rate > 6.0x   → P2 (slow burn — notify on-call)

Run once to bootstrap, re-run to update thresholds.

Usage:
    python infrastructure/cloudwatch_setup.py --action create
    python infrastructure/cloudwatch_setup.py --action delete
    python infrastructure/cloudwatch_setup.py --action list
"""

import argparse
import json
import os

import boto3
import yaml
from dotenv import load_dotenv
from loguru import logger

load_dotenv("config/.env")


class CloudWatchAlarmManager:

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        aws_cfg = self.config["aws"]
        self.namespace = aws_cfg["cloudwatch_namespace"]
        self.region = aws_cfg["region"]
        self.sns_arn = os.path.expandvars(aws_cfg["sns_topic_arn"])
        self.pipelines = self.config["monitoring"]["pipelines_to_monitor"]

        self.cw = boto3.client("cloudwatch", region_name=self.region)

    # ── Alarm Definitions ─────────────────────────────────────────────────────

    BURN_RATE_ALARMS = [
        {
            "window": "1h",
            "metric": "SLOBurnRate",
            "threshold": 14.4,
            "period": 3600,
            "eval_periods": 1,
            "description": "1-hour burn rate > 14.4x — fast burn consuming 2% error budget per hour",
        },
        {
            "window": "6h",
            "metric": "SLOBurnRate",
            "threshold": 6.0,
            "period": 21600,
            "eval_periods": 1,
            "description": "6-hour burn rate > 6x — slow burn consuming 5% error budget per 6 hours",
        },
    ]

    SLO_NAMES = ["error_rate", "latency_p95", "data_freshness"]

    # ── Create Alarms ─────────────────────────────────────────────────────────

    def create_all_alarms(self) -> int:
        """Create burn rate alarms for every pipeline × SLO × window combination."""
        created = 0

        for pipeline in self.pipelines:
            for slo in self.SLO_NAMES:
                for alarm_def in self.BURN_RATE_ALARMS:
                    alarm_name = f"LakehouseSRE-{pipeline}-{slo}-BurnRate{alarm_def['window']}"

                    try:
                        self.cw.put_metric_alarm(
                            AlarmName=alarm_name,
                            AlarmDescription=alarm_def["description"],
                            ActionsEnabled=True,
                            AlarmActions=[self.sns_arn],
                            OKActions=[self.sns_arn],
                            MetricName=alarm_def["metric"],
                            Namespace=self.namespace,
                            Statistic="Average",
                            Dimensions=[
                                {"Name": "PipelineName", "Value": pipeline},
                                {"Name": "SLOName",      "Value": slo},
                            ],
                            Period=alarm_def["period"],
                            EvaluationPeriods=alarm_def["eval_periods"],
                            Threshold=alarm_def["threshold"],
                            ComparisonOperator="GreaterThanThreshold",
                            TreatMissingData="notBreaching",
                            Tags=[
                                {"Key": "Project",     "Value": "LakehouseSRE"},
                                {"Key": "Pipeline",    "Value": pipeline},
                                {"Key": "SLO",         "Value": slo},
                                {"Key": "Window",      "Value": alarm_def["window"]},
                            ],
                        )
                        logger.info(f"Created alarm: {alarm_name}")
                        created += 1

                    except Exception as e:
                        logger.error(f"Failed to create alarm {alarm_name}: {e}")

        # Also create a latency SLO alarm directly on pipeline duration
        created += self._create_latency_alarms()

        logger.info(f"Alarm setup complete. Created {created} alarms.")
        return created

    def _create_latency_alarms(self) -> int:
        """Create direct latency alarms (p95 > alert threshold in seconds)."""
        created = 0
        alert_threshold_s = self.config["slo"]["pipeline_latency_alert_seconds"]

        for pipeline in self.pipelines:
            alarm_name = f"LakehouseSRE-{pipeline}-latency-p95-direct"
            try:
                self.cw.put_metric_alarm(
                    AlarmName=alarm_name,
                    AlarmDescription=(
                        f"Pipeline p95 latency > {alert_threshold_s}s "
                        f"(SLO threshold: {self.config['slo']['pipeline_latency_p95_seconds']}s)"
                    ),
                    ActionsEnabled=True,
                    AlarmActions=[self.sns_arn],
                    OKActions=[self.sns_arn],
                    ExtendedStatistic="p95",
                    MetricName="PipelineDurationSeconds",
                    Namespace=self.namespace,
                    Dimensions=[
                        {"Name": "PipelineName", "Value": pipeline},
                        {"Name": "Status",       "Value": "SUCCESS"},
                    ],
                    Period=300,
                    EvaluationPeriods=3,
                    Threshold=float(alert_threshold_s),
                    ComparisonOperator="GreaterThanThreshold",
                    TreatMissingData="notBreaching",
                )
                logger.info(f"Created latency alarm: {alarm_name}")
                created += 1
            except Exception as e:
                logger.error(f"Failed to create latency alarm {alarm_name}: {e}")

        return created

    # ── List / Delete ─────────────────────────────────────────────────────────

    def list_alarms(self) -> list:
        """List all LakehouseSRE alarms and their current states."""
        try:
            response = self.cw.describe_alarms(AlarmNamePrefix="LakehouseSRE-")
            alarms = response.get("MetricAlarms", [])

            print(f"\n{'Alarm Name':<65} {'State':<10} {'Threshold'}")
            print("-" * 90)
            for a in sorted(alarms, key=lambda x: x["AlarmName"]):
                print(
                    f"{a['AlarmName']:<65} "
                    f"{a['StateValue']:<10} "
                    f"{a['Threshold']}"
                )
            print(f"\nTotal: {len(alarms)} alarms")
            return alarms
        except Exception as e:
            logger.error(f"Failed to list alarms: {e}")
            return []

    def delete_all_alarms(self) -> None:
        """Delete all LakehouseSRE alarms (use with caution)."""
        alarms = self.list_alarms()
        names = [a["AlarmName"] for a in alarms]

        if not names:
            logger.info("No alarms to delete.")
            return

        confirm = input(f"\nDelete {len(names)} alarms? (yes/no): ")
        if confirm.lower() != "yes":
            logger.info("Deletion cancelled.")
            return

        # CloudWatch delete allows max 100 alarm names per call
        for i in range(0, len(names), 100):
            batch = names[i:i + 100]
            self.cw.delete_alarms(AlarmNames=batch)
            logger.info(f"Deleted {len(batch)} alarms.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CloudWatch Alarm Manager")
    parser.add_argument(
        "--action",
        choices=["create", "list", "delete"],
        required=True,
        help="Action to perform on CloudWatch alarms",
    )
    args = parser.parse_args()

    manager = CloudWatchAlarmManager()

    if args.action == "create":
        count = manager.create_all_alarms()
        print(f"\n✅ Created {count} CloudWatch alarms.")

    elif args.action == "list":
        manager.list_alarms()

    elif args.action == "delete":
        manager.delete_all_alarms()
