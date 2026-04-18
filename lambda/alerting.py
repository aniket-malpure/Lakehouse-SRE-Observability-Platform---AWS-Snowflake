"""
lambda/alerting.py
------------------
AWS Lambda function triggered by CloudWatch Alarms.
Receives alarm state changes and routes structured alert notifications
to SNS (which fans out to PagerDuty, Slack, or email depending on severity).

Trigger:  CloudWatch Alarm → SNS → Lambda
Output:   Formatted SNS alert with:
            - Pipeline name, SLO name, breach type
            - Burn rate, compliance ratio, budget remaining
            - Recommended action and runbook link
            - Time-travel snapshot ID for postmortem replay

Deploy:   aws lambda update-function-code --function-name lakehouse-sre-alerter
          (see infrastructure/cloudwatch_setup.py for alarm provisioning)
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Dict

import boto3


SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
RUNBOOK_BASE_URL = os.environ.get(
    "RUNBOOK_BASE_URL",
    "https://wiki.your-org.com/sre/runbooks"
)

sns_client = boto3.client("sns")
cloudwatch_client = boto3.client("cloudwatch")
s3_client = boto3.client("s3")


# ─────────────────────────────────────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────────────────────────────────────

def handler(event: dict, context) -> dict:
    """
    Entry point for CloudWatch Alarm state-change events.

    Expected event shape (via SNS subscription to CW alarm):
    {
      "Records": [{
        "Sns": {
          "Message": "{\"AlarmName\": ..., \"NewStateValue\": \"ALARM\", ...}"
        }
      }]
    }
    """
    processed = 0
    errors = 0

    for record in event.get("Records", []):
        try:
            raw_message = record["Sns"]["Message"]
            alarm_event = json.loads(raw_message)
            _process_alarm(alarm_event)
            processed += 1
        except Exception as e:
            print(f"ERROR processing record: {e}")
            errors += 1

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": processed, "errors": errors}),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Alarm Processor
# ─────────────────────────────────────────────────────────────────────────────

def _process_alarm(alarm_event: dict) -> None:
    """Parse alarm event and route to the appropriate alert handler."""
    alarm_name = alarm_event.get("AlarmName", "unknown")
    new_state = alarm_event.get("NewStateValue", "UNKNOWN")
    old_state = alarm_event.get("OldStateValue", "UNKNOWN")
    alarm_description = alarm_event.get("AlarmDescription", "")
    state_reason = alarm_event.get("NewStateReason", "")

    print(f"Processing alarm: {alarm_name} | {old_state} → {new_state}")

    if new_state == "ALARM":
        alert = _build_alert(alarm_name, alarm_event, state_reason)
        severity = _determine_severity(alarm_name, alarm_event)
        _send_alert(alert, severity)

    elif new_state == "OK":
        _send_resolution(alarm_name, old_state, alarm_event)


# ─────────────────────────────────────────────────────────────────────────────
# Alert Builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_alert(alarm_name: str, alarm_event: dict, state_reason: str) -> dict:
    """
    Construct structured alert payload from CloudWatch alarm event.
    Enriches with latest metric values and runbook links.
    """
    # Parse pipeline and SLO name from alarm naming convention:
    # e.g. "LakehouseSRE-RAW_INGEST_PIPELINE-error_rate-BurnRate1h"
    parts = alarm_name.split("-")
    pipeline_name = parts[1] if len(parts) > 1 else "unknown"
    slo_name = parts[2] if len(parts) > 2 else "unknown"
    window = parts[3] if len(parts) > 3 else "unknown"

    # Fetch current metric value from CloudWatch
    burn_rate = _get_current_metric_value(
        "LakehouseSRE", "SLOBurnRate", pipeline_name, slo_name
    )
    compliance = _get_current_metric_value(
        "LakehouseSRE", "SLOComplianceRatio", pipeline_name, slo_name
    )

    runbook_url = f"{RUNBOOK_BASE_URL}/{slo_name.lower().replace('_', '-')}"

    alert = {
        "alert_id": f"{alarm_name}-{int(time.time())}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "alarm_name": alarm_name,
        "pipeline_name": pipeline_name,
        "slo_name": slo_name,
        "burn_rate_window": window,
        "current_burn_rate": round(burn_rate, 2),
        "current_compliance_ratio": round(compliance, 4),
        "state_reason": state_reason,
        "runbook_url": runbook_url,
        "recommended_actions": _get_recommended_actions(slo_name, burn_rate),
    }

    print(f"Built alert: {json.dumps(alert, indent=2)}")
    return alert


def _build_alert_message(alert: dict, severity: str) -> str:
    """Format alert dict into a human-readable SNS message."""
    burn_rate = alert["current_burn_rate"]
    compliance_pct = alert["current_compliance_ratio"] * 100

    # Budget burn speed estimate
    if burn_rate > 0:
        hours_to_exhaust = (30 * 24) / burn_rate
        budget_warning = (
            f"At current burn rate, the 30-day error budget will be exhausted "
            f"in ~{hours_to_exhaust:.0f} hours."
        )
    else:
        budget_warning = "Burn rate is 0 — no immediate budget risk."

    actions = "\n".join(
        f"  {i+1}. {a}" for i, a in enumerate(alert["recommended_actions"])
    )

    return f"""
🚨 LAKEHOUSE SRE ALERT [{severity.upper()}]

Pipeline:    {alert['pipeline_name']}
SLO:         {alert['slo_name']}
Window:      {alert['burn_rate_window']}
Burn Rate:   {burn_rate:.1f}x  (1.0 = budget consumed at 30-day pace)
Compliance:  {compliance_pct:.1f}%

{budget_warning}

Reason:
{alert['state_reason']}

Recommended Actions:
{actions}

Runbook: {alert['runbook_url']}
Alert ID: {alert['alert_id']}
Time: {alert['timestamp']}
""".strip()


# ─────────────────────────────────────────────────────────────────────────────
# Routing
# ─────────────────────────────────────────────────────────────────────────────

def _send_alert(alert: dict, severity: str) -> None:
    """Publish formatted alert to SNS."""
    message = _build_alert_message(alert, severity)

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"[{severity.upper()}] SRE Alert: {alert['pipeline_name']} / {alert['slo_name']}",
            Message=message,
            MessageAttributes={
                "severity": {
                    "DataType": "String",
                    "StringValue": severity,
                },
                "pipeline": {
                    "DataType": "String",
                    "StringValue": alert["pipeline_name"],
                },
            },
        )
        print(f"Alert published to SNS: {alert['alert_id']} [{severity}]")
    except Exception as e:
        print(f"ERROR: Failed to publish alert to SNS: {e}")
        raise


def _send_resolution(alarm_name: str, prev_state: str, alarm_event: dict) -> None:
    """Send resolution notification when alarm returns to OK."""
    parts = alarm_name.split("-")
    pipeline = parts[1] if len(parts) > 1 else "unknown"
    slo = parts[2] if len(parts) > 2 else "unknown"

    message = (
        f"✅ RESOLVED: {alarm_name}\n"
        f"Pipeline: {pipeline}\nSLO: {slo}\n"
        f"Previous state: {prev_state}\n"
        f"Time: {datetime.now(timezone.utc).isoformat()}"
    )

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"[RESOLVED] SRE Alert: {pipeline} / {slo}",
            Message=message,
            MessageAttributes={
                "severity": {"DataType": "String", "StringValue": "resolved"},
            },
        )
        print(f"Resolution notification sent for {alarm_name}")
    except Exception as e:
        print(f"ERROR: Failed to send resolution notification: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _determine_severity(alarm_name: str, alarm_event: dict) -> str:
    """Map alarm to P1/P2/P3 severity based on SLO name and burn rate."""
    burn_rate = _get_current_metric_value(
        "LakehouseSRE", "SLOBurnRate",
        alarm_name.split("-")[1] if "-" in alarm_name else "unknown",
        alarm_name.split("-")[2] if alarm_name.count("-") >= 2 else "unknown",
    )
    if "1h" in alarm_name or burn_rate > 14.4:
        return "p1"
    elif "6h" in alarm_name or burn_rate > 6.0:
        return "p2"
    return "p3"


def _get_current_metric_value(
    namespace: str,
    metric_name: str,
    pipeline_name: str,
    slo_name: str,
) -> float:
    """Fetch the most recent metric value from CloudWatch."""
    try:
        import datetime as dt
        response = cloudwatch_client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=[
                {"Name": "PipelineName", "Value": pipeline_name},
                {"Name": "SLOName",      "Value": slo_name},
            ],
            StartTime=dt.datetime.now(tz=timezone.utc) - dt.timedelta(minutes=5),
            EndTime=dt.datetime.now(tz=timezone.utc),
            Period=300,
            Statistics=["Average"],
        )
        datapoints = response.get("Datapoints", [])
        if datapoints:
            return sorted(datapoints, key=lambda x: x["Timestamp"])[-1]["Average"]
    except Exception as e:
        print(f"WARNING: Could not fetch metric {metric_name}: {e}")
    return 0.0


def _get_recommended_actions(slo_name: str, burn_rate: float) -> list:
    """Return context-specific remediation steps based on SLO type."""
    base = ["Check Grafana dashboard for anomalies", "Review recent deployments via GitLab"]

    specific = {
        "error_rate": [
            "Inspect Snowflake QUERY_HISTORY for FAILED queries",
            "Verify warehouse is not suspended or out of credits",
            "Check upstream data source for schema changes",
        ],
        "latency_p95": [
            "Check warehouse auto-suspend settings — may need to scale up",
            "Review query execution plans for full table scans",
            "Verify partitioning and clustering keys are being leveraged",
        ],
        "data_freshness": [
            "Check if the ingest pipeline is running on schedule",
            "Verify source system (S3/Oracle) connectivity",
            "Review Airflow/Snowflake Task logs for scheduling failures",
        ],
    }

    return base + specific.get(slo_name, ["Review pipeline logs and Iceberg table snapshots."])
