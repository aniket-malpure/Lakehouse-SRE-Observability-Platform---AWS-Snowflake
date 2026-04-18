# Lakehouse SRE Observability Platform — AWS + Snowflake

## Architecture Overview

```
Snowflake Pipelines
        │
        ▼
OpenTelemetry SDK  ──────────────────────────────────►  AWS CloudWatch
(instruments pipeline code)                              (metrics + alarms)
        │                                                        │
        ▼                                                        ▼
Prometheus Exporter                                      AWS Lambda
(custom /metrics endpoint)                          (alarm → SNS alerting)
        │
        ▼
   Prometheus
(scrape + store metrics)
        │
        ▼
   Grafana Dashboards
  (PromQL visualizations)
        │
        ▼
Apache Iceberg on S3
(data lake with ACID, time-travel, schema evolution)
```

## Components

| Component | Purpose | Tech |
|---|---|---|
| `metrics_collector.py` | Scrapes Snowflake query/pipeline metrics | Python, snowflake-connector, prometheus_client |
| `otel_instrumentor.py` | Instruments pipeline code with OpenTelemetry spans | opentelemetry-sdk |
| `iceberg_writer.py` | Writes pipeline output to Iceberg tables on S3 | pyiceberg, boto3 |
| `pipeline_monitor.py` | Orchestrates full monitoring loop | Python |
| `lambda/alerting.py` | CloudWatch alarm → SNS notification handler | AWS Lambda, boto3 |
| `infrastructure/cloudwatch_setup.py` | Provisions CloudWatch alarms via boto3 | boto3 |
| `prometheus/prometheus.yml` | Prometheus scrape config | YAML |
| `grafana/dashboard.json` | Pre-built Grafana dashboard with SLI/SLO panels | JSON |
| `docker-compose.yml` | Local dev stack | Docker |

## SLIs / SLOs Tracked

- **Pipeline Latency SLO**: p95 < 5 minutes (alert at > 4 min)
- **Data Freshness SLO**: Latest partition age < 30 minutes
- **Error Rate SLO**: Failed pipelines < 1% per hour
- **Snowflake Credit SLI**: Credits consumed per pipeline run
- **Row Count Anomaly**: ±20% deviation from 7-day rolling average

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variables
cp config/.env.example config/.env
# Fill in Snowflake credentials, AWS keys, SNS ARN

# 3. Start local observability stack
docker-compose up -d

# 4. Run pipeline monitor
python src/pipeline_monitor.py

# 5. Access Grafana
open http://localhost:3000  # admin / admin
```

## Project Structure

```
lakehouse-sre-observability/
├── src/
│   ├── metrics_collector.py     # Snowflake → Prometheus metrics
│   ├── otel_instrumentor.py     # OpenTelemetry tracing
│   ├── iceberg_writer.py        # Iceberg table writes on S3
│   ├── pipeline_monitor.py      # Main orchestrator
│   └── slo_tracker.py           # SLI/SLO computation + burn rate
├── lambda/
│   └── alerting.py              # CloudWatch alarm handler
├── infrastructure/
│   └── cloudwatch_setup.py      # CloudWatch alarms provisioning
├── prometheus/
│   └── prometheus.yml           # Scrape config
├── grafana/
│   └── dashboard.json           # Pre-built dashboard
├── config/
│   ├── config.yaml              # App config
│   └── .env.example             # Environment template
├── tests/
│   ├── test_metrics_collector.py
│   └── test_slo_tracker.py
├── docker-compose.yml
└── requirements.txt
```
