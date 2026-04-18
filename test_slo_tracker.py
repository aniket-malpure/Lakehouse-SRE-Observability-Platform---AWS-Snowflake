"""
tests/test_slo_tracker.py
-------------------------
Unit tests for SLOTracker burn rate computation and compliance logic.
Run with: pytest tests/test_slo_tracker.py -v
"""

import time
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

from slo_tracker import SLOTracker, SLIReading, SLOStatus


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def base_config():
    return {
        "slo": {
            "pipeline_latency_p95_seconds": 300,
            "pipeline_latency_alert_seconds": 240,
            "data_freshness_max_minutes": 30,
            "error_rate_threshold_pct": 1.0,
            "row_count_anomaly_pct": 20.0,
            "burn_rate_alert_multiplier": 14.4,
        },
        "monitoring": {
            "pipelines_to_monitor": [
                "RAW_INGEST_PIPELINE",
                "CURATED_TRANSFORM_PIPELINE",
            ]
        },
    }


@pytest.fixture
def tracker(base_config):
    return SLOTracker(base_config)


# ─────────────────────────────────────────────────────────────────────────────
# Error Rate SLO
# ─────────────────────────────────────────────────────────────────────────────

class TestErrorRateSLO:

    def test_good_reading_within_threshold(self, tracker):
        reading = tracker.record_error_rate("RAW_INGEST_PIPELINE", 0.005)
        assert reading.is_good_event is True
        assert reading.measured_value == 0.005

    def test_bad_reading_exceeds_threshold(self, tracker):
        reading = tracker.record_error_rate("RAW_INGEST_PIPELINE", 0.02)
        assert reading.is_good_event is False

    def test_exactly_at_threshold_is_good(self, tracker):
        reading = tracker.record_error_rate("RAW_INGEST_PIPELINE", 0.01)
        assert reading.is_good_event is True

    def test_zero_error_rate_is_good(self, tracker):
        reading = tracker.record_error_rate("RAW_INGEST_PIPELINE", 0.0)
        assert reading.is_good_event is True


# ─────────────────────────────────────────────────────────────────────────────
# Latency SLO
# ─────────────────────────────────────────────────────────────────────────────

class TestLatencySLO:

    def test_latency_within_slo(self, tracker):
        reading = tracker.record_pipeline_latency("CURATED_TRANSFORM_PIPELINE", 200.0)
        assert reading.is_good_event is True

    def test_latency_exceeds_slo(self, tracker):
        reading = tracker.record_pipeline_latency("CURATED_TRANSFORM_PIPELINE", 400.0)
        assert reading.is_good_event is False

    def test_latency_exactly_at_slo(self, tracker):
        reading = tracker.record_pipeline_latency("CURATED_TRANSFORM_PIPELINE", 300.0)
        assert reading.is_good_event is True


# ─────────────────────────────────────────────────────────────────────────────
# Data Freshness SLO
# ─────────────────────────────────────────────────────────────────────────────

class TestDataFreshnessSLO:

    def test_fresh_data_within_slo(self, tracker):
        reading = tracker.record_data_freshness("RAW_INGEST_PIPELINE", 10.0)
        assert reading.is_good_event is True

    def test_stale_data_breaches_slo(self, tracker):
        reading = tracker.record_data_freshness("RAW_INGEST_PIPELINE", 45.0)
        assert reading.is_good_event is False


# ─────────────────────────────────────────────────────────────────────────────
# Burn Rate Computation
# ─────────────────────────────────────────────────────────────────────────────

class TestBurnRateComputation:

    def test_zero_burn_rate_with_all_good_readings(self, tracker):
        pipeline = "RAW_INGEST_PIPELINE"
        for _ in range(10):
            tracker.record_error_rate(pipeline, 0.001)

        status = tracker.compute_burn_rate(pipeline, "error_rate")
        assert status.burn_rate == 0.0
        assert status.compliance_ratio == 1.0
        assert status.is_breached is False

    def test_high_burn_rate_triggers_alert(self, tracker):
        pipeline = "RAW_INGEST_PIPELINE"
        # Record many bad events to drive burn rate up
        for _ in range(20):
            tracker.record_error_rate(pipeline, 0.50)  # 50% error rate

        status = tracker.compute_burn_rate(pipeline, "error_rate")
        assert status.burn_rate > 14.4
        assert status.is_breached is True
        assert status.compliance_ratio < 1.0

    def test_compute_all_burn_rates_returns_correct_count(self, tracker):
        statuses = tracker.compute_all_burn_rates()
        # 2 pipelines × 3 SLOs = 6
        assert len(statuses) == 6

    def test_budget_remaining_decreases_with_burn(self, tracker):
        pipeline = "CURATED_TRANSFORM_PIPELINE"
        for _ in range(10):
            tracker.record_error_rate(pipeline, 0.001)  # good
        status_good = tracker.compute_burn_rate(pipeline, "error_rate")

        for _ in range(10):
            tracker.record_error_rate(pipeline, 0.50)   # bad
        status_bad = tracker.compute_burn_rate(pipeline, "error_rate")

        assert status_bad.budget_remaining <= status_good.budget_remaining

    def test_slo_status_has_two_windows(self, tracker):
        pipeline = "RAW_INGEST_PIPELINE"
        tracker.record_error_rate(pipeline, 0.001)
        status = tracker.compute_burn_rate(pipeline, "error_rate")
        assert len(status.windows) == 2
        window_labels = {w.window_label for w in status.windows}
        assert "1h" in window_labels
        assert "6h" in window_labels

    def test_to_dict_is_serializable(self, tracker):
        import json
        pipeline = "RAW_INGEST_PIPELINE"
        tracker.record_error_rate(pipeline, 0.001)
        status = tracker.compute_burn_rate(pipeline, "error_rate")
        d = status.to_dict()
        # Should not raise
        serialized = json.dumps(d)
        assert "pipeline_name" in serialized

    def test_unknown_pipeline_returns_empty_compliance(self, tracker):
        status = tracker.compute_burn_rate("NONEXISTENT_PIPELINE", "error_rate")
        # No readings → default to compliant
        assert status.compliance_ratio == 1.0
        assert status.burn_rate == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Eviction / Retention
# ─────────────────────────────────────────────────────────────────────────────

class TestRetention:

    def test_readings_are_stored(self, tracker):
        tracker.record_error_rate("RAW_INGEST_PIPELINE", 0.001)
        readings = tracker._readings.get("RAW_INGEST_PIPELINE", {}).get("error_rate", [])
        assert len(readings) == 1

    def test_multiple_readings_accumulate(self, tracker):
        for i in range(5):
            tracker.record_error_rate("RAW_INGEST_PIPELINE", 0.001)
        readings = tracker._readings["RAW_INGEST_PIPELINE"]["error_rate"]
        assert len(readings) == 5
