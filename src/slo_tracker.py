"""
slo_tracker.py
--------------
Computes SLI measurements and SLO burn rates for each monitored pipeline.
Uses the multi-window burn rate model (short + long window) to detect
both fast burns (immediate outage) and slow burns (gradual degradation).

SLOs Tracked:
  1. Error Rate      — < 1% failed pipeline runs over 1-hour window
  2. Pipeline Latency — p95 execution time < 5 minutes
  3. Data Freshness  — target table updated within last 30 minutes

Burn Rate Model:
  - Alert if 1h burn rate > 14.4x   (consumes 2% budget in 1 hour)
  - Alert if 6h burn rate > 6x      (consumes 5% budget in 6 hours)
  - These thresholds preserve 30-day SLO error budget
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
import time
from loguru import logger


# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SLIReading:
    pipeline_name: str
    slo_name: str
    measured_value: float       # e.g. 0.005 for 0.5% error rate
    threshold: float            # e.g. 0.01 for 1% SLO target
    is_good_event: bool         # True if this reading is within SLO

@dataclass
class BurnRateWindow:
    window_label: str           # "1h" or "6h"
    window_seconds: int
    burn_rate: float
    alert_threshold: float      # 14.4 for 1h, 6.0 for 6h
    is_alerting: bool

@dataclass
class SLOStatus:
    pipeline_name: str
    slo_name: str
    compliance_ratio: float     # 0.0 to 1.0 (1.0 = fully compliant)
    budget_remaining: float     # fraction of error budget remaining
    is_breached: bool
    burn_rate: float            # current short-window burn rate
    windows: List[BurnRateWindow] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["windows"] = [asdict(w) for w in self.windows]
        return d


# ─────────────────────────────────────────────────────────────────────────────
# SLO Tracker
# ─────────────────────────────────────────────────────────────────────────────

class SLOTracker:
    """
    Maintains a rolling history of SLI readings and computes real-time
    burn rates and compliance ratios for each pipeline SLO.
    """

    # 30-day SLO error budget thresholds for burn rate alerts
    BURN_RATE_WINDOWS = [
        {"label": "1h",  "seconds": 3600,  "threshold": 14.4},
        {"label": "6h",  "seconds": 21600, "threshold": 6.0},
    ]

    def __init__(self, config: dict):
        self.slo_config = config["slo"]
        self.pipelines = config["monitoring"]["pipelines_to_monitor"]

        # In-memory ring buffer: {pipeline: {slo_name: [(timestamp, is_good)]}}
        self._readings: Dict[str, Dict[str, List]] = {}
        self._retention_seconds = 7 * 24 * 3600  # keep 7 days of readings

    # ── Record SLI Reading ────────────────────────────────────────────────────

    def record_error_rate(self, pipeline: str, error_rate: float) -> SLIReading:
        """Record an error rate SLI reading."""
        threshold = self.slo_config["error_rate_threshold_pct"] / 100.0
        is_good = error_rate <= threshold

        reading = SLIReading(
            pipeline_name=pipeline,
            slo_name="error_rate",
            measured_value=error_rate,
            threshold=threshold,
            is_good_event=is_good,
        )
        self._store(pipeline, "error_rate", is_good)

        if not is_good:
            logger.warning(
                f"SLO breach: {pipeline} error rate {error_rate:.2%} "
                f"exceeds {threshold:.2%} threshold"
            )
        return reading

    def record_pipeline_latency(self, pipeline: str, p95_seconds: float) -> SLIReading:
        """Record a latency SLI reading (p95)."""
        threshold = self.slo_config["pipeline_latency_p95_seconds"]
        is_good = p95_seconds <= threshold

        reading = SLIReading(
            pipeline_name=pipeline,
            slo_name="latency_p95",
            measured_value=p95_seconds,
            threshold=threshold,
            is_good_event=is_good,
        )
        self._store(pipeline, "latency_p95", is_good)

        if not is_good:
            logger.warning(
                f"SLO breach: {pipeline} p95 latency {p95_seconds:.0f}s "
                f"exceeds {threshold:.0f}s threshold"
            )
        return reading

    def record_data_freshness(self, pipeline: str, staleness_minutes: float) -> SLIReading:
        """Record a data freshness SLI reading."""
        threshold = self.slo_config["data_freshness_max_minutes"]
        is_good = staleness_minutes <= threshold

        reading = SLIReading(
            pipeline_name=pipeline,
            slo_name="data_freshness",
            measured_value=staleness_minutes,
            threshold=float(threshold),
            is_good_event=is_good,
        )
        self._store(pipeline, "data_freshness", is_good)

        if not is_good:
            logger.warning(
                f"Freshness SLO breach: {pipeline} is {staleness_minutes:.0f}m stale "
                f"(max: {threshold}m)"
            )
        return reading

    # ── Burn Rate Computation ─────────────────────────────────────────────────

    def compute_burn_rate(self, pipeline: str, slo_name: str) -> SLOStatus:
        """
        Compute multi-window burn rate for a specific pipeline + SLO.

        Burn rate = (bad_events / total_events) / slo_error_rate_target
        A burn rate of 1.0 means you're consuming the error budget at exactly
        the rate that would exhaust it in 30 days.
        """
        slo_target_error_rate = self._get_slo_error_rate(slo_name)
        now = time.time()

        window_results = []
        current_burn_rate = 0.0

        for w in self.BURN_RATE_WINDOWS:
            window_seconds = w["seconds"]
            threshold = w["threshold"]

            readings_in_window = self._get_readings_in_window(
                pipeline, slo_name, now - window_seconds
            )

            if not readings_in_window:
                window_results.append(BurnRateWindow(
                    window_label=w["label"],
                    window_seconds=window_seconds,
                    burn_rate=0.0,
                    alert_threshold=threshold,
                    is_alerting=False,
                ))
                continue

            total = len(readings_in_window)
            bad = sum(1 for (_, is_good) in readings_in_window if not is_good)
            observed_error_rate = bad / total if total > 0 else 0.0

            burn_rate = (
                observed_error_rate / slo_target_error_rate
                if slo_target_error_rate > 0
                else 0.0
            )

            is_alerting = burn_rate > threshold

            if is_alerting:
                logger.error(
                    f"BURN RATE ALERT [{w['label']}]: {pipeline}/{slo_name} "
                    f"burn_rate={burn_rate:.1f}x (threshold={threshold}x)"
                )

            window_results.append(BurnRateWindow(
                window_label=w["label"],
                window_seconds=window_seconds,
                burn_rate=round(burn_rate, 3),
                alert_threshold=threshold,
                is_alerting=is_alerting,
            ))

            if w["label"] == "1h":
                current_burn_rate = burn_rate

        # Compliance ratio: fraction of good events in 1h window
        readings_1h = self._get_readings_in_window(pipeline, slo_name, now - 3600)
        if readings_1h:
            good = sum(1 for (_, is_good) in readings_1h if is_good)
            compliance = good / len(readings_1h)
        else:
            compliance = 1.0

        # Error budget remaining (approximation over 30-day window)
        budget_remaining = max(0.0, 1.0 - (current_burn_rate / 30.0))

        is_breached = any(w.is_alerting for w in window_results)

        return SLOStatus(
            pipeline_name=pipeline,
            slo_name=slo_name,
            compliance_ratio=round(compliance, 4),
            budget_remaining=round(budget_remaining, 4),
            is_breached=is_breached,
            burn_rate=round(current_burn_rate, 3),
            windows=window_results,
        )

    def compute_all_burn_rates(self) -> List[SLOStatus]:
        """Compute burn rates for all pipelines and SLOs."""
        statuses = []
        slo_names = ["error_rate", "latency_p95", "data_freshness"]

        for pipeline in self.pipelines:
            for slo_name in slo_names:
                status = self.compute_burn_rate(pipeline, slo_name)
                statuses.append(status)

        breached = [s for s in statuses if s.is_breached]
        if breached:
            logger.error(
                f"{len(breached)} SLO burn rate alert(s) firing: "
                + ", ".join(f"{s.pipeline_name}/{s.slo_name}" for s in breached)
            )
        else:
            logger.info(f"All {len(statuses)} SLOs within burn rate budget.")

        return statuses

    # ── Internal Helpers ──────────────────────────────────────────────────────

    def _store(self, pipeline: str, slo_name: str, is_good: bool) -> None:
        if pipeline not in self._readings:
            self._readings[pipeline] = {}
        if slo_name not in self._readings[pipeline]:
            self._readings[pipeline][slo_name] = []

        self._readings[pipeline][slo_name].append((time.time(), is_good))
        self._evict_old_readings(pipeline, slo_name)

    def _evict_old_readings(self, pipeline: str, slo_name: str) -> None:
        cutoff = time.time() - self._retention_seconds
        self._readings[pipeline][slo_name] = [
            (ts, is_good)
            for (ts, is_good) in self._readings[pipeline][slo_name]
            if ts > cutoff
        ]

    def _get_readings_in_window(
        self, pipeline: str, slo_name: str, since_ts: float
    ) -> List:
        return [
            (ts, is_good)
            for (ts, is_good)
            in self._readings.get(pipeline, {}).get(slo_name, [])
            if ts >= since_ts
        ]

    def _get_slo_error_rate(self, slo_name: str) -> float:
        """Return the SLO error rate target as a fraction (0-1) for burn rate math."""
        mapping = {
            "error_rate":     self.slo_config["error_rate_threshold_pct"] / 100.0,
            "latency_p95":    0.05,  # 5% of requests may exceed latency SLO
            "data_freshness": 0.05,  # 5% of checks may be stale
        }
        return mapping.get(slo_name, 0.01)
