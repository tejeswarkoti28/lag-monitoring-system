"""
Kafka Consumer Lag Monitor — MVP Demo
=====================================
Single-file FastAPI backend that simulates Kafka consumer-group lag for the
Walmart Canada catalog/PNO topic set, detects threshold breaches, dedupes
alerts, posts to Slack, and exposes a small JSON API for the dashboard.

Run:    python app.py
        # then open http://localhost:8000

Production migration: replace `DataSource.poll_all()` with a real Lenses /
Prometheus query. Nothing else needs to change.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import sqlite3
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Load environment from a .env file in the project root, if present.
# This lets the operator drop SLACK_WEBHOOK_PNO_TEAM / _CATALOG_TEAM /
# _SHIPPING_TEAM (and the SLACK_WEBHOOK_URL fallback) into a `.env` next
# to app.py without exporting them in the shell. Done BEFORE any
# os.environ reads below.
try:
    from dotenv import load_dotenv
    load_dotenv(
        dotenv_path=os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ".env"
        ),
        override=False,
    )
except ImportError:
    # python-dotenv is optional — the app still runs if env vars are
    # exported the old-fashioned way.
    pass

# =============================================================================
# Configuration
# =============================================================================
# All runtime configuration — threshold, polling cadence, the catalog of
# Walmart Canada jobs we monitor, and the Slack webhook routing table.
# This is the file you edit (along with the DataSource class below) when
# pointing the system at production data.

THRESHOLD_MESSAGES: int = 4_000_000          # 4M message lag = breach
POLL_INTERVAL_SECONDS: float = 5.0           # matches the manual sweep cadence
RE_ALERT_INTERVAL_SECONDS: int = 30 * 60     # (kept for reference; engine uses edge-trigger, no periodic re-alerts)
HISTORY_RETENTION_MINUTES: int = 60          # how much in-memory history we keep per job
WARMUP_MINUTES: int = 30                     # pre-seed history so the dashboard isn't empty
ENVIRONMENTS: list[str] = ["eus", "scus"]
DB_PATH: str = os.environ.get("LAG_MONITOR_DB", "lag_monitor.db")

# --- Job catalog -------------------------------------------------------------
# These are the real Walmart Canada topics + consumer groups from the manual
# Excel sheet. In production: this list is the same; only the DataSource
# implementation changes.
JOB_CATALOG: list[dict] = [
    {
        "topic": "canada-pno-offeringestion-events",
        "consumer_group": "ca-priceoffer-3P-offer-ingestion-job-4",
        "team": "PNO Team",
        "channel": "#pno-team",
    },
    {
        "topic": "canada-pno-offerranked-events",
        "consumer_group": "ca-priceoffer-clearcache-5",
        "team": "PNO Team",
        "channel": "#pno-team",
    },
    {
        "topic": "ca-price-offer-unifiedrollup-offer-events",
        "consumer_group": "ca-priceoffer-rollup",
        "team": "PNO Team",
        "channel": "#pno-team",
    },
    {
        "topic": "ca-price-offer-unifiedrollup-invent",
        "consumer_group": "ca-priceoffer-rollup-inventory",
        "team": "PNO Team",
        "channel": "#pno-team",
    },
    {
        "topic": "canada-pno-shipping-region",
        "consumer_group": "ca-priceoffer-shipping-trigger-job-10",
        "team": "Shipping Team",
        "channel": "#shipping-team",
    },
    {
        "topic": "canada-catalog-sku-index-events",
        "consumer_group": "ca-catalog-product-ingestion-prod",
        "team": "Catalog Team",
        "channel": "#catalog-team",
    },
    {
        "topic": "canada-catalog-sku-events",
        "consumer_group": "ca-catalog-sku-stager-21-prod",
        "team": "Catalog Team",
        "channel": "#catalog-team",
    },
    {
        "topic": "canada-catalog-delta-feed-products",
        "consumer_group": "ca-usp-catalog-adapter-prod-g1",
        "team": "Catalog Team",
        "channel": "#catalog-team",
    },
    {
        "topic": "canada-pno-shippingPriceCalculation",
        "consumer_group": "ca-priceoffer-shippingPrice-calculation",
        "team": "Shipping Team",
        "channel": "#shipping-team",
    },
]

# Pre-designated breach jobs so the demo lights up immediately on startup.
# job_id format: "<topic>::<env>"
PRESEEDED_BREACHES: set[str] = {
    "canada-catalog-sku-events::scus",
    "ca-price-offer-unifiedrollup-invent::eus",
}

# --- Slack routing -----------------------------------------------------------
# Per-team webhook env vars. If a per-team var isn't set, fall back to
# SLACK_WEBHOOK_URL. If that isn't set either, alerts are in-app only.
SLACK_TEAM_ENV_VARS: dict[str, str] = {
    "PNO Team": "SLACK_WEBHOOK_PNO_TEAM",
    "Catalog Team": "SLACK_WEBHOOK_CATALOG_TEAM",
    "Shipping Team": "SLACK_WEBHOOK_SHIPPING_TEAM",
}


def slack_webhook_for(team: str) -> Optional[str]:
    """Resolve the Slack webhook URL for a team, falling back to the default."""
    env_var = SLACK_TEAM_ENV_VARS.get(team)
    if env_var:
        url = os.environ.get(env_var)
        if url:
            return url
    return os.environ.get("SLACK_WEBHOOK_URL") or None


# Per-team on-call mention strings. The value should be a Slack mention
# token — typically a user-group mention like `<!subteam^S0123ABCD|@pno-oncall>`,
# or a plain channel mention like `<!channel>` / `<!here>`. If unset, the
# alert falls back to `<!channel>` so the whole team channel is paged.
SLACK_ONCALL_ENV_VARS: dict[str, str] = {
    "PNO Team": "SLACK_ONCALL_PNO",
    "Catalog Team": "SLACK_ONCALL_CATALOG",
    "Shipping Team": "SLACK_ONCALL_SHIPPING",
}


def slack_oncall_tag(team: str) -> str:
    """Resolve the on-call mention tag for a team. Defaults to <!channel>."""
    env_var = SLACK_ONCALL_ENV_VARS.get(team)
    if env_var:
        v = os.environ.get(env_var)
        if v:
            return v.strip()
    return "<!channel>"


def slack_configured() -> bool:
    """True if at least one webhook is configured."""
    if os.environ.get("SLACK_WEBHOOK_URL"):
        return True
    return any(os.environ.get(v) for v in SLACK_TEAM_ENV_VARS.values())


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.replace(microsecond=0).isoformat()


# IST = UTC + 05:30 (no DST). All Slack messages and dashboard time labels
# render in IST primarily because that is what the operator reads at a glance,
# while the underlying alert log keeps a UTC ISO-8601 string for portability.
from datetime import timedelta as _td
IST = timezone(_td(hours=5, minutes=30), name="IST")


def to_ist(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(IST)


def ist_clock(ts: datetime) -> str:
    """HH:MM IST."""
    return to_ist(ts).strftime("%H:%M")


def ist_full(ts: datetime) -> str:
    """YYYY-MM-DD HH:MM:SS IST."""
    return to_ist(ts).strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# Data Source — REPLACE THIS CLASS FOR PRODUCTION
# =============================================================================
# The single boundary between simulated demo data and real Kafka/Lenses data.
# In production:
#   - Replace `poll_all()` with a Prometheus query against the Lenses metrics
#     endpoint (or a direct AdminClient call against the Kafka cluster).
#   - The return contract — list[LagReading] — does NOT change.
#   - The rest of the system (alert engine, DB, UI, Slack routing) is reused
#     as-is.
#
# The simulator gives each job a stable "personality" seeded once on startup
# so behavior is consistent across polls. Two pre-designated jobs sit in
# breach territory so the demo shows alerts immediately.

@dataclass
class LagReading:
    """A single point-in-time lag observation for one job."""
    job_id: str                      # "<topic>::<env>"
    topic: str
    consumer_group: str
    environment: str                 # "eus" | "scus"
    team: str
    channel: str
    consumer_group_lag: int          # the "Consumer Group Lag" graph
    topic_lag: int                   # the "Consumer Group / Topic Lag" graph
    timestamp: datetime

    @property
    def lag(self) -> int:
        """Effective lag = max of the two graphs (matches manual workflow)."""
        return max(self.consumer_group_lag, self.topic_lag)


@dataclass
class _JobPersonality:
    """Seeded once at startup, controls how a job's lag drifts over time."""
    baseline: float            # mean lag in messages
    amplitude: float           # sinusoidal swing
    period_seconds: float      # how fast it oscillates
    noise: float               # random jitter scale
    drift_into_breach: bool    # if True, biased above threshold
    phase: float               # sin phase offset


class DataSource:
    """
    Simulated Kafka lag source.

    Production replacement:
        class DataSource:
            def __init__(self, prom_client): self._prom = prom_client
            def poll_all(self) -> list[LagReading]:
                # query Prometheus / Lenses for kafka_consumergroup_lag
                # for every (topic, consumer_group, env) triplet in JOB_CATALOG
                ...
    """

    def __init__(self) -> None:
        self._rng = random.Random(42)            # deterministic personalities
        self._jobs: list[dict] = []              # flattened list of all 18 jobs
        self._personalities: dict[str, _JobPersonality] = {}
        self._injections: dict[str, float] = {}  # job_id -> unix_ts when injection ends
        self._start_ts: float = time.time()
        self._build_jobs()

    # ---- internals ----------------------------------------------------------
    def _build_jobs(self) -> None:
        for entry in JOB_CATALOG:
            for env in ENVIRONMENTS:
                job_id = f"{entry['topic']}::{env}"
                job = {
                    "job_id": job_id,
                    "topic": entry["topic"],
                    "consumer_group": entry["consumer_group"],
                    "environment": env,
                    "team": entry["team"],
                    "channel": entry["channel"],
                }
                self._jobs.append(job)
                drift = job_id in PRESEEDED_BREACHES
                self._personalities[job_id] = _JobPersonality(
                    baseline=(
                        THRESHOLD_MESSAGES * 1.35 if drift
                        else self._rng.uniform(150_000, 1_800_000)
                    ),
                    amplitude=(
                        self._rng.uniform(800_000, 1_500_000) if drift
                        else self._rng.uniform(80_000, 600_000)
                    ),
                    period_seconds=self._rng.uniform(120, 420),
                    noise=self._rng.uniform(40_000, 180_000),
                    drift_into_breach=drift,
                    phase=self._rng.uniform(0, math.tau),
                )

    def jobs(self) -> list[dict]:
        return list(self._jobs)

    # ---- public API ---------------------------------------------------------
    def inject_spike(self, job_id: str, duration_seconds: int = 120) -> bool:
        """Force a job above threshold for `duration_seconds`. Demo control panel."""
        if job_id not in self._personalities:
            return False
        self._injections[job_id] = time.time() + max(5, duration_seconds)
        return True

    def clear_injection(self, job_id: str) -> bool:
        return self._injections.pop(job_id, None) is not None

    def is_injecting(self, job_id: str) -> bool:
        end = self._injections.get(job_id)
        if end is None:
            return False
        if time.time() >= end:
            self._injections.pop(job_id, None)
            return False
        return True

    def poll_all(self, *, at: Optional[float] = None) -> list[LagReading]:
        """Return the current lag reading for every job. Called every 5s."""
        ts = at if at is not None else time.time()
        when = datetime.fromtimestamp(ts, tz=timezone.utc)
        readings: list[LagReading] = []
        for job in self._jobs:
            cg_lag, t_lag = self._compute_lag(job["job_id"], ts)
            readings.append(
                LagReading(
                    job_id=job["job_id"],
                    topic=job["topic"],
                    consumer_group=job["consumer_group"],
                    environment=job["environment"],
                    team=job["team"],
                    channel=job["channel"],
                    consumer_group_lag=cg_lag,
                    topic_lag=t_lag,
                    timestamp=when,
                )
            )
        return readings

    # ---- math ---------------------------------------------------------------
    # Composite lag model — built up from multiple layers so the trace
    # looks like real Kafka ops data:
    #
    #   1. Short oscillation (2-7 min) — normal producer/consumer ebb
    #   2. Daily / weekly / monthly seasonality — traffic patterns
    #   3. Sparse "incidents" — occasional multi-hour elevated periods
    #   4. Producer bursts — sudden short-lived spikes (~minutes)
    #   5. Consumer rebalances — lag drops to ~0 then ramps back
    #   6. Step-shift / capacity changes — long-lived level changes
    #   7. Per-second jitter — measurement noise, occasionally bursting
    #
    # Each layer is deterministic in `(job_id, ts)` so the chart is
    # reproducible across reloads. Production replacement: drop all of
    # this and have poll_all() return real readings.
    def _compute_lag(self, job_id: str, ts: float) -> tuple[int, int]:
        p = self._personalities[job_id]
        elapsed = ts - self._start_ts

        # ---- 1. short oscillation -----------------------------------------
        wave = math.sin((elapsed / p.period_seconds) * math.tau + p.phase)

        # ---- 2. seasonality (day / week / month) --------------------------
        daily   = math.sin((elapsed / 86400.0) * math.tau + p.phase)
        weekly  = math.sin((elapsed / (86400.0 * 7)) * math.tau + p.phase * 0.7)
        monthly = math.sin((elapsed / (86400.0 * 30)) * math.tau + p.phase * 0.3)
        long_term = (
            daily   * p.baseline * 0.18 +
            weekly  * p.baseline * 0.10 +
            monthly * p.baseline * 0.06
        )

        # ---- 3. sparse multi-hour incidents -------------------------------
        incident = 0.0
        ibucket = int(ts // (3600 * 18))
        irng = random.Random(hash((job_id, "incident", ibucket)) & 0xFFFFFFFF)
        if p.drift_into_breach and irng.random() < 0.55:
            within = (ts - ibucket * 3600 * 18) / (3600 * 18)
            shape = math.sin(within * math.pi)
            incident = shape * THRESHOLD_MESSAGES * irng.uniform(0.4, 0.9)
        elif (not p.drift_into_breach) and irng.random() < 0.04:
            within = (ts - ibucket * 3600 * 18) / (3600 * 18)
            shape = math.sin(within * math.pi) ** 2
            incident = shape * THRESHOLD_MESSAGES * irng.uniform(0.5, 1.3)

        # ---- 4. producer bursts (3-12 min spikes) -------------------------
        burst = 0.0
        bbucket = int(ts // (60 * 17))
        brng = random.Random(hash((job_id, "burst", bbucket)) & 0xFFFFFFFF)
        if brng.random() < (0.35 if p.drift_into_breach else 0.15):
            burst_dur = brng.uniform(180, 720)
            burst_start = bbucket * 60 * 17 + brng.uniform(0, 60 * 17 - burst_dur)
            offset = ts - burst_start
            if 0 <= offset <= burst_dur:
                # log-normal-ish bell curve
                k = offset / burst_dur
                shape = (4 * k * (1 - k)) ** 1.4   # taller, narrower bell
                mag = brng.uniform(0.25, 1.1)
                if p.drift_into_breach:
                    mag *= 1.4
                burst = shape * THRESHOLD_MESSAGES * mag

        # ---- 5. consumer rebalance (sudden drop to near-zero, then ramp) -
        rebalance_factor = 1.0
        rbucket = int(ts // (3600 * 4))   # rebalance opportunities every 4h
        rrng = random.Random(hash((job_id, "reb", rbucket)) & 0xFFFFFFFF)
        if rrng.random() < 0.18:
            reb_at = rbucket * 3600 * 4 + rrng.uniform(0, 3600 * 4)
            ramp_dur = rrng.uniform(120, 540)
            offset = ts - reb_at
            if 0 <= offset <= ramp_dur:
                # smooth catch-up ramp from 0 back to 1
                k = offset / ramp_dur
                rebalance_factor = max(0.0, k * k * (3 - 2 * k))   # smoothstep
                # tiny residual so we don't divide by zero in noise scaling
                rebalance_factor = max(0.05, rebalance_factor)

        # ---- 6. step shifts / capacity changes ----------------------------
        # Discrete level changes that persist across multi-day blocks. Use
        # a deterministic per-week random multiplier in [0.7, 1.35].
        sbucket = int(ts // (86400 * 5))
        srng = random.Random(hash((job_id, "step", sbucket)) & 0xFFFFFFFF)
        step_mult = srng.uniform(0.78, 1.30)

        # ---- 7. jitter (per-3s, occasionally a high-noise pocket) --------
        seed = hash((job_id, int(ts // 3))) & 0xFFFFFFFF
        rng = random.Random(seed)
        # noisy pockets — every ~7 min, 30% chance of 2x noise for a few min
        nbucket = int(ts // (60 * 7))
        nrng = random.Random(hash((job_id, "noise", nbucket)) & 0xFFFFFFFF)
        noise_mult = 2.4 if nrng.random() < 0.30 else 1.0
        jitter = rng.gauss(0, p.noise * noise_mult)

        base = (
            p.baseline * step_mult
            + wave * p.amplitude
            + long_term
            + incident
            + burst
        )
        cg_lag = max(0, int((base + jitter) * rebalance_factor))

        # Topic-lag graph — correlated but not identical (different consumer
        # offset accounting can lag the CG metric by a fraction).
        topic_offset = rng.gauss(0, p.noise * noise_mult * 0.6) + 0.15 * wave * p.amplitude
        t_base = (
            p.baseline * step_mult
            + 0.92 * wave * p.amplitude
            + long_term * 0.95
            + incident * 0.92
            + burst * 0.88
        )
        t_lag = max(0, int((t_base + topic_offset) * rebalance_factor))

        # Override with active injection: push well above threshold.
        if self.is_injecting(job_id):
            spike = int(THRESHOLD_MESSAGES * 1.5 + rng.uniform(0, 800_000))
            return spike, max(0, int(spike * 0.95))

        return cg_lag, t_lag

    # ---- synthesized history ------------------------------------------------
    # Used when the dashboard requests a longer window than we have buffered
    # in memory (anything past the 60-min retention). The personality is
    # deterministic, so we can compute lag at any past timestamp.
    def synthesize_history(
        self,
        job_id: str,
        *,
        start_ts: float,
        end_ts: float,
        step_seconds: float,
    ) -> list[dict]:
        if job_id not in self._personalities:
            return []
        out: list[dict] = []
        n = max(1, int((end_ts - start_ts) / step_seconds))
        for i in range(n + 1):
            ts = start_ts + i * step_seconds
            if ts > end_ts:
                break
            cg, tp = self._compute_lag(job_id, ts)
            out.append({
                "ts": datetime.fromtimestamp(ts, tz=timezone.utc)
                          .replace(microsecond=0).isoformat(),
                "cg_lag": cg,
                "topic_lag": tp,
                "lag": max(cg, tp),
            })
        return out


# =============================================================================
# Database
# =============================================================================
# SQLite for two purposes:
#   1. Persisted alert log (weekly accountability reporting).
#   2. Per-team breakdown queries (the "turn 'please reduce lag' into an SLA
#      conversation" capability that the manual workflow can't produce).

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    environment TEXT NOT NULL,
    team TEXT NOT NULL,
    channel TEXT NOT NULL,
    alert_type TEXT NOT NULL CHECK(alert_type IN ('breach', 'resolved')),
    lag_value INTEGER NOT NULL,
    threshold INTEGER NOT NULL,
    delivered_to_slack INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_team ON alerts(team, created_at);
"""


class AlertDB:
    def __init__(self, path: str) -> None:
        self.path = path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init(self) -> None:
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    def insert_alert(
        self,
        *,
        job_id: str,
        topic: str,
        consumer_group: str,
        environment: str,
        team: str,
        channel: str,
        alert_type: str,
        lag_value: int,
        delivered_to_slack: bool,
        created_at: datetime,
    ) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO alerts
                   (job_id, topic, consumer_group, environment, team, channel,
                    alert_type, lag_value, threshold, delivered_to_slack, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    job_id, topic, consumer_group, environment, team, channel,
                    alert_type, int(lag_value), THRESHOLD_MESSAGES,
                    1 if delivered_to_slack else 0, iso(created_at),
                ),
            )
            return cur.lastrowid

    def recent_alerts(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def alerts_in_last_hours(self, hours: int) -> list[dict]:
        cutoff = iso(datetime.fromtimestamp(time.time() - hours * 3600, tz=timezone.utc))
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts WHERE created_at >= ? ORDER BY id DESC", (cutoff,)
            ).fetchall()
            return [dict(r) for r in rows]

    def count_in_last_hours(self, hours: int, alert_type: str = "breach") -> int:
        cutoff = iso(datetime.fromtimestamp(time.time() - hours * 3600, tz=timezone.utc))
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM alerts WHERE alert_type=? AND created_at >= ?",
                (alert_type, cutoff),
            ).fetchone()
            return int(row["c"])

    def team_breakdown(self, hours: int) -> list[dict]:
        cutoff = iso(datetime.fromtimestamp(time.time() - hours * 3600, tz=timezone.utc))
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT team,
                          SUM(CASE WHEN alert_type='breach' THEN 1 ELSE 0 END) AS breach_count,
                          SUM(CASE WHEN alert_type='resolved' THEN 1 ELSE 0 END) AS resolved_count
                   FROM alerts
                   WHERE created_at >= ?
                   GROUP BY team
                   ORDER BY breach_count DESC""",
                (cutoff,),
            ).fetchall()
            return [dict(r) for r in rows]


# =============================================================================
# Alert Engine
# =============================================================================
# Dedups alerts so we don't spam:
#   - First breach -> fire alert, mark job as "in breach"
#   - Sustained breach -> re-alert only every RE_ALERT_INTERVAL_SECONDS
#   - Lag drops below threshold -> fire "resolved" alert, clear breach state

@dataclass
class _BreachState:
    last_alerted_at: float          # unix ts of most recent alert send
    first_breached_at: float        # unix ts of when this breach started


@dataclass
class AlertEvent:
    """An alert decision produced by the engine for a single reading."""
    type: str                       # "breach" | "resolved"
    reading: LagReading
    duration_seconds: float = 0.0


class AlertEngine:
    """
    Strict edge-trigger semantics:

      * lag goes from below -> at/above threshold:   fire ONE 'breach' alert
      * lag stays at/above threshold:                 emit nothing (silent)
      * lag goes from at/above -> below threshold:   fire ONE 'resolved' alert
      * lag re-crosses upward later:                  fire a NEW 'breach' alert

    This avoids spamming a team while they are working an incident — they
    get exactly one ping per "incident" plus one 'all clear' when it ends.
    """

    def __init__(self) -> None:
        self._state: dict[str, _BreachState] = {}

    def evaluate(self, reading: LagReading) -> Optional[AlertEvent]:
        ts = reading.timestamp.timestamp()
        in_breach = reading.lag >= THRESHOLD_MESSAGES
        prev = self._state.get(reading.job_id)

        if in_breach:
            if prev is None:
                # NEW breach — fire once, then stay silent for as long as
                # the job remains in breach.
                self._state[reading.job_id] = _BreachState(
                    last_alerted_at=ts, first_breached_at=ts,
                )
                return AlertEvent(type="breach", reading=reading)
            # already in breach — explicitly suppress.
            return None

        # not in breach
        if prev is not None:
            duration = ts - prev.first_breached_at
            del self._state[reading.job_id]
            # next upward crossing will re-arm and fire a fresh breach alert.
            return AlertEvent(type="resolved", reading=reading, duration_seconds=duration)
        return None


# =============================================================================
# Slack Notifier
# =============================================================================

def _fmt_millions(n: int) -> str:
    return f"{n / 1_000_000:.2f}M"


# Human-friendly situation note — gets included in the Slack message in
# addition to the structured fields so the post reads like an SRE wrote it.
_TEAM_DOMAIN: dict[str, str] = {
    "PNO Team": "price-and-offer",
    "Catalog Team": "catalog ingestion",
    "Shipping Team": "shipping",
}
_TEAM_HINT: dict[str, str] = {
    "PNO Team": (
        "Worth peeking at the upstream offer-ingestion rate first — when this "
        "one backs up it usually means a publisher is bursting faster than "
        "the rollup can fan out."
    ),
    "Catalog Team": (
        "First place to look is the SKU-stager throughput. If staging is "
        "healthy and lag is still climbing, the downstream search index "
        "writer is probably the bottleneck."
    ),
    "Shipping Team": (
        "Shipping calculation usually backs up when the region service is "
        "slow to respond. Quick check: shipping-trigger consumer rate vs "
        "producer rate over the last 15 min."
    ),
}


def _human_note(event: "AlertEvent") -> str:
    """Render a friendly, paragraph-style note for the Slack message."""
    r = event.reading
    env_up = r.environment.upper()
    lag_m = r.lag / 1_000_000
    over_pct = int(round((r.lag - THRESHOLD_MESSAGES) / THRESHOLD_MESSAGES * 100))
    domain = _TEAM_DOMAIN.get(r.team, r.team.lower())
    hint = _TEAM_HINT.get(r.team, "")
    minutes = int(event.duration_seconds // 60)

    if event.type == "resolved":
        if minutes >= 1:
            return (
                f"All clear — `{r.topic}` in {env_up} dropped back below the "
                f"4M threshold. Total breach window was about {minutes} minute"
                f"{'s' if minutes != 1 else ''}, current lag is "
                f"{_fmt_millions(r.lag)}. No action needed from {r.team} "
                f"unless this climbs again within the next hour."
            )
        return (
            f"All clear — `{r.topic}` ({env_up}) recovered to "
            f"{_fmt_millions(r.lag)}. Brief blip, no follow-up needed."
        )

    # breach
    if minutes >= 1:
        # sustained / re-alert
        return (
            f"Still in breach: `{r.topic}` in {env_up} has been over the "
            f"4M threshold for ~{minutes} minute"
            f"{'s' if minutes != 1 else ''} now (currently {_fmt_millions(r.lag)}, "
            f"{over_pct:+d}% over). This is a re-alert — if {r.team} is "
            f"already on it, please ignore. {hint}"
        )

    # initial breach
    return (
        f"Heads up {r.team} — `{r.topic}` in {env_up} just crossed the 4M "
        f"lag threshold and is sitting at {_fmt_millions(r.lag)} "
        f"({over_pct:+d}% over). Consumer group is `{r.consumer_group}`. "
        f"This is on the {domain} side, so it's yours. {hint} "
        f"You won't get another ping unless it recovers and re-breaches — so if it's still elevated in 30 minutes, that means the team is on it and we're staying out of the way."
    )


class SlackNotifier:
    """Posts rich attachments to per-team Slack webhooks."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(5.0))

    async def close(self) -> None:
        await self._client.aclose()

    async def send(self, event: AlertEvent) -> bool:
        """Returns True if Slack accepted the post; False if not configured / failed."""
        webhook = slack_webhook_for(event.reading.team)
        if not webhook:
            return False
        payload = self._build_payload(event)
        try:
            r = await self._client.post(webhook, json=payload)
            return 200 <= r.status_code < 300
        except Exception as exc:
            print(f"[slack] post failed: {exc}", file=sys.stderr)
            return False

    @staticmethod
    def _build_payload(event: AlertEvent) -> dict:
        r = event.reading
        is_breach = event.type == "breach"
        color = "#f85149" if is_breach else "#3fb950"
        oncall = slack_oncall_tag(r.team)
        env_up = r.environment.upper()
        over_pct = int(round(
            (r.lag - THRESHOLD_MESSAGES) / THRESHOLD_MESSAGES * 100
        ))

        # Top-level text — visible everywhere (notifications, channel
        # preview, mobile). Drafted to read like a teammate posted it:
        # leads with the on-call tag, then a short hand-written paragraph
        # explaining the situation and asking for help.
        ist_now = ist_clock(r.timestamp)
        if is_breach:
            # Short, drafted message — no long explanation, just enough for
            # the team to know what + where + how-bad.
            heading = (
                f"{oncall} :rotating_light: hey *{r.team}* — lag breach on "
                f"`{r.topic}` ({env_up}) at {ist_now} IST: "
                f"*{_fmt_millions(r.lag)}* ({over_pct:+d}% over the "
                f"{_fmt_millions(THRESHOLD_MESSAGES)} threshold). "
                f"Could someone take a look?"
            )
        else:
            mins = int(event.duration_seconds // 60) if event.duration_seconds else 0
            window = f" Breach lasted ~{mins}m." if mins >= 1 else ""
            heading = (
                f"{oncall} :white_check_mark: *{r.team}* — `{r.topic}` "
                f"({env_up}) recovered to *{_fmt_millions(r.lag)}* at "
                f"{ist_now} IST.{window}"
            )

        title = (
            f":rotating_light: Kafka lag breach — {r.topic}"
            if is_breach
            else f":white_check_mark: Recovered — {r.topic}"
        )
        fields = [
            {"title": "Topic", "value": r.topic, "short": False},
            {"title": "Consumer Group", "value": r.consumer_group, "short": False},
            {"title": "Environment", "value": env_up, "short": True},
            {"title": "Team", "value": r.team, "short": True},
            {"title": "Channel", "value": r.channel, "short": True},
            {"title": "Lag (max of CG / topic graphs)", "value": _fmt_millions(r.lag), "short": True},
            {"title": "Threshold", "value": _fmt_millions(THRESHOLD_MESSAGES), "short": True},
            {"title": "Time", "value": f"{ist_full(r.timestamp)} IST\n{iso(r.timestamp)} UTC", "short": True},
        ]
        if event.duration_seconds and not is_breach:
            mins = int(event.duration_seconds // 60)
            fields.append(
                {"title": "Breach duration", "value": f"{mins} min", "short": True}
            )

        return {
            "text": heading,
            "mrkdwn": True,
            "attachments": [{
                "color": color,
                "title": title,
                "fields": fields,
                "footer": "Kafka Lag Monitor · one ping per breach",
                "ts": int(r.timestamp.timestamp()),
            }],
        }


# =============================================================================
# Monitor Loop
# =============================================================================
# Keeps the most recent readings + per-job in-memory history, and routes
# alert decisions through Slack + the SQLite log.

@dataclass
class JobState:
    job_id: str
    topic: str
    consumer_group: str
    environment: str
    team: str
    channel: str
    history: list[dict] = field(default_factory=list)   # [{ts, cg, topic, lag}]
    current: Optional[LagReading] = None


class Monitor:
    def __init__(
        self,
        source: DataSource,
        engine: AlertEngine,
        notifier: SlackNotifier,
        db: AlertDB,
    ) -> None:
        self.source = source
        self.engine = engine
        self.notifier = notifier
        self.db = db
        self.jobs: dict[str, JobState] = {}
        self.last_poll_ts: Optional[datetime] = None
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        for j in source.jobs():
            self.jobs[j["job_id"]] = JobState(
                job_id=j["job_id"], topic=j["topic"],
                consumer_group=j["consumer_group"], environment=j["environment"],
                team=j["team"], channel=j["channel"],
            )

    # ---- warmup / poll ------------------------------------------------------
    def warmup(self, minutes: int = WARMUP_MINUTES) -> None:
        """Synthesize `minutes` of history at POLL_INTERVAL_SECONDS spacing."""
        now = time.time()
        steps = int((minutes * 60) / POLL_INTERVAL_SECONDS)
        for i in range(steps, 0, -1):
            ts = now - i * POLL_INTERVAL_SECONDS
            for r in self.source.poll_all(at=ts):
                self._record_history(r)

    async def run(self) -> None:
        """Main polling loop. Cancel-safe."""
        try:
            while not self._stopping.is_set():
                await self._poll_once()
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=POLL_INTERVAL_SECONDS)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    async def _poll_once(self) -> None:
        readings = self.source.poll_all()
        for r in readings:
            self._record_history(r)
            event = self.engine.evaluate(r)
            if event is not None:
                await self._handle_event(event)
        self.last_poll_ts = now_utc()

    def _record_history(self, r: LagReading) -> None:
        st = self.jobs.get(r.job_id)
        if st is None:
            return
        st.current = r
        st.history.append({
            "ts": iso(r.timestamp),
            "cg_lag": r.consumer_group_lag,
            "topic_lag": r.topic_lag,
            "lag": r.lag,
        })
        # Trim to retention window
        cutoff = time.time() - HISTORY_RETENTION_MINUTES * 60
        st.history = [
            h for h in st.history
            if datetime.fromisoformat(h["ts"]).timestamp() >= cutoff
        ]

    async def _handle_event(self, event: AlertEvent) -> None:
        delivered = await self.notifier.send(event)
        self.db.insert_alert(
            job_id=event.reading.job_id,
            topic=event.reading.topic,
            consumer_group=event.reading.consumer_group,
            environment=event.reading.environment,
            team=event.reading.team,
            channel=event.reading.channel,
            alert_type=event.type,
            lag_value=event.reading.lag,
            delivered_to_slack=delivered,
            created_at=event.reading.timestamp,
        )

    # ---- lifecycle ----------------------------------------------------------
    def start(self) -> None:
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


# =============================================================================
# FastAPI app
# =============================================================================

# Module-level singletons so endpoints can reach them.
_source = DataSource()
_engine = AlertEngine()
_db = AlertDB(DB_PATH)
_notifier = SlackNotifier()
_monitor = Monitor(_source, _engine, _notifier, _db)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Pre-seed history so the dashboard isn't empty on first load
    _monitor.warmup(minutes=WARMUP_MINUTES)
    # Force the pre-seeded breach jobs into an active alerted state by
    # running one synchronous evaluation BEFORE the loop starts. Their
    # personality keeps them above threshold; this just ensures the alert
    # fires on the first real poll.
    _monitor.start()
    try:
        yield
    finally:
        await _monitor.stop()
        await _notifier.close()


app = FastAPI(title="Kafka Lag Monitor", lifespan=lifespan)


# Static & root --------------------------------------------------------------
_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/")
def root():
    index = os.path.join(_static_dir, "index.html")
    if not os.path.isfile(index):
        return JSONResponse(
            {"error": "static/index.html not found", "static_dir": _static_dir},
            status_code=500,
        )
    return FileResponse(index)


# Health ---------------------------------------------------------------------
@app.get("/api/health")
def health():
    return {
        "ok": True,
        "last_poll_at": iso(_monitor.last_poll_ts) if _monitor.last_poll_ts else None,
        "slack_configured": slack_configured(),
        "threshold": THRESHOLD_MESSAGES,
        "poll_interval_seconds": POLL_INTERVAL_SECONDS,
        "jobs_monitored": len(_monitor.jobs),
    }


# Status ---------------------------------------------------------------------
@app.get("/api/status")
def status():
    items = []
    breaching = 0
    for st in _monitor.jobs.values():
        cur = st.current
        is_breach = bool(cur and cur.lag >= THRESHOLD_MESSAGES)
        if is_breach:
            breaching += 1
        items.append({
            "job_id": st.job_id,
            "topic": st.topic,
            "consumer_group": st.consumer_group,
            "environment": st.environment,
            "team": st.team,
            "channel": st.channel,
            "lag": cur.lag if cur else 0,
            "consumer_group_lag": cur.consumer_group_lag if cur else 0,
            "topic_lag": cur.topic_lag if cur else 0,
            "status": "breach" if is_breach else "ok",
            "injecting": _source.is_injecting(st.job_id),
            "timestamp": iso(cur.timestamp) if cur else None,
            "sparkline": [h["lag"] for h in st.history[-180:]],
        })
    items.sort(key=lambda j: (0 if j["status"] == "breach" else 1, j["topic"], j["environment"]))
    return {
        "jobs": items,
        "summary": {
            "monitored": len(items),
            "breaching": breaching,
            "healthy": len(items) - breaching,
            "alerts_24h": _db.count_in_last_hours(24, "breach"),
            "last_poll_at": iso(_monitor.last_poll_ts) if _monitor.last_poll_ts else None,
            "slack_configured": slack_configured(),
            "threshold": THRESHOLD_MESSAGES,
        },
    }


# Per-job history ------------------------------------------------------------
# Granularity table — keeps the response under ~1000 points regardless of
# window. The dashboard chooses `minutes`; we choose the bucket size.
def _bucket_seconds_for(minutes: int) -> float:
    if minutes <= 60:        return 5.0          # raw 5s polls (~720 pts at 60m)
    if minutes <= 360:       return 30.0         # 6h:    ~720 pts
    if minutes <= 1440:      return 120.0        # 24h:   ~720 pts
    if minutes <= 10_080:    return 900.0        # 7d:    ~672 pts
    if minutes <= 43_200:    return 3_600.0      # 30d:   ~720 pts
    return 4 * 3_600.0                           # 6mo:   ~1080 pts


@app.get("/api/job/{job_id}/history")
def job_history(job_id: str, minutes: int = 30):
    st = _monitor.jobs.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail=f"unknown job_id: {job_id}")
    minutes = max(1, min(minutes, 60 * 24 * 31 * 6))   # cap at ~6 months
    end_ts = time.time()
    start_ts = end_ts - minutes * 60
    step = _bucket_seconds_for(minutes)

    if minutes <= 60:
        # Use the in-memory buffer (raw 5-second polls).
        series = [
            h for h in st.history
            if datetime.fromisoformat(h["ts"]).timestamp() >= start_ts
        ]
    else:
        # Synthesize from the deterministic personality over the requested
        # window. In production this branch would query the historical TSDB
        # (Prometheus / Lenses) at the same cadence.
        series = _source.synthesize_history(
            job_id, start_ts=start_ts, end_ts=end_ts, step_seconds=step,
        )
    return {
        "job_id": job_id,
        "topic": st.topic,
        "consumer_group": st.consumer_group,
        "environment": st.environment,
        "team": st.team,
        "channel": st.channel,
        "threshold": THRESHOLD_MESSAGES,
        "minutes": minutes,
        "step_seconds": step,
        "synthesized": minutes > 60,
        "history": series,
    }


# Alerts ---------------------------------------------------------------------
@app.get("/api/alerts")
def alerts(limit: int = 50):
    return {"alerts": _db.recent_alerts(limit=limit)}


# Team breakdown -------------------------------------------------------------
@app.get("/api/team-breakdown")
def team_breakdown(hours: int = 24):
    breakdown = _db.team_breakdown(hours=hours)
    # Make sure every team appears, even if zero alerts
    teams_seen = {row["team"] for row in breakdown}
    for entry in JOB_CATALOG:
        if entry["team"] not in teams_seen:
            breakdown.append({
                "team": entry["team"],
                "breach_count": 0,
                "resolved_count": 0,
            })
            teams_seen.add(entry["team"])
    breakdown.sort(key=lambda r: -int(r["breach_count"]))
    return {"hours": hours, "breakdown": breakdown}


# Inject / clear -------------------------------------------------------------
@app.post("/api/inject/{job_id}")
def inject(job_id: str, duration: int = 120):
    ok = _source.inject_spike(job_id, duration_seconds=duration)
    if not ok:
        raise HTTPException(status_code=404, detail=f"unknown job_id: {job_id}")
    return {"ok": True, "job_id": job_id, "duration": duration}


@app.post("/api/clear/{job_id}")
def clear(job_id: str):
    cleared = _source.clear_injection(job_id)
    return {"ok": True, "job_id": job_id, "cleared": cleared}


# =============================================================================
# Entrypoint
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("app:app", host=host, port=port, reload=False, log_level="info")
