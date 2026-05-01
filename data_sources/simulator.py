"""
Simulated data source — for laptop demos and pre-VDI development.

Generates a realistic 7-layer composite lag signal per job, deterministic
across reloads (seeded once at startup). Two streams (consumer_group_lag
and topic_lag) are simulated semi-independently so the modal's two graphs
diverge minute-to-minute the way real Kafka metrics do.
"""
from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .base import DataSource, LagReading


THRESHOLD_MESSAGES: int = 4_000_000


@dataclass
class _JobPersonality:
    """Seeded once at startup, controls how a job's lag drifts over time."""
    baseline: float
    amplitude: float
    period_seconds: float
    noise: float
    drift_into_breach: bool
    phase: float


class SimulatedDataSource(DataSource):
    """Synthetic Kafka lag — see module docstring for layer breakdown."""

    def __init__(
        self,
        *,
        catalog: list[dict],
        environments: list[str],
        preseeded_breaches: Optional[set[str]] = None,
    ) -> None:
        super().__init__(catalog=catalog, environments=environments)
        self._rng = random.Random(42)
        self._personalities: dict[str, _JobPersonality] = {}
        self._injections: dict[str, dict] = {}
        self._start_ts: float = time.time()
        self._preseeded = preseeded_breaches or set()
        for j in self._jobs:
            drift = j["job_id"] in self._preseeded
            self._personalities[j["job_id"]] = _JobPersonality(
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

    # ---- public API --------------------------------------------------------
    def poll_all(self, *, at: Optional[float] = None) -> list[LagReading]:
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

    # ---- demo controls -----------------------------------------------------
    def inject_spike(
        self,
        job_id: str,
        *,
        stream: str = "cg",
        duration_seconds: int = 120,
    ) -> bool:
        if job_id not in self._personalities:
            return False
        if stream not in ("cg", "topic"):
            stream = "cg"
        self._injections[job_id] = {
            "end": time.time() + max(5, duration_seconds),
            "stream": stream,
        }
        return True

    def clear_injection(self, job_id: str) -> bool:
        return self._injections.pop(job_id, None) is not None

    def is_injecting(self, job_id: str) -> bool:
        return self.active_injection(job_id) is not None

    def active_injection(self, job_id: str) -> Optional[dict]:
        inj = self._injections.get(job_id)
        if inj is None:
            return None
        if time.time() >= inj["end"]:
            self._injections.pop(job_id, None)
            return None
        return inj

    # ---- math --------------------------------------------------------------
    # 7 layers: short oscillation, daily/weekly/monthly seasonality, sparse
    # incidents, producer bursts, consumer rebalance ramps, step shifts,
    # jitter. All deterministic in (job_id, ts).
    def _compute_lag(self, job_id: str, ts: float) -> tuple[int, int]:
        p = self._personalities[job_id]
        elapsed = ts - self._start_ts

        wave = math.sin((elapsed / p.period_seconds) * math.tau + p.phase)

        daily   = math.sin((elapsed / 86400.0) * math.tau + p.phase)
        weekly  = math.sin((elapsed / (86400.0 * 7)) * math.tau + p.phase * 0.7)
        monthly = math.sin((elapsed / (86400.0 * 30)) * math.tau + p.phase * 0.3)
        long_term = (
            daily   * p.baseline * 0.18 +
            weekly  * p.baseline * 0.10 +
            monthly * p.baseline * 0.06
        )

        incident = 0.0
        ibucket = int(ts // (3600 * 18))
        irng = random.Random(hash((job_id, "incident", ibucket)) & 0xFFFFFFFF)
        if irng.random() < 0.018:
            within = (ts - ibucket * 3600 * 18) / (3600 * 18)
            shape = math.sin(within * math.pi) ** 2
            incident = shape * THRESHOLD_MESSAGES * irng.uniform(0.45, 1.05)

        burst = 0.0
        bbucket = int(ts // (60 * 17))
        brng = random.Random(hash((job_id, "burst", bbucket)) & 0xFFFFFFFF)
        if brng.random() < 0.12:
            burst_dur = brng.uniform(180, 720)
            burst_start = bbucket * 60 * 17 + brng.uniform(0, 60 * 17 - burst_dur)
            offset = ts - burst_start
            if 0 <= offset <= burst_dur:
                k = offset / burst_dur
                shape = (4 * k * (1 - k)) ** 1.4
                mag = brng.uniform(0.10, 0.55)
                burst = shape * THRESHOLD_MESSAGES * mag

        rebalance_factor = 1.0
        rbucket = int(ts // (3600 * 4))
        rrng = random.Random(hash((job_id, "reb", rbucket)) & 0xFFFFFFFF)
        if rrng.random() < 0.18:
            reb_at = rbucket * 3600 * 4 + rrng.uniform(0, 3600 * 4)
            ramp_dur = rrng.uniform(120, 540)
            offset = ts - reb_at
            if 0 <= offset <= ramp_dur:
                k = offset / ramp_dur
                rebalance_factor = max(0.0, k * k * (3 - 2 * k))
                rebalance_factor = max(0.05, rebalance_factor)

        sbucket = int(ts // (86400 * 5))
        srng = random.Random(hash((job_id, "step", sbucket)) & 0xFFFFFFFF)
        step_mult = srng.uniform(0.78, 1.30)

        seed = hash((job_id, int(ts // 3))) & 0xFFFFFFFF
        rng = random.Random(seed)
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

        # Topic-lag stream — independent-ish signal
        t_period = p.period_seconds * 1.45
        t_phase = (p.phase + math.pi / 3.0) % math.tau
        t_wave = math.sin((elapsed / t_period) * math.tau + t_phase)
        t_amplitude = p.amplitude * 0.62

        t_jitter_seed = hash((job_id, "topic-jitter", int(ts // 4))) & 0xFFFFFFFF
        t_rng = random.Random(t_jitter_seed)
        t_jitter = t_rng.gauss(0, p.noise * noise_mult * 0.7)

        t_burst = 0.0
        tb_bucket = int(ts // (60 * 23))
        tb_rng = random.Random(hash((job_id, "tburst", tb_bucket)) & 0xFFFFFFFF)
        if tb_rng.random() < 0.10:
            tb_dur = tb_rng.uniform(150, 600)
            tb_start = tb_bucket * 60 * 23 + tb_rng.uniform(0, 60 * 23 - tb_dur)
            offset_t = ts - tb_start
            if 0 <= offset_t <= tb_dur:
                k = offset_t / tb_dur
                shape = (4 * k * (1 - k)) ** 1.6
                t_burst = shape * THRESHOLD_MESSAGES * tb_rng.uniform(0.08, 0.40)

        t_base = (
            p.baseline * step_mult * 0.88
            + t_wave * t_amplitude
            + long_term * 0.70
            + incident * 0.95
            + t_burst
        )
        t_lag = max(0, int((t_base + t_jitter) * rebalance_factor))

        # Injection: spike only the selected stream
        inj = self.active_injection(job_id)
        if inj is not None:
            spike = int(THRESHOLD_MESSAGES * 1.5 + rng.uniform(0, 800_000))
            if inj["stream"] == "topic":
                return cg_lag, spike
            return spike, t_lag

        return cg_lag, t_lag
