"""
Data source interface.

Every implementation (simulator, Prometheus, Lenses, ...) inherits DataSource
and overrides poll_all(). Optional methods (synthesize_history, inject_spike)
have safe defaults so production sources only implement what they need.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


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


class DataSource(abc.ABC):
    """
    Abstract data source. The monitoring engine only depends on this interface.

    Required:
      - poll_all() — return current lag readings for every job

    Optional (defaults are no-ops):
      - synthesize_history() — historical time series for a job (Prometheus
        range query in production; deterministic sim in demo)
      - inject_spike() / clear_injection() / is_injecting() — demo controls
        only the simulator implements; production sources return False.
    """

    def __init__(self, *, catalog: list[dict], environments: list[str]) -> None:
        self._catalog = catalog
        self._environments = environments
        self._jobs: list[dict] = []
        for entry in catalog:
            for env in environments:
                self._jobs.append({
                    "job_id": f"{entry['topic']}::{env}",
                    "topic": entry["topic"],
                    "consumer_group": entry["consumer_group"],
                    "environment": env,
                    "team": entry["team"],
                    "channel": entry["channel"],
                })

    def jobs(self) -> list[dict]:
        return list(self._jobs)

    # ---- required ----------------------------------------------------------
    @abc.abstractmethod
    def poll_all(self, *, at: Optional[float] = None) -> list[LagReading]:
        """Return one LagReading per job at the current moment (or `at`)."""
        ...

    # ---- optional ----------------------------------------------------------
    def synthesize_history(
        self,
        job_id: str,
        *,
        start_ts: float,
        end_ts: float,
        step_seconds: float,
    ) -> list[dict]:
        """Return historical readings between start_ts and end_ts.

        Default: empty list. Real sources should query their TSDB.
        Each item: {"ts": iso8601, "cg_lag": int, "topic_lag": int, "lag": int}
        """
        return []

    def inject_spike(
        self,
        job_id: str,
        *,
        stream: str = "cg",
        duration_seconds: int = 120,
    ) -> bool:
        """Demo-only: force a job above threshold. Production sources return False."""
        return False

    def clear_injection(self, job_id: str) -> bool:
        return False

    def is_injecting(self, job_id: str) -> bool:
        return False

    def active_injection(self, job_id: str) -> Optional[dict]:
        """Return the active injection record (with 'stream' key) or None."""
        return None
