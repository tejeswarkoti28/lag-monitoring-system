"""
Data source interface.

Implementations (currently only LensesDataSource) inherit DataSource and
override poll_all(). Historical data is no longer the data source's
responsibility — it's owned by HistoryDB, which persists every poll to
SQLite. Long-range chart views read from there, not from the source.
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
    """Abstract data source. The monitoring engine only depends on this
    interface — `poll_all()` is the only required method.
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

    @abc.abstractmethod
    def poll_all(self, *, at: Optional[float] = None) -> list[LagReading]:
        """Return one LagReading per job at the current moment (or `at`)."""
        ...
