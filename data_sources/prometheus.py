"""
Prometheus data source.

The Walmart "Lenses ConsumerGroup Lag" Grafana dashboard is backed by a
Prometheus instance scraping a Lenses-aware exporter. The metric is
`lenses_topic_consumer_lag` with labels {job, ooa, oop, ooe, topic,
consumerGroup}. We don't talk to Prometheus directly — we route through
Grafana's datasource proxy because the upstream requires Walmart auth
headers that Grafana adds for us.

Two responsibilities here:

  1. `poll_all()` — bulk-fetch current lag for every job in the catalog.
     This drives the alert engine and the in-memory ring used by the
     live sparkline. Same shape as before the refactor.

  2. `query_instant()` / `query_range()` — generic PromQL execution.
     Used by the panel system so charts can render any panel's expr,
     not just the lag metric.

Connection config (URL, auth, static labels) comes from
config/data_sources.json — passed in via the constructor.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from .base import DataSource, LagReading


class PrometheusDataSource(DataSource):

    def __init__(
        self,
        *,
        catalog: list[dict],
        environments: list[str],
        base_url: str,
        auth_token: Optional[str] = None,
        static_labels: Optional[dict] = None,
        verify_ssl: bool = True,
        timeout_seconds: float = 5.0,
    ) -> None:
        super().__init__(catalog=catalog, environments=environments)
        if not base_url:
            raise RuntimeError(
                "PrometheusDataSource: base_url is required. "
                "Set it in config/data_sources.json."
            )
        self._base_url = base_url.rstrip("/")
        self._auth_token = auth_token
        self._static_labels = dict(static_labels or {})
        # verify_ssl=False is intended for internal corporate hosts whose
        # certs are signed by an internal CA Python doesn't trust by default.
        # Only safe when you're already inside the corporate network.
        self._client = httpx.Client(timeout=timeout_seconds, verify=verify_ssl)

    @property
    def static_labels(self) -> dict:
        return dict(self._static_labels)

    # ---- DataSource interface ---------------------------------------------
    def poll_all(self, *, at: Optional[float] = None) -> list[LagReading]:
        """Current lag for every job. Used by the alert engine."""
        ts = at if at is not None else time.time()
        when = datetime.fromtimestamp(ts, tz=timezone.utc)
        out: list[LagReading] = []
        for job in self._jobs:
            cg_lag = self._lag_query(job, aggregation="max")
            topic_lag = self._lag_query(job, aggregation="sum")
            out.append(
                LagReading(
                    job_id=job["job_id"],
                    topic=job["topic"],
                    consumer_group=job["consumer_group"],
                    environment=job["environment"],
                    team=job["team"],
                    channel=job["channel"],
                    consumer_group_lag=int(cg_lag or 0),
                    topic_lag=int(topic_lag or 0),
                    timestamp=when,
                )
            )
        return out

    # ---- generic query API used by the panel system -----------------------
    def query_instant(self, expr: str) -> Optional[float]:
        """Run an instant PromQL query, return a single scalar (or 0 on miss)."""
        try:
            r = self._client.get(
                f"{self._base_url}/api/v1/query",
                params={"query": expr},
                headers=self._headers(),
            )
            r.raise_for_status()
            data = r.json()
            results = data.get("data", {}).get("result", [])
            if not results:
                return 0.0
            return float(results[0].get("value", [None, "0"])[1])
        except Exception as exc:
            print(f"[prometheus] instant query failed: {expr!r}: {exc}")
            return 0.0

    def query_range(
        self,
        expr: str,
        *,
        start_ts: float,
        end_ts: float,
        step_seconds: float,
    ) -> list[tuple[float, float]]:
        """Run a range PromQL query, return [(ts, value), ...] sorted by ts.

        Returns an empty list if the query has no data or fails — callers
        should treat that as 'metric is absent' rather than an error.
        """
        try:
            r = self._client.get(
                f"{self._base_url}/api/v1/query_range",
                params={
                    "query": expr,
                    "start": f"{start_ts:.3f}",
                    "end": f"{end_ts:.3f}",
                    "step": f"{step_seconds:.3f}",
                },
                headers=self._headers(),
            )
            r.raise_for_status()
            data = r.json()
            results = data.get("data", {}).get("result", [])
            if not results:
                return []
            values = results[0].get("values", [])
            return [(float(t), float(v)) for t, v in values]
        except Exception as exc:
            print(f"[prometheus] range query failed: {expr!r}: {exc}")
            return []

    # ---- internals --------------------------------------------------------
    def _lag_query(self, job: dict, *, aggregation: str) -> Optional[float]:
        """Build the lag PromQL for one job and run it."""
        labels = ",".join([
            f'{k}="{v}"' for k, v in self._static_labels.items()
        ] + [
            f'ooe="{job["environment"]}"',
            f'topic="{job["topic"]}"',
            f'consumerGroup="{job["consumer_group"]}"',
        ])
        expr = f'{aggregation}(lenses_topic_consumer_lag{{{labels}}})'
        return self.query_instant(expr)

    def _headers(self) -> dict:
        if self._auth_token:
            return {"Authorization": f"Bearer {self._auth_token}"}
        return {}
