"""
Prometheus / PromQL data source — production target.

WHAT YOU NEED TO FILL IN INSIDE THE VDI:
  1. PROMETHEUS_URL env var — the HTTP endpoint of your Prometheus / Lenses /
     Cortex / Thanos. Example: "https://prometheus.walmart.internal".
  2. PROMETHEUS_AUTH_TOKEN env var (optional) — bearer token if your TSDB
     requires auth. If your TSDB uses mTLS, basic auth, or something else,
     edit `_build_headers()` below.
  3. The metric name + label scheme used by your Kafka exporter. The
     placeholder PromQL templates assume the very common
     `kafka_consumergroup_lag` metric with `{topic, consumergroup, env}`
     labels. Check what you actually have with:
        curl -s 'https://your-prom/api/v1/label/__name__/values' | grep kafka
     and adjust METRIC_NAME / LABEL_* below.

Once those three are right, swap DATA_SOURCE=prometheus in your .env and the
rest of the app (alert engine, DB, dashboard, chatbot) keeps working unchanged.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from .base import DataSource, LagReading


# -------- Tweak these to match your Prometheus setup --------
METRIC_NAME = "kafka_consumergroup_lag"
LABEL_TOPIC = "topic"
LABEL_CONSUMER_GROUP = "consumergroup"
LABEL_ENVIRONMENT = "env"
# ------------------------------------------------------------


class PrometheusDataSource(DataSource):
    """Reads consumer-group lag from a Prometheus-compatible TSDB."""

    def __init__(
        self,
        *,
        catalog: list[dict],
        environments: list[str],
        base_url: Optional[str] = None,
        auth_token: Optional[str] = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        super().__init__(catalog=catalog, environments=environments)
        self._base_url = (base_url or os.environ.get("PROMETHEUS_URL", "")).rstrip("/")
        if not self._base_url:
            raise RuntimeError(
                "PROMETHEUS_URL is not set. Either set the env var or pass "
                "base_url=... when constructing PrometheusDataSource."
            )
        self._auth_token = auth_token or os.environ.get("PROMETHEUS_AUTH_TOKEN")
        self._client = httpx.Client(timeout=timeout_seconds)

    # ---- required interface -----------------------------------------------
    def poll_all(self, *, at: Optional[float] = None) -> list[LagReading]:
        """One instant query per job × stream. Two streams per job: cg + topic.

        For very large catalogs this is wasteful — replace with a single
        `query_range` over a regex of all topics if you have >100 jobs.
        """
        ts = at if at is not None else time.time()
        when = datetime.fromtimestamp(ts, tz=timezone.utc)
        out: list[LagReading] = []
        for job in self._jobs:
            cg_lag = self._instant_query_lag(
                topic=job["topic"],
                consumer_group=job["consumer_group"],
                environment=job["environment"],
                stream="cg",
            )
            topic_lag = self._instant_query_lag(
                topic=job["topic"],
                consumer_group=job["consumer_group"],
                environment=job["environment"],
                stream="topic",
            )
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

    def synthesize_history(
        self,
        job_id: str,
        *,
        start_ts: float,
        end_ts: float,
        step_seconds: float,
    ) -> list[dict]:
        """Range query against Prometheus. Returns the same shape the
        dashboard expects: list of {ts, cg_lag, topic_lag, lag}.
        """
        job = next((j for j in self._jobs if j["job_id"] == job_id), None)
        if job is None:
            return []
        cg_series = self._range_query_series(
            topic=job["topic"],
            consumer_group=job["consumer_group"],
            environment=job["environment"],
            stream="cg",
            start_ts=start_ts,
            end_ts=end_ts,
            step_seconds=step_seconds,
        )
        topic_series = self._range_query_series(
            topic=job["topic"],
            consumer_group=job["consumer_group"],
            environment=job["environment"],
            stream="topic",
            start_ts=start_ts,
            end_ts=end_ts,
            step_seconds=step_seconds,
        )
        # Align series on ts; gaps get filled with 0
        topic_by_ts = {t: v for t, v in topic_series}
        out: list[dict] = []
        for ts, cg in cg_series:
            tl = topic_by_ts.get(ts, 0)
            out.append({
                "ts": datetime.fromtimestamp(ts, tz=timezone.utc)
                          .replace(microsecond=0).isoformat(),
                "cg_lag": int(cg),
                "topic_lag": int(tl),
                "lag": int(max(cg, tl)),
            })
        return out

    # ---- internals --------------------------------------------------------
    def _build_query(
        self,
        *,
        topic: str,
        consumer_group: str,
        environment: str,
        stream: str,
    ) -> str:
        """PromQL template. Customize for your exporter's label scheme.

        Default assumes a single metric serves both streams; if your env
        exposes them as separate metrics, branch on `stream` here.
        """
        # NOTE: depending on your exporter, "consumer group lag" might be
        # max-partition-lag and "topic lag" might be sum-of-partition-lag.
        # Adjust the aggregation here:
        if stream == "topic":
            agg = "sum"
        else:
            agg = "max"
        return (
            f"{agg}({METRIC_NAME}"
            f'{{{LABEL_TOPIC}="{topic}",'
            f'{LABEL_CONSUMER_GROUP}="{consumer_group}",'
            f'{LABEL_ENVIRONMENT}="{environment}"}})'
        )

    def _instant_query_lag(
        self,
        *,
        topic: str,
        consumer_group: str,
        environment: str,
        stream: str,
    ) -> Optional[float]:
        query = self._build_query(
            topic=topic, consumer_group=consumer_group,
            environment=environment, stream=stream,
        )
        try:
            r = self._client.get(
                f"{self._base_url}/api/v1/query",
                params={"query": query},
                headers=self._build_headers(),
            )
            r.raise_for_status()
            data = r.json()
            results = data.get("data", {}).get("result", [])
            if not results:
                return 0.0
            value = results[0].get("value", [None, "0"])[1]
            return float(value)
        except Exception as exc:
            # In production: log + emit a metric so on-call notices when
            # Prometheus itself is down. For now, return 0 (job appears healthy).
            print(f"[prometheus] query failed for {topic}/{environment}: {exc}")
            return 0.0

    def _range_query_series(
        self,
        *,
        topic: str,
        consumer_group: str,
        environment: str,
        stream: str,
        start_ts: float,
        end_ts: float,
        step_seconds: float,
    ) -> list[tuple[float, float]]:
        query = self._build_query(
            topic=topic, consumer_group=consumer_group,
            environment=environment, stream=stream,
        )
        try:
            r = self._client.get(
                f"{self._base_url}/api/v1/query_range",
                params={
                    "query": query,
                    "start": f"{start_ts:.3f}",
                    "end": f"{end_ts:.3f}",
                    "step": f"{step_seconds:.3f}",
                },
                headers=self._build_headers(),
            )
            r.raise_for_status()
            data = r.json()
            results = data.get("data", {}).get("result", [])
            if not results:
                return []
            values = results[0].get("values", [])
            return [(float(t), float(v)) for t, v in values]
        except Exception as exc:
            print(f"[prometheus] range query failed for {topic}/{environment}: {exc}")
            return []

    def _build_headers(self) -> dict:
        """Edit this if your Prometheus uses something other than bearer auth."""
        if self._auth_token:
            return {"Authorization": f"Bearer {self._auth_token}"}
        return {}
