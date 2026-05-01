"""
Lenses data source — production target.

Lenses (lenses.io) is a real-time Kafka management/inspection platform. It
exposes consumer-group lag through a REST API. Unlike Prometheus, Lenses
does NOT store historical lag — it only returns the current value. Historical
data for the dashboard's longer time-range views is persisted by *us* into
SQLite (see core/HistoryDB) on every poll.

WHAT YOU NEED TO FILL IN INSIDE THE VDI:
  1. LENSES_URL env var — base URL of your Lenses install, e.g.
     "https://lenses.walmart.internal" (no trailing slash).
  2. Auth — pick ONE:
       - LENSES_API_TOKEN  (preferred; service-account token)
       - LENSES_USERNAME + LENSES_PASSWORD (interactive login flow)
  3. The exact endpoint paths your Lenses build exposes. The defaults
     below match Lenses 5.x. If your version differs, override
     LENSES_LAG_ENDPOINT_TEMPLATE in .env or edit _build_url() below.

Once those are set, swap DATA_SOURCE=lenses in your .env and the rest of the
app (alert engine, alert DB, dashboard, chatbot, history persistence) keeps
working unchanged.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from .base import DataSource, LagReading


# Default Lenses 5.x endpoint shape:
#   GET /api/v1/kafka/consumers/{group}
# returns JSON with topic-level lag arrays. Override via env if your build
# uses a different path layout.
DEFAULT_LAG_ENDPOINT = "/api/v1/kafka/consumers/{group}"


class LensesDataSource(DataSource):
    """Reads consumer-group lag from a Lenses REST API."""

    def __init__(
        self,
        *,
        catalog: list[dict],
        environments: list[str],
        base_url: Optional[str] = None,
        api_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        super().__init__(catalog=catalog, environments=environments)
        self._base_url = (base_url or os.environ.get("LENSES_URL", "")).rstrip("/")
        if not self._base_url:
            raise RuntimeError(
                "LENSES_URL is not set. Either set the env var or pass "
                "base_url=... when constructing LensesDataSource."
            )
        self._api_token = api_token or os.environ.get("LENSES_API_TOKEN")
        self._username = username or os.environ.get("LENSES_USERNAME")
        self._password = password or os.environ.get("LENSES_PASSWORD")
        self._endpoint_template = os.environ.get(
            "LENSES_LAG_ENDPOINT_TEMPLATE", DEFAULT_LAG_ENDPOINT,
        )
        self._client = httpx.Client(timeout=timeout_seconds)
        self._session_token: Optional[str] = None
        # Refresh interactive sessions every 30 minutes
        self._session_acquired_at: float = 0.0
        self._session_ttl_seconds: float = 30 * 60

    # ---- required interface -----------------------------------------------
    def poll_all(self, *, at: Optional[float] = None) -> list[LagReading]:
        """One Lenses HTTP call per consumer group. With 18 jobs × 5s polls
        that's 18 calls every 5s = ~3.6 req/sec — well within Lenses limits.
        For very large catalogs (>200 jobs) batch by group prefix or move to
        Lenses' SQL Studio query API.
        """
        ts = at if at is not None else time.time()
        when = datetime.fromtimestamp(ts, tz=timezone.utc)
        out: list[LagReading] = []
        # Cache per-(group, env) so we don't double-fetch when the same
        # consumer-group entry appears in multiple env rows
        seen: dict[tuple[str, str], dict] = {}
        for job in self._jobs:
            key = (job["consumer_group"], job["environment"])
            if key not in seen:
                seen[key] = self._fetch_consumer_group(
                    consumer_group=job["consumer_group"],
                    environment=job["environment"],
                ) or {}
            payload = seen[key]
            cg_lag, topic_lag = self._extract_lags(payload, job["topic"])
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

    # ---- internals --------------------------------------------------------
    def _fetch_consumer_group(
        self,
        *,
        consumer_group: str,
        environment: str,
    ) -> Optional[dict]:
        """Hit the Lenses consumer-group endpoint. Return the parsed JSON
        body, or None on any failure (we treat as 0 lag rather than crash).
        """
        url = self._build_url(consumer_group=consumer_group, environment=environment)
        try:
            r = self._client.get(url, headers=self._build_headers())
            if r.status_code == 401:
                # Session expired — re-login if we're using user/pass
                self._session_token = None
                if self._username and self._password:
                    self._login()
                    r = self._client.get(url, headers=self._build_headers())
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            print(f"[lenses] fetch failed for {consumer_group}/{environment}: {exc}")
            return None

    def _build_url(self, *, consumer_group: str, environment: str) -> str:
        """Construct the URL. Many Lenses installs are environment-scoped at
        the URL level (one Lenses per env); others embed env as a query param.
        Adjust here for your topology.
        """
        path = self._endpoint_template.format(
            group=consumer_group, env=environment,
        )
        return f"{self._base_url}{path}"

    def _build_headers(self) -> dict:
        """Lenses supports either:
          - x-kafka-lenses-token: <api-token>     (service account)
          - Bearer auth from a /api/login session  (interactive)
        """
        if self._api_token:
            return {"x-kafka-lenses-token": self._api_token}
        if not self._session_token or self._session_expired():
            self._login()
        if self._session_token:
            return {"x-kafka-lenses-token": self._session_token}
        return {}

    def _session_expired(self) -> bool:
        return (time.time() - self._session_acquired_at) > self._session_ttl_seconds

    def _login(self) -> None:
        """Interactive login flow — used only when no API token is set."""
        if not (self._username and self._password):
            return
        try:
            r = self._client.post(
                f"{self._base_url}/api/login",
                json={"user": self._username, "password": self._password},
            )
            r.raise_for_status()
            data = r.json()
            # Lenses returns the token as plain text in some versions, JSON
            # with "token" key in others. Be lenient.
            self._session_token = (
                data if isinstance(data, str) else data.get("token") or data.get("access_token")
            )
            self._session_acquired_at = time.time()
        except Exception as exc:
            print(f"[lenses] login failed: {exc}")
            self._session_token = None

    @staticmethod
    def _extract_lags(payload: dict, topic: str) -> tuple[int, int]:
        """Pull the two lag streams out of a Lenses consumer-group response.

        The exact JSON shape varies across Lenses versions. Common paths:
          - payload['topics'][i]['lag']                  (sum across partitions)
          - payload['topics'][i]['partitions'][j]['lag'] (per-partition)
        We try both. Override this method if your build returns a different
        shape — it's the only place that needs adjustment.
        """
        if not payload:
            return 0, 0
        topics = payload.get("topics") or payload.get("topicSubscriptions") or []
        for t in topics:
            if t.get("topic") != topic and t.get("name") != topic:
                continue
            # Topic-level aggregate: sum across partitions
            topic_lag = int(t.get("lag") or t.get("totalLag") or 0)
            # Consumer-group-level for this topic: max across partitions
            partitions = t.get("partitions") or []
            cg_lag = 0
            for p in partitions:
                pl = int(p.get("lag") or p.get("messagesBehind") or 0)
                if pl > cg_lag:
                    cg_lag = pl
            if cg_lag == 0 and topic_lag > 0:
                # If we couldn't find partition data, fall back to topic-level
                cg_lag = topic_lag
            return cg_lag, topic_lag
        return 0, 0
