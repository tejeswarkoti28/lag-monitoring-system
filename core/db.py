"""
core/db.py — Database layer.

Contains:
  AlertDB     — stores breach/resolved events in SQLite
  HistoryDB   — three-tier time-series store (raw 7d → 1min rollup 90d → 1hr rollup 3yr)
  ResponseCache — simple TTL cache for read-heavy API endpoints
  bucket_seconds_for — picks the right downsampling bucket for a time window
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional

from core.config import (
    THRESHOLD_MESSAGES,
    RAW_RETENTION_DAYS,
    ROLLUP_1M_RETENTION_DAYS,
    ROLLUP_1H_RETENTION_DAYS,
    iso,
)

# =============================================================================
# Alerts table
# =============================================================================
_SCHEMA = """
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
            conn.executescript(_SCHEMA)

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
                    alert_type, lag_value, threshold,
                    delivered_to_slack, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    job_id, topic, consumer_group, environment, team, channel,
                    alert_type, int(lag_value), THRESHOLD_MESSAGES,
                    1 if delivered_to_slack else 0, iso(created_at),
                ),
            )
            return cur.lastrowid

    def recent_alerts(self, limit: int = 50, hours: int = 24) -> list[dict]:
        cutoff = iso(
            datetime.fromtimestamp(time.time() - hours * 3600, tz=timezone.utc)
        )
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts WHERE created_at >= ? "
                "ORDER BY id DESC LIMIT ?",
                (cutoff, limit),
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


# =============================================================================
# Historical lag persistence — three-tier storage
# =============================================================================
# Mirrors how Prometheus + recording rules work:
#
#   lag_history      raw 5s readings      kept RAW_RETENTION_DAYS  (default 7d)
#   lag_history_1m   1-minute rollups     kept 1M_RETENTION_DAYS   (default 90d)
#   lag_history_1h   1-hour rollups       kept 1H_RETENTION_DAYS   (default 3yr)
#
# query() routes to the cheapest tier for the requested window:
#   bucket < 60s   → lag_history      full resolution, only recent data
#   60s – 3599s    → lag_history_1m   pre-averaged, much smaller scan
#   ≥ 3600s        → lag_history_1h   tiny scan, perfect for 7d+ charts

_HISTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS lag_history (
    job_id    TEXT    NOT NULL,
    ts        INTEGER NOT NULL,
    cg_lag    INTEGER NOT NULL,
    topic_lag INTEGER NOT NULL,
    PRIMARY KEY (job_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_lag_history_ts ON lag_history(ts);

CREATE TABLE IF NOT EXISTS job_id_map (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS lag_history_1m (
    jid    INTEGER NOT NULL,
    ts     INTEGER NOT NULL,
    cg_sum INTEGER NOT NULL,
    tp_sum INTEGER NOT NULL,
    n      INTEGER NOT NULL,
    PRIMARY KEY (jid, ts)
);
CREATE INDEX IF NOT EXISTS idx_lag_1m_ts ON lag_history_1m(ts);

CREATE TABLE IF NOT EXISTS lag_history_1h (
    jid    INTEGER NOT NULL,
    ts     INTEGER NOT NULL,
    cg_sum INTEGER NOT NULL,
    tp_sum INTEGER NOT NULL,
    n      INTEGER NOT NULL,
    PRIMARY KEY (jid, ts)
);
CREATE INDEX IF NOT EXISTS idx_lag_1h_ts ON lag_history_1h(ts);
"""


class HistoryDB:
    """Three-tier time-series store: raw (7d) → 1-min rollup (90d) → 1-hr rollup (3yr).

    query() automatically routes to the cheapest tier for the requested window.
    All tables live in the same SQLite file as AlertDB; WAL mode allows
    concurrent reads while the Monitor is inserting.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._jid_cache: dict[str, int] = {}
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init(self) -> None:
        with self._conn() as conn:
            conn.executescript(_HISTORY_SCHEMA)
            for row in conn.execute("SELECT id, job_id FROM job_id_map"):
                self._jid_cache[row["job_id"]] = row["id"]

    def _get_jid(self, job_id: str, conn: sqlite3.Connection) -> int:
        if job_id in self._jid_cache:
            return self._jid_cache[job_id]
        conn.execute("INSERT OR IGNORE INTO job_id_map (job_id) VALUES (?)", (job_id,))
        row = conn.execute("SELECT id FROM job_id_map WHERE job_id = ?", (job_id,)).fetchone()
        jid = int(row["id"])
        self._jid_cache[job_id] = jid
        return jid

    def insert_batch(self, rows: list[tuple[str, int, int, int]]) -> int:
        """Bulk insert. Each row: (job_id, ts_seconds, cg_lag, topic_lag).
        Writes to raw table and upserts into both rollup tables atomically.
        """
        if not rows:
            return 0
        with self._conn() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO lag_history (job_id, ts, cg_lag, topic_lag) VALUES (?, ?, ?, ?)",
                rows,
            )
            buckets_1m: dict[tuple, list] = {}
            buckets_1h: dict[tuple, list] = {}
            for job_id, ts, cg_lag, topic_lag in rows:
                jid = self._get_jid(job_id, conn)
                for bucket_sec, store in ((60, buckets_1m), (3600, buckets_1h)):
                    key = (jid, (ts // bucket_sec) * bucket_sec)
                    if key in store:
                        b = store[key]; b[0] += cg_lag; b[1] += topic_lag; b[2] += 1
                    else:
                        store[key] = [cg_lag, topic_lag, 1]

            _UPSERT = (
                "INSERT INTO {t} (jid, ts, cg_sum, tp_sum, n) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (jid, ts) DO UPDATE SET "
                "cg_sum = cg_sum + excluded.cg_sum, "
                "tp_sum = tp_sum + excluded.tp_sum, "
                "n      = n + 1"
            )
            conn.executemany(
                _UPSERT.format(t="lag_history_1m"),
                [(jid, ts, c, p, n) for (jid, ts), (c, p, n) in buckets_1m.items()],
            )
            conn.executemany(
                _UPSERT.format(t="lag_history_1h"),
                [(jid, ts, c, p, n) for (jid, ts), (c, p, n) in buckets_1h.items()],
            )
        return len(rows)

    def query(
        self,
        *,
        job_id: str,
        start_ts: float,
        end_ts: float,
        bucket_seconds: int,
    ) -> list[dict]:
        bucket_seconds = max(1, int(bucket_seconds))
        if bucket_seconds >= 3600:
            jid = self._jid_cache.get(job_id)
            return [] if jid is None else self._query_rollup("lag_history_1h", jid, start_ts, end_ts, bucket_seconds)
        if bucket_seconds >= 60:
            jid = self._jid_cache.get(job_id)
            return [] if jid is None else self._query_rollup("lag_history_1m", jid, start_ts, end_ts, bucket_seconds)
        return self._query_raw(job_id, start_ts, end_ts, bucket_seconds)

    def _query_raw(self, job_id: str, start_ts: float, end_ts: float, bucket_seconds: int) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT (ts/?) * ? AS bucket_ts, AVG(cg_lag) AS cg_lag, AVG(topic_lag) AS topic_lag
                   FROM lag_history WHERE job_id = ? AND ts >= ? AND ts <= ?
                   GROUP BY bucket_ts ORDER BY bucket_ts""",
                (bucket_seconds, bucket_seconds, job_id, int(start_ts), int(end_ts)),
            ).fetchall()
        return self._to_dicts(rows)

    def _query_rollup(self, table: str, jid: int, start_ts: float, end_ts: float, bucket_seconds: int) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT (ts/?) * ? AS bucket_ts,
                           CAST(SUM(cg_sum) AS REAL)/SUM(n) AS cg_lag,
                           CAST(SUM(tp_sum) AS REAL)/SUM(n) AS topic_lag
                    FROM {table} WHERE jid = ? AND ts >= ? AND ts <= ?
                    GROUP BY bucket_ts ORDER BY bucket_ts""",
                (bucket_seconds, bucket_seconds, jid, int(start_ts), int(end_ts)),
            ).fetchall()
        return self._to_dicts(rows)

    @staticmethod
    def _to_dicts(rows) -> list[dict]:
        out = []
        for r in rows:
            cg = int(r["cg_lag"] or 0)
            tp = int(r["topic_lag"] or 0)
            out.append({
                "ts": datetime.fromtimestamp(int(r["bucket_ts"]), tz=timezone.utc)
                          .replace(microsecond=0).isoformat(),
                "cg_lag": cg,
                "topic_lag": tp,
                "lag": max(cg, tp),
            })
        return out

    def cleanup(self) -> dict:
        """Trim each tier to its configured retention window."""
        now = int(time.time())
        deleted: dict[str, int] = {}
        with self._conn() as conn:
            for table, days in (
                ("lag_history",    RAW_RETENTION_DAYS),
                ("lag_history_1m", ROLLUP_1M_RETENTION_DAYS),
                ("lag_history_1h", ROLLUP_1H_RETENTION_DAYS),
            ):
                cur = conn.execute(f"DELETE FROM {table} WHERE ts < ?", (now - days * 86400,))
                deleted[table] = cur.rowcount or 0
        return deleted

    def total_rows(self) -> int:
        with self._conn() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM lag_history").fetchone()
            return int(row["c"])


def bucket_seconds_for(minutes: int) -> int:
    """Pick a bucket size that returns ~500–720 points for the requested window.

    Range    Bucket    Points   Table used
    30m      5s        360      lag_history      (raw)
    3h       30s       360      lag_history      (raw)
    6h       30s       720      lag_history      (raw)
    12h      60s       720      lag_history_1m   (rollup)
    24h      120s      720      lag_history_1m   (rollup)
    7d       1800s     336      lag_history_1h   (rollup)
    30d      3600s     720      lag_history_1h   (rollup)
    """
    if minutes <= 180:     return 30
    if minutes <= 360:     return 30
    if minutes <= 720:     return 60
    if minutes <= 1440:    return 120
    if minutes <= 2880:    return 300
    if minutes <= 21_600:  return 1800
    if minutes <= 43_200:  return 3600
    if minutes <= 129_600: return 14400
    return 28800


# =============================================================================
# Response cache — TTL-based in-memory cache for read-heavy endpoints
# =============================================================================

class ResponseCache:
    def __init__(self, ttl: float = 30.0) -> None:
        self._ttl = ttl
        self._store: dict[str, tuple[float, object]] = {}

    def get(self, key: str) -> Optional[object]:
        entry = self._store.get(key)
        if entry and time.time() - entry[0] < self._ttl:
            return entry[1]
        return None

    def set(self, key: str, value: object) -> None:
        self._store[key] = (time.time(), value)
