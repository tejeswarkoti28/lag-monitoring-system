"""
Kafka Consumer Lag Monitor — FastAPI backend
============================================
Polls a pluggable DataSource every POLL_INTERVAL_SECONDS, evaluates breaches
through an edge-trigger AlertEngine, persists alerts to SQLite, and routes
Slack notifications per team. Also serves a static dashboard and an AI
chatbot endpoint that can answer questions about the live data.

Run:    python app.py     # then open http://localhost:8000

Two pluggable seams:
  * data_sources/  — simulator (default) or Prometheus (production).
                     Switch via DATA_SOURCE=simulator|prometheus.
  * ai/           — LLM-backed chatbot. OpenAI default, swappable.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import time
import urllib.parse
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta as _td, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from data_sources import DataSource, LagReading, get_data_source

# Load .env from the project root before any os.environ reads.
try:
    from dotenv import load_dotenv
    load_dotenv(
        dotenv_path=os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ".env"
        ),
        override=False,
    )
except ImportError:
    pass


# =============================================================================
# Configuration
# =============================================================================
HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.environ.get(
    "LAG_MONITOR_CONFIG",
    os.path.join(HERE, "config", "jobs.json"),
)


def _load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


_cfg = _load_config(CONFIG_PATH)
THRESHOLD_MESSAGES: int = int(_cfg.get("threshold_messages", 4_000_000))
ENVIRONMENTS: list[str] = list(_cfg.get("environments", ["eus", "scus"]))
JOB_CATALOG: list[dict] = list(_cfg.get("jobs", []))

POLL_INTERVAL_SECONDS: float = float(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
PUBLIC_URL: str = os.environ.get(
    "LAG_MONITOR_PUBLIC_URL", "http://localhost:8000"
).rstrip("/")
HISTORY_RETENTION_MINUTES: int = 60
WARMUP_MINUTES: int = 30
DB_PATH: str = os.environ.get("LAG_MONITOR_DB", "lag_monitor.db")


# --- Slack routing -----------------------------------------------------------
SLACK_TEAM_ENV_VARS: dict[str, str] = {
    "PNO Team": "SLACK_WEBHOOK_PNO_TEAM",
    "Catalog Team": "SLACK_WEBHOOK_CATALOG_TEAM",
    "Shipping Team": "SLACK_WEBHOOK_SHIPPING_TEAM",
}


def slack_webhook_for(team: str) -> Optional[str]:
    env_var = SLACK_TEAM_ENV_VARS.get(team)
    if env_var:
        url = os.environ.get(env_var)
        if url:
            return url
    return os.environ.get("SLACK_WEBHOOK_URL") or None


SLACK_ONCALL_ENV_VARS: dict[str, str] = {
    "PNO Team": "SLACK_ONCALL_PNO",
    "Catalog Team": "SLACK_ONCALL_CATALOG",
    "Shipping Team": "SLACK_ONCALL_SHIPPING",
}


def slack_oncall_tag(team: str) -> str:
    env_var = SLACK_ONCALL_ENV_VARS.get(team)
    if env_var:
        v = os.environ.get(env_var)
        if v:
            return v.strip()
    return "<!channel>"


def slack_configured() -> bool:
    if os.environ.get("SLACK_WEBHOOK_URL"):
        return True
    return any(os.environ.get(v) for v in SLACK_TEAM_ENV_VARS.values())


# --- Time helpers ------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.replace(microsecond=0).isoformat()


IST = timezone(_td(hours=5, minutes=30), name="IST")


def to_ist(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(IST)


def ist_clock(ts: datetime) -> str:
    return to_ist(ts).strftime("%H:%M")


def ist_full(ts: datetime) -> str:
    return to_ist(ts).strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# Database
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
# Alert engine — pure edge-trigger, one alert per crossing
# =============================================================================
@dataclass
class _BreachState:
    first_breached_at: float


@dataclass
class AlertEvent:
    type: str                         # "breach" | "resolved"
    reading: LagReading
    duration_seconds: float = 0.0


class AlertEngine:
    def __init__(self) -> None:
        self._state: dict[str, _BreachState] = {}

    def evaluate(self, reading: LagReading) -> Optional[AlertEvent]:
        ts = reading.timestamp.timestamp()
        in_breach = reading.lag >= THRESHOLD_MESSAGES
        prev = self._state.get(reading.job_id)
        if in_breach:
            if prev is None:
                self._state[reading.job_id] = _BreachState(first_breached_at=ts)
                return AlertEvent(type="breach", reading=reading)
            return None
        if prev is not None:
            duration = ts - prev.first_breached_at
            del self._state[reading.job_id]
            return AlertEvent(type="resolved", reading=reading, duration_seconds=duration)
        return None


def _fmt_millions(n: int) -> str:
    return f"{n / 1_000_000:.2f}M"


class SlackNotifier:
    """Posts to the breached job's team channel."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(5.0))

    async def close(self) -> None:
        await self._client.aclose()

    async def send(self, event: AlertEvent) -> bool:
        webhook = slack_webhook_for(event.reading.team)
        if not webhook:
            return False
        if event.type == "breach":
            payload = self._build_breach_payload(event)
        elif event.type == "resolved":
            payload = self._build_resolved_payload(event)
        else:
            return False
        try:
            r = await self._client.post(webhook, json=payload)
            return 200 <= r.status_code < 300
        except Exception as exc:
            print(f"[slack] post failed: {exc}", file=sys.stderr)
            return False

    @staticmethod
    def _build_breach_payload(event: AlertEvent) -> dict:
        r = event.reading
        oncall = slack_oncall_tag(r.team)
        env_up = r.environment.upper()
        over_pct = int(round(
            (r.lag - THRESHOLD_MESSAGES) / THRESHOLD_MESSAGES * 100
        ))
        graph_url = f"{PUBLIC_URL}/?job={urllib.parse.quote(r.job_id, safe='')}"

        heading = (
            f"{oncall} :rotating_light: *Kafka Lag Breach — {r.team}*\n"
            f"Consumer group lag on `{r.topic}` ({env_up}) has crossed "
            f"the {_fmt_millions(THRESHOLD_MESSAGES)} alert threshold as "
            f"of *{ist_clock(r.timestamp)} IST*. Current lag is "
            f"*{_fmt_millions(r.lag)}* ({over_pct:+d}% over). "
            f"Kindly investigate at the earliest and take the necessary "
            f"action to drain the lag. Triage details below."
        )

        fields = [
            {"title": "Topic", "value": r.topic, "short": False},
            {"title": "Consumer Group", "value": r.consumer_group, "short": False},
            {"title": "Environment", "value": env_up, "short": True},
            {"title": "Team", "value": r.team, "short": True},
            {"title": "Channel", "value": r.channel, "short": True},
            {"title": "Lag (max of CG / topic graphs)",
             "value": _fmt_millions(r.lag), "short": True},
            {"title": "Threshold",
             "value": _fmt_millions(THRESHOLD_MESSAGES), "short": True},
            {"title": "Time",
             "value": f"{ist_full(r.timestamp)} IST\n{iso(r.timestamp)} UTC",
             "short": True},
        ]
        attachment = {
            "color": "#f85149",
            "title": f"Breach details — {r.topic}",
            "fields": fields,
            "footer": "Kafka Consumer Lag Monitor · automated notification",
            "ts": int(r.timestamp.timestamp()),
            "actions": [
                {"type": "button",
                 "text": "📈 View Live Graph",
                 "url": graph_url,
                 "style": "primary"},
            ],
        }
        return {"text": heading, "mrkdwn": True, "attachments": [attachment]}

    @staticmethod
    def _build_resolved_payload(event: AlertEvent) -> dict:
        r = event.reading
        oncall = slack_oncall_tag(r.team)
        env_up = r.environment.upper()
        mins = int(event.duration_seconds // 60) if event.duration_seconds else 0
        if mins >= 1:
            duration_label = f"~{mins} minute{'s' if mins != 1 else ''}"
        else:
            duration_label = f"{int(event.duration_seconds)} seconds"
        graph_url = f"{PUBLIC_URL}/?job={urllib.parse.quote(r.job_id, safe='')}"

        heading = (
            f"{oncall} :white_check_mark: *Lag Drained — {r.team}*\n"
            f"Good news, team — consumer group lag on `{r.topic}` "
            f"({env_up}) has been successfully drained and is now back "
            f"below the {_fmt_millions(THRESHOLD_MESSAGES)} threshold "
            f"(currently *{_fmt_millions(r.lag)}*, as of "
            f"*{ist_clock(r.timestamp)} IST*). Total breach duration: "
            f"*{duration_label}*. Thank you for the prompt action — much "
            f"appreciated! :tada:"
        )

        fields = [
            {"title": "Topic", "value": r.topic, "short": False},
            {"title": "Environment", "value": env_up, "short": True},
            {"title": "Team", "value": r.team, "short": True},
            {"title": "Current Lag", "value": _fmt_millions(r.lag), "short": True},
            {"title": "Breach Duration", "value": duration_label, "short": True},
            {"title": "Resolved At",
             "value": f"{ist_full(r.timestamp)} IST\n{iso(r.timestamp)} UTC",
             "short": False},
        ]
        attachment = {
            "color": "#3fb950",
            "title": f"Recovery confirmed — {r.topic}",
            "fields": fields,
            "footer": "Kafka Consumer Lag Monitor · automated notification",
            "ts": int(r.timestamp.timestamp()),
            "actions": [
                {"type": "button",
                 "text": "📈 View Live Graph",
                 "url": graph_url,
                 "style": "primary"},
            ],
        }
        return {"text": heading, "mrkdwn": True, "attachments": [attachment]}


# =============================================================================
# Monitor loop
# =============================================================================
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

    def warmup(self, minutes: int = WARMUP_MINUTES) -> None:
        now = time.time()
        steps = int((minutes * 60) / POLL_INTERVAL_SECONDS)
        for i in range(steps, 0, -1):
            ts = now - i * POLL_INTERVAL_SECONDS
            for r in self.source.poll_all(at=ts):
                self._record_history(r)

    async def run(self) -> None:
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
# Wire it all up
# =============================================================================
_source: DataSource = get_data_source(catalog=JOB_CATALOG, environments=ENVIRONMENTS)
_engine = AlertEngine()
_db = AlertDB(DB_PATH)
_notifier = SlackNotifier()
_monitor = Monitor(_source, _engine, _notifier, _db)


# Chatbot — only constructed if the configured LLM provider has credentials.
# If OPENAI_API_KEY is missing, we fall back to "chatbot disabled" mode so the
# rest of the app still runs.
def _build_chatbot():
    try:
        from ai import Chatbot, ToolRegistry, build_llm_client
    except ImportError as exc:
        print(f"[chatbot] AI package import failed: {exc}", file=sys.stderr)
        return None
    try:
        llm = build_llm_client()
    except Exception as exc:
        print(f"[chatbot] disabled — {exc}", file=sys.stderr)
        return None
    tools = ToolRegistry(
        monitor=_monitor, db=_db, source=_source, threshold=THRESHOLD_MESSAGES,
    )
    return Chatbot(llm=llm, tools=tools)


_chatbot = _build_chatbot()


@asynccontextmanager
async def lifespan(_: FastAPI):
    _monitor.warmup(minutes=WARMUP_MINUTES)
    _monitor.start()
    try:
        yield
    finally:
        await _monitor.stop()
        await _notifier.close()


app = FastAPI(title="Kafka Lag Monitor", lifespan=lifespan)


# Static & root --------------------------------------------------------------
_static_dir = os.path.join(HERE, "static")
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
        "data_source": os.environ.get("DATA_SOURCE", "simulator"),
        "chatbot_available": _chatbot is not None,
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
            "injecting_stream": (
                _source.active_injection(st.job_id) or {}
            ).get("stream"),
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
@app.get("/api/job/{job_id}/history")
def job_history(job_id: str, minutes: int = 30):
    st = _monitor.jobs.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail=f"unknown job_id: {job_id}")
    minutes = max(1, min(minutes, HISTORY_RETENTION_MINUTES))   # capped to live buffer
    end_ts = time.time()
    start_ts = end_ts - minutes * 60

    series = [
        h for h in st.history
        if datetime.fromisoformat(h["ts"]).timestamp() >= start_ts
    ]
    return {
        "job_id": job_id,
        "topic": st.topic,
        "consumer_group": st.consumer_group,
        "environment": st.environment,
        "team": st.team,
        "channel": st.channel,
        "threshold": THRESHOLD_MESSAGES,
        "minutes": minutes,
        "step_seconds": POLL_INTERVAL_SECONDS,
        "history": series,
    }


# Alerts ---------------------------------------------------------------------
@app.get("/api/alerts")
def alerts(limit: int = 50, hours: int = 24):
    return {"alerts": _db.recent_alerts(limit=limit, hours=hours)}


# Team breakdown -------------------------------------------------------------
# Aggregated per-team alert counts. Mirrors the chatbot's get_team_breakdown
# tool so the dashboard can render the accountability board without going
# through the LLM.
@app.get("/api/team-breakdown")
def team_breakdown(hours: int = 168):
    hours = max(1, min(int(hours), 720))
    rows = _db.recent_alerts(limit=10_000, hours=hours)
    per_team: dict[str, dict] = {}
    for r in rows:
        t = r.get("team", "Unknown")
        bucket = per_team.setdefault(t, {
            "team": t,
            "breach_count": 0,
            "resolved_count": 0,
            "topics_affected": set(),
        })
        if r.get("alert_type") == "breach":
            bucket["breach_count"] += 1
        elif r.get("alert_type") == "resolved":
            bucket["resolved_count"] += 1
        bucket["topics_affected"].add(r.get("topic", ""))
    out = []
    for t, b in per_team.items():
        out.append({
            "team": t,
            "breach_count": b["breach_count"],
            "resolved_count": b["resolved_count"],
            "topics_affected": sorted(x for x in b["topics_affected"] if x),
        })
    out.sort(key=lambda x: -x["breach_count"])
    return {"window_hours": hours, "teams": out}


# Inject / clear (demo controls — work only with simulator) ------------------
@app.post("/api/inject/{job_id}")
def inject(job_id: str, stream: str = "cg", duration: int = 120):
    if stream not in ("cg", "topic"):
        raise HTTPException(status_code=400, detail="stream must be 'cg' or 'topic'")
    ok = _source.inject_spike(job_id, stream=stream, duration_seconds=duration)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail=(
                f"unknown job_id or current data source does not support injection: {job_id}. "
                f"(Inject only works with DATA_SOURCE=simulator.)"
            ),
        )
    return {"ok": True, "job_id": job_id, "stream": stream, "duration": duration}


@app.post("/api/clear/{job_id}")
def clear(job_id: str):
    cleared = _source.clear_injection(job_id)
    return {"ok": True, "job_id": job_id, "cleared": cleared}


# Chatbot --------------------------------------------------------------------
from routes.chat import build_chat_router
app.include_router(build_chat_router(_chatbot))


# =============================================================================
# Entrypoint
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("app:app", host=host, port=port, reload=False, log_level="info")
