"""
core/config.py — All configuration constants, environment variables,
time helpers, and Slack routing helpers.

Everything here is pure (no I/O after startup) so it can be imported
safely by any other module without risk of circular imports.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta as _td, timezone
from typing import Optional

# Project root — two levels up from this file (core/config.py → core/ → root)
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG_PATH = os.environ.get(
    "LAG_MONITOR_CONFIG",
    os.path.join(HERE, "config", "jobs.json"),
)


def _load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


_cfg = _load_config(CONFIG_PATH)

# Real Walmart consumer-group lag values run in the low thousands per partition,
# not millions. The env override exists so you can tune without editing JSON.
THRESHOLD_MESSAGES: int = int(
    os.environ.get("LAG_THRESHOLD") or _cfg.get("threshold_messages", 5000)
)
JOB_CATALOG: list[dict] = list(_cfg.get("jobs", []))

POLL_INTERVAL_SECONDS: float = float(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
_UI_POLLS_PER_CYCLE: int = max(1, int(os.environ.get("UI_POLLS_PER_CYCLE", "3")))
UI_POLL_INTERVAL_SECONDS: float = POLL_INTERVAL_SECONDS / _UI_POLLS_PER_CYCLE
PUBLIC_URL: str = os.environ.get(
    "LAG_MONITOR_PUBLIC_URL", "http://localhost:8000"
).rstrip("/")
DB_PATH: str = os.environ.get("LAG_MONITOR_DB", "lag_monitor.db")


# ---------------------------------------------------------------------------
# Slack routing helpers
# Convention: team name → env var key via  team.upper().replace(" ", "_")
# "Team"         → SLACK_WEBHOOK_TEAM  /  SLACK_ONCALL_TEAM
# "Catalog Team" → SLACK_WEBHOOK_CATALOG_TEAM  /  SLACK_ONCALL_CATALOG_TEAM
# ---------------------------------------------------------------------------

def _team_env_key(team: str) -> str:
    return team.upper().replace(" ", "_")


def slack_webhook_for(team: str) -> Optional[str]:
    url = os.environ.get(f"SLACK_WEBHOOK_{_team_env_key(team)}")
    return url or os.environ.get("SLACK_WEBHOOK_URL") or None


def slack_oncall_tag(team: str) -> str:
    v = os.environ.get(f"SLACK_ONCALL_{_team_env_key(team)}")
    return v.strip() if v else "<!channel>"


def slack_configured() -> bool:
    return any(
        v for k, v in os.environ.items()
        if k.startswith("SLACK_WEBHOOK_") and v
    )


# ---------------------------------------------------------------------------
# Time helpers — all timestamps are UTC internally; display converts to IST
# ---------------------------------------------------------------------------

IST = timezone(_td(hours=5, minutes=30), name="IST")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.replace(microsecond=0).isoformat()


def to_ist(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(IST)


def ist_clock(ts: datetime) -> str:
    return to_ist(ts).strftime("%H:%M")


def ist_full(ts: datetime) -> str:
    return to_ist(ts).strftime("%Y-%m-%d %H:%M:%S")
