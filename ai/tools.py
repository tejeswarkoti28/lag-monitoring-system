"""
Tools the chatbot can call.

Each tool is:
  1. A JSON-schema definition (TOOL_DEFS) that the LLM sees.
  2. A method on ToolRegistry that actually fetches the data.

The registry holds references to the live monitor, AlertDB, and DataSource so
tools query the same data the rest of the app sees — no duplication.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Optional


# OpenAI tool-spec format. Other providers (Anthropic, Gemini) accept slightly
# different schemas; the LLMClient implementation adapts as needed.
TOOL_DEFS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_current_status",
            "description": (
                "Get the current state of all monitored Kafka jobs — current lag, "
                "breach status, team, environment, and last-poll timestamp. "
                "Use this for any 'what is broken right now' / 'who is healthy' question."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_alerts",
            "description": (
                "Return recent breach + resolved alerts from the alert log. "
                "Filter by team and time window. Use this for trend questions, "
                "postmortems, or 'how many breaches did X have'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "team": {
                        "type": "string",
                        "description": "Optional team filter, e.g. 'Catalog Team', 'PNO Team', 'Shipping Team'.",
                    },
                    "hours": {
                        "type": "integer",
                        "description": "Lookback window in hours. Default 24, max 720 (30d).",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return. Default 50, max 500.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_team_breakdown",
            "description": (
                "Aggregated breach counts per team over a time window. Use this for "
                "accountability or comparison questions ('which team breached most this week')."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "hours": {
                        "type": "integer",
                        "description": "Window in hours. Default 168 (7d).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_job_history",
            "description": (
                "Time-series lag readings for a single job over the last N minutes. "
                "Use this when explaining a specific graph or analyzing a breach shape "
                "(burst vs sustained vs rebalance)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Job identifier in the form '<topic>::<environment>', e.g. 'canada-catalog-sku-events::scus'.",
                    },
                    "minutes": {
                        "type": "integer",
                        "description": "Window in minutes. Default 60.",
                    },
                },
                "required": ["job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_jobs",
            "description": (
                "List every monitored job with topic, consumer group, environment, "
                "and team. Use this when the user asks 'what do you monitor' / "
                "'what jobs does Catalog Team own'."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
]


class ToolRegistry:
    """Executes tool calls against the running app's data layer."""

    def __init__(
        self,
        *,
        monitor: Any,
        db: Any,
        history_db: Any,
        source: Any,
        threshold: int,
    ) -> None:
        self.monitor = monitor
        self.db = db
        self.history_db = history_db
        self.source = source
        self.threshold = threshold

    # ---- dispatcher --------------------------------------------------------
    def execute(self, name: str, arguments: dict) -> Any:
        method = getattr(self, name, None)
        if method is None or not callable(method):
            return {"error": f"unknown tool: {name}"}
        try:
            return method(**(arguments or {}))
        except TypeError as exc:
            return {"error": f"bad arguments for {name}: {exc}"}
        except Exception as exc:
            return {"error": f"tool {name} failed: {exc}"}

    # ---- tool implementations ---------------------------------------------
    def get_current_status(self) -> dict:
        items = []
        breaching = 0
        for st in self.monitor.jobs.values():
            cur = st.current
            is_breach = bool(cur and cur.lag >= self.threshold)
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
                "timestamp": cur.timestamp.replace(microsecond=0).isoformat() if cur else None,
            })
        return {
            "summary": {
                "monitored": len(items),
                "breaching": breaching,
                "healthy": len(items) - breaching,
                "threshold": self.threshold,
            },
            "jobs": items,
        }

    def get_recent_alerts(
        self,
        team: Optional[str] = None,
        hours: int = 24,
        limit: int = 50,
    ) -> dict:
        hours = max(1, min(int(hours), 720))
        limit = max(1, min(int(limit), 500))
        # AlertDB.recent_alerts already has a window arg
        rows = self.db.recent_alerts(limit=limit, hours=hours)
        if team:
            team_lower = team.lower().strip()
            rows = [r for r in rows if (r.get("team", "").lower().strip() == team_lower)]
        return {
            "window_hours": hours,
            "team_filter": team,
            "count": len(rows),
            "alerts": rows,
        }

    def get_team_breakdown(self, hours: int = 168) -> dict:
        hours = max(1, min(int(hours), 720))
        rows = self.db.recent_alerts(limit=10_000, hours=hours)
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

    def get_job_history(self, job_id: str, minutes: int = 60) -> dict:
        minutes = max(1, min(int(minutes), 60 * 24 * 90))   # cap at 90 days
        st = self.monitor.jobs.get(job_id)
        if st is None:
            return {"error": f"unknown job_id: {job_id}"}
        end_ts = time.time()
        start_ts = end_ts - minutes * 60
        # Use the same bucketing the dashboard uses, so the LLM sees the
        # same shape the human would see. For very long ranges keep the
        # payload small so it fits in the model's context window.
        from app import bucket_seconds_for     # local import to avoid cycle at module load
        bucket = bucket_seconds_for(minutes)
        series = self.history_db.query(
            job_id=job_id, start_ts=start_ts, end_ts=end_ts, bucket_seconds=bucket,
        )
        # Cap at 120 points so the LLM context stays small even for 6-month windows
        if len(series) > 120:
            stride = max(1, len(series) // 120)
            series = series[::stride]
        return {
            "job_id": job_id,
            "topic": st.topic,
            "consumer_group": st.consumer_group,
            "environment": st.environment,
            "team": st.team,
            "minutes": minutes,
            "bucket_seconds": bucket,
            "threshold": self.threshold,
            "points": len(series),
            "history": series,
        }

    def list_jobs(self) -> dict:
        items = [{
            "job_id": st.job_id,
            "topic": st.topic,
            "consumer_group": st.consumer_group,
            "environment": st.environment,
            "team": st.team,
            "channel": st.channel,
        } for st in self.monitor.jobs.values()]
        return {"count": len(items), "jobs": items}
