"""
routes/status.py — API endpoints that return live operational data.

Endpoints:
  GET  /api/health          — liveness check + feature flags
  GET  /api/status          — per-job lag + summary (used by dashboard every 5s)
  GET  /api/alerts          — recent breach/resolved events from SQLite
  GET  /api/team-breakdown  — per-team alert counts for a rolling window
  GET  /api/topics          — topic catalog (populates the topic dropdown)
  GET  /api/panels          — panel definitions (tells the frontend what charts to draw)
  POST /api/slack/test      — sends a test message to every configured webhook
"""
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from core.config import (
    JOB_CATALOG,
    POLL_INTERVAL_SECONDS,
    THRESHOLD_MESSAGES,
    _team_env_key,
    iso,
    slack_configured,
    slack_webhook_for,
)


def build_status_router(monitor, db, notifier, panels, chatbot) -> APIRouter:
    """
    monitor  — Monitor instance (for live job state)
    db       — AlertDB instance (for alert history)
    notifier — SlackNotifier instance (for the test endpoint)
    panels   — PanelRegistry instance (for /api/panels)
    chatbot  — Chatbot | None (so /api/health can report chatbot_available)
    """
    router = APIRouter()

    @router.get("/api/health")
    def health():
        return {
            "ok": True,
            "last_poll_at": iso(monitor.last_poll_ts) if monitor.last_poll_ts else None,
            "slack_configured": slack_configured(),
            "threshold": THRESHOLD_MESSAGES,
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
            "jobs_monitored": len(monitor.jobs),
            "data_source": "lenses",
            "chatbot_available": chatbot is not None,
        }

    @router.get("/api/status")
    def status():
        items = []
        breaching = 0
        for st in monitor.jobs.values():
            cur = st.current
            is_breach = bool(cur and cur.lag >= THRESHOLD_MESSAGES)
            if is_breach:
                breaching += 1
            items.append({
                "job_id": st.job_id,
                "topic": st.topic,
                "consumer_group": st.consumer_group,
                "environment": st.environment,
                "description": st.description,
                "team": st.team,
                "channel": st.channel,
                "lag": cur.lag if cur else 0,
                "consumer_group_lag": cur.consumer_group_lag if cur else 0,
                "topic_lag": cur.topic_lag if cur else 0,
                "status": "breach" if is_breach else "ok",
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
                "alerts_24h": db.count_in_last_hours(24, "breach"),
                "last_poll_at": iso(monitor.last_poll_ts) if monitor.last_poll_ts else None,
                "slack_configured": slack_configured(),
                "threshold": THRESHOLD_MESSAGES,
            },
        }

    @router.get("/api/alerts")
    def alerts(limit: int = 50, hours: int = 24):
        return {"alerts": db.recent_alerts(limit=limit, hours=hours)}

    @router.get("/api/team-breakdown")
    def team_breakdown(hours: int = 168):
        hours = max(1, min(int(hours), 720))
        rows = db.recent_alerts(limit=10_000, hours=hours)
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
        out = [
            {
                "team": t,
                "breach_count": b["breach_count"],
                "resolved_count": b["resolved_count"],
                "topics_affected": sorted(x for x in b["topics_affected"] if x),
            }
            for t, b in per_team.items()
        ]
        out.sort(key=lambda x: -x["breach_count"])
        return {"window_hours": hours, "teams": out}

    @router.get("/api/topics")
    def topics():
        envs = sorted({env for j in JOB_CATALOG for env in j.get("environments", [])})
        return {
            "environments": envs,
            "topics": [
                {
                    "topic": entry["topic"],
                    "consumer_group": entry["consumer_group"],
                    "description": entry.get("description", ""),
                    "team": entry.get("team", ""),
                    "channel": entry.get("channel", ""),
                }
                for entry in JOB_CATALOG
            ],
        }

    @router.get("/api/panels")
    def panels_list():
        return {
            "panels": panels.to_json(),
            "sections": list(panels.sections().keys()),
        }

    @router.post("/api/slack/test")
    async def slack_test():
        teams_seen: dict[str, str] = {}  # webhook_url -> label (dedupe duplicate URLs)

        for team_name in {j.get("team", "") for j in JOB_CATALOG if j.get("team")}:
            url = os.environ.get(f"SLACK_WEBHOOK_{_team_env_key(team_name)}")
            if url:
                teams_seen.setdefault(url, team_name)

        default_url = os.environ.get("SLACK_WEBHOOK_URL")
        if default_url:
            teams_seen.setdefault(default_url, "default")

        if not teams_seen:
            return JSONResponse(
                {
                    "ok": False,
                    "error": (
                        "No Slack webhooks configured. Set SLACK_WEBHOOK_URL or "
                        "SLACK_WEBHOOK_<TEAM> in .env, then restart."
                    ),
                    "results": [],
                },
                status_code=400,
            )

        results = []
        for url, label in teams_seen.items():
            delivered, detail = await notifier.send_test(url, label)
            results.append({"label": label, "delivered": delivered, "detail": detail})
        overall_ok = all(r["delivered"] for r in results)
        return {"ok": overall_ok, "results": results}

    return router
