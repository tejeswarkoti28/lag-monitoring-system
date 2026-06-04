"""
routes/status.py — API endpoints that return live operational data.

Endpoints:
  GET  /api/health          — liveness check + feature flags
  GET  /api/status          — per-job lag + summary (used by dashboard every 15s)
  GET  /api/breach-count    — actual breach count from data source (last 24h)
  GET  /api/alerts          — breach alerts sent by this app (SQLite)
  GET  /api/team-breakdown  — per-team alert counts for a rolling window
  GET  /api/topics          — topic catalog (populates the topic dropdown)
  GET  /api/panels          — panel definitions (tells the frontend what charts to draw)
  POST /api/slack/test      — sends a test message to every configured webhook
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from core.config import (
    JOB_CATALOG,
    POLL_INTERVAL_SECONDS,
    UI_POLL_INTERVAL_SECONDS,
    THRESHOLD_MESSAGES,
    _team_env_key,
    iso,
    slack_configured,
    slack_webhook_for,
)
from core.db import ResponseCache

_breach_cache = ResponseCache(ttl=300.0)  # 5-minute cache for breach count


def build_status_router(monitor, db, notifier, panels, chatbot) -> APIRouter:
    router = APIRouter()

    @router.get("/api/health")
    def health():
        return {
            "ok": True,
            "last_poll_at": iso(monitor.last_poll_ts) if monitor.last_poll_ts else None,
            "slack_configured": slack_configured(),
            "threshold": THRESHOLD_MESSAGES,
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
            "ui_poll_interval_seconds": UI_POLL_INTERVAL_SECONDS,
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
            })
        items.sort(key=lambda j: (0 if j["status"] == "breach" else 1, j["topic"], j["environment"]))
        return {
            "jobs": items,
            "summary": {
                "monitored": len(items),
                "breaching": breaching,
                "healthy": len(items) - breaching,
                "last_poll_at": iso(monitor.last_poll_ts) if monitor.last_poll_ts else None,
                "slack_configured": slack_configured(),
                "threshold": THRESHOLD_MESSAGES,
            },
        }

    @router.get("/api/breach-count")
    def breach_count(hours: int = 24):
        """Count actual breach periods from the data source for the given window.
        Cached for 5 minutes — independent of app uptime.
        """
        hours = max(1, min(int(hours), 720))
        cache_key = f"breach_count:{hours}"
        cached = _breach_cache.get(cache_key)
        if cached is not None:
            return cached

        from data_sources.prometheus import PrometheusDataSource
        if not isinstance(monitor.source, PrometheusDataSource):
            return {"hours": hours, "breach_periods": 0}

        end_ts = time.time()
        start_ts = end_ts - hours * 3600
        step_seconds = max(60, (hours * 3600) // 300)

        count = 0
        for st in monitor.jobs.values():
            labels = ",".join([
                f'job="{st.job}"',
                f'ooa="{st.ooa}"',
                f'oop="{st.oop}"',
                f'ooe="{st.environment}"',
                f'topic="{st.topic}"',
                f'consumerGroup="{st.consumer_group}"',
            ])
            expr = f'max(lenses_topic_consumer_lag{{{labels}}})'
            series = monitor.source.query_range(
                expr, start_ts=start_ts, end_ts=end_ts, step_seconds=step_seconds
            )
            in_breach = False
            for _, val in series:
                if int(val) >= THRESHOLD_MESSAGES:
                    if not in_breach:
                        in_breach = True
                        count += 1
                else:
                    in_breach = False

        result = {"hours": hours, "breach_periods": count}
        _breach_cache.set(cache_key, result)
        return result

    @router.get("/api/alerts")
    def alerts(limit: int = 50, hours: int = 24):
        """Returns breach alerts sent by this app. Excludes resolved events."""
        rows = db.recent_alerts(limit=limit, hours=hours, alert_type="breach")
        return {"alerts": rows}

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
                    "environments": entry.get("environments", []),
                    "job": entry["job"],
                    "ooa": entry["ooa"],
                    "oop": entry["oop"],
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
        teams_seen: dict[str, str] = {}

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
