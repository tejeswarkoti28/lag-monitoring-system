"""
routes/history.py — Time-series chart endpoints.

Endpoints:
  GET /api/job/{job_id}/history      — downsampled lag history for a single job
  GET /api/panel/{panel_id}/range    — PromQL range query for a declarative panel
"""
from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, HTTPException

from core.config import HISTORY_RETENTION_DAYS, THRESHOLD_MESSAGES
from core.db import ResponseCache, bucket_seconds_for
from data_sources.prometheus import PrometheusDataSource
from panels import Panel

_cache = ResponseCache(ttl=30.0)


def _panel_step_for(minutes: int) -> float:
    """Pick a step that yields ~500-720 points per range, like Grafana."""
    if minutes <= 30:      return 5
    if minutes <= 360:     return 30
    if minutes <= 720:     return 60
    if minutes <= 1440:    return 120
    if minutes <= 2880:    return 300
    if minutes <= 21_600:  return 1800
    if minutes <= 43_200:  return 3600
    if minutes <= 129_600: return 14400
    return 28800


def build_history_router(monitor, history_db, panels, data_sources) -> APIRouter:
    """
    monitor      — Monitor instance (for job metadata lookup)
    history_db   — HistoryDB instance
    panels       — PanelRegistry instance
    data_sources — dict[str, DataSource] (all configured sources)
    """
    router = APIRouter()

    @router.get("/api/job/{job_id}/history")
    def job_history(job_id: str, minutes: int = 30):
        st = monitor.jobs.get(job_id)
        if st is None:
            raise HTTPException(status_code=404, detail=f"unknown job_id: {job_id}")
        max_minutes = HISTORY_RETENTION_DAYS * 24 * 60
        minutes = max(1, min(minutes, max_minutes))

        cache_key = f"hist:{job_id}:{minutes}"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

        end_ts = time.time()
        start_ts = end_ts - minutes * 60
        bucket = bucket_seconds_for(minutes)
        series = history_db.query(
            job_id=job_id, start_ts=start_ts, end_ts=end_ts, bucket_seconds=bucket,
        )
        result = {
            "job_id": job_id,
            "topic": st.topic,
            "consumer_group": st.consumer_group,
            "environment": st.environment,
            "team": st.team,
            "channel": st.channel,
            "threshold": THRESHOLD_MESSAGES,
            "minutes": minutes,
            "step_seconds": bucket,
            "history": series,
        }
        _cache.set(cache_key, result)
        return result

    @router.get("/api/panel/{panel_id}/range")
    def panel_range(
        panel_id: str,
        minutes: int = 720,
        env: Optional[str] = None,
        topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
        step_seconds: Optional[int] = None,
    ):
        try:
            panel = panels.get(panel_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

        ds = data_sources.get(panel.data_source)
        if ds is None:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"panel {panel.id!r} references unknown data source "
                    f"{panel.data_source!r}"
                ),
            )
        if not isinstance(ds, PrometheusDataSource):
            raise HTTPException(
                status_code=500,
                detail="only PrometheusDataSource supports range queries today",
            )

        minutes = max(1, min(int(minutes), 60 * 24 * 90))
        end_ts = time.time()
        start_ts = end_ts - minutes * 60
        step = float(step_seconds) if step_seconds else _panel_step_for(minutes)

        try:
            expr = panel.build_query(
                static_labels=ds.static_labels,
                env=env, topic=topic, consumer_group=consumer_group,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        cache_key = f"panel:{panel_id}:{minutes}:{env}:{topic}:{consumer_group}"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached

        series = ds.query_range(
            expr, start_ts=start_ts, end_ts=end_ts, step_seconds=step,
        )
        result = {
            "panel_id": panel_id,
            "title": panel.title,
            "expr": expr,
            "scope": {"env": env, "topic": topic, "consumer_group": consumer_group},
            "minutes": minutes,
            "step_seconds": step,
            "unit": panel.unit,
            "y_min": panel.y_min,
            "y_max": panel.y_max,
            "color": panel.color,
            "show_threshold": panel.show_threshold,
            "threshold": THRESHOLD_MESSAGES,
            "points": [{"ts": ts, "value": v} for ts, v in series],
        }
        _cache.set(cache_key, result)
        return result

    return router
