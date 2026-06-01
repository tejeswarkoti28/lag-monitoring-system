"""
core/monitor.py — The background polling loop.

Monitor.run() ticks every POLL_INTERVAL_SECONDS:
  1. Asks the DataSource for the latest lag readings
  2. Passes each reading through AlertEngine
  3. If an event fires, sends it to Slack and persists it in AlertDB
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional

from core.alerting import AlertEngine, AlertEvent, SlackNotifier
from core.config import POLL_INTERVAL_SECONDS, now_utc
from core.db import AlertDB
from data_sources import DataSource, LagReading


@dataclass
class JobState:
    job_id: str
    topic: str
    consumer_group: str
    environment: str
    team: str
    channel: str
    description: str = ""
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
        self.last_poll_ts: Optional[object] = None
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

        for j in source.jobs():
            self.jobs[j["job_id"]] = JobState(
                job_id=j["job_id"],
                topic=j["topic"],
                consumer_group=j["consumer_group"],
                environment=j["environment"],
                team=j["team"],
                channel=j["channel"],
                description=j.get("description", ""),
            )

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
            st = self.jobs.get(r.job_id)
            if st:
                st.current = r
            event = self.engine.evaluate(r)
            if event is not None:
                await self._handle_event(event)
        self.last_poll_ts = now_utc()

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
