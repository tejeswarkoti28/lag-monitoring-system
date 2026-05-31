"""
core/alerting.py — Alert detection and Slack notification.

AlertEngine  — pure edge-trigger: fires exactly one event per threshold crossing.
SlackNotifier — posts breach/resolved messages to the right team channel.
"""
from __future__ import annotations

import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Optional

import httpx

from core.config import (
    THRESHOLD_MESSAGES,
    PUBLIC_URL,
    slack_webhook_for,
    slack_oncall_tag,
    ist_clock,
    ist_full,
    iso,
    now_utc,
)
from data_sources import LagReading


# =============================================================================
# Alert engine — pure edge-trigger, one alert per crossing
# =============================================================================

@dataclass
class _BreachState:
    first_breached_at: float   # unix timestamp when the breach started


@dataclass
class AlertEvent:
    type: str                  # "breach" | "resolved"
    reading: LagReading
    duration_seconds: float = 0.0


class AlertEngine:
    def __init__(self) -> None:
        # Maps job_id → _BreachState; only present while the job is in breach
        self._state: dict[str, _BreachState] = {}

    def evaluate(self, reading: LagReading) -> Optional[AlertEvent]:
        """Called once per job per poll. Returns an event or None."""
        ts = reading.timestamp.timestamp()
        in_breach = reading.lag >= THRESHOLD_MESSAGES
        prev = self._state.get(reading.job_id)

        if in_breach:
            if prev is None:
                self._state[reading.job_id] = _BreachState(first_breached_at=ts)
                return AlertEvent(type="breach", reading=reading)
            return None  # already in breach — stay silent

        if prev is not None:
            duration = ts - prev.first_breached_at
            del self._state[reading.job_id]
            return AlertEvent(type="resolved", reading=reading, duration_seconds=duration)

        return None  # healthy and was healthy before — nothing to report


# =============================================================================
# Slack notifier
# =============================================================================

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

    async def send_test(self, webhook_url: str, label: str) -> tuple[bool, str]:
        """Send a clearly-marked test message to verify a webhook works."""
        payload = {
            "text": (
                f":test_tube: *Slack Integration Test — {label}*\n"
                f"This is a test message from the Kafka Consumer Lag Monitor at "
                f"*{ist_clock(now_utc())} IST*. If you see this, alerts for "
                f"*{label}* are wired up correctly. No action required."
            ),
            "mrkdwn": True,
            "attachments": [{
                "color": "#58a6ff",
                "footer": "Kafka Consumer Lag Monitor · webhook verification",
                "ts": int(time.time()),
            }],
        }
        try:
            r = await self._client.post(webhook_url, json=payload)
            if 200 <= r.status_code < 300:
                return True, "ok"
            return False, f"http_{r.status_code}"
        except Exception as exc:
            print(f"[slack] test post failed: {exc}", file=sys.stderr)
            return False, type(exc).__name__

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
                {"type": "button", "text": "📈 View Live Graph",
                 "url": graph_url, "style": "primary"},
            ],
        }
        return {"text": heading, "mrkdwn": True, "attachments": [attachment]}

    @staticmethod
    def _build_resolved_payload(event: AlertEvent) -> dict:
        r = event.reading
        oncall = slack_oncall_tag(r.team)
        env_up = r.environment.upper()
        mins = int(event.duration_seconds // 60) if event.duration_seconds else 0
        duration_label = (
            f"~{mins} minute{'s' if mins != 1 else ''}" if mins >= 1
            else f"{int(event.duration_seconds)} seconds"
        )
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
                {"type": "button", "text": "📈 View Live Graph",
                 "url": graph_url, "style": "primary"},
            ],
        }
        return {"text": heading, "mrkdwn": True, "attachments": [attachment]}
