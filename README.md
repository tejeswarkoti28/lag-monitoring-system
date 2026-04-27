# Kafka Consumer Lag Monitor — MVP Demo

A self-contained, locally-runnable demo of an automated replacement for the
manual Grafana sweep that takes ~2 hours per day across our Walmart Canada
catalog and PNO jobs.

The simulator runs entirely offline. The architecture is built so the **only**
thing that needs to change for production is one Python class
(`DataSource`) that today returns simulated lag values and tomorrow will
query Lenses / Prometheus.

---

## What this replaces

| Today (manual)                                  | This system                          |
|-------------------------------------------------|--------------------------------------|
| Excel sheet of ~20 Grafana dashboard links       | Job catalog as code (single list)    |
| ~80 visual graph checks per sweep                | Continuous polling every 5 s          |
| ~2 hours per day                                 | 0 minutes per day                     |
| Breaches still missed                            | 100% coverage, dedup'd Slack alerts   |
| No record of "who was breaching when"            | SQLite alert log per team / per topic |
| No accountability data                           | Per-team breach counts (24h, 7d, …)   |

---

## Setup & run

```bash
# 1. install
pip install -r requirements.txt

# 2. (optional) configure Slack — without this, alerts still appear in
#    the in-app feed, just not in Slack
cp .env.example .env
# edit .env, set SLACK_WEBHOOK_URL or per-team webhooks (no need to
# source / export — python-dotenv loads .env on startup automatically)

# 3. run
python app.py
# → open http://localhost:8000
```

`.env` is git-ignored (see `.gitignore`); never commit real webhook URLs.
Use `.env.example` as a template — the keys it ships with are the only
ones the app reads.

The dashboard pre-seeds 30 minutes of synthetic history on startup, so it
is never blank. Two jobs are pre-designated to be in active breach so the
demo immediately shows red cards and live alerts.

### End-to-end self-test

```bash
python e2e_test.py
```

Boots the server on port 8765 against a temp DB, hits every endpoint,
exercises the inject + clear flow, asserts a breach alert fires within
~5 s, then shuts down cleanly. Use this as a smoke test before the
manager pitch.

---

## Alerting model — one ping per breach + 30-min reminders until ack

The alert engine is **edge-triggered**: a single alert fires the moment lag
crosses the 4M threshold, and a single "all clear" fires when it drops back
below. While the breach is active, the engine sends **reminder pings every
30 minutes** until either:

1. someone **acknowledges** the alert, or
2. the lag drains back below the threshold (resolved).

Once acknowledged, the engine goes silent for the rest of that breach —
even if the lag stays elevated for hours. The team needs ONE response to
turn off pings, not constant attention.

Long-running breach? No problem. If the lag genuinely takes 3-4 hours to
drain and the team has acknowledged at minute 15, only **two** Slack
messages were sent: the initial alert and the eventual "all clear". If
unacknowledged, reminders keep coming every 30 min indefinitely (cadence
is `REMINDER_INTERVAL_SECONDS` in `app.py`; capacity is uncapped by default
via `MAX_REMINDERS = 0`).

### How acknowledgement works

There are **two click paths** to acknowledge — both produce the exact same
effect on the engine state:

1. **Click the ACK link inside the Slack message.** Every breach alert ends
   with `· ✅ Acknowledge` — that's a hyperlink to
   `{LAG_MONITOR_PUBLIC_URL}/ack/{token}`. Clicking opens a small browser
   confirmation page and stops further reminders for this breach.
2. **Click the `ACKNOWLEDGE` button in the dashboard's live alert feed.**
   It POSTs to `/api/ack/{alert_id}` with `by=dashboard`.

Why click links and not Slack reactions or `/ack` slash-commands? Reactions
and slash-commands require a full **Slack App** with the **Events API**
(or Slash Commands) configured, plus a publicly-reachable callback URL,
plus a bot user added to every team channel. The link-based ack is the
standard "incoming-webhook-only" pattern — works the moment you paste an
incoming-webhook URL into `.env`, no Slack-app setup needed. PagerDuty's
basic Slack integration uses the same pattern.

If you later want reaction/reply ack, the upgrade path is:

- Create a Slack App in `api.slack.com/apps` with the `app_mentions:read`,
  `reactions:read`, and `chat:write` scopes.
- Wire the Events API at `your-domain/slack/events` and verify it.
- Replace `SlackNotifier` to post via the bot token, and add an event
  handler that calls `_engine.acknowledge(...)` when a recognised reaction
  is added.

This is ~half a day of work. The link approach in the MVP costs zero
Slack-app setup and is enough to demo + pilot.

### What the team sees in Slack over a long incident

```
22:30  <!channel> 🚨 hey *Catalog Team* — lag breach on `canada-catalog-sku-events`
       (SCUS) at 22:30 IST: *6.40M* (+60% over the 4.00M threshold).
       Could someone take a look? · ✅ Acknowledge · 📈 Open dashboard

23:00  <!channel> ⚠️ *REMINDER #1* — `canada-catalog-sku-events` (SCUS) lag breach
       is *still unacknowledged* after 30m. Currently *6.40M* (+60% over). Can
       someone please pick this up? · ✅ Acknowledge · 📈 Open dashboard

23:30  <!channel> ⚠️ *REMINDER #2* — ... still unacknowledged after 1h ...
00:00  <!channel> ⚠️ *REMINDER #3* — ... still unacknowledged after 1h30m ...
00:30  <!channel> 🚨 *REMINDER #4* — ... *STILL UNACKNOWLEDGED* after 2h ...
                  (tone escalates once it crosses 2 hours)
       (somebody finally clicks ✅ Acknowledge — silence from this point)

03:15  <!channel> ✅ *Catalog Team* — `canada-catalog-sku-events` (SCUS)
       recovered to *1.80M* at 03:15 IST. Breach lasted ~4h45m.
```

Total messages over a 4-hour-45-minute incident: **6** (initial + 4
reminders + recovery). Without the engine, you'd be paging humans every
poll cycle, every 5 seconds, for 4+ hours — a recipe for alert fatigue.

### Audit data

Every alert (initial + each reminder + each resolved) is persisted to
SQLite with an `acknowledged_at` and `acknowledged_by` column. That gives
you the building blocks for **MTTA (mean time to acknowledge)** reporting
per team and per topic — turn into a dashboard column whenever you're ready.

---

## Manager demo script — 6-minute pitch

Run the server, open the dashboard, and walk through this in order.

### 1. Open the dashboard (15 s)

Land on `http://localhost:8000`. Talk track:

> "This is the same data my Excel sheet covers — 9 topics across `eus`
> and `scus`, 18 jobs total. The grid you're looking at would take me
> ~2 hours to sweep manually today. The system polls all 18 every 5
> seconds, continuously."

Point to:
- The **stat strip** — jobs monitored / breaching / healthy / alerts (24h)
- The **polling indicator** — top right, pulsing green dot
- The **2 red cards** — these are pre-seeded at breach to mirror the
  state we hit in production yesterday (or today, depending on timing).

### 2. Click into a breaching card (45 s)

> "Today, when I see a possible breach in Grafana, I have to open
> *two* graphs to confirm — Consumer Group Lag and Consumer Group /
> Topic Lag. The system already does this for me, but the modal
> shows both graphs side-by-side because that matches the workflow
> you've been seeing for years."

The modal shows both graphs with full axes:
- **Y-axis** ticks in millions of messages, with the 4M threshold
  drawn as a dashed orange line and labelled.
- **X-axis** is real time (UTC), 6 tick marks spanning the last
  30 minutes.
- A pinned label on the latest data point shows the current value
  and timestamp.
- Hover anywhere on the chart for a crosshair + tooltip with the
  exact lag value and second-resolution timestamp at that point.

Close the modal.

### 3. Show the alert feed (30 s)

> "Right side is a live alert feed. Each event has the topic, the
> environment, the team, the channel, the lag value, and a green
> ✓ DELIVERED tag if Slack accepted the post. Everything here is also
> persisted to SQLite, which gives us the next thing I want to show you."

### 4. Show the team accountability board (30 s)

> "This is the bit we don't have today. Per-team breach counts,
> last 24 hours. Today, when lag is high, I send a Slack message that
> says 'please look into this'. With this data, it's a different
> conversation: 'Catalog team, you breached 14 times last week, here
> are the topics, here is the duration.' That's an SLA conversation,
> not a please-fix-it conversation."

### 5. Live demo — inject a spike (90 s — **this is the moment**)

In the control panel under the job grid:
1. Pick a healthy job from the dropdown — pick a deliberately unrelated
   one to make the routing point. Recommended:
   `canada-pno-offeringestion-events :: EUS` (PNO Team).
2. Click **Inject Spike**.

> "I've just told the system that this job's lag has crossed 4M.
> Watch the card."

Within 5 seconds:
- The chosen card flips to red, sparkline jumps above the threshold line.
- A new `▲ BREACH` entry slides into the alert feed on the right.
- If Slack is configured, a richly-formatted message lands in
  `#pno-team` (or the appropriate team's channel). Pull up Slack
  side-by-side for full effect.
- The `DELIVERED ✓` tag appears next to the alert.

> "Three things to notice: the routing is per-team — that alert went
> to PNO, not to Catalog. The Slack message has the topic, the
> consumer group, the environment, and the lag value — everything you
> need to triage without opening Grafana. And the dedup logic means
> if this stays in breach, we won't spam — re-alerts only every 30
> minutes."

After ~2 minutes the injection clears and a green `✓ RESOLVED` alert
fires.

### 6. The migration story (30 s)

> "Everything you've seen runs on simulated data. Pointing this at
> production means replacing one class — `DataSource.poll_all()` —
> with a Prometheus query against the Lenses metrics endpoint. The
> alert engine, the Slack routing, the SQLite log, the dashboard,
> the per-team accountability board — all of that stays unchanged.
> Today: 2 hours of manual work and missed breaches. After we ship
> this: continuous coverage, zero manual time, and SLA-grade data we
> don't have today."

---

## ROI talking points (one-liners)

- **Time:** 2 hrs/day × 250 working days = **500 hrs/year reclaimed**.
- **Coverage:** 80 manual checks per sweep → **~17,000 checks/day**, 100% coverage.
- **Detection latency:** worst-case ~2 hrs (next sweep) → **5 seconds**.
- **Net new capability:** per-team accountability data, not possible from the manual workflow.
- **Migration risk:** isolated to one class (`DataSource`); all other components are reused.

---

## What to change when you get production access

Two places, by design:

### 1. `JOB_CATALOG` in `app.py`
   The list of `(topic, consumer_group, team, channel)` tuples. This is
   the same set we already track in the Excel sheet. Add or remove
   entries here.

### 2. The `DataSource` class in `app.py`
   Replace the simulated `poll_all()` with a real implementation that
   queries Lenses / Prometheus. The return type — `list[LagReading]` —
   does not change. Sketch:

   ```python
   class DataSource:
       def __init__(self, prom_url: str): self.prom_url = prom_url
       def poll_all(self) -> list[LagReading]:
           # PromQL: kafka_consumergroup_lag{topic="...", consumergroup="...", env="..."}
           # Build a LagReading for each (topic, consumer_group, env) in JOB_CATALOG.
           ...
   ```

   Everything else — the alert engine, the dedup window, the SQLite
   log, the Slack notifier, the dashboard, the team accountability
   board — keeps working unchanged.

---

## API reference

| Method | Path                                       | Purpose                              |
|--------|--------------------------------------------|--------------------------------------|
| GET    | `/`                                        | Dashboard HTML                       |
| GET    | `/api/health`                              | Liveness + config snapshot           |
| GET    | `/api/status`                              | All jobs, current lag, summary       |
| GET    | `/api/job/{job_id}/history?minutes=30`     | Time series for one job              |
| GET    | `/api/alerts?limit=50`                     | Recent alerts, newest first          |
| GET    | `/api/team-breakdown?hours=24`             | Per-team breach counts               |
| POST   | `/api/inject/{job_id}?duration=120`        | Push a job above threshold (demo)    |
| POST   | `/api/clear/{job_id}`                      | Cancel an active injection           |

`job_id` format: `<topic>::<environment>` — e.g.
`canada-catalog-sku-events::scus`.

---

## File layout

```
app.py                # all backend logic, in clearly-divided sections
static/index.html     # dashboard (HTML + CSS + JS, no build step)
requirements.txt      # fastapi, uvicorn, httpx, python-dotenv
.env.example          # template for SLACK_WEBHOOK_* env vars
.env                  # local secrets (git-ignored)
.gitignore            # excludes .env, *.db, __pycache__, etc.
e2e_test.py           # end-to-end smoke test
lag_monitor.db        # created on first run; SQLite alert log
```

Run with `python app.py` from this directory; `uvicorn` is invoked
internally on `0.0.0.0:8000` (override with `HOST` / `PORT`).
