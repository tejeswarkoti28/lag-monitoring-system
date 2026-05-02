# Kafka Consumer Lag Monitor

An AI-augmented replacement for the manual Grafana sweep that takes ~2 hours/day
across Walmart Canada's Catalog and Price-and-Offer Kafka jobs.

- **Continuous polling** — 18 consumer groups (9 topics × 2 environments), every 5 seconds, against a real Lenses cluster.
- **Edge-triggered alerts** — one Slack ping per breach, one per recovery, per-team channel routing.
- **Per-team accountability** — every alert persisted to SQLite; per-team breach counts queryable via API or chatbot.
- **AI assistant** — a Gemini-powered chatbot inside the dashboard that answers questions about live status, trends, and historical incidents using read-only tool calls against your own data.
- **9 chart time-ranges** — 30m / 6h / 12h / 24h / 2d / 15d / 1mo / 3mo / 6mo, all backed by the local time-series we own (Lenses doesn't keep history; we do).

---

## What this replaces

| Today (manual)                              | This system                              |
|---------------------------------------------|------------------------------------------|
| Excel sheet of dashboard links              | Job catalog as code — `config/jobs.json` |
| ~80 visual graph checks per sweep           | Continuous polling every 5 s             |
| ~2 hours per day                            | 0 minutes per day                        |
| Breaches occasionally missed                | 100% coverage with Slack delivery        |
| No record of "who breached when"            | SQLite alert log + history time-series   |
| No accountability data                      | Per-team breach counts (24h / 7d / 30d)  |
| No way to ask "is this normal?"             | Ask the AI assistant                     |

---

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# fill in LENSES_URL + auth, GEMINI_API_KEY, Slack webhooks
python app.py
# → http://localhost:8000
```

Required env vars (full list in [`.env.example`](.env.example)):

| Var | Purpose |
|---|---|
| `LENSES_URL` | Base URL of your Lenses install. **App will not start without this.** |
| `LENSES_API_TOKEN` *or* `LENSES_USERNAME`+`LENSES_PASSWORD` | Lenses auth |
| `GEMINI_API_KEY` | Free at https://ai.google.dev. Without it the dashboard works; AI button is greyed out. |
| `SLACK_WEBHOOK_*` | Per-team incoming-webhook URLs. Without these, alerts are in-app/DB only — supported mode. |

---

## Architecture

```
LensesDataSource.poll_all()  →  AlertEngine.evaluate()  →  SlackNotifier.send()
                                                        →  AlertDB.insert_alert()
                             →  in-memory ring (60 min, used for live sparklines)
                             →  HistoryDB.insert_batch  (used for chart time-ranges)

POST /api/chat → Chatbot.reply() → Gemini ←─ tool calls ─→ ToolRegistry
                                                        ↓
                                            Monitor / AlertDB / HistoryDB
```

Two pluggable seams:

- **Data source** — [`data_sources/`](data_sources/). Today: Lenses only. Add Prometheus, Confluent Cloud, or kafka-python as a sibling file + one factory line.
- **LLM provider** — [`ai/llm_client.py`](ai/llm_client.py). Today: Gemini default, OpenAI swappable. Add Anthropic / Vertex / local Ollama as a new client class + one factory branch.

The internal message and tool-spec format follows OpenAI's chat-completions schema. `GeminiClient` adapts to Gemini's native shape internally so the rest of the chatbot stays provider-agnostic.

---

## Historical lag persistence

**Lenses does not store historical lag — only the current value.** So we own the time series.

Every 5-second poll appends 18 rows to a `lag_history` table in the same SQLite file as `alerts`. The dashboard's 9 time-range buttons all read from there with server-side bucketing, so each chart returns ~500–720 points regardless of window.

| Range | Bucket | Approx points |
|---|---|---|
| 30m | raw 5s | 360 |
| 6h | 30s | 720 |
| 12h | 1m | 720 |
| 24h | 2m | 720 |
| 2d | 5m | 576 |
| 15d | 30m | 720 |
| 1mo | 1h | 720 |
| 3mo | 4h | 540 |
| 6mo | 8h | 540 |

Storage: ~30 MB/day. 90-day retention (default) ≈ 2.7 GB. A retention sweep runs at startup; for long-uptime deployments, promote it to a daily background task.

**Note:** the longest ranges (1mo / 3mo / 6mo) only have data once the monitor has been running long enough to collect it. Lenses cannot backfill.

---

## AI assistant

A floating sparkle button (bottom-right) opens a slide-in chat panel. The bot has read-only access to five tools:

| Tool | Use case |
|---|---|
| `get_current_status` | "What's broken right now?" |
| `get_recent_alerts` | "Show me last week's breaches for Catalog Team" |
| `get_team_breakdown` | "Which team breached most this week?" |
| `get_job_history` | "What does the lag on canada-catalog-sku-events look like over the past 24 hours?" |
| `list_jobs` | "What jobs do we monitor?" |

The chatbot is a wrapper around Gemini's tool-call loop, capped at 6 hops to prevent runaway recursion. Tools are server-side Python functions that hit the same SQLite tables and in-process objects the rest of the app uses — there's no duplicated logic and no second copy of state.

Useful demo queries:
- *"What's the current state of Shipping Team?"*
- *"How does this week compare to last week?"*
- *"Generate a postmortem for the breach on `canada-catalog-sku-events::scus`."*

---

## Alert engine

Pure edge-trigger. One alert per crossing.

| Transition | Action |
|---|---|
| below → at/above threshold | Fire `breach`, route to team's Slack channel |
| stays in breach | Silent — no reminders, no re-alerts |
| at/above → below | Fire `resolved`, clear state |

`reading.lag = max(consumer_group_lag, topic_lag)` — both streams feed into the breach decision; the chart modal shows them side-by-side.

Every alert is persisted to SQLite with `delivered_to_slack` / `created_at` columns, so you have a full audit trail and the building blocks for MTTA reporting per team.

---

## Plug-and-play (production wire-up)

When you get into the VDI:

1. **`.env`** — set `LENSES_URL`, auth credentials, `GEMINI_API_KEY`, Slack webhooks.
2. **`data_sources/lenses.py`** — three places marked at the top of the file:
   - `_build_url()` — adjust the endpoint template if your Lenses build differs from 5.x.
   - `_build_headers()` — adjust if your Lenses uses a different auth scheme than `x-kafka-lenses-token`.
   - `_extract_lags()` — adjust if your Lenses returns the lag data in a different JSON shape (most likely place to need a tweak).
3. **`config/jobs.json`** — already populated with 9 topics across PNO / Catalog / Shipping. Edit if any names need correction.

Everything else — alert engine, history DB, Slack routing, dashboard UI, chatbot tools — stays unchanged.

---

## API reference

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | Dashboard HTML |
| GET | `/api/health` | Liveness + config snapshot |
| GET | `/api/status` | All jobs, current lag, summary |
| GET | `/api/job/{job_id}/history?minutes=30` | Time series for one job (any window 1m–90d) |
| GET | `/api/alerts?limit=50&hours=24` | Recent alerts, newest first |
| GET | `/api/team-breakdown?hours=168` | Per-team breach counts over a window |
| POST | `/api/chat` | Body: `{"message": "...", "history": [...]}`. Returns the AI assistant's reply. |
| GET | `/api/chat/health` | `{available: bool}` — used by the dashboard to grey out the AI button when no API key is set |

`job_id` format: `<topic>::<environment>` — e.g. `canada-catalog-sku-events::scus`.

---

## Project layout

```
app.py                  FastAPI backend, alert engine, AlertDB + HistoryDB, monitor loop
config/jobs.json        Job catalog (9 topics × 3 teams)
data_sources/
  base.py               DataSource interface + LagReading dataclass
  lenses.py             Lenses REST client (production)
  __init__.py           Factory
ai/
  chatbot.py            Tool-call orchestrator
  llm_client.py         Gemini default, OpenAI alternate
  prompts.py            System prompt
  tools.py              5 read-only tool definitions + ToolRegistry
routes/chat.py          POST /api/chat
static/index.html       Dashboard (HTML + CSS + JS, no build step)
requirements.txt        FastAPI, uvicorn, httpx, python-dotenv, google-genai, openai
.env.example            Env var template
Dockerfile              Production container build
```

---

## Deployment

The target deployment is a Linux VM/VDI inside the Walmart network with line-of-sight to Lenses. Run `python app.py` directly on the box (in a venv) or build the Docker container (`docker build -t lag-monitor .`) and run that. Either way, the app listens on `0.0.0.0:8000` by default and reads its config from `.env` on startup.

For deployments that need to outlive the polling-loop process (auto-restart, log aggregation, etc.), wrap it with `systemd`, run inside Kubernetes, or deploy the container to Cloud Run with `--cpu-always-allocated --min-instances=1` so the polling loop isn't paused between requests.
