# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Run

```bash
pip install -r requirements.txt
python app.py                # serves http://localhost:8000
```

The app cannot start without a working data source — see `config/data_sources.json`.

## Architecture

FastAPI backend ([app.py](app.py)) + static dashboard ([static/index.html](static/index.html), no build step) + four Python packages:
- `data_sources/` — pluggable data source clients
- `panels/` — declarative chart definitions, loaded from `config/panels.json`
- `ai/` — Gemini-backed chatbot
- `routes/` — HTTP routes (currently just `/api/chat`)

The runtime is a polling loop:

```
PrometheusDataSource.poll_all()  →  AlertEngine.evaluate()  →  SlackNotifier.send()
                                                            →  AlertDB.insert_alert()
                                 →  in-memory ring (60 min, used by /api/status)
                                 →  HistoryDB.insert_batch  (90-day retention)

GET /api/panel/{id}/range        →  PrometheusDataSource.query_range()
                                 →  Panel.build_query() does $var substitution

POST /api/chat → Chatbot.reply() → Gemini ←─ tool calls ─→ ToolRegistry
                                                        ↓
                                            Monitor / AlertDB / HistoryDB
```

`Monitor.run()` ticks every `POLL_INTERVAL_SECONDS` (5s). Lifespan also runs `HistoryDB.cleanup_older_than(LAG_HISTORY_RETENTION_DAYS)` once per startup.

## Three pluggable seams

### 1. Data sources — `config/data_sources.json`

Each top-level key is a named data source. The current single entry, `production`, points at Walmart's Prometheus via the Grafana datasource proxy:

```json
{
  "production": {
    "type": "prometheus_via_grafana_proxy",
    "url": "https://grafana.mms.walmart.net/api/datasources/proxy/uid/production",
    "auth": null,
    "static_labels": {
      "job": "managed-kafka-consumer-service",
      "ooa": "kafka-v2-ca-shared-secure-prod",
      "oop": "lenses"
    }
  }
}
```

`static_labels` are pre-substituted into PromQL queries (so panels can stay deployment-agnostic). To add a new source:

1. Append a top-level key to `data_sources.json` (e.g. `staging`).
2. If the protocol is the same (Prometheus), nothing else changes — `data_sources/__init__.py:build_data_source()` dispatches on `type`.
3. If the protocol is different (Lenses REST direct, kafka-python admin, Confluent Cloud), add a new file in `data_sources/`, subclass `DataSource`, override `poll_all()` and the generic `query_range()` method. Then add a branch to `build_data_source()`.

`DataSource` ([base.py](data_sources/base.py)) requires only `poll_all()`. The Prometheus implementation also exposes `query_instant()` and `query_range()` — these are what the panel system uses.

### 2. Panels — `config/panels.json`

Each panel is a declarative chart definition:

```json
{
  "id": "consumer_group_lag",
  "title": "Consumer Group Lag",
  "section": "consumer_metrics",
  "data_source": "production",
  "expr": "max(lenses_topic_consumer_lag{job=\"$job\",ooa=\"$ooa\",oop=\"$oop\",ooe=\"$env\",topic=\"$topic\",consumerGroup=\"$consumer_group\"})",
  "scope": ["env", "topic", "consumer_group"],
  "unit": "short",
  "y_min": 0,
  "color": "#84d957"
}
```

`expr` is a PromQL template. Variables starting with `$` are substituted at query time:
- Static labels from the data source (`$job`, `$ooa`, `$oop`)
- Per-request scope variables (`$env`, `$topic`, `$consumer_group`) — listed in `scope`

`scope` tells the renderer which template variables this panel needs. Lenses Status only needs `env`. Lag panels need all three.

To add a panel: append an entry to `panels.json` with a unique `id`. The dashboard renders it automatically inside the named `section`.

[`PanelRegistry`](panels/registry.py) loads, indexes, and serialises the panel list. [`Panel.build_query()`](panels/base.py) does the substitution and raises `ValueError` if a required scope variable is missing.

### 3. AI chatbot — `ai/`

[`build_llm_client()`](ai/llm_client.py) returns a `GeminiClient`. Default model: `gemini-2.5-flash`. Requires `GEMINI_API_KEY` (free at https://ai.google.dev).

[`Chatbot.reply()`](ai/chatbot.py) wraps a tool-call loop with `MAX_TOOL_HOPS=6`. [`ToolRegistry`](ai/tools.py) has five read-only tools: `get_current_status`, `get_recent_alerts`, `get_team_breakdown`, `get_job_history`, `list_jobs`. Tool implementations query the same in-process `Monitor`/`AlertDB`/`HistoryDB` the rest of the app uses — no duplicated logic.

If `GEMINI_API_KEY` is missing, `_build_chatbot()` returns `None`, `/api/chat` returns 503, and the dashboard greys out the AI button. Supported mode, not a failure.

## Job catalog — `config/jobs.json`

Lists the consumer groups we monitor for breach detection. Each entry is fanned out across `environments` to produce `job_id = <topic>::<env>`.

The dashboard's topic dropdown is populated from this file. The PromQL panels use the selected topic + consumer group to fill `$topic` / `$consumer_group` template variables.

`threshold_messages` is the breach threshold (override via `LAG_THRESHOLD` env var).

## Dashboard layout

Two-column grid: one column per environment (eus / scus). Inside each column, panels are grouped by their `section`. Today there's only one section — `consumer_metrics` — with four panels: Lenses Status, Consumer Group/Topic Lag, Consumer Group Lag, Rebalancing Status.

A **topic dropdown** at the top selects which (topic, consumer_group) pair to render across both env columns. A **range bar** (30m / 6h / 12h / 24h / 2d / 15d / 1mo / 3mo / 6mo) selects the time window — applied to all charts simultaneously. Same UX as Grafana.

The right sidebar shows the **live alert feed**. Bottom-right has the **AI sparkle button** that opens a slide-in chat panel.

Charts re-fetch every 30 seconds; status/alerts/clock refresh every 1–5 seconds.

## Alert semantics

[`AlertEngine`](app.py) is **pure edge-trigger**: exactly one alert per crossing.

- below → at/above threshold: fire `breach`, route to team's Slack channel
- stays in breach: silent
- at/above → below: fire `resolved`, clear state

`reading.lag` = `max(consumer_group_lag, topic_lag)`. The breach decision uses the max.

## Slack routing

`slack_webhook_for(team)` resolves `SLACK_WEBHOOK_<TEAM>` per-team, falling back to `SLACK_WEBHOOK_URL`. If neither is set, alerts are in-app/DB only — supported mode. `slack_oncall_tag(team)` resolves the mention prefix (defaults to `<!channel>`).

## SQLite schema

Same database file holds two tables:

```sql
CREATE TABLE alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id, topic, consumer_group, environment, team, channel,
    alert_type, lag_value, threshold, delivered_to_slack, created_at
);

CREATE TABLE lag_history (
    job_id TEXT NOT NULL,
    ts INTEGER NOT NULL,           -- unix timestamp seconds
    cg_lag INTEGER NOT NULL,
    topic_lag INTEGER NOT NULL,
    PRIMARY KEY (job_id, ts)
);
```

Storage: ~30 MB/day for 18 jobs at 5s polling. 90-day retention default ≈ 2.7 GB.

## Time conventions

Storage uses UTC ISO-8601 (alerts) and UNIX-seconds INTEGER (lag_history). Slack messages and dashboard labels render in IST via `to_ist` / `ist_clock` / `ist_full`.

## Project context

- **Target deployment:** Walmart Canada VDI, Linux box `app@10.238.161.135`. The dashboard pulls real data through Grafana's datasource proxy — no Prometheus credentials needed locally; Grafana adds Walmart auth headers transparently.
- **Career framing:** the user is using this as a portfolio piece. The AI chatbot (Gemini) and the Grafana-style dashboard are the two differentiators.
- **Recent change:** dashboard pivoted from per-job grid to per-environment columns with declarative panels. Panel definitions moved to `config/panels.json`. Data source connection moved to `config/data_sources.json`. The system metrics panels (CPU, memory, disk) from the original Grafana dashboard were intentionally NOT included — they describe Lenses host health, not consumer lag.
