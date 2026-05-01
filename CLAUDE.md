# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Run / test

```bash
pip install -r requirements.txt
python app.py                # serves http://localhost:8000
python e2e_test.py           # boots a temp server on port 8765 and exercises every endpoint + inject/clear flow
```

There is no lint or unit-test suite — `e2e_test.py` is the smoke test. It spawns `app.py` as a subprocess against a throwaway DB at `/tmp/e2e_lag.db`, so running it on Windows requires a path that exists (the script hard-codes the POSIX path; works as-is on Linux/VDI).

Server env (full list in [.env.example](.env.example)): `HOST`, `PORT`, `LAG_MONITOR_DB`, `LAG_MONITOR_PUBLIC_URL`, `LAG_MONITOR_CONFIG`, `POLL_INTERVAL_SECONDS`, `LAG_HISTORY_RETENTION_DAYS`, plus `DATA_SOURCE`, `LENSES_*`, `PROMETHEUS_*`, `GEMINI_API_KEY`, `OPENAI_API_KEY`, `LLM_PROVIDER`, the Slack webhook routing vars, and the on-call mention vars. `python-dotenv` auto-loads `.env` from the project root.

## Architecture

FastAPI backend ([app.py](app.py)) + static dashboard ([static/index.html](static/index.html), no build step) + three pluggable Python packages: `data_sources/`, `ai/`, `routes/`.

The runtime is a polling loop with two pluggable seams:

```
DataSource.poll_all()  →  AlertEngine.evaluate()  →  SlackNotifier.send()
                                                  →  AlertDB.insert_alert()
                       →  in-memory ring (60 min, used for live sparklines)
                       →  HistoryDB.insert_batch  (used for chart time-ranges)

POST /api/chat → Chatbot.reply() → LLMClient.chat() ←─ tool calls ─→ ToolRegistry
                                                                   ↓
                                                     Monitor / AlertDB / HistoryDB
```

`Monitor.run()` ticks every `POLL_INTERVAL_SECONDS` (5s). On startup, `Monitor.warmup()` populates 30 minutes of history (real for Lenses if it has had time to run, fabricated for the simulator) so the dashboard isn't blank on first launch. The lifespan also runs `HistoryDB.cleanup_older_than(LAG_HISTORY_RETENTION_DAYS)` once per startup.

### Pluggable data source

[`get_data_source()`](data_sources/__init__.py) picks the implementation by env var:

- `DATA_SOURCE=simulator` (default) → [`SimulatedDataSource`](data_sources/simulator.py) — deterministic 7-layer composite (oscillation, daily/weekly/monthly seasonality, sparse incidents, producer bursts, consumer rebalance ramps, step shifts, jitter), seeded once at startup. Supports `inject_spike()` for the demo control panel.
- `DATA_SOURCE=lenses` → [`LensesDataSource`](data_sources/lenses.py) — production target. **Lenses is real-time only — it does not store historical lag.** Three things to fill in: `LENSES_URL`, auth (either `LENSES_API_TOKEN` or `LENSES_USERNAME` + `LENSES_PASSWORD`), and the lag-extraction logic in `_extract_lags()` if your Lenses version returns a different JSON shape than the default.
- `DATA_SOURCE=prometheus` → [`PrometheusDataSource`](data_sources/prometheus.py) — alternative for clusters that already export Kafka metrics to a TSDB. Fill in `PROMETHEUS_URL`, `PROMETHEUS_AUTH_TOKEN`, and the metric/label scheme constants at the top of the file.

Every consumer (Monitor, ToolRegistry) depends on the [`DataSource`](data_sources/base.py) interface, not concrete classes.

### Historical lag persistence (HistoryDB)

Because Lenses only returns current values, **we own the time series.** Every 5-second poll appends 18 rows to the `lag_history` table in the same SQLite file as `alerts`:

```sql
CREATE TABLE lag_history (
    job_id TEXT NOT NULL,
    ts INTEGER NOT NULL,           -- unix timestamp seconds
    cg_lag INTEGER NOT NULL,
    topic_lag INTEGER NOT NULL,
    PRIMARY KEY (job_id, ts)
);
```

[`HistoryDB.query()`](app.py) returns downsampled series — bucket size chosen by [`bucket_seconds_for(minutes)`](app.py) so any range returns ~500–720 points. Storage at default retention (90 days) ≈ 2.7 GB. Cleanup runs once at startup; for long-uptime deployments, promote it to a daily background task.

The dashboard's 9 time-range buttons (30m / 6h / 12h / 24h / 2d / 15d / 1mo / 3mo / 6mo) all hit `/api/job/{job_id}/history` with a `minutes` parameter. Buckets are chosen server-side; the chart never sees more than ~720 points regardless of window.

The in-memory ring buffer per `JobState` is still used for the live sparkline on the job-grid card — that path avoids a SQLite round-trip every 5s on the homepage. The chart modal goes through HistoryDB.

### AI chatbot

[`build_llm_client()`](ai/llm_client.py) picks by `LLM_PROVIDER` env var:

- `LLM_PROVIDER=gemini` (default) → [`GeminiClient`](ai/llm_client.py) using `google-genai`. Default model `gemini-2.5-flash`. Requires `GEMINI_API_KEY` (free at https://ai.google.dev).
- `LLM_PROVIDER=openai` → [`OpenAIClient`](ai/llm_client.py) using `openai` SDK. Default model `gpt-4o-mini`. Requires `OPENAI_API_KEY`.

The internal message + tool format is OpenAI's chat-completions schema (de-facto standard); GeminiClient translates to Gemini's native shape internally via `_to_gemini_messages` and `_to_gemini_tools`. The Chatbot stays provider-agnostic.

[`Chatbot.reply()`](ai/chatbot.py) wraps the tool-call loop with `MAX_TOOL_HOPS=6` to prevent runaway recursion. [`ToolRegistry`](ai/tools.py) has five read-only tools: `get_current_status`, `get_recent_alerts`, `get_team_breakdown`, `get_job_history`, `list_jobs`. `get_job_history` uses HistoryDB with the same bucketing as the dashboard so the LLM sees the same shape humans see.

If the configured provider's API key is missing on startup, `_build_chatbot()` returns `None` and `/api/chat` returns 503; dashboard shows the AI button greyed out. **Supported mode, not a failure.**

### Job catalog

[`config/jobs.json`](config/jobs.json) — 9 topic+consumer-group entries fanned out across `environments` (default `["eus", "scus"]`) → 18 jobs. Override the path with `LAG_MONITOR_CONFIG`. **Edit the JSON, never hardcode in Python.**

`job_id` format: `<topic>::<env>` (env lowercase).

### Alert semantics

[`AlertEngine`](app.py) is **pure edge-trigger**: exactly one alert per crossing.

- below → at/above threshold: fire `breach`
- stays in breach: silent (**no reminders, no re-alerts**)
- at/above → below: fire `resolved`, clear state

`reading.lag` = `max(consumer_group_lag, topic_lag)`. The two streams are simulated semi-independently in the simulator (shared seasonality, divergent minute-to-minute) because real Kafka exposes them as related-but-distinct quantities. The breach decision uses the max; the dashboard renders both side-by-side.

### Slack routing

`slack_webhook_for(team)` resolves `SLACK_WEBHOOK_<TEAM>` per-team, falling back to `SLACK_WEBHOOK_URL`. If neither is set, `SlackNotifier.send()` returns `False` and the alert is in-app/DB-only — supported mode. `slack_oncall_tag(team)` resolves the mention prefix (defaults to `<!channel>`).

### Time conventions

Storage uses UTC ISO-8601 (alerts table) and UNIX-seconds INTEGER (`lag_history.ts`). Slack messages and dashboard labels render in IST via `to_ist` / `ist_clock` / `ist_full` — operators read IST at a glance.

## Project context

- **Target deployment:** Walmart Canada VDI, running on a Linux box accessed via SSH (PuTTY or OpenSSH). The simulator is for laptop development; production uses `DATA_SOURCE=lenses` against the internal Lenses install.
- **Career framing:** the user is using this as a portfolio piece. The AI chatbot (Gemini) is a deliberate differentiator — keep additions polished and demo-friendly. See [memory/](../../.claude/projects/) for the open refactor agreements.
- **Recent refactor:** simulator extracted to `data_sources/simulator.py`, Lenses + Prometheus implementations added (`data_sources/lenses.py`, `data_sources/prometheus.py`), JOB_CATALOG moved to `config/jobs.json`, AI chatbot package added (`ai/`) with Gemini default, `/api/chat` endpoint added (`routes/chat.py`), `lag_history` table + `HistoryDB` class added so the dashboard's 9 time-ranges (30m–6mo) all read from persisted data, `/api/team-breakdown` endpoint added.
