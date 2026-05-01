# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Run / test

```bash
pip install -r requirements.txt
python app.py                # serves http://localhost:8000
python e2e_test.py           # boots a temp server on port 8765 and exercises every endpoint + inject/clear flow
```

There is no lint or unit-test suite — `e2e_test.py` is the smoke test. It spawns `app.py` as a subprocess against a throwaway DB at `/tmp/e2e_lag.db`, so running it on Windows requires a path that exists (the script hard-codes the POSIX path; works as-is on Linux/VDI).

Server env (full list in [.env.example](.env.example)): `HOST`, `PORT`, `LAG_MONITOR_DB`, `LAG_MONITOR_PUBLIC_URL`, `LAG_MONITOR_CONFIG`, `POLL_INTERVAL_SECONDS`, plus `DATA_SOURCE`, `OPENAI_API_KEY`, `LLM_PROVIDER`, the Slack webhook routing vars, and the on-call mention vars. `python-dotenv` auto-loads `.env` from the project root.

## Architecture

FastAPI backend ([app.py](app.py)) + static dashboard ([static/index.html](static/index.html), no build step) + three pluggable Python packages: `data_sources/`, `ai/`, `routes/`.

The runtime is a polling loop with two pluggable seams:

```
DataSource.poll_all()  →  AlertEngine.evaluate()  →  SlackNotifier.send()
                                                  →  AlertDB.insert_alert()
                       →  Monitor.jobs[*].history (in-memory ring, 60 min)

POST /api/chat → Chatbot.reply() → LLMClient.chat() ←─ tool calls ─→ ToolRegistry
                                                                   ↓
                                                     reads Monitor / AlertDB / DataSource
```

`Monitor.run()` ticks every `POLL_INTERVAL_SECONDS` (5s). On startup, `Monitor.warmup()` synthesises 30 minutes of history so the dashboard is never blank.

### Plug-and-play seams

**`data_sources/`** — pick implementation via the `DATA_SOURCE` env var:

- `simulator` (default) → [`SimulatedDataSource`](data_sources/simulator.py) — deterministic 7-layer composite (oscillation, daily/weekly/monthly seasonality, sparse incidents, producer bursts, consumer rebalance ramps, step shifts, jitter), seeded once at startup. Supports `inject_spike()` for the demo control panel.
- `prometheus` → [`PrometheusDataSource`](data_sources/prometheus.py) — production target. Skeleton with three explicit TODOs: `PROMETHEUS_URL`, `PROMETHEUS_AUTH_TOKEN`, and the metric/label scheme constants at the top of the file (defaults to `kafka_consumergroup_lag{topic, consumergroup, env}`). When wired, the rest of the app — engine, DB, Slack routing, dashboard, chatbot — works unchanged.

The factory [`get_data_source()`](data_sources/__init__.py) is the only place that knows which implementation to instantiate. Every consumer (Monitor, ToolRegistry) depends on the [`DataSource`](data_sources/base.py) interface, not concrete classes. **Don't add behaviour to `DataSource` that callers depend on beyond `poll_all()`, `synthesize_history()`, and the optional injection methods.**

**`ai/`** — chatbot subsystem. Pick LLM provider via `LLM_PROVIDER` (default `openai`):

- [`build_llm_client()`](ai/llm_client.py) — factory; add new providers as new branches.
- [`Chatbot.reply()`](ai/chatbot.py) — wraps the tool-call loop, capped at `MAX_TOOL_HOPS=6` to prevent runaway recursion.
- [`ToolRegistry`](ai/tools.py) — five read-only tools the LLM can invoke: `get_current_status`, `get_recent_alerts`, `get_team_breakdown`, `get_job_history`, `list_jobs`. Each method queries the same in-process objects (Monitor, AlertDB, DataSource) the rest of the app uses — no duplicated logic.
- [`SYSTEM_PROMPT`](ai/prompts.py) — terse role brief; tone, format conventions (IST timestamps, "5.20M" not "5,200,000"), postmortem structure.

If `OPENAI_API_KEY` is missing on startup, `_build_chatbot()` returns `None` and the app runs in "chatbot-disabled" mode — `/api/chat` returns 503, dashboard shows the AI button greyed out. **This is a supported mode, not a failure.**

### Job catalog

[`config/jobs.json`](config/jobs.json) — the 9 topic+consumer-group entries fanned out across `environments` (default `["eus", "scus"]`) → 18 jobs. Override the path with `LAG_MONITOR_CONFIG`. **Edit the JSON, never hardcode in Python.**

`job_id` format: `<topic>::<env>` (env lowercase).

### Alert semantics

[`AlertEngine`](app.py) is **pure edge-trigger**: exactly one alert per crossing.

- below → at/above threshold: fire `breach`
- stays in breach: silent (**no reminders, no re-alerts**)
- at/above → below: fire `resolved`, clear state

`reading.lag` = `max(consumer_group_lag, topic_lag)`. The two streams are simulated semi-independently (shared seasonality, divergent minute-to-minute) because real Kafka exposes them as related-but-distinct quantities. The breach decision uses the max; the dashboard renders both side-by-side.

### Slack routing

`slack_webhook_for(team)` resolves `SLACK_WEBHOOK_<TEAM>` per-team, falling back to `SLACK_WEBHOOK_URL`. If neither is set, `SlackNotifier.send()` returns `False` and the alert is in-app/DB-only — supported mode. `slack_oncall_tag(team)` resolves the mention prefix (defaults to `<!channel>`).

### History endpoint

[`/api/job/{job_id}/history`](app.py) is **capped at the in-memory retention window** (default 60 min). Older synthesized views were stripped because they were simulator-only and misleading. When `DATA_SOURCE=prometheus`, lift the cap and let the data source's `synthesize_history()` answer (it queries `query_range` against the TSDB).

The dashboard intentionally only requests the live 30-minute window — see `RANGE_PRESETS` in [static/index.html](static/index.html). To restore longer ranges in the UI, re-add entries to that array AND remove the server-side cap.

### SQLite

[`AlertDB`](app.py) writes to `lag_monitor.db` (WAL mode). Schema is `alerts` only — `(job_id, topic, consumer_group, environment, team, channel, alert_type, lag_value, threshold, delivered_to_slack, created_at)`. The DB files are git-ignored and rebuilt at runtime.

### Time conventions

Storage uses UTC ISO-8601. Slack messages and dashboard labels render in IST via `to_ist` / `ist_clock` / `ist_full` — operators read IST at a glance.

## Project context

- **Target deployment:** Walmart Canada VDI, running on a Linux box accessed via SSH (PuTTY or OpenSSH). The simulator is for laptop development; production uses `DATA_SOURCE=prometheus` against an internal TSDB.
- **Career framing:** the user is using this as a portfolio piece. The AI chatbot is a deliberate differentiator — keep additions polished and demo-friendly. See [memory/](../../.claude/projects/) for the open refactor agreements.
- **What changed in this refactor:** simulator extracted to `data_sources/simulator.py`, Prometheus skeleton added in `data_sources/prometheus.py`, JOB_CATALOG moved to `config/jobs.json`, AI chatbot package added (`ai/`), `/api/chat` endpoint added (`routes/chat.py`), 5 fake time-range views removed from the modal (only the live 30m view remains), `/api/team-breakdown` endpoint added.
