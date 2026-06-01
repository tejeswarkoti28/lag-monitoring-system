# Adding a New Job — Complete Guide

This guide covers everything needed to add a new Kafka consumer group to the lag monitoring dashboard.
No code changes are required. Only one file needs to be edited: `config/jobs.json`.

---

## Step 1 — What you need from the stakeholder

Two things only:

1. **The Grafana dashboard link** for that job
2. **One-line description** of what the job does (e.g. `"Pricing RT to UBER"`)

Everything else comes from the link.

---

## Step 2 — Extract from the Grafana URL

Open the link. The URL contains all the Prometheus labels you need:

```
https://grafana.mms.walmart.net/d/o7eRAUOpwerz3/lenses-consumergroup-lag
  ?var-assembly=kafka-v2-ca-shared-secure-prod     ← this is your  ooa
  &var-platform=lenses                              ← this is your  oop
  &var-environment=scus                             ← this is your  environments
  &var-topic=canada-pno-offercalculation-events     ← this is your  topic
  &var-consumerGroup=ca-priceoffer-winning-offer-rank-2-prod  ← consumer_group
```

| URL parameter | Field in `jobs.json` |
|---|---|
| `var-topic` | `topic` |
| `var-consumerGroup` | `consumer_group` |
| `var-assembly` | `ooa` |
| `var-platform` | `oop` |
| `var-environment` | `environments` (as array) |

The `job` field is always `managed-kafka-consumer-service` — verify it hasn't changed by
confirming the URL uses `var-datasource=production`.

---

## Step 3 — Verify data exists in Prometheus

**Before touching any file**, confirm the job is actually being monitored.
Run this on the server:

```bash
venv/bin/python3 -c "
import os, httpx, urllib3
from dotenv import load_dotenv
load_dotenv()
urllib3.disable_warnings()

base = 'https://grafana.mms.walmart.net/api/datasources/proxy/uid/production'
cookie = os.environ.get('GRAFANA_COOKIE', '')
headers = {'Cookie': cookie, 'User-Agent': 'Mozilla/5.0', 'Referer': base + '/'}

# Fill these from the Grafana URL
job   = 'managed-kafka-consumer-service'
ooa   = 'kafka-v2-ca-shared-secure-prod'   # var-assembly
oop   = 'lenses'                            # var-platform
ooe   = 'scus'                              # var-environment
topic = 'your-topic-name'                   # var-topic
cg    = 'your-consumer-group'               # var-consumerGroup

q = (f'max(lenses_topic_consumer_lag{{'
     f'job=\"{job}\",ooa=\"{ooa}\",oop=\"{oop}\","
     f'ooe=\"{ooe}\",topic=\"{topic}\",consumerGroup=\"{cg}\"}})')

r = httpx.Client(verify=False).get(f'{base}/api/v1/query', params={'query': q}, headers=headers)
result = r.json().get('data', {}).get('result', [])
print('LAG:', result[0]['value'][1] if result else 'NOT FOUND — do not add this job yet')
"
```

- **Returns a number** → job is live in Prometheus, proceed to Step 4
- **Returns NOT FOUND** → the values from the URL are wrong, or the service is not being
  monitored by Lenses yet. Do not add the job until this passes.

---

## Step 4 — Make the entry in `config/jobs.json`

Add one block inside the `"jobs"` array:

```json
{
  "topic": "<var-topic>",
  "consumer_group": "<var-consumerGroup>",
  "description": "<one line from stakeholder>",
  "team": "<team name>",
  "channel": "#<slack-channel>",
  "job": "managed-kafka-consumer-service",
  "ooa": "<var-assembly>",
  "oop": "<var-platform>",
  "environments": ["<var-environment>"]
}
```

No other file needs to be touched.

---

## Step 5 — Restart the app

```bash
python app.py
```

The app reads `jobs.json` only at startup. Changes don't take effect until restart.

---

## What happens after restart — the full data flow

```
config/jobs.json
   │
   ▼
core/config.py            Loads the file at startup, builds JOB_CATALOG list
   │
   ▼
data_sources/base.py      Fans out each job × its environments into individual job dicts.
                          Each dict carries: topic, consumer_group, environment,
                          job, ooa, oop, team, channel, description
   │
   ▼
data_sources/prometheus.py  Builds PrometheusDataSource with the full job list
   │
   ├──► Poll loop (every 5s)
   │       For each job, queries Prometheus:
   │       max(lenses_topic_consumer_lag{job, ooa, oop, ooe, topic, consumerGroup})
   │       → updates Monitor.jobs[job_id].current (live lag reading)
   │       → AlertEngine checks if lag crossed threshold
   │            → if breach/resolved: writes to SQLite alerts table
   │                                  sends Slack notification
   │
   └──► GET /api/status (polled by dashboard every 5s)
           Returns live lag + status for every job
           → Dashboard job cards update their lag value and breach indicator

Browser opens dashboard
   │
   ▼
GET /api/topics            Returns full job catalog with all Prometheus labels
   │
   ▼
_buildJobs() in app.js     Builds one card per job using per-job environments
   │
   ▼
Job cards rendered         One card per job showing team, env, current lag
   │
   ▼
loadCardSparkline()        Calls /api/panel/consumer_group_lag/range
                           passing: env, topic, consumer_group, job, ooa, oop
                           → Backend queries Prometheus range (last 24h)
                           → Sparkline drawn on the card

User clicks a job card
   │
   ▼
Modal opens                Calls /api/panel/{panel_id}/range for each panel
                           passing: env, topic, consumer_group, job, ooa, oop, minutes
                           → Backend queries Prometheus with the job's exact labels
                           → Charts rendered with full history (Prometheus retention)
                           → User can select 30m / 6h / 2d / 15d etc.
```

---

## Slack alerts for the new job

If the team already exists in `.env`:
```
SLACK_WEBHOOK_TEAM=https://hooks.slack.com/...
```
Alerts route automatically. No code change needed.

If it is a new team, add to `.env` and restart:
```
SLACK_WEBHOOK_<TEAM_NAME_UPPERCASE>=https://hooks.slack.com/...
```

---

## The one rule

> **If Step 3 does not return a number, the job does not go into `jobs.json`.**

Everything else is just filling in a template.
