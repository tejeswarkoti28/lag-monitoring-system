"""
app.py — Entry point. Wires all modules together and starts the server.

This file intentionally contains NO business logic. It just:
  1. Loads .env
  2. Imports from core/ and routes/
  3. Creates the FastAPI app + mounts routers
  4. Starts uvicorn when run directly

To understand the system, start here and follow the imports:
  core/config.py   — all config constants, time helpers, Slack helpers
  core/db.py       — AlertDB (events), HistoryDB (time-series), ResponseCache
  core/alerting.py — AlertEngine (breach detection), SlackNotifier
  core/monitor.py  — Monitor (background polling loop)
  routes/status.py — /api/health, /api/status, /api/alerts, /api/topics, /api/panels, /api/slack/test
  routes/history.py— /api/job/{id}/history, /api/panel/{id}/range
  routes/chat.py   — /api/chat (AI assistant)
  static/index.html— dashboard HTML
  static/style.css — dashboard CSS
  static/app.js    — dashboard JavaScript
"""
from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Load .env BEFORE importing core/config.py, because config reads os.environ
# at module level.
try:
    from dotenv import load_dotenv
    load_dotenv(
        dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
        override=False,
    )
except ImportError:
    pass

from core.config import DB_PATH, JOB_CATALOG
from core.db import AlertDB, HistoryDB
from core.alerting import AlertEngine, SlackNotifier
from core.monitor import Monitor
from data_sources import build_all_data_sources, get_primary_data_source
from panels.registry import default_panel_registry
from routes.status import build_status_router
from routes.history import build_history_router
from routes.chat import build_chat_router

# =============================================================================
# Create all the shared objects (singletons for the lifetime of the process)
# =============================================================================
_data_sources = build_all_data_sources(catalog=JOB_CATALOG)
_source        = get_primary_data_source(catalog=JOB_CATALOG)
_panels        = default_panel_registry()
_engine        = AlertEngine()
_db            = AlertDB(DB_PATH)
_history_db    = HistoryDB(DB_PATH)
_notifier      = SlackNotifier()
_monitor       = Monitor(_source, _engine, _notifier, _db, _history_db)


def _build_chatbot():
    try:
        from ai import Chatbot, ToolRegistry, build_llm_client
    except ImportError as exc:
        print(f"[chatbot] AI package import failed: {exc}", file=sys.stderr)
        return None
    try:
        llm = build_llm_client()
    except Exception as exc:
        print(f"[chatbot] disabled — {exc}", file=sys.stderr)
        return None
    from core.config import THRESHOLD_MESSAGES
    tools = ToolRegistry(
        monitor=_monitor, db=_db, history_db=_history_db,
        source=_source, threshold=THRESHOLD_MESSAGES,
    )
    return Chatbot(llm=llm, tools=tools)


_chatbot = _build_chatbot()


# =============================================================================
# FastAPI app lifecycle
# =============================================================================
@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        deleted = _history_db.cleanup()
        total = sum(deleted.values())
        if total:
            print(f"[history] purged {total} rows "
                  f"(raw={deleted['lag_history']}, "
                  f"1m={deleted['lag_history_1m']}, "
                  f"1h={deleted['lag_history_1h']})")
    except Exception as exc:
        print(f"[history] cleanup failed: {exc}", file=sys.stderr)
    _monitor.start()
    try:
        yield
    finally:
        await _monitor.stop()
        await _notifier.close()


app = FastAPI(title="Kafka Lag Monitor", lifespan=lifespan)

# =============================================================================
# Static files + root
# =============================================================================
_HERE = os.path.dirname(os.path.abspath(__file__))
_static_dir = os.path.join(_HERE, "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/")
def root():
    index = os.path.join(_static_dir, "index.html")
    if not os.path.isfile(index):
        return JSONResponse(
            {"error": "static/index.html not found", "static_dir": _static_dir},
            status_code=500,
        )
    return FileResponse(index)


# =============================================================================
# Mount all routers
# =============================================================================
app.include_router(build_status_router(_monitor, _db, _notifier, _panels, _chatbot))
app.include_router(build_history_router(_monitor, _history_db, _panels, _data_sources))
app.include_router(build_chat_router(_chatbot))


# =============================================================================
# Entrypoint
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("app:app", host=host, port=port, reload=False, log_level="info")
