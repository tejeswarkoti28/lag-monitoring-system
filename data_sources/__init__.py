"""
Data source layer.

Reads connection configs from config/data_sources.json. Today there's a
single entry called "production" pointing at Walmart's Prometheus via the
Grafana datasource proxy. To add a new source, append a top-level key in
that JSON file and (if it's a different protocol) implement a new class
analogous to PrometheusDataSource — then dispatch on the `type` field below.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from .base import DataSource, LagReading
from .prometheus import PrometheusDataSource


def load_configs(config_path: Optional[Path] = None) -> dict:
    if config_path is None:
        here = Path(__file__).resolve().parent.parent
        config_path = Path(
            os.environ.get("DATA_SOURCES_CONFIG", here / "config" / "data_sources.json")
        )
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def build_data_source(
    name: str,
    config: dict,
    *,
    catalog: list[dict],
) -> DataSource:
    kind = config.get("type")
    if kind == "prometheus_via_grafana_proxy" or kind == "prometheus":
        return PrometheusDataSource(
            catalog=catalog,
            base_url=config["url"],
            auth_token=config.get("auth"),
            verify_ssl=bool(config.get("verify_ssl", True)),
            timeout_seconds=float(config.get("timeout_seconds", 30.0)),
        )
    raise ValueError(
        f"data source {name!r} has unsupported type {kind!r}. "
        f"Supported: 'prometheus_via_grafana_proxy', 'prometheus'."
    )


def build_all_data_sources(*, catalog: list[dict]) -> dict[str, DataSource]:
    """Build every data source from config and return them keyed by name."""
    configs = load_configs()
    return {
        name: build_data_source(name, cfg, catalog=catalog)
        for name, cfg in configs.items()
    }


def get_primary_data_source(*, catalog: list[dict]) -> DataSource:
    """Return the data source the Monitor / alert engine should poll.

    Today there's only one. When more are added, this picks 'production' by
    convention; override with the PRIMARY_DATA_SOURCE env var if needed.
    """
    sources = build_all_data_sources(catalog=catalog)
    name = os.environ.get("PRIMARY_DATA_SOURCE", "production")
    if name not in sources:
        if not sources:
            raise RuntimeError("config/data_sources.json defines no sources")
        name = next(iter(sources))
    return sources[name]


__all__ = [
    "DataSource",
    "LagReading",
    "PrometheusDataSource",
    "build_all_data_sources",
    "build_data_source",
    "get_primary_data_source",
    "load_configs",
]
