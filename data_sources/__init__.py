"""
Data source layer.

The DataSource abstraction is the single seam between the monitoring engine
and whatever supplies lag readings — simulator (for laptop demos), Prometheus
(production), Lenses, Confluent, etc. Pick which implementation to load via
the DATA_SOURCE env var; the rest of the app stays unchanged.
"""
from __future__ import annotations

import os
from typing import Optional

from .base import DataSource, LagReading


def get_data_source(catalog: list[dict], environments: list[str]) -> DataSource:
    """Factory: pick a DataSource implementation based on env config.

    DATA_SOURCE=simulator  (default) — synthetic data, runs anywhere, no creds
    DATA_SOURCE=lenses                — Lenses REST API (production target)
    DATA_SOURCE=prometheus            — Prometheus / TSDB (alternative)
    """
    kind = os.environ.get("DATA_SOURCE", "simulator").lower().strip()

    if kind == "simulator":
        from .simulator import SimulatedDataSource
        return SimulatedDataSource(catalog=catalog, environments=environments)

    if kind == "lenses":
        from .lenses import LensesDataSource
        return LensesDataSource(catalog=catalog, environments=environments)

    if kind == "prometheus":
        from .prometheus import PrometheusDataSource
        return PrometheusDataSource(catalog=catalog, environments=environments)

    raise ValueError(
        f"Unknown DATA_SOURCE={kind!r}. "
        f"Supported: 'simulator', 'lenses', 'prometheus'."
    )


__all__ = ["DataSource", "LagReading", "get_data_source"]
