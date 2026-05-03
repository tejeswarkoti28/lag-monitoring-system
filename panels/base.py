"""
Panel — declarative description of one Grafana-style chart.

A Panel knows:
  - what data source it queries
  - the PromQL template (with $env, $topic, $consumer_group placeholders)
  - which scope variables it needs filled in at query time
  - rendering hints (unit, color, y-axis bounds)

Panels do NOT execute their own queries — they describe the query, and a
DataSource runs it. This keeps panels portable across data sources.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class Panel:
    id: str
    title: str
    section: str
    data_source: str
    expr_template: str
    scope: list[str]                # subset of {"env", "topic", "consumer_group"}
    unit: str = "short"
    y_min: Optional[float] = None
    y_max: Optional[float] = None
    color: str = "#84d957"
    show_threshold: bool = False
    description: str = ""

    def build_query(
        self,
        *,
        static_labels: dict,
        env: Optional[str] = None,
        topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
    ) -> str:
        """Substitute every $variable in expr_template with a concrete value.

        Order of substitution:
          1. static_labels from the data source ($job, $ooa, $oop, ...)
          2. per-request scope variables ($env, $topic, $consumer_group)

        Missing required scope variables raise ValueError so callers don't
        silently produce a malformed query.
        """
        result = self.expr_template

        for key, value in static_labels.items():
            result = result.replace(f"${key}", value)

        scope_values = {"env": env, "topic": topic, "consumer_group": consumer_group}
        for required in self.scope:
            value = scope_values.get(required)
            if value is None:
                raise ValueError(
                    f"panel {self.id!r} requires scope {required!r} but none was provided"
                )
            result = result.replace(f"${required}", value)

        return result
