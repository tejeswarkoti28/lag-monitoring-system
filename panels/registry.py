"""
PanelRegistry — loads panels from config/panels.json and exposes lookup by id.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from .base import Panel


def load_panels(config_path: Path) -> list[Panel]:
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    panels = []
    for entry in data.get("panels", []):
        panels.append(Panel(
            id=entry["id"],
            title=entry["title"],
            section=entry["section"],
            data_source=entry["data_source"],
            expr_template=entry["expr"],
            scope=list(entry.get("scope", [])),
            unit=entry.get("unit", "short"),
            y_min=entry.get("y_min"),
            y_max=entry.get("y_max"),
            color=entry.get("color", "#84d957"),
            show_threshold=bool(entry.get("show_threshold", False)),
            description=entry.get("description", ""),
        ))
    return panels


class PanelRegistry:
    """Indexes panels by id; exposes section-grouped iteration for rendering."""

    def __init__(self, panels: list[Panel]) -> None:
        self._by_id: dict[str, Panel] = {p.id: p for p in panels}
        self._panels: list[Panel] = list(panels)

    def get(self, panel_id: str) -> Panel:
        if panel_id not in self._by_id:
            raise KeyError(f"unknown panel: {panel_id!r}")
        return self._by_id[panel_id]

    def all(self) -> list[Panel]:
        return list(self._panels)

    def sections(self) -> dict[str, list[Panel]]:
        """Panels grouped by their section, preserving definition order."""
        out: dict[str, list[Panel]] = {}
        for p in self._panels:
            out.setdefault(p.section, []).append(p)
        return out

    def to_json(self) -> list[dict]:
        """Serialised view for the /api/panels endpoint."""
        return [
            {
                "id": p.id,
                "title": p.title,
                "section": p.section,
                "scope": p.scope,
                "unit": p.unit,
                "y_min": p.y_min,
                "y_max": p.y_max,
                "color": p.color,
                "show_threshold": p.show_threshold,
                "description": p.description,
            }
            for p in self._panels
        ]


def default_panel_registry() -> PanelRegistry:
    here = Path(__file__).resolve().parent.parent
    config_path = Path(os.environ.get("PANELS_CONFIG", here / "config" / "panels.json"))
    return PanelRegistry(load_panels(config_path))
