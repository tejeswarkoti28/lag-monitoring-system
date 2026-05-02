"""Panel registry.

Panels are declarative — defined in config/panels.json — so adding a new chart
to the dashboard is editing JSON, not Python. The Panel dataclass below carries
each chart's metadata; the registry loader reads the config file and returns a
list of Panel objects the rest of the app can index by id.
"""
from .base import Panel
from .registry import load_panels, PanelRegistry

__all__ = ["Panel", "PanelRegistry", "load_panels"]
