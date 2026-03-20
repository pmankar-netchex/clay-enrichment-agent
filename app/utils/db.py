"""
Lightweight DuckDB result helpers — no internal app imports.
"""

from typing import Any


def fetchall_as_dicts(result) -> list[dict[str, Any]]:
    """Convert a DuckDB query result to a list of dicts without requiring pandas/numpy."""
    cols = [desc[0] for desc in result.description]
    return [dict(zip(cols, row)) for row in result.fetchall()]
