"""Utilities for iterating over configured table partitions."""

from typing import Dict, Iterable


def get_partitions(config: Dict) -> Iterable[Dict]:
    """Yield partition dictionaries from the loaded configuration."""
    part_cfg = config.get("partitioning", {})
    scope = part_cfg.get("scope", [])
    for part in scope:
        yield part
