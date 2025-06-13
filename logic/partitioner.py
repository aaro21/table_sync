"""Utilities for iterating over configured table partitions."""

from typing import Dict, Iterable


def get_partitions(config: Dict) -> Iterable[Dict]:
    """Yield partition dictionaries expanded from the config's partitioning scope.
    Supports optional weekly partitioning using 'weeks' lists.
    Falls back to month-only partitions if no weeks are specified."""

    scope = config.get("partitioning", {}).get("scope", [])

    for entry in scope:
        year = str(entry["year"])
        month = str(entry["month"]).zfill(2)

        if "weeks" in entry:
            for week in entry["weeks"]:
                yield {
                    "year": year,
                    "month": month,
                    "week": str(week)
                }
        else:
            yield {
                "year": year,
                "month": month
            }
