from __future__ import annotations


def format_partition(partition: dict) -> str:
    """Return a short string describing *partition* for progress bars."""
    if not partition:
        return "all"

    parts = []
    year = partition.get("year")
    month = partition.get("month")
    week = partition.get("week")

    if year is not None:
        parts.append(str(year))
    if month is not None:
        parts.append(str(month))
    if week is not None:
        parts.append(f"W{week}")

    return "-".join(parts)

