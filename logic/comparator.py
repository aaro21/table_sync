"""Functions for comparing row dictionaries between databases."""

from __future__ import annotations

from typing import Any

from dateutil import parser


def values_equal(source_val: Any, dest_val: Any) -> bool:
    """Return ``True`` if the two provided values should be considered equal."""
    # Attempt numeric comparison with tolerance
    try:
        a = float(source_val)
        b = float(dest_val)
        if abs(a - b) < 1e-5:
            return True
    except Exception:
        pass

    # Attempt date comparison ignoring time component
    try:
        d1 = parser.parse(str(source_val)).date()
        d2 = parser.parse(str(dest_val)).date()
        if d1 == d2:
            return True
    except Exception:
        pass

    # Fallback to string equality
    return str(source_val) == str(dest_val)


def compare_rows(source_row: dict, dest_row: dict, column_map: dict) -> list[dict]:
    """
    Compares two rows column-by-column using the logical column names.

    Returns a list of mismatched columns with source and destination values.
    Each mismatch is represented as:
        {
            "column": "logical_column_name",
            "source_value": ...,
            "dest_value": ...
        }
    """
    mismatches = []
    for logical_col in column_map.keys():
        src_val = source_row.get(logical_col)
        dest_val = dest_row.get(logical_col)
        if not values_equal(src_val, dest_val):
            mismatches.append({
                "column": logical_col,
                "source_value": src_val,
                "dest_value": dest_val,
            })
    return mismatches
