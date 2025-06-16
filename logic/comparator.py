from __future__ import annotations

from decimal import Decimal
from datetime import datetime

"""Functions for comparing row dictionaries between databases."""

from typing import Any, Iterable, Optional
import hashlib
from concurrent.futures import ThreadPoolExecutor

from dateutil import parser
from tqdm import tqdm

from utils.logger import debug_log


def normalize_value(val: Any) -> str:
    """Normalize a single value for hashing consistency."""
    if val is None:
        return "NULL"
    if isinstance(val, float):
        return f"{val:.5f}"
    return str(val).strip()


def compute_row_hash(row: dict) -> str:
    """Generate a consistent hash for the provided row."""
    values = [normalize_value(v) for v in row.values()]
    joined = "|".join(values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


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


def sanitize(value: Any) -> Any:
    """Normalize *value* for comparison purposes."""
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        pass
    try:
        return parser.parse(str(value)).date()
    except Exception:
        pass
    return str(value).strip()


def sanitize_row(row: dict, columns: Iterable[str]) -> dict:
    """Return a new row dict with sanitized values for *columns*."""
    return {col: sanitize(row.get(col)) for col in columns}


def compare_row_pair(args: tuple) -> Optional[list[dict]]:
    """Compare a single pair of rows for multiprocessing."""
    source_row, dest_row, column_map, config = args

    if config.get("comparison", {}).get("use_row_hash", False):
        if compute_row_hash(source_row) == compute_row_hash(dest_row):
            return None

    return compare_rows(
        source_row,
        dest_row,
        column_map,
        use_row_hash=config.get("comparison", {}).get("use_row_hash", False),
        config=config,
    )


def compare_row_pair_by_pk(args: tuple) -> dict:
    """Compare two rows and return mismatches keyed by primary key."""
    pk, src_row, dest_row, columns, config = args
    include_nulls = config.get("comparison", {}).get("include_nulls", False)
    use_row_hash = config.get("comparison", {}).get("use_row_hash", False)

    src_hash = dest_hash = None
    if use_row_hash:
        src_hash = compute_row_hash(src_row)
        dest_hash = compute_row_hash(dest_row)
        if src_hash == dest_hash:
            return {"primary_key": pk, "mismatches": []}

    mismatches: list[dict] = []
    for col in columns:
        src_val = sanitize(src_row.get(col))
        dest_val = sanitize(dest_row.get(col))
        if values_equal(src_val, dest_val):
            continue
        if not include_nulls and (src_val is None or dest_val is None):
            continue
        mismatch = {
            "column": col,
            "source_value": src_val,
            "dest_value": dest_val,
        }
        if use_row_hash:
            mismatch["source_hash"] = src_hash
            mismatch["dest_hash"] = dest_hash
        mismatches.append(mismatch)

    return {"primary_key": pk, "mismatches": mismatches}


def compare_rows(
    source_row: dict,
    dest_row: dict,
    column_map: dict,
    use_row_hash: bool = False,
    config: Optional[dict] = None,
) -> list[dict]:
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
    pk_field = next(iter(column_map.keys()))
    debug_log(
        f"Comparing source row {source_row.get(pk_field)}",
        config,
        level="high",
    )
    src_hash = dest_hash = None

    if use_row_hash:
        src_hash = compute_row_hash(source_row)
        dest_hash = compute_row_hash(dest_row)
        debug_log(
            f"Source hash: {src_hash}, Dest hash: {dest_hash}",
            config,
            level="high",
        )
        if src_hash == dest_hash:
            debug_log(
                f"Skipping row {source_row.get(pk_field)} - hashes match",
                config,
                level="high",
            )
            return mismatches
    column_iter = column_map.keys()
    for logical_col in column_iter:
        src_val = sanitize(source_row.get(logical_col))
        dest_val = sanitize(dest_row.get(logical_col))
        if not values_equal(src_val, dest_val):
            debug_log(
                f"MISMATCH: col={logical_col}, src={src_val}, dest={dest_val}",
                config,
                level="high",
            )
            mismatch = {
                "column": logical_col,
                "source_value": src_val,
                "dest_value": dest_val,
            }
            if use_row_hash:
                mismatch["source_hash"] = src_hash
                mismatch["dest_hash"] = dest_hash
            mismatches.append(mismatch)

    return mismatches


def compare_row_pairs(
    row_pairs: Iterable[tuple],
    *,
    parallel: bool = False,
    workers: int = 4,
) -> list[dict]:
    """Compare row pairs and return mismatch details keyed by primary key."""

    pairs = list(row_pairs)
    if not pairs:
        return []

    column_map = pairs[0][2]
    config = pairs[0][3]
    columns = list(column_map.keys())
    only_cols = config.get("comparison", {}).get("only_columns")
    if only_cols:
        columns = [c for c in columns if c in only_cols]

    pk_col = config.get("columns", {}).get("primary_key", config.get("primary_key"))
    use_row_hash = config.get("comparison", {}).get("use_row_hash", False)

    mismatched_pairs: list[tuple[dict, dict]] = []

    for src_row, dest_row, _col_map, cfg in pairs:
        if use_row_hash:
            if compute_row_hash(src_row) == compute_row_hash(dest_row):
                continue
        mismatched_pairs.append((src_row, dest_row))

    tasks = [
        (src[pk_col], src, dest, columns, config)
        for src, dest in mismatched_pairs
    ]

    if not tasks:
        return []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(
            tqdm(
                executor.map(compare_row_pair_by_pk, tasks),
                total=len(tasks),
                desc="Parallel mismatch comparison",
            )
        )

    return [r for r in results if r["mismatches"]]
