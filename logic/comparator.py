from __future__ import annotations

from decimal import Decimal
from datetime import datetime

"""Functions for comparing row dictionaries between databases."""

from typing import Any, Iterable, Optional
import hashlib
from concurrent.futures import ProcessPoolExecutor

from dateutil import parser
from tqdm import tqdm
import pandas as pd
import numpy as np

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
) -> list[Optional[list[dict]]]:
    """Compare a sequence of row pairs with a progress bar."""
    if parallel:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            return list(
                tqdm(
                    pool.map(compare_row_pair, row_pairs),
                    total=len(row_pairs),
                    desc="Comparing rows",
                )
            )

    pairs = list(row_pairs)
    if not pairs:
        return []

    column_map = pairs[0][2]
    config = pairs[0][3]
    columns = list(column_map.keys())
    only_cols = config.get("comparison", {}).get("only_columns")
    if only_cols:
        columns = [c for c in columns if c in only_cols]
    primary_key = config.get("primary_key")
    use_row_hash = config.get("comparison", {}).get("use_row_hash", False)

    results: list[Optional[list[dict]]] = [None] * len(pairs)
    mismatched_pairs = []
    mismatched_indices = []
    hashes: list[tuple[str | None, str | None]] = []

    for idx, (src_row, dest_row, _col_map, cfg) in enumerate(
        tqdm(pairs, desc="Comparing rows", total=len(pairs))
    ):
        src_hash = dest_hash = None
        if use_row_hash:
            src_hash = compute_row_hash(src_row)
            dest_hash = compute_row_hash(dest_row)
            if src_hash == dest_hash:
                continue
        mismatched_pairs.append((src_row, dest_row))
        mismatched_indices.append(idx)
        hashes.append((src_hash, dest_hash))
        results[idx] = []

    if mismatched_pairs:
        src_df = pd.DataFrame([src for src, _ in mismatched_pairs])
        dest_df = pd.DataFrame([dest for _, dest in mismatched_pairs])

        for col in columns:
            src_df[col] = src_df[col].astype(str).str.strip()
            dest_df[col] = dest_df[col].astype(str).str.strip()
            if config.get("comparison", {}).get("normalize_types"):
                src_numeric = pd.to_numeric(src_df[col], errors="ignore")
                dest_numeric = pd.to_numeric(dest_df[col], errors="ignore")
                if pd.api.types.is_numeric_dtype(src_numeric) and pd.api.types.is_numeric_dtype(dest_numeric):
                    src_df[col] = src_numeric
                    dest_df[col] = dest_numeric
                else:
                    src_date = pd.to_datetime(src_df[col], errors="ignore")
                    dest_date = pd.to_datetime(dest_df[col], errors="ignore")
                    if pd.api.types.is_datetime64_any_dtype(src_date) and pd.api.types.is_datetime64_any_dtype(dest_date):
                        src_df[col] = src_date.dt.date
                        dest_df[col] = dest_date.dt.date

        src_df["primary_key"] = [src[primary_key] for src, _ in mismatched_pairs]

        diffs = src_df[columns].values != dest_df[columns].values
        print(
            f"Starting column diff scan on {len(mismatched_pairs)} mismatched rows and {len(columns)} columns"
        )
        mismatch_coords = np.where(diffs)
        loop = tqdm(
            zip(*mismatch_coords),
            total=int(diffs.sum()),
            desc="Comparing mismatched columns",
        )
        for row_idx, col_idx in loop:
            logical_col = columns[col_idx]
            pair_idx = mismatched_indices[row_idx]
            mismatch = {
                "column": logical_col,
                "source_value": src_df.iat[row_idx, col_idx],
                "dest_value": dest_df.iat[row_idx, col_idx],
            }
            if use_row_hash:
                mismatch["source_hash"] = hashes[row_idx][0]
                mismatch["dest_hash"] = hashes[row_idx][1]
            results[pair_idx].append(mismatch)

    for idx in mismatched_indices:
        if results[idx] == []:
            results[idx] = None

    return results
