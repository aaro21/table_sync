from __future__ import annotations

from decimal import Decimal
from datetime import datetime

"""Functions for comparing row dictionaries between databases."""

from typing import Any, Iterable, Optional, Tuple, List
import xxhash
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

from dateutil import parser

from utils.logger import debug_log
from tqdm import tqdm
import pandas as pd


def normalize_value(val: Any) -> str:
    """Normalize a single value for hashing consistency.

    The same sanitization rules used for direct value comparisons are
    applied here so that logically equivalent values (e.g. ``18.2`` and
    ``18.20`` or date strings with and without a time component) hash to
    the same value.
    """
    val = sanitize(val)
    if val is None:
        return "NULL"
    if isinstance(val, float):
        return f"{val:.5f}"
    return str(val).strip()


def compute_row_hash(row: dict) -> str:
    """Generate a consistent and fast hash for the provided row."""
    ordered_keys = sorted(row.keys())
    h = xxhash.xxh64()
    for k in ordered_keys:
        val = normalize_value(row.get(k))
        h.update(val.encode("utf-8"))
    return h.hexdigest()


def _hash_row(row: dict) -> str:
    """Top-level function so it can be used with multiprocessing."""
    return compute_row_hash(row)


def compute_row_hashes_parallel(rows: list[dict], *, workers: int = 4, mode: str = "thread") -> list[str]:
    """Compute hashes for rows in parallel using thread or process mode."""
    Executor = ThreadPoolExecutor if mode == "thread" else ProcessPoolExecutor
    debug_log(f"Using {Executor.__name__} for parallel hashing", None, level="low")
    with Executor(max_workers=workers) as executor:
        return list(executor.map(_hash_row, rows))


def _hash_pair(pair: tuple) -> tuple[str, str]:
    """Return ``(src_hash, dest_hash)`` for a row pair."""
    src_row, dest_row = pair[0], pair[1]
    return compute_row_hash(src_row), compute_row_hash(dest_row)


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


def compare_row_pair_by_pk(
    src_row: dict,
    dest_row: dict,
    columns: Iterable[str],
    config: dict,
    *,
    hashes: Optional[Tuple[str, str]] = None,
    partition: Optional[dict] = None,
) -> Optional[dict]:
    """Compare two rows and return mismatches keyed by primary key."""
    pk_col = config.get("columns", {}).get("primary_key", config.get("primary_key"))
    pk = src_row.get(pk_col)
    include_nulls = config.get("comparison", {}).get("include_nulls", False)
    use_row_hash = config.get("comparison", {}).get("use_row_hash", False)

    src_hash = dest_hash = None
    if use_row_hash:
        if hashes:
            src_hash, dest_hash = hashes
        else:
            src_hash = compute_row_hash(src_row)
            dest_hash = compute_row_hash(dest_row)
        if src_hash == dest_hash:
            return None

    debug_log(f"Comparing row with PK={pk} using columns: {columns}", config, level="high")
    for col in columns:
        src_val = sanitize(src_row.get(col))
        dest_val = sanitize(dest_row.get(col))
        debug_log(f"  {col}: src={src_val} dest={dest_val}", config, level="high")

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

    # Only emit a log entry if mismatches are found. Suppress messages when
    # all columns match to avoid noisy output when processing thousands of
    # rows during reconciliation.
    if mismatches:
        debug_log(
            f"Row {pk}: {len(mismatches)} mismatching columns",
            config,
            level="medium",
        )
        result = {"primary_key": pk, "mismatches": mismatches}
        if partition is not None:
            result["partition"] = partition
        return result
    return None


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


def compare_row_pairs_serial(
    row_pairs: Iterable[tuple],
    *,
    progress=None,
) -> Iterable[dict]:
    """Yield mismatch details for each pair keyed by primary key.

    ``row_pairs`` must yield ``(src_row, dest_row, columns, config)`` tuples and
    may optionally include a partition mapping as a 5th element. The previous
    implementation materialised all pairs before processing which meant progress
    bars only appeared once the entire input was exhausted. This streaming
    version processes each pair as it arrives so progress is updated in real
    time. Row hashing and column filtering are handled per pair and results are
    yielded immediately.
    """

    # Collect all rows for batch comparison
    src_rows, dest_rows, partitions = [], [], []
    col_maps, configs = [], []

    for item in row_pairs:
        if len(item) == 4:
            src_row, dest_row, col_map, config = item
            part = None
        else:
            src_row, dest_row, col_map, config, part = item
        src_rows.append(src_row)
        dest_rows.append(dest_row)
        col_maps.append(col_map)
        configs.append(config)
        partitions.append(part)

    if not src_rows or not dest_rows:
        return

    columns = list(col_maps[0].keys())
    df_src = pd.DataFrame(src_rows)[columns]
    df_dest = pd.DataFrame(dest_rows)[columns]

    # Sanitize using vectorized logic
    df_src = df_src.applymap(sanitize)
    df_dest = df_dest.applymap(sanitize)

    from concurrent.futures import ThreadPoolExecutor

    mismatches = []

    def compare_column(col):
        results = []
        src_col = df_src[col]
        dest_col = df_dest[col]
        equal = src_col.eq(dest_col)
        for idx, match in enumerate(equal):
            if match:
                continue
            if not configs[idx].get("comparison", {}).get("include_nulls", False):
                if pd.isnull(src_col.iloc[idx]) or pd.isnull(dest_col.iloc[idx]):
                    continue
            mismatch = {
                "primary_key": src_rows[idx].get(configs[idx].get("columns", {}).get("primary_key", configs[idx].get("primary_key"))),
                "column": col,
                "source_value": src_col.iloc[idx],
                "dest_value": dest_col.iloc[idx],
            }
            if partitions[idx] is not None:
                mismatch["partition"] = partitions[idx]
            results.append(mismatch)
        return results

    with ThreadPoolExecutor() as executor:
        future_to_col = {executor.submit(compare_column, col): col for col in columns}
        for future in future_to_col:
            results = future.result()
            mismatches.extend(results)
            if progress is not None:
                progress.update(len(results))

    for result in mismatches:
        debug_log(f"Yielding mismatch result for PK={result.get('primary_key')}: {result}", configs[0], level="high")
        yield result


def compare_row_pairs_parallel_detailed(
    row_pairs: Iterable[tuple],
    *,
    workers: int = 4,
    progress=None,
    parallel_mode: str = "thread",
) -> Iterable[dict]:
    """Yield mismatch details for each pair keyed by primary key in parallel.

    This function uses ThreadPoolExecutor to parallelize detailed row comparisons.
    ``row_pairs`` must yield ``(src_row, dest_row, columns, config)`` tuples and
    may optionally include a partition mapping as a 5th element.
    """

    cfg_ref: Optional[dict] = None

    # Determine total count up front when possible so callers like ``tqdm``
    # can display bounded progress bars. If ``row_pairs`` has no length,
    # ``total`` remains ``None`` and the bar will be unbounded.
    total = None
    try:  # pragma: no cover - ``row_pairs`` may not be sized
        total = len(row_pairs)  # type: ignore[arg-type]
    except Exception:
        pass

    if progress is not None and total is not None:
        progress.total = total
        progress.refresh()

    def _prepare(item: tuple) -> tuple:
        if len(item) == 4:
            src_row, dest_row, col_map, config = item
            part = None
        else:
            src_row, dest_row, col_map, config, part = item
        return src_row, dest_row, col_map, config, part

    tasks = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for item in row_pairs:
            src_row, dest_row, col_map, config, part = _prepare(item)
            cfg_ref = cfg_ref or config
            cols = list(col_map.keys())
            only_cols = config.get("comparison", {}).get("only_columns")
            if only_cols:
                cols = [c for c in cols if c in only_cols]
            src_hashes, dest_hashes = compute_row_hashes_parallel([src_row, dest_row], workers=2, mode="thread")
            src_hash, dest_hash = src_hashes[0], dest_hashes[1]
            tasks.append(
                executor.submit(
                    compare_row_pair_by_pk,
                    src_row,
                    dest_row,
                    cols,
                    config,
                    partition=part,
                    hashes=(src_hash, dest_hash),
                )
            )

        if progress is not None and total is None:
            progress.total = len(tasks)
            progress.refresh()

        for fut in as_completed(tasks):
            result = fut.result()
            if progress is not None:
                if hasattr(progress, "update"):
                    progress.update(1)
                else:
                    progress.n += 1
                    progress.refresh()
            if result:
                yield result


def compare_row_pairs(
    row_pairs: Iterable[tuple],
    *,
    workers: int = 4,
    progress=None,
) -> Iterable[dict]:
    config = None
    for item in row_pairs:
        if len(item) >= 4:
            config = item[3]
            break

    use_parallel = config.get("comparison", {}).get("parallel", False) if config else False
    parallel_mode = config.get("comparison", {}).get("parallel_mode", "thread") if config else "thread"

    if use_parallel:
        return compare_row_pairs_parallel_detailed(row_pairs, workers=workers, progress=progress)
    return compare_row_pairs_serial(row_pairs, progress=progress)
