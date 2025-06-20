from __future__ import annotations

from decimal import Decimal
from datetime import datetime

"""Functions for comparing row dictionaries between databases."""

from typing import Any, Iterable, Optional, Tuple, List
import gc
import xxhash
import multiprocessing as mp
import platform
import os
from itertools import islice, chain
from pqdm.processes import pqdm
from pqdm.threads import pqdm as pqdm_threads
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    as_completed,
)

from dateutil import parser

from utils.logger import debug_log
from tqdm import tqdm

if platform.system() == "Windows":
    mp.freeze_support()
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


def compute_row_hashes_parallel(
    rows: list[dict], *, workers: int = 4, mode: str = None
) -> list[str]:
    """Compute hashes for rows in parallel using thread or process mode."""
    if mode is None:
        mode = "process" if platform.system() == "Windows" else "thread"
    if mode not in ("thread", "process"):
        debug_log(f"Unknown parallel mode '{mode}', defaulting to 'thread'", None, level="medium")
        mode = "thread"
    pq = pqdm_threads if mode == "thread" else pqdm
    debug_log(
        f"Using {'threads' if mode == 'thread' else 'processes'} for parallel hashing (mode={mode})",
        None,
        level="high",
    )
    return list(
        pq(rows, n_jobs=workers, function=_hash_row, desc="Hashing rows", disable=True)
    )


def _hash_pair(pair: tuple) -> tuple[str, str]:
    """Return ``(src_hash, dest_hash)`` for a row pair."""
    src_row, dest_row = pair[0], pair[1]
    return compute_row_hash(src_row), compute_row_hash(dest_row)


def discard_matching_rows_by_hash(
    src_rows: list[dict],
    dest_rows: list[dict],
    primary_key: str,
    *,
    workers: int = 4,
    mode: str = "thread",
    config: dict | None = None,
) -> tuple[list[dict], list[dict], int, int]:
    """Return filtered row lists with matching hashes removed."""

    source_by_pk = {row[primary_key]: row for row in src_rows}
    dest_by_pk = {row[primary_key]: row for row in dest_rows}

    intersect = sorted(set(source_by_pk) & set(dest_by_pk))
    if not intersect:
        return src_rows, dest_rows, 0, 0

    debug_log(
        f"Hashing {len(intersect)} intersecting rows for cleanup",
        config,
        level="medium",
    )

    src_hashes = compute_row_hashes_parallel(
        [source_by_pk[k] for k in intersect], workers=workers, mode=mode
    )
    dest_hashes = compute_row_hashes_parallel(
        [dest_by_pk[k] for k in intersect], workers=workers, mode=mode
    )

    discarded = 0
    for pk, s_h, d_h in zip(intersect, src_hashes, dest_hashes):
        if s_h == d_h:
            source_by_pk.pop(pk, None)
            dest_by_pk.pop(pk, None)
            discarded += 1

    kept = len(intersect) - discarded

    filtered_src = sorted(source_by_pk.values(), key=lambda r: r[primary_key])
    filtered_dest = sorted(dest_by_pk.values(), key=lambda r: r[primary_key])

    return filtered_src, filtered_dest, discarded, kept


def _chunked(iterable: Iterable, size: int) -> Iterable[list]:
    """Yield lists of *size* elements from *iterable*."""
    data = list(iterable)
    for i in range(0, len(data), size):
        yield data[i:i + size]


def _filter_pairs_by_hash(
    row_pairs: Iterable[tuple],
    *,
    workers: int = 4,
    chunk_size: int = 100_000,
    mode: str = "process" if platform.system() == "Windows" else "thread",
) -> Iterable[tuple]:
    """Yield row pairs where source and destination row hashes differ.

    ``mode`` selects ``"thread"`` or ``"process"`` execution for hash
    computation.
    """
    debug_log(
        f"_filter_pairs_by_hash using mode={mode}",
        None,
        level="high",
    )

    def process(chunk: list[tuple], chunk_idx: int) -> list[tuple]:
        debug_log(
            f"Processing chunk {chunk_idx + 1} with {len(chunk)} pairs",
            None,
            level="medium",
        )
        src_rows = [p[0] for p in chunk]
        dest_rows = [p[1] for p in chunk]
        src_hashes = compute_row_hashes_parallel(src_rows, workers=workers, mode=mode)
        dest_hashes = compute_row_hashes_parallel(dest_rows, workers=workers, mode=mode)
        result: list[tuple] = []
        for pair, s_h, d_h in zip(chunk, src_hashes, dest_hashes):
            if s_h != d_h:
                result.append(pair)
        return result

    # Stream chunks to avoid materializing all pairs at once
    for i, chunk in enumerate(
        tqdm(_chunked(list(row_pairs), chunk_size), desc="Filtering mismatched row hashes", unit="chunk")
    ):
        for pair in process(chunk, i):
            yield pair
        # Explicitly free memory from processed chunk
        del chunk
        gc.collect()


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

    debug_log(
        f"Comparing row with PK={pk} using columns: {columns}", config, level="high"
    )
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

    if progress is not None:
        progress.total = len(src_rows)
        if hasattr(progress, "refresh"):
            progress.refresh()

    columns = list(col_maps[0].keys())
    only_cols = configs[0].get("comparison", {}).get("only_columns")
    if only_cols:
        columns = [c for c in columns if c in only_cols]

    df_src = pd.DataFrame(src_rows)[columns]
    df_dest = pd.DataFrame(dest_rows)[columns]

    # Sanitize using vectorized logic
    df_src = df_src.applymap(sanitize)
    df_dest = df_dest.applymap(sanitize)

    from concurrent.futures import ThreadPoolExecutor

    mismatches = []

    def compare_column(col):
        results = []
        src_col = df_src[col].to_numpy()
        dest_col = df_dest[col].to_numpy()
        equal_mask = src_col == dest_col

        for idx, match in enumerate(equal_mask):
            if match:
                continue
            if not configs[idx].get("comparison", {}).get("include_nulls", False):
                if pd.isnull(src_col[idx]) or pd.isnull(dest_col[idx]):
                    continue
            mismatch = {
                "primary_key": src_rows[idx].get(
                    configs[idx]
                    .get("columns", {})
                    .get("primary_key", configs[idx].get("primary_key"))
                ),
                "column": col,
                "source_value": src_col[idx],
                "dest_value": dest_col[idx],
            }
            if partitions[idx] is not None:
                mismatch["partition"] = partitions[idx]
            results.append(mismatch)
        return results

    with ThreadPoolExecutor(max_workers=min(len(columns), (os.cpu_count() or 1))) as executor:
        futures = [executor.submit(compare_column, col) for col in columns]
        for future in as_completed(futures):
            results = future.result()
            mismatches.extend(results)
            if progress is not None:
                if hasattr(progress, "update"):
                    progress.update(len(results))
                else:
                    progress.n += len(results)
                    progress.refresh()

    for result in mismatches:
        debug_log(
            f"Yielding mismatch result for PK={result.get('primary_key')}: {result}",
            configs[0],
            level="high",
        )
        yield result

    # Explicitly free memory from large structures
    del df_src, df_dest, mismatches
    gc.collect()


def compare_row_pairs_parallel_detailed(
    row_pairs: Iterable[tuple],
    *,
    workers: int = 4,
    progress=None,
    parallel_mode: str = "thread",
) -> Iterable[dict]:
    """Yield mismatch details for each pair keyed by primary key in parallel.

    This function parallelizes detailed row comparisons using either threads or
    processes depending on ``parallel_mode``. Accepted values are ``"thread"``
    (default) and ``"process"``. When ``parallel_mode`` is ``"batch"`` the call
    is delegated to :func:`compare_row_pairs_parallel_batch`.
    ``row_pairs`` must yield ``(src_row, dest_row, columns, config)`` tuples and
    may optionally include a partition mapping as a 5th element.
    """

    if parallel_mode == "batch":
        return compare_row_pairs_parallel_batch(
            row_pairs, workers=workers, progress=progress, parallel_mode="thread"
        )

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

    def prepare_row_pairs_generator():
        nonlocal cfg_ref
        for item in row_pairs:
            if len(item) == 4:
                src_row, dest_row, col_map, config = item
                part = None
            else:
                src_row, dest_row, col_map, config, part = item
            cfg_ref = cfg_ref or config
            cols = [
                c
                for c in col_map.keys()
                if not config.get("comparison", {}).get("only_columns")
                or c in config["comparison"]["only_columns"]
            ]
            yield {
                "src_row": src_row,
                "dest_row": dest_row,
                "columns": cols,
                "config": config,
                "partition": part,
            }

    # If progress is not None and total is None, try to estimate total
    # This is not possible without materializing, so skip.

    debug_log(f"Executor: {'ThreadPoolExecutor' if parallel_mode == 'thread' else 'ProcessPoolExecutor'} with {workers} workers", cfg_ref, level="medium")
    executor_cls = ThreadPoolExecutor if parallel_mode == "thread" else ProcessPoolExecutor
    if parallel_mode == "process" and platform.system() == "Windows":
        mp.freeze_support()
    with executor_cls(max_workers=workers) as executor:
        futures = [
            executor.submit(compare_row_pair_by_pk, **kwargs)
            for kwargs in prepare_row_pairs_generator()
        ]

        for future in as_completed(futures):
            result = future.result()
            if progress is not None:
                if hasattr(progress, "update"):
                    progress.update(1)
                else:
                    progress.n += 1
                    progress.refresh()
            if result:
                yield result

    # Explicitly free memory from large structures if any
    gc.collect()


def compare_row_pairs_parallel_batch(
    row_pairs: Iterable[tuple],
    *,
    workers: int = 4,
    progress=None,
    chunk_size: int = 100_000,
    parallel_mode: str = "process" if platform.system() == "Windows" else "thread",
) -> Iterable[dict]:
    """Filter row pairs by hash in chunks then compare mismatched pairs.

    ``parallel_mode`` controls whether hashing and comparison use threads or
    processes. Accepted values mirror those of
    :func:`compare_row_pairs_parallel_detailed`.
    """
    debug_log(
        f"compare_row_pairs_parallel_batch using parallel_mode={parallel_mode}",
        None,
        level="high",
    )
    filtered = _filter_pairs_by_hash(
        row_pairs, workers=workers, chunk_size=chunk_size, mode=parallel_mode
    )
    for result in compare_row_pairs_parallel_detailed(
        filtered, workers=workers, progress=progress, parallel_mode="thread"
    ):
        yield result


def compare_row_pairs(
    row_pairs: Iterable[tuple],
    *,
    workers: int = 4,
    progress=None,
) -> Iterable[dict]:
    it = iter(row_pairs)
    first = next(it, None)
    if first is None:
        return []

    config = first[3] if len(first) >= 4 else None

    use_parallel = (
        config.get("comparison", {}).get("parallel", False) if config else False
    )
    parallel_mode = (
        config.get("comparison", {}).get("parallel_mode", "thread")
        if config
        else "thread"
    )

    pairs = chain([first], it)

    if use_parallel:
        if parallel_mode == "batch":
            return compare_row_pairs_parallel_batch(
                pairs,
                workers=workers,
                progress=progress,
                parallel_mode=parallel_mode,
            )
        return compare_row_pairs_parallel_detailed(
            pairs, workers=workers, progress=progress, parallel_mode=parallel_mode
        )
    return compare_row_pairs_serial(pairs, progress=progress)
