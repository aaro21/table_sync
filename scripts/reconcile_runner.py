"""Command line entry point for running table reconciliation."""

import argparse
from typing import Any
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import subprocess
import sys
import os
import json

from logic.config_loader import load_config
from connectors.oracle_connector import get_oracle_connection
from connectors.sqlserver_connector import get_sqlserver_connection
from logic.partitioner import get_partitions
from runners.reconcile import fetch_rows
from logic.comparator import compare_row_pairs
from logic.reporter import DiscrepancyWriter
from utils.logger import debug_log
from utils import format_partition
from tqdm import tqdm
from collections import defaultdict
import threading

from scripts.fix_mismatches import main as fix_mismatches_main


def fetch_all_rows(**kwargs):
    """Helper to materialize rows in a worker thread."""
    return list(fetch_rows(**kwargs))


def process_partition(
    partition: dict,
    config: dict,
    src_env: dict,
    dest_env: dict,
    *,
    src_schema: str,
    dest_schema: str,
    src_table: str,
    dest_table: str,
    src_cols: dict,
    dest_cols: dict,
    primary_key: str,
    output_schema: str,
    output_table: str,
    year_column: str,
    month_column: str,
    week_column: str | None,
    src_dialect: str,
    dest_dialect: str,
    workers: int,
    pbar,
    use_row_hash: bool,
    sample: list,
    seen_pks: set,
    start_event: threading.Event,
    done_event: threading.Event,
) -> None:
    """Process a single partition using its own database connections."""

    start_event.wait()
    with get_oracle_connection(src_env, config) as src_conn, get_sqlserver_connection(dest_env, config) as dest_conn:
        dest_conn.cursor().fast_executemany = True
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_src = executor.submit(
                fetch_all_rows,
                conn=src_conn,
                schema=src_schema,
                table=src_table,
                columns=src_cols,
                partition=partition,
                primary_key=primary_key,
                year_column=year_column,
                month_column=month_column,
                dialect=src_dialect,
                week_column=week_column,
                config=config,
                limit=config.get("limit"),
                pk_value=config.get("record_pk"),
            )
            future_dest = executor.submit(
                fetch_all_rows,
                conn=dest_conn,
                schema=dest_schema,
                table=dest_table,
                columns=dest_cols,
                partition=partition,
                primary_key=primary_key,
                year_column=year_column,
                month_column=month_column,
                dialect=dest_dialect,
                week_column=week_column,
                config=config,
                limit=config.get("limit"),
                pk_value=config.get("record_pk"),
            )

            src_rows = future_src.result()
            dest_rows = future_dest.result()

        # Signal that queries for this partition have completed
        done_event.set()

        debug_log(
            f"Fetched {len(src_rows)} source rows and {len(dest_rows)} destination rows",
            config,
            level="medium",
        )

        with DiscrepancyWriter(dest_conn, output_schema, output_table) as writer:

            def write_record(record: dict) -> None:
                debug_log(f"WRITING: {record}", config, level="medium")
                writer.write(record)
                if config.get("output_mismatches"):
                    print(record)

            src_iter = iter(src_rows)
            dest_iter = iter(dest_rows)

            src_row = next(src_iter, None)
            dest_row = next(dest_iter, None)

            total_rows = max(len(src_rows), len(dest_rows))

            def row_pairs():
                use_bar = total_rows <= 1000
                bar_ctx = (
                    tqdm(total=total_rows, desc="processing row pairs", unit="row")
                    if use_bar
                    else nullcontext()
                )
                with bar_ctx as progress:
                    if use_bar:
                        progress.set_postfix_str(format_partition(partition))
                    nonlocal src_row, dest_row
                    while src_row is not None or dest_row is not None:
                        if src_row is not None:
                            assert primary_key in src_row, f"Primary key '{primary_key}' missing in source row: {src_row}"
                        if dest_row is not None:
                            assert primary_key in dest_row, f"Primary key '{primary_key}' missing in destination row: {dest_row}"

                        src_key = src_row[primary_key] if src_row else None
                        dest_key = dest_row[primary_key] if dest_row else None

                        if src_row and dest_row and src_key == dest_key:
                            yield (src_row, dest_row, src_cols, config, partition)
                            src_row = next(src_iter, None)
                            dest_row = next(dest_iter, None)
                        elif dest_row is None or (src_row and src_key < dest_key):
                            write_record({
                                "primary_key": src_key,
                                "type": "missing_in_dest",
                                "column": None,
                                "source_value": src_row,
                                "dest_value": None,
                                "year": partition["year"],
                                "month": partition["month"],
                                "week": partition.get("week"),
                            })
                            src_row = next(src_iter, None)
                        else:
                            write_record({
                                "primary_key": dest_key,
                                "type": "extra_in_dest",
                                "column": None,
                                "source_value": None,
                                "dest_value": dest_row,
                                "year": partition["year"],
                                "month": partition["month"],
                                "week": partition.get("week"),
                            })
                            dest_row = next(dest_iter, None)

                        if use_bar:
                            progress.update(1)

            for result in compare_row_pairs(
                row_pairs(),
                workers=workers,
                progress=pbar,
            ):
                debug_log(f"Compare result: {result}", config, level="low")
                src_key = result["primary_key"]
                part = result.get("partition", {})
                if result["mismatches"]:
                    if src_key not in seen_pks and len(sample) < 2:
                        sample.append((src_key, result["mismatches"][0]))
                        seen_pks.add(src_key)
                    for diff in result["mismatches"]:
                        write_record(
                            {
                                "primary_key": src_key,
                                "type": "mismatch",
                                "column": diff["column"],
                                "source_value": diff["source_value"],
                                "dest_value": diff["dest_value"],
                                **(
                                    {
                                        "source_hash": diff.get("source_hash"),
                                        "dest_hash": diff.get("dest_hash"),
                                    }
                                    if use_row_hash
                                    else {}
                                ),
                                "year": part.get("year"),
                                "month": part.get("month"),
                                "week": part.get("week"),
                            }
                        )

            writer.flush()

        partition_env = {
            "PARTITION_YEAR": str(partition.get("year", "")),
            "PARTITION_MONTH": str(partition.get("month", "")),
            "PARTITION_WEEK": str(partition.get("week", "")),
        }

        debug_log(
            f"Running fix_mismatches.py for partition: {partition_env}",
            config,
            level="low",
        )

        original_environ = os.environ.copy()
        os.environ.update(partition_env)

        sys.argv = ["fix_mismatches", "--apply"]

        fix_mismatches_main()

        os.environ.clear()
        os.environ.update(original_environ)


def main():
    """Run the reconciliation process based on ``config/config.yaml``."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help="path to the config YAML file to use",
    )
    parser.add_argument(
        "--debug",
        nargs="?",
        const="high",
        choices=["low", "medium", "high"],
        help="set debug level",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="maximum number of rows to fetch per table",
    )
    parser.add_argument(
        "--output-mismatches",
        action="store_true",
        help="print discrepancy records to stdout",
    )
    parser.add_argument(
        "--record",
        help="process only the record with this primary key value",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    if args.debug:
        config["debug"] = args.debug
    if args.limit:
        config["limit"] = args.limit
    if args.output_mismatches:
        config["output_mismatches"] = True
    if args.record:
        config["record_pk"] = args.record

    debug_log("Starting reconciliation run", config, level="low")

    src_env = config["source"]["resolved_env"]
    dest_env = config["destination"]["resolved_env"]
    src_schema = config["source"].get("schema", "")
    dest_schema = config["destination"].get("schema", "")
    src_table = config["source"]["table"]
    dest_table = config["destination"]["table"]
    src_cols = config["source"]["columns"]
    dest_cols = config["destination"]["columns"]
    primary_key = config["primary_key"]
    output_schema = config["output"].get("schema", "")
    output_table = config["output"]["table"]

    year_column = config["partitioning"]["year_column"]
    month_column = config["partitioning"]["month_column"]
    week_column = config["partitioning"].get("week_column")

    src_dialect = config["source"].get("type", "sqlserver").lower()
    dest_dialect = config["destination"].get("type", "sqlserver").lower()
    use_row_hash = config.get("comparison", {}).get("use_row_hash", False)
    use_parallel = config.get("comparison", {}).get("parallel", False)

    try:
        sample: list[tuple[Any, dict]] = []
        seen_pks: set[Any] = set()
        workers = config.get("comparison", {}).get("workers", 4)
        partitions = list(get_partitions(config))

        groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
        order: list[tuple[str, str]] = []
        for part in partitions:
            key = (part["year"], part["month"])
            if key not in groups:
                order.append(key)
            groups[key].append(part)

        with tqdm(total=len(partitions), desc="partitions", unit="part") as partbar, tqdm(desc="mismatches found", unit="row") as pbar:
            for key in order:
                parts = groups[key]
                if len(parts) > 1 and all("week" in p for p in parts):
                    start_event = threading.Event()
                    start_event.set()
                    with ThreadPoolExecutor(max_workers=len(parts)) as executor:
                        futures = []
                        prev = start_event
                        for p in parts:
                            partbar.set_postfix_str(format_partition(p))
                            partbar.update(1)
                            next_evt = threading.Event()
                            futures.append(
                                executor.submit(
                                    process_partition,
                                    p,
                                    config,
                                    src_env,
                                    dest_env,
                                    src_schema=src_schema,
                                    dest_schema=dest_schema,
                                    src_table=src_table,
                                    dest_table=dest_table,
                                    src_cols=src_cols,
                                    dest_cols=dest_cols,
                                    primary_key=primary_key,
                                    output_schema=output_schema,
                                    output_table=output_table,
                                    year_column=year_column,
                                    month_column=month_column,
                                    week_column=week_column,
                                    src_dialect=src_dialect,
                                    dest_dialect=dest_dialect,
                                    workers=workers,
                                    pbar=pbar,
                                    use_row_hash=use_row_hash,
                                    sample=sample,
                                    seen_pks=seen_pks,
                                    start_event=prev,
                                    done_event=next_evt,
                                )
                            )
                            prev = next_evt
                        for fut in futures:
                            fut.result()
                else:
                    for p in parts:
                        partbar.set_postfix_str(format_partition(p))
                        partbar.update(1)
                        start_evt = threading.Event()
                        start_evt.set()
                        done_evt = threading.Event()
                        process_partition(
                            p,
                            config,
                            src_env,
                            dest_env,
                            src_schema=src_schema,
                            dest_schema=dest_schema,
                            src_table=src_table,
                            dest_table=dest_table,
                            src_cols=src_cols,
                            dest_cols=dest_cols,
                            primary_key=primary_key,
                            output_schema=output_schema,
                            output_table=output_table,
                            year_column=year_column,
                            month_column=month_column,
                            week_column=week_column,
                            src_dialect=src_dialect,
                            dest_dialect=dest_dialect,
                            workers=workers,
                            pbar=pbar,
                            use_row_hash=use_row_hash,
                            sample=sample,
                            seen_pks=seen_pks,
                            start_event=start_evt,
                            done_event=done_evt,
                        )

        for pk, diff in sample:
            debug_log(
                f"Sample mismatch PK {pk}, column {diff['column']}: {diff['source_value']} -> {diff['dest_value']}",
                config,
                level="medium",
            )

        debug_log("Reconciliation complete", config, level="low")

    except Exception as exc:  # pragma: no cover - runtime failure
        debug_log(f"Reconciliation failed: {exc}", config, level="low")
        raise


if __name__ == "__main__":
    main()
