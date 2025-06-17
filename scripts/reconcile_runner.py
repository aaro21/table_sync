"""Command line entry point for running table reconciliation."""

import argparse
from typing import Any

from logic.config_loader import load_config
from connectors.oracle_connector import get_oracle_connection
from connectors.sqlserver_connector import get_sqlserver_connection
from logic.partitioner import get_partitions
from runners.reconcile import fetch_rows
from logic.comparator import compare_row_pairs
from logic.reporter import DiscrepancyWriter
from utils.logger import debug_log
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


def main():
    """Run the reconciliation process based on ``config/config.yaml``."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        nargs="?",
        const="high",
        choices=["low", "medium", "high"],
        help="set debug level",
    )
    args = parser.parse_args()

    config = load_config()
    if args.debug:
        config["debug"] = args.debug

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
        with get_oracle_connection(src_env, config) as src_conn, get_sqlserver_connection(dest_env, config) as dest_conn, DiscrepancyWriter(dest_conn, output_schema, output_table) as writer:

            def generate_pairs():
                for partition in get_partitions(config):
                    debug_log(
                        f"Partition: {partition}",
                        config,
                        level="low",
                    )

                    with ThreadPoolExecutor(max_workers=2) as executor:
                        src_future = executor.submit(
                            lambda: list(
                                fetch_rows(
                                    src_conn,
                                    src_schema,
                                    src_table,
                                    src_cols,
                                    partition,
                                    primary_key,
                                    year_column,
                                    month_column,
                                    dialect=src_dialect,
                                    week_column=week_column,
                                    config=config,
                                )
                            )
                        )
                        dest_future = executor.submit(
                            lambda: list(
                                fetch_rows(
                                    dest_conn,
                                    dest_schema,
                                    dest_table,
                                    dest_cols,
                                    partition,
                                    primary_key,
                                    year_column,
                                    month_column,
                                    dialect=dest_dialect,
                                    week_column=week_column,
                                    config=config,
                                )
                            )
                        )
                        src_rows = src_future.result()
                        dest_rows = dest_future.result()

                    debug_log(
                        f"Fetched {len(src_rows)} source rows and {len(dest_rows)} destination rows",
                        config,
                        level="medium",
                    )

                    src_iter = iter(src_rows)
                    dest_iter = iter(dest_rows)

                    src_row = next(src_iter, None)
                    dest_row = next(dest_iter, None)

                    total_rows = max(len(src_rows), len(dest_rows))

                    with tqdm(total=total_rows, desc="processing rows", unit="row") as progress:
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
                                writer.write({
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
                                writer.write({
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

                            progress.update(1)

            sample: list[tuple[Any, dict]] = []
            seen_pks = set()
            with tqdm(desc="mismatched rows", unit="row") as pbar:
                for result in compare_row_pairs(
                    generate_pairs(),
                    parallel=use_parallel,
                    progress=pbar,
                ):
                    src_key = result["primary_key"]
                    part = result.get("partition", {})
                    if result["mismatches"]:
                        if src_key not in seen_pks and len(sample) < 2:
                            sample.append((src_key, result["mismatches"][0]))
                            seen_pks.add(src_key)
                        for diff in result["mismatches"]:
                            writer.write({
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
                            })
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
