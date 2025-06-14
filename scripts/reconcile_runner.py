"""Command line entry point for running table reconciliation."""

import argparse
from concurrent.futures import ProcessPoolExecutor

from logic.config_loader import load_config
from connectors.oracle_connector import get_oracle_connection
from connectors.sqlserver_connector import get_sqlserver_connection
from logic.partitioner import get_partitions
from runners.reconcile import fetch_rows
from logic.comparator import compare_rows, compare_row_pair
from logic.reporter import DiscrepancyWriter
from utils.logger import debug_log


def main():
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

    with get_oracle_connection(src_env, config) as src_conn, get_sqlserver_connection(dest_env, config) as dest_conn:
        writer = DiscrepancyWriter(dest_conn, output_schema, output_table)

        for partition in get_partitions(config):
            debug_log(
                f"Partition: {partition}",
                config,
                level="low",
            )

            src_rows = list(
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
            dest_rows = list(
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

            debug_log(
                f"Fetched {len(src_rows)} source rows and {len(dest_rows)} destination rows",
                config,
                level="medium",
            )

            src_iter = iter(src_rows)
            dest_iter = iter(dest_rows)

            src_row = next(src_iter, None)
            dest_row = next(dest_iter, None)

            row_pairs = []

            while src_row is not None or dest_row is not None:
                src_key = src_row[primary_key] if src_row else None
                dest_key = dest_row[primary_key] if dest_row else None

                if src_row and dest_row and src_key == dest_key:
                    row_pairs.append((src_row, dest_row, src_cols, config))
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
                    })
                    dest_row = next(dest_iter, None)

            if row_pairs:
                if use_parallel:
                    with ProcessPoolExecutor(max_workers=4) as pool:
                        results = list(pool.map(compare_row_pair, row_pairs))
                else:
                    results = [compare_row_pair(p) for p in row_pairs]

                for pair, diffs in zip(row_pairs, results):
                    src_key = pair[0][primary_key]
                    if not diffs:
                        continue
                    for diff in diffs:
                        writer.write({
                            "primary_key": src_key,
                            "type": "mismatch",
                            "column": diff["column"],
                            "source_value": diff["source_value"],
                            "dest_value": diff["dest_value"],
                            **({
                                "source_hash": diff.get("source_hash"),
                                "dest_hash": diff.get("dest_hash"),
                            } if use_row_hash else {}),
                            "year": partition["year"],
                            "month": partition["month"],
                        })

        writer.close()


if __name__ == "__main__":
    main()
