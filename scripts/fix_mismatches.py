"""Fix mismatched rows by updating only the incorrect column."""

from __future__ import annotations

import argparse
from collections import defaultdict
from typing import Dict, Optional

from logic.partitioner import get_partitions


from logic.config_loader import load_config
from connectors.sqlserver_connector import get_sqlserver_connection
from utils.logger import debug_log
from tqdm import tqdm


# Note: record_insert_datetime is managed during initial load, not needed during fix phase.

def fix_mismatches(config: Dict, *, dry_run: Optional[bool] = None) -> None:
    """Apply or print updates for mismatched rows using set-based joins."""

    dry_run = (
        dry_run
        if dry_run is not None
        else config.get("updates", {}).get("dry_run", True)
    )
    skip_nulls = config.get("updates", {}).get("skip_nulls", True)

    dest_env = config["destination"]["resolved_env"]
    dest_schema = config["destination"].get("schema", "")
    dest_table = config["destination"]["table"]
    dest_cols = config["destination"]["columns"]

    output_schema = config["output"].get("schema", "")
    output_table = config["output"]["table"]

    primary_key = config["primary_key"]
    year_logical = config["partitioning"]["year_column"]
    month_logical = config["partitioning"]["month_column"]

    pk_col = dest_cols[primary_key]
    year_col = dest_cols.get(year_logical, year_logical)
    month_col = dest_cols.get(month_logical, month_logical)

    full_dest = f"[{dest_schema}].[{dest_table}]" if dest_schema else f"[{dest_table}]"
    full_output = f"[{output_schema}].[{output_table}]" if output_schema else f"[{output_table}]"

    with get_sqlserver_connection(dest_env, config) as conn:
        cur = conn.cursor()
        summary: Dict[str, Dict] = defaultdict(lambda: {"updates": 0, "columns": defaultdict(int)})

        partitions = list(get_partitions(config)) or [{}]
        with tqdm(total=len(partitions), desc="partitions", unit="part") as part_bar:
            for partition in partitions:
                part_bar.update(1)
                part_params = []
                where_clauses = ["[type] = 'mismatch'"]
                if partition.get("year"):
                    part_params.append(partition["year"])
                    where_clauses.append("[year] = ?")
                if partition.get("month"):
                    part_params.append(partition["month"])
                    where_clauses.append("[month] = ?")

                if partition.get("week"):
                    part_params.append(partition["week"])
                    where_clauses.append("src.[week] = ?")

                debug_log(f"Processing partition {partition}", config, level="low")

                cur.execute(
                    f"SELECT DISTINCT [column] FROM {full_output} WHERE {' AND '.join(where_clauses)}",
                    tuple(part_params),
                )
                columns_to_update = [row[0] for row in cur.fetchall()]

                with tqdm(columns_to_update, desc="columns", unit="col", leave=False) as col_bar:
                    for col in columns_to_update:
                        dest_column = dest_cols.get(col, col)

                        join_on = [
                            f"dest.[{pk_col}] = src.primary_key",
                            f"dest.[{year_col}] = src.[year]",
                            f"dest.[{month_col}] = src.[month]",
                        ]
                        if partition.get("week"):
                            week_key = config["partitioning"].get("week_column", "week")
                            dest_week_col = dest_cols.get(week_key, week_key)
                            join_on.append(f"dest.[{dest_week_col}] = src.[week]")

                        join_clause = " AND ".join(join_on)

                        where = where_clauses + ["src.[column] = ?"]
                        params = part_params + [col]
                        if skip_nulls:
                            where.append("src.source_value IS NOT NULL AND src.source_value <> ''")

                        update_sql = (
                            f"UPDATE dest SET dest.[{dest_column}] = src.source_value "
                            f"FROM {full_dest} dest INNER JOIN {full_output} src ON {join_clause} "
                            f"WHERE {' AND '.join(where)}"
                        )

                        debug_log(f"Prepared update SQL: {update_sql} | {params}", config, level="medium")

                        if dry_run:
                            print(update_sql, tuple(params))

                            delete_sql = (
                                f"DELETE FROM {full_output} "
                                f"WHERE primary_key IN ("
                                f"SELECT src.primary_key FROM {full_output} src "
                                f"JOIN {full_dest} dest ON {join_clause} "
                                f"WHERE {' AND '.join(where + [f'src.[column] = ?'])}"
                                f")"
                            )
                            print(delete_sql, tuple(params + [col]))
                            affected = 0
                        else:
                            try:
                                cur.execute(update_sql, tuple(params))
                                affected = cur.rowcount
                                conn.commit()
                            except Exception as e:
                                debug_log(f"Error executing update: {e}", config, level="low")
                                continue

                        # Delete the updated records from the output table to prevent reprocessing
                        delete_sql = (
                            f"DELETE src FROM {full_output} src "
                            f"INNER JOIN {full_dest} dest ON {join_clause} "
                            f"WHERE {' AND '.join(where)} AND src.[column] = ?"
                        )

                        try:
                            debug_log(f"Prepared delete SQL: {delete_sql} | {tuple(params + [col])}", config, level="medium")
                            cur.execute(delete_sql, tuple(params + [col]))
                            conn.commit()
                        except Exception as e:
                            debug_log(f"Error executing delete: {e}", config, level="low")
                            continue

                        col_bar.update(1)

                        key = f"{partition.get('year', 'ALL')}-{partition.get('month', 'ALL')}"
                        if partition.get('week'):
                            key += f"-{partition['week']}"
                        summary[key]["updates"] += affected
                        summary[key]["columns"][col] += affected

        for part, info in summary.items():
            print(f"Partition: {part}")
            print(f"\u2714 Rows updated: {info['updates']}")
            for col, cnt in info["columns"].items():
                print(f"\u2192 {col}: {cnt} rows")


def main() -> None:
    """Command line wrapper around :func:`fix_mismatches`."""

    parser = argparse.ArgumentParser(description="Apply fixes for mismatched rows")
    parser.add_argument("--apply", action="store_true", help="execute updates instead of dry run")
    args = parser.parse_args()

    config = load_config(config_path="config/config.yaml")
    dry_run = not args.apply

    fix_mismatches(config, dry_run=dry_run)


if __name__ == "__main__":
    main()
