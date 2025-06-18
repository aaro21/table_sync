"""Fix mismatched rows by updating only the incorrect column."""

from __future__ import annotations

import argparse
from collections import defaultdict
from typing import Dict, Optional

from logic.partitioner import get_partitions


from logic.config_loader import load_config
from connectors.sqlserver_connector import get_sqlserver_connection
from utils.logger import debug_log




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

    full_dest = f"{dest_schema}.{dest_table}" if dest_schema else dest_table
    full_output = f"{output_schema}.{output_table}" if output_schema else output_table

    with get_sqlserver_connection(dest_env, config) as conn:
        cur = conn.cursor()
        summary: Dict[str, Dict] = defaultdict(lambda: {"updates": 0, "columns": defaultdict(int)})

        for partition in get_partitions(config):
            part_params = [partition["year"], partition["month"]]
            where_clauses = ["[type] = 'mismatch'", "[year] = ?", "[month] = ?"]

            if "week" in partition:
                part_params.append(partition["week"])
                where_clauses.append("src.[week] = ?")

            debug_log(f"Processing partition {partition}", config, level="low")

            cur.execute(
                f"SELECT DISTINCT [column] FROM {full_output} WHERE {' AND '.join(where_clauses)}",
                tuple(part_params),
            )
            columns_to_update = [row[0] for row in cur.fetchall()]

            for col in columns_to_update:
                dest_column = dest_cols.get(col, col)

                join_on = [
                    f"dest.[{pk_col}] = src.primary_key",
                    f"dest.[{year_col}] = src.[year]",
                    f"dest.[{month_col}] = src.[month]",
                ]
                if "week" in partition:
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
                    affected = 0
                else:
                    cur.execute(update_sql, tuple(params))
                    affected = cur.rowcount
                    conn.commit()

                    # Delete the updated records from the output table to prevent reprocessing
                    delete_where = where_clauses + ["[column] = ?", f"[{pk_col}] = dest.[{pk_col}]", f"[{year_col}] = dest.[{year_col}]", f"[{month_col}] = dest.[{month_col}]"]
                    delete_params = part_params + [col]

                    if "week" in partition:
                        delete_where.append("[week] = ?")
                        delete_params.append(partition["week"])

                    delete_sql = (
                        f"DELETE FROM {full_output} "
                        f"WHERE primary_key IN ("
                        f"SELECT src.primary_key FROM {full_output} src "
                        f"JOIN {full_dest} dest ON {join_clause} "
                        f"WHERE {' AND '.join(where)}"
                        f") AND [column] = ?"
                    )

                    debug_log(f"Prepared delete SQL: {delete_sql} | {tuple(params + [col])}", config, level="medium")
                    cur.execute(delete_sql, tuple(params + [col]))
                    conn.commit()

                key = f"{partition['year']}-{partition['month']}" + (f"-{partition['week']}" if 'week' in partition else '')
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
    parser.add_argument("--apply", action="store_true", help="execute updates")
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false", help="disable dry run")
    args = parser.parse_args()

    config = load_config()
    dry_run = args.dry_run
    if args.apply:
        dry_run = False

    fix_mismatches(config, dry_run=dry_run)


if __name__ == "__main__":
    main()
