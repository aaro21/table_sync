"""Fix mismatched rows by updating only the incorrect column."""

from __future__ import annotations

import argparse
from collections import defaultdict
from typing import Dict, Optional

from logic.config_loader import load_config
from connectors.sqlserver_connector import get_sqlserver_connection
from utils.logger import debug_log


def parse_partition(part: str) -> Dict[str, str]:
    """Return a partition dict from ``YYYY-MM`` string."""
    year, month = part.split("-")
    return {"year": year, "month": month}


def fix_mismatches(
    config: Dict,
    *,
    target_partition: Optional[Dict[str, str]] = None,
    apply_fixes: Optional[bool] = None,
    dry_run: Optional[bool] = None,
) -> None:
    """Apply or print updates for mismatched rows."""

    apply_fixes = apply_fixes if apply_fixes is not None else config.get("apply_fixes", False)
    dry_run = dry_run if dry_run is not None else config.get("dry_run", True)
    skip_nulls = config.get("skip_null_updates", True)

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
        sql = (
            f"SELECT primary_key, [column], source_value, [year], [month] "
            f"FROM {full_output} WHERE type = ?"
        )
        params = ["mismatch"]
        if target_partition:
            sql += " AND [year] = ? AND [month] = ?"
            params.extend([target_partition["year"], target_partition["month"]])

        debug_log(f"Fetching mismatches with: {sql} | {tuple(params)}", config, level="medium")
        cur.execute(sql, tuple(params))

        summary: Dict[str, Dict] = defaultdict(
            lambda: {"total": 0, "updates": 0, "nulls": 0, "columns": defaultdict(int)}
        )

        for pk, col, src_val, yr, mon in cur.fetchall():
            part_key = f"{yr}-{str(mon).zfill(2)}"
            info = summary[part_key]
            info["total"] += 1

            if skip_nulls and (src_val is None or src_val == ""):
                info["nulls"] += 1
                continue

            dest_column = dest_cols.get(col, col)
            update_sql = (
                f"UPDATE {full_dest} SET [{dest_column}] = ? "
                f"WHERE [{pk_col}] = ? AND [{year_col}] = ? AND [{month_col}] = ?"
            )
            update_params = (src_val, pk, yr, mon)

            debug_log(f"Prepared update: {update_sql} | {update_params}", config, level="high")

            if dry_run or not apply_fixes:
                print(update_sql, update_params)
            else:
                cur.execute(update_sql, update_params)
                conn.commit()

            info["updates"] += 1
            info["columns"][col] += 1

        for part, info in summary.items():
            print(f"Partition: {part}")
            print(f"\u2714 Total mismatches found: {info['total']}")
            print(f"\u2714 Updates generated: {info['updates']}")
            print(f"\u2714 Nulls skipped: {info['nulls']}")
            for col, cnt in info["columns"].items():
                print(f"\u2192 {col}: {cnt} updates")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply fixes for mismatched rows")
    parser.add_argument("--partition", help="Target partition YYYY-MM")
    parser.add_argument("--apply", action="store_true", help="execute updates")
    parser.add_argument("--no-dry-run", dest="dry_run", action="store_false", help="disable dry run")
    args = parser.parse_args()

    config = load_config()
    partition = parse_partition(args.partition) if args.partition else None
    fix_mismatches(config, target_partition=partition, apply_fixes=args.apply, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

