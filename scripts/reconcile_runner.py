"""Command line entry point for running table reconciliation."""

from logic.config_loader import load_config
from connectors.oracle_connector import get_oracle_connection
from connectors.sqlserver_connector import get_sqlserver_connection
from logic.partitioner import get_partitions
from runners.reconcile import fetch_rows
from logic.comparator import compare_rows
from logic.reporter import write_discrepancies_to_csv


def main():
    config = load_config()

    src_env = config["source"]["connection"]
    dest_env = config["destination"]["connection"]
    src_schema = config["source"].get("schema", "")
    dest_schema = config["destination"].get("schema", "")
    src_table = config["source"]["table"]
    dest_table = config["destination"]["table"]
    src_cols = config["source"]["columns"]
    dest_cols = config["destination"]["columns"]
    primary_key = config["primary_key"]
    output_path = config["output"]["path"]

    all_discrepancies = []

    with get_oracle_connection(src_env) as src_conn, get_sqlserver_connection(dest_env) as dest_conn:
        for partition in get_partitions(config):
            print(f"Checking partition: {partition}")

            src_rows = fetch_rows(src_conn, src_schema, src_table, src_cols, partition, primary_key)
            dest_rows = fetch_rows(dest_conn, dest_schema, dest_table, dest_cols, partition, primary_key)

            common_keys = src_rows.keys() & dest_rows.keys()
            missing_keys = src_rows.keys() - dest_rows.keys()
            extra_keys = dest_rows.keys() - src_rows.keys()

            for key in common_keys:
                diffs = compare_rows(src_rows[key], dest_rows[key], src_cols)
                for diff in diffs:
                    all_discrepancies.append({
                        "primary_key": key,
                        "type": "mismatch",
                        "column": diff["column"],
                        "source_value": diff["source_value"],
                        "dest_value": diff["dest_value"],
                        "year": partition["year"],
                        "month": partition["month"]
                    })

            for key in missing_keys:
                all_discrepancies.append({
                    "primary_key": key,
                    "type": "missing_in_dest",
                    "column": None,
                    "source_value": src_rows[key],
                    "dest_value": None,
                    "year": partition["year"],
                    "month": partition["month"]
                })

            for key in extra_keys:
                all_discrepancies.append({
                    "primary_key": key,
                    "type": "extra_in_dest",
                    "column": None,
                    "source_value": None,
                    "dest_value": dest_rows[key],
                    "year": partition["year"],
                    "month": partition["month"]
                })

    write_discrepancies_to_csv(all_discrepancies, output_path)


if __name__ == "__main__":
    main()

