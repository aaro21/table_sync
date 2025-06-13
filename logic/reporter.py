"""Output utilities for writing discrepancy reports."""

import csv
import os
import pyodbc


def write_discrepancies_to_csv(discrepancies: list[dict], output_path: str):
    """
    Writes a list of discrepancy records to a CSV file.

    Each record in the list should be a dictionary with consistent keys.
    """
    if not discrepancies:
        print("No discrepancies found.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=discrepancies[0].keys())
        writer.writeheader()
        writer.writerows(discrepancies)

    print(f"Discrepancy report written to: {output_path}")


def write_discrepancies_to_table(
    discrepancies: list[dict],
    conn: pyodbc.Connection,
    schema: str,
    table: str,
):
    """Write discrepancy records to a table in SQL Server."""

    if not discrepancies:
        print("No discrepancies found.")
        return

    cursor = conn.cursor()

    full_table = f"{schema}.{table}" if schema else table

    # Drop table if it already exists
    drop_sql = f"IF OBJECT_ID('{full_table}', 'U') IS NOT NULL DROP TABLE {full_table}" 
    cursor.execute(drop_sql)

    columns = list(discrepancies[0].keys())
    column_defs = ", ".join(f"[{c}] NVARCHAR(MAX)" for c in columns)
    create_sql = f"CREATE TABLE {full_table} ({column_defs})"
    cursor.execute(create_sql)

    placeholders = ", ".join("?" for _ in columns)
    insert_sql = (
        f"INSERT INTO {full_table} ({', '.join('[' + c + ']' for c in columns)}) "
        f"VALUES ({placeholders})"
    )

    for record in discrepancies:
        values = [record.get(c) for c in columns]
        cursor.execute(insert_sql, values)

    conn.commit()

    print(f"Discrepancy report written to table: {full_table}")
