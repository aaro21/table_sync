"""Query helpers used by the reconciliation runner."""

from typing import Dict, Iterable


def fetch_rows(
    conn,
    schema: str,
    table: str,
    columns: Dict,
    partition: Dict,
    primary_key: str,
    batch_size: int = 1000,
) -> Iterable[Dict]:
    """Yield rows filtered by partition in primary key order."""
    year = partition["year"]
    month = partition["month"]

    col_list = [f"{v}" for v in columns.values()]
    select_clause = ", ".join(col_list)

    full_table = f"{schema}.{table}" if schema else table

    query = f"""
        SELECT {select_clause}
        FROM {full_table}
        WHERE {columns['year']} = ? AND {columns['month']} = ?
        ORDER BY {columns[primary_key]}
    """

    cursor = conn.cursor()
    cursor.execute(query, (year, month))
    cursor.arraysize = batch_size

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield dict(zip(col_list, row))
