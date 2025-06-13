"""Query helpers used by the reconciliation runner."""

from typing import Dict, Iterable


def fetch_rows(
    conn,
    schema: str,
    table: str,
    columns: Dict,
    partition: Dict,
    primary_key: str,
    year_column: str,
    month_column: str,
    batch_size: int = 1000,
    dialect: str = "sqlserver",  # default to sqlserver
) -> Iterable[Dict]:
    """Yield rows filtered by partition in primary key order."""
    # Cast partition identifiers to strings so that filtering works for
    # both numeric and varchar column types. This avoids implicit type
    # conversion issues when year/month columns are stored as VARCHAR.
    year = str(partition["year"])
    month = str(partition["month"])

    logical_cols = list(columns.keys())
    physical_cols = [columns[c] for c in logical_cols]
    select_clause = ", ".join(physical_cols)

    full_table = f"{schema}.{table}" if schema else table

    if dialect == "oracle":
        query = f"""
            SELECT {select_clause}
            FROM {full_table}
            WHERE {year_column} = :1 AND {month_column} = :2
            ORDER BY {columns[primary_key]}
        """
        params = (year, month)
    else:  # assume sqlserver
        query = f"""
            SELECT {select_clause}
            FROM {full_table}
            WHERE {year_column} = ? AND {month_column} = ?
            ORDER BY {columns[primary_key]}
        """
        params = (year, month)

    print(f"Executing query: {query.strip()} | Params: {params}")
    cursor = conn.cursor()
    cursor.execute(query, params)
    cursor.arraysize = batch_size

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield dict(zip(logical_cols, row))
