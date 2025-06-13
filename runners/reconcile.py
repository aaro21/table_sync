"""Query helpers used by the reconciliation runner."""

from typing import Dict, Iterable, Optional

from utils.logger import debug_log


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
    week_column: Optional[str] = None,
    config: Optional[dict] = None,
) -> Iterable[Dict]:
    """Yield rows filtered by partition in primary key order."""
    # Cast partition identifiers to strings so that filtering works for
    # both numeric and varchar column types. This avoids implicit type
    # conversion issues when year/month columns are stored as VARCHAR.
    year = str(partition["year"])
    month = str(partition["month"])
    week = str(partition.get("week")) if "week" in partition else None

    logical_cols = list(columns.keys())
    physical_cols = [columns[c] for c in logical_cols]
    select_clause = ", ".join(physical_cols)

    full_table = f"{schema}.{table}" if schema else table

    dialect = dialect.lower()
    if dialect == "oracle":
        query = f"""
            SELECT {select_clause}
            FROM {full_table}
            WHERE {year_column} = :1 AND {month_column} = :2"""
        params = [year, month]
        if "week" in partition and week_column:
            query += f" AND {week_column} = :3"
            params.append(week)
        query += f" ORDER BY {columns[primary_key]}"
        params = tuple(params)
    elif dialect == "sqlserver":
        query = f"""
            SELECT {select_clause}
            FROM {full_table}
            WHERE {year_column} = ? AND {month_column} = ?"""
        params = [year, month]
        if "week" in partition and week_column:
            query += f" AND {week_column} = ?"
            params.append(week)
        query += f" ORDER BY {columns[primary_key]}"
        params = tuple(params)
    else:
        raise ValueError(f"Unsupported SQL dialect: {dialect}")

    debug_log(f"Executing query: {query.strip()} | Params: {params}", config)
    read_cursor = conn.cursor()
    read_cursor.execute(query, params)
    read_cursor.arraysize = batch_size

    while True:
        rows = read_cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield dict(zip(logical_cols, row))
