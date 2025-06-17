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
    limit: Optional[int] = None,
) -> Iterable[Dict]:
    """Yield rows filtered by partition in primary key order.

    Parameters
    ----------
    limit:
        Optional maximum number of rows returned. ``None`` means no limit.
        When provided, the value is applied directly in the SQL query using
        ``FETCH FIRST``/``OFFSET`` syntax depending on the dialect.
    """
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
        param_idx = 3
        if "week" in partition and week_column:
            query += f" AND {week_column} = :3"
            params.append(week)
            param_idx = 4
        query += f" ORDER BY {columns[primary_key]}"
        if limit is not None:
            query += f" FETCH FIRST :{param_idx} ROWS ONLY"
            params.append(int(limit))
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
        if limit is not None:
            query += " OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
            params.append(int(limit))
        params = tuple(params)
    else:
        raise ValueError(f"Unsupported SQL dialect: {dialect}")

    debug_log(
        f"Executing query: {query.strip()} | Params: {params}",
        config,
        level="medium",
    )
    read_cursor = conn.cursor()
    try:
        read_cursor.execute(query, params)
    except Exception as exc:  # pragma: no cover - database error
        debug_log(f"Query execution failed: {exc}", config, level="low")
        raise
    read_cursor.arraysize = batch_size

    while True:
        rows = read_cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield dict(zip(logical_cols, row))

