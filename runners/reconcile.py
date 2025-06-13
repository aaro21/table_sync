

"""Query helpers used by the reconciliation runner."""


def fetch_rows(
    conn,
    schema: str,
    table: str,
    columns: dict,
    partition: dict,
    primary_key: str,
) -> dict:
    """
    Fetches rows from a table filtered by year and month, returning a dict keyed by primary key.
    """
    year = partition["year"]
    month = partition["month"]

    col_list = [f"{v}" for v in columns.values()]
    select_clause = ", ".join(col_list)

    full_table = f"{schema}.{table}" if schema else table

    query = f"""
        SELECT {select_clause}
        FROM {full_table}
        WHERE {columns['year']} = ? AND {columns['month']} = ?
    """

    cursor = conn.cursor()
    cursor.execute(query, (year, month))
    rows = cursor.fetchall()

    result = {}
    for row in rows:
        row_dict = dict(zip(col_list, row))
        row_key = row_dict[columns[primary_key]]
        result[row_key] = row_dict

    return result
