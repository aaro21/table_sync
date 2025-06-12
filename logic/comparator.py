"""Functions for comparing row dictionaries between databases."""


def compare_rows(source_row: dict, dest_row: dict, column_map: dict) -> list[dict]:
    """
    Compares two rows column-by-column using the logical column names.

    Returns a list of mismatched columns with source and destination values.
    Each mismatch is represented as:
        {
            "column": "logical_column_name",
            "source_value": ...,
            "dest_value": ...
        }
    """
    mismatches = []
    for logical_col, _ in column_map.items():
        src_val = source_row.get(column_map[logical_col])
        dest_val = dest_row.get(column_map[logical_col])
        if src_val != dest_val:
            mismatches.append({
                "column": logical_col,
                "source_value": src_val,
                "dest_value": dest_val
            })
    return mismatches
