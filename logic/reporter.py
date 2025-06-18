"""Output utilities for writing discrepancy reports."""

import csv
import os
from typing import Iterable, List, Dict, Any
from utils.logger import debug_log
try:
    import pyodbc
except Exception:  # pragma: no cover - optional dependency
    pyodbc = None  # type: ignore


class DiscrepancyWriter:
    """Incrementally write discrepancy records to a SQL Server table."""

    def __init__(self, conn: Any, schema: str, table: str, batch_size: int = 1000):
        self.conn = conn
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.columns: List[str] | None = None
        self.buffer: List[Dict] = []
        self.prepared = False

    def _full_table(self) -> str:
        return f"{self.schema}.{self.table}" if self.schema else self.table

    def _prepare_table(self, record: Dict):
        if self.prepared:
            return
        self.columns = list(record.keys())
        column_defs = ", ".join(
            f"[{c}] VARCHAR(500)" if c in ("primary_key", "column") else f"[{c}] VARCHAR(MAX)"
            for c in self.columns
        ) + ", [record_insert_datetime] DATETIME DEFAULT GETDATE()"
        cursor = self.conn.cursor()
        full_table = self._full_table()
        create_sql = f"""
        IF OBJECT_ID('{full_table}', 'U') IS NULL
        BEGIN
            CREATE TABLE {full_table} ({column_defs})
        END
        """
        cursor.execute(create_sql)
        debug_log(f"Ensuring discrepancy table exists: {full_table}", {}, level="low")
        self.conn.commit()
        self.prepared = True

    def write(self, record: Dict):
        self._prepare_table(record)
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        debug_log("Flushing buffer to SQL Server", {}, level="low")
        if not self.buffer:
            debug_log("Flush called but buffer is empty.", {}, level="low")
            return
        cursor = self.conn.cursor()
        full_table = self._full_table()
        temp_table = "#temp_discrepancies"

        try:
            self._create_temp_table(cursor, temp_table)
            self._bulk_insert_temp(cursor, temp_table, self.buffer)
            self._merge_temp_into_target(cursor, temp_table, full_table)
            self.conn.commit()
            debug_log(f"Merged {len(self.buffer)} rows from temp into {full_table}", {}, level="low")
        except Exception as e:
            debug_log(f"Failed to merge rows into {full_table}: {e}", {}, level="high")
            raise
        self.buffer.clear()

    def _create_temp_table(self, cursor, temp_table: str):
        if "record_insert_datetime" not in self.columns:
            self.columns.append("record_insert_datetime")
        column_defs = ", ".join(
            f"[{c}] VARCHAR(500)" if c in ("primary_key", "column") else f"[{c}] VARCHAR(MAX)"
            for c in self.columns
        )
        cursor.execute(f"IF OBJECT_ID('tempdb..{temp_table}') IS NOT NULL DROP TABLE {temp_table}")
        cursor.execute(f"CREATE TABLE {temp_table} ({column_defs})")

    def _bulk_insert_temp(self, cursor, temp_table: str, records: List[Dict]):
        from datetime import datetime
        if "record_insert_datetime" not in self.columns:
            self.columns.append("record_insert_datetime")
        for rec in records:
            if "record_insert_datetime" not in rec:
                rec["record_insert_datetime"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_sql = f"INSERT INTO {temp_table} ({', '.join(f'[{c}]' for c in self.columns)}) VALUES ({', '.join('?' for _ in self.columns)})"
        values = [[rec[c] for c in self.columns] for rec in records]
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, values)
        cursor.fast_executemany = False

    def _merge_temp_into_target(self, cursor, temp_table: str, target_table: str):
        key_cols = [c for c in ("primary_key", "column") if c in self.columns]
        if not key_cols:
            raise ValueError("Cannot merge without key columns")

        on_clause = " AND ".join(f"target.[{c}] = source.[{c}]" for c in key_cols)
        update_cols = [c for c in self.columns if c not in key_cols]
        update_clause = ", ".join(f"target.[{c}] = source.[{c}]" for c in update_cols)
        insert_cols = ", ".join(f"[{c}]" for c in self.columns)
        insert_vals = ", ".join(f"source.[{c}]" for c in self.columns)

        merge_sql = f"""
        MERGE {target_table} AS target
        USING {temp_table} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
        """
        cursor.execute(merge_sql)

    def _merge_records(self, cursor, full_table: str, records: List[Dict]):
        # Only treat these as key columns if they are present in the record. This
        # allows ``DiscrepancyWriter`` to be used with arbitrary schemas in
        # tests or one-off scripts where ``primary_key``/``column`` fields may be
        # absent. If no key columns are available we fall back to a simple
        # ``INSERT`` statement.
        key_cols = [c for c in ("primary_key", "column") if c in self.columns]

        for rec in records:
            if not key_cols:
                insert_cols = ", ".join(f"[{col}]" for col in self.columns)
                insert_vals = ", ".join("?" for _ in self.columns)
                insert_sql = f"INSERT INTO {full_table} ({insert_cols}) VALUES ({insert_vals})"
                cursor.execute(insert_sql, [rec[col] for col in self.columns])
                continue

            update_set = ", ".join(f"[{col}] = ?" for col in self.columns if col not in key_cols)
            insert_cols = ", ".join(f"[{col}]" for col in self.columns)
            insert_vals = ", ".join("?" for _ in self.columns)

            on_clause = " AND ".join(f"target.[{col}] = source.[{col}]" for col in key_cols)
            using_select = ", ".join(f"? AS [{col}]" for col in key_cols)

            merge_sql = f"""
            MERGE {full_table} AS target
            USING (SELECT {using_select}) AS source
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals});
            """
            values = [rec[col] for col in key_cols] + [rec[col] for col in self.columns if col not in key_cols] + [rec[col] for col in self.columns]
            cursor.execute(merge_sql, values)

    def close(self):
        self.flush()

    # ------------------------------------------------------------------
    # Context manager helpers make ``DiscrepancyWriter`` usable with ``with``
    # statements which guarantees that ``flush`` is called.
    # ------------------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


def write_discrepancies_to_csv(discrepancies: Iterable[Dict], output_path: str):
    """Write discrepancy records to a CSV file."""
    discrepancies = list(discrepancies)
    if not discrepancies:
        print("No discrepancies found.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=discrepancies[0].keys())
        writer.writeheader()
        writer.writerows(discrepancies)

    print(f"Discrepancy report written to: {output_path}")
