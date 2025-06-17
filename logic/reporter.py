"""Output utilities for writing discrepancy reports."""

import csv
import os
from typing import Iterable, List, Dict, Any
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
        cursor = self.conn.cursor()
        full_table = self._full_table()
        drop_sql = f"IF OBJECT_ID('{full_table}', 'U') IS NOT NULL DROP TABLE {full_table}"
        cursor.execute(drop_sql)
        column_defs = ", ".join(f"[{c}] NVARCHAR(MAX)" for c in self.columns)
        create_sql = f"CREATE TABLE {full_table} ({column_defs})"
        cursor.execute(create_sql)
        self.conn.commit()
        self.prepared = True

    def write(self, record: Dict):
        self._prepare_table(record)
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        cursor = self.conn.cursor()
        full_table = self._full_table()
        placeholders = ", ".join("?" for _ in self.columns)
        insert_sql = (
            f"INSERT INTO {full_table} ({', '.join('[' + c + ']' for c in self.columns)}) "
            f"VALUES ({placeholders})"
        )
        for rec in self.buffer:
            cursor.execute(insert_sql, [rec.get(c) for c in self.columns])
        self.conn.commit()
        self.buffer.clear()

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
