import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from runners.reconcile import fetch_rows

class DummyCursor:
    def __init__(self):
        self.executed = None
        self.arraysize = None
    def execute(self, query, params):
        self.executed = params
    def fetchmany(self, size):
        if self.arraysize is None:
            self.arraysize = size
        return []

class DummyConn:
    def __init__(self):
        self.cursor_obj = DummyCursor()
    def cursor(self):
        return self.cursor_obj

def test_fetch_rows_casts_partition_to_str():
    conn = DummyConn()
    columns = {"id": "id", "year": "yr", "month": "mon"}
    partition = {"year": 2021, "month": 1}
    list(fetch_rows(conn, "dbo", "t", columns, partition, "id", "yr", "mon"))
    assert conn.cursor_obj.executed == ("2021", "1")


def test_fetch_rows_filters_week_sqlserver():
    conn = DummyConn()
    columns = {"id": "id", "year": "yr", "month": "mon", "week": "wk"}
    partition = {"year": 2021, "month": 1, "week": 2}
    list(
        fetch_rows(
            conn,
            "dbo",
            "t",
            columns,
            partition,
            "id",
            "yr",
            "mon",
            dialect="sqlserver",
            week_column="wk",
        )
    )
    assert conn.cursor_obj.executed == ("2021", "1", "2")


def test_fetch_rows_filters_week_oracle():
    conn = DummyConn()
    columns = {"id": "id", "year": "yr", "month": "mon", "week": "wk"}
    partition = {"year": 2021, "month": 1, "week": 3}
    list(
        fetch_rows(
            conn,
            "dbo",
            "t",
            columns,
            partition,
            "id",
            "yr",
            "mon",
            dialect="oracle",
            week_column="wk",
        )
    )
    assert conn.cursor_obj.executed == ("2021", "1", "3")
