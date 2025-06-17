import types
from logic.reporter import DiscrepancyWriter

class DummyConn:
    def __init__(self):
        self.cursor_obj = types.SimpleNamespace(executed=[], rowcount=0)
    def cursor(self):
        def execute(sql, *params):
            self.cursor_obj.executed.append(sql)
        def executemany(sql, vals):
            self.cursor_obj.executed.append(sql)
            self.cursor_obj.rowcount += len(vals)
        self.cursor_obj.execute = execute
        self.cursor_obj.executemany = executemany
        return self.cursor_obj
    def commit(self):
        pass


def test_discrepancy_writer_context_manager():
    conn = DummyConn()
    with DiscrepancyWriter(conn, "dbo", "t", batch_size=1) as writer:
        writer.write({"a": "1"})
        writer.write({"a": "2"})
    # ensure flush was called and table created
    assert conn.cursor().executed


def test_discrepancy_writer_adds_columns():
    conn = DummyConn()
    with DiscrepancyWriter(conn, "dbo", "t", batch_size=2) as writer:
        writer.write({"a": "1"})
        writer.write({"a": "2", "b": "x"})
    executed = "\n".join(conn.cursor().executed)
    assert "ALTER TABLE" in executed and "[b]" in executed
