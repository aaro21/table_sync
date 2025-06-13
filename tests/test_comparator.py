import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from logic.comparator import compare_rows


def test_compare_rows():
    src = {"id": 1, "col": "a"}
    dest = {"id": 1, "col": "b"}
    columns = {"id": "id", "col": "col"}
    diffs = compare_rows(src, dest, columns)
    assert diffs == [{"column": "col", "source_value": "a", "dest_value": "b"}]
