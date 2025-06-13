import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from logic.comparator import compare_rows, compute_row_hash
from decimal import Decimal


def test_compare_rows():
    src = {"id": 1, "col": "a"}
    dest = {"id": 1, "col": "b"}
    columns = {"id": "id", "col": "col"}
    diffs = compare_rows(src, dest, columns)
    assert diffs == [{"column": "col", "source_value": "a", "dest_value": "b"}]


def test_compare_rows_numeric_tolerance():
    src = {"id": 1, "amount": Decimal("-265.23")}
    dest = {"id": 1, "amount": -265.230000}
    columns = {"id": "id", "amount": "amount"}
    diffs = compare_rows(src, dest, columns)
    assert diffs == []


def test_compare_rows_date_equality():
    src = {"id": 1, "date": "2020-10-04 00:00:00.0000000"}
    dest = {"id": 1, "date": "2020-10-04"}
    columns = {"id": "id", "date": "date"}
    diffs = compare_rows(src, dest, columns)
    assert diffs == []


def test_compute_row_hash_and_skip():
    src = {"id": 1, "col": "a"}
    dest = {"id": 1, "col": "a"}
    columns = {"id": "id", "col": "col"}
    diffs = compare_rows(src, dest, columns, use_row_hash=True)
    assert diffs == []


def test_compare_rows_row_hash_mismatch():
    src = {"id": 1, "col": "a"}
    dest = {"id": 1, "col": "b"}
    columns = {"id": "id", "col": "col"}
    expected_src_hash = compute_row_hash(src)
    expected_dest_hash = compute_row_hash(dest)
    diffs = compare_rows(src, dest, columns, use_row_hash=True)
    assert diffs == [
        {
            "column": "col",
            "source_value": "a",
            "dest_value": "b",
            "source_hash": expected_src_hash,
            "dest_hash": expected_dest_hash,
        }
    ]
