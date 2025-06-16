import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from logic.comparator import compare_rows, compute_row_hash, compare_row_pairs
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


def test_compare_row_pairs_dataframe():
    src1 = {"id": 1, "col": "a"}
    dest1 = {"id": 1, "col": "a"}
    src2 = {"id": 2, "col": "b"}
    dest2 = {"id": 2, "col": "c"}
    columns = {"id": "id", "col": "col"}
    config = {"primary_key": "id", "comparison": {"use_row_hash": True}}
    pairs = [
        (src1, dest1, columns, config),
        (src2, dest2, columns, config),
    ]
    results = compare_row_pairs(pairs)
    assert results[0] is None
    expected_src_hash = compute_row_hash(src2)
    expected_dest_hash = compute_row_hash(dest2)
    assert results[1] == [
        {
            "column": "col",
            "source_value": "b",
            "dest_value": "c",
            "source_hash": expected_src_hash,
            "dest_hash": expected_dest_hash,
        }
    ]


def test_compare_row_pairs_only_columns():
    src = {"id": 1, "col": "a", "extra": "x"}
    dest = {"id": 1, "col": "a", "extra": "y"}
    columns = {"id": "id", "col": "col", "extra": "extra"}
    config = {"primary_key": "id", "comparison": {"only_columns": ["col"]}}
    results = compare_row_pairs([(src, dest, columns, config)])
    assert results[0] is None


def test_compare_row_pairs_normalize_types():
    src = {"id": 1, "amount": Decimal("10.00")}
    dest = {"id": 1, "amount": "10"}
    columns = {"id": "id", "amount": "amount"}
    config = {"primary_key": "id", "comparison": {"normalize_types": True}}
    results = compare_row_pairs([(src, dest, columns, config)])
    assert results[0] is None
