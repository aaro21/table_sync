import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from logic.partitioner import get_partitions


def test_get_partitions_month_only():
    config = {
        "partitioning": {
            "scope": [
                {"year": 2021, "month": 1},
                {"year": 2021, "month": 2},
            ]
        }
    }

    parts = list(get_partitions(config))
    assert parts == [
        {"year": "2021", "month": "01"},
        {"year": "2021", "month": "02"},
    ]


def test_get_partitions_with_weeks():
    config = {
        "partitioning": {
            "scope": [
                {"year": 2022, "month": 5, "weeks": [1, 2]},
            ]
        }
    }

    parts = list(get_partitions(config))
    assert parts == [
        {"year": "2022", "month": "05", "week": "1"},
        {"year": "2022", "month": "05", "week": "2"},
    ]
