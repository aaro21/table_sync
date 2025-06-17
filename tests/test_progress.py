from logic.comparator import compare_row_pairs

class DummyProgress:
    def __init__(self):
        self.n = 0
        self.total = None
        self.refresh_calls = 0
    def refresh(self):
        self.refresh_calls += 1

def test_compare_row_pairs_updates_progress():
    src = {"id": 1, "col": "a"}
    dest = {"id": 1, "col": "b"}
    columns = {"id": "id", "col": "col"}
    config = {"primary_key": "id"}
    progress = DummyProgress()
    list(compare_row_pairs([(src, dest, columns, config)], progress=progress))
    assert progress.total == 1
    assert progress.n == 1
    assert progress.refresh_calls >= 1
