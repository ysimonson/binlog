import tempfile
from binlog import binlog


def test_push():
    with tempfile.NamedTemporaryFile(suffix="binlog.db") as f:
        store = binlog.SqliteStore(f.name)
        store.push(binlog.Entry(1, "pytest_sqlite_push", [1, 2, 3]))

def test_remove():
    with tempfile.NamedTemporaryFile(suffix="binlog.db") as f:
        store = binlog.SqliteStore(f.name)
        insert_sample_data(store)
        assert store.range(None, None, None).count() == 10
        store.range(2, None, None).remove()
        assert store.range(None, None, None).count() == 1
        store.range(None, None, "pytest_sqlite").remove()
        assert store.range(None, None, None).count() == 0

def test_iter():
    with tempfile.NamedTemporaryFile(suffix="binlog.db") as f:
        store = binlog.SqliteStore(f.name)
        insert_sample_data(store)
        results = list(store.range(None, None, None).iter())
        assert len(results) == 10
        for i in range(1, 11):
            result = results[i - 1]
            assert result.timestamp == i
            assert result.name == "pytest_sqlite"
            assert result.value == [i]

def insert_sample_data(store):
    for i in range(1, 11):
        store.push(binlog.Entry(i, "pytest_sqlite", [i]))
