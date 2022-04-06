import tempfile
from binlog import binlog


def test_push():
    with tempfile.NamedTemporaryFile(suffix="binlog.db") as f:
        store = binlog.SqliteStore(f.name)
        store.push(binlog.Entry(1, "pytest_sqlite_push", [1, 2, 3]))
