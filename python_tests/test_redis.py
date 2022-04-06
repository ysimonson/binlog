import tempfile
from binlog import binlog


def test_push():
    store = binlog.RedisStreamStore("redis://localhost:6379", 10)
    entry = binlog.Entry(1, "pytest_redis_push", [1, 2, 3])
    sub = store.subscribe("pytest_redis_push")
    store.push(entry)
    assert next(sub) == entry
