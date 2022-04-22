import tempfile
import time
from binlog import binlog


def test_pubsub():
    store = binlog.RedisStreamStore("redis://localhost:6379")
    entry = binlog.Entry(1, "pytest_redis_push", [1, 2, 3])
    sub = store.subscribe("pytest_redis_push")
    store.push(entry)
    sub_entry = next(sub)
    assert entry.timestamp == sub_entry.timestamp
    assert entry.name == sub_entry.name
    assert entry.value == sub_entry.value

