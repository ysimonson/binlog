# binlog

[![Test](https://github.com/ysimonson/binlog/actions/workflows/test.yml/badge.svg)](https://github.com/ysimonson/binlog/actions/workflows/test.yml)
[![crates.io](https://img.shields.io/crates/v/binlog.svg)](https://crates.io/crates/binlog)
[![API docs](https://docs.rs/binlog/badge.svg)](https://docs.rs/binlog)

A rust library for creating and managing logs of arbitrary binary data.

The underlying storage of logs are pluggable via the implementation of a couple of [traits](https://github.com/ysimonson/binlog/blob/main/src/traits.rs). Binlog includes built-in implementations via sqlite storage, and in-memory-only. Additionally, python bindings allow you to use (a subset of) binlog from python.

## Using from rust

A small example:

```rust
use binlog::{Entry, Error, Range, SqliteStore, Store};
use std::borrow::Cow;
use string_cache::Atom;

/// Demonstrates the sqlite store, with results in `example.db`. You may want to delete that before
/// running this to see the results of this on an empty database.
fn main() -> Result<(), Error> {
    // Create a new datastore with sqlite backing. The result will be stored in example.db, with
    // default compression options. In-memory is also possible via
    // `binlog::MemoryStore::default()`.
    let store = SqliteStore::new("example.db", None)?;

    // Add 10 entries.
    for i in 1..11u8 {
        let entry = Entry::new_with_timestamp(i as i64, Atom::from("sqlite_example"), vec![i]);
        store.push(Cow::Owned(entry))?;
    }

    // Queries are done via `range`. Here we grab entries with any timestamp and any name.
    let range = store.range(.., None)?;
    // Count the number of entries.
    println!("initial count: {}", range.count()?);
    // We can also iterate on the entries.
    for entry in range.iter()? {
        println!("entry: {:?}", entry?);
    }

    // Remove the entries with 4 <= ts <= 6 and with the name `sqlite_example`.
    store.range(4..=6, Some(Atom::from("sqlite_example")))?.remove()?;

    // Now get the range of entries with 5 <= ts and with the name `sqlite_example`.
    let range = store.range(5.., Some(Atom::from("sqlite_example")))?;
    println!("count after range deletion: {}", range.count()?);
    for entry in range.iter()? {
        println!("entry: {:?}", entry?);
    }

    Ok(())
}
```

## Using from python

A small example:

```python
from binlog import binlog
store = binlog.SqliteStore("example.db")
store.push(binlog.Entry(1, "pytest_push", [1, 2, 3]))
```

## Testing

### Unit tests

Tests can be run via `make test`. This will also be run in CI.

### Benchmarks

WIP.

### Fuzzing

A fuzzer is available, ensuring the the sqlite and in-memory datastores operate identically. Run it via `make fuzz`.

### Checks

Lint and formatting checks can be run via `make check`. Equivalent checks will also run in CI.
