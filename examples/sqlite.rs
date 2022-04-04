use binlog::{Entry, Error, Range, RangeableStore, SqliteStore, Store};
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
