#![no_main]
use std::borrow::Cow;
use std::ops;

use arbitrary::Arbitrary;
use binlog::{Entry, MemoryStore, Range, SqliteStore, Store};
use libfuzzer_sys::fuzz_target;
use string_cache::DefaultAtom as Atom;
use tempfile::NamedTempFile;

macro_rules! cmp_result {
    ($memory_value:expr, $sqlite_value:expr) => {
        match ($memory_value, $sqlite_value) {
            (Err(memory_err), Err(sqlite_err)) => {
                assert_eq!(format!("{:?}", memory_err), format!("{:?}", sqlite_err));
                None
            }
            (Err(err), Ok(_)) => {
                panic!("sqlite result ok, but memory result errored: {}", err)
            }
            (Ok(_), Err(err)) => {
                panic!("memory result ok, but sqlite result errored: {}", err)
            }
            (Ok(memory_value), Ok(sqlite_value)) => Some((memory_value, sqlite_value)),
        }
    };
}

macro_rules! cmp {
    ($memory_value:expr, $sqlite_value:expr) => {
        match ($memory_value, $sqlite_value) {
            (Ok(memory_value), Ok(sqlite_value)) => {
                assert_eq!(memory_value, sqlite_value);
            }
            (memory_value, sqlite_value) => {
                assert_eq!(format!("{:?}", memory_value), format!("{:?}", sqlite_value));
            }
        }
    };
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
enum Op {
    Push(i64, String, Vec<u8>),
    Len(ArbitraryMicrosRange, Option<String>),
    Remove(ArbitraryMicrosRange, Option<String>),
    Iter(ArbitraryMicrosRange, Option<String>),
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
enum ArbitraryMicrosBound {
    Included(i64),
    Excluded(i64),
    Unbounded,
}

impl ArbitraryMicrosBound {
    fn to_bound(&self) -> ops::Bound<i64> {
        match self {
            ArbitraryMicrosBound::Included(micros) => ops::Bound::Included(*micros),
            ArbitraryMicrosBound::Excluded(micros) => ops::Bound::Excluded(*micros),
            ArbitraryMicrosBound::Unbounded => ops::Bound::Unbounded,
        }
    }
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
struct ArbitraryMicrosRange {
    start_bound: ArbitraryMicrosBound,
    end_bound: ArbitraryMicrosBound,
}

impl ArbitraryMicrosRange {
    fn to_bounds(&self) -> (ops::Bound<i64>, ops::Bound<i64>) {
        (self.start_bound.to_bound(), self.end_bound.to_bound())
    }
}

fuzz_target!(|ops: Vec<Op>| {
    let file = NamedTempFile::new().unwrap().into_temp_path();

    let memory_log = MemoryStore::default();
    let sqlite_log = SqliteStore::new(file, None).unwrap();

    let get_ranges = |range: ArbitraryMicrosRange, name: Option<String>| {
        let range = range.to_bounds();
        let name = name.map(Atom::from);
        let memory_range = memory_log.range(range, name.clone());
        let sqlite_range = sqlite_log.range(range, name);
        cmp_result!(memory_range, sqlite_range)
    };

    for op in ops {
        match op {
            Op::Push(timestamp, name, value) => {
                let entry = Entry::new_with_timestamp(timestamp, Atom::from(name), value);
                let memory_value = memory_log.push(Cow::Borrowed(&entry));
                let sqlite_value = sqlite_log.push(Cow::Owned(entry));
                cmp!(memory_value, sqlite_value);
            }
            Op::Len(range, name) => {
                if let Some((memory_range, sqlite_range)) = get_ranges(range, name) {
                    cmp!(memory_range.count(), sqlite_range.count());
                }
            }
            Op::Remove(range, name) => {
                if let Some((memory_range, sqlite_range)) = get_ranges(range, name) {
                    cmp!(memory_range.remove(), sqlite_range.remove());
                }
            }
            Op::Iter(range, name) => {
                if let Some((memory_range, sqlite_range)) = get_ranges(range, name) {
                    if let Some((mut memory_iter, mut sqlite_iter)) =
                        cmp_result!(memory_range.iter(), sqlite_range.iter())
                    {
                        loop {
                            match (memory_iter.next(), sqlite_iter.next()) {
                                (Some(memory_value), Some(sqlite_value)) => {
                                    cmp!(memory_value, sqlite_value)
                                }
                                (Some(value), None) => {
                                    panic!("sqlite range is done, but memory range is still iterating: {:?}", value);
                                }
                                (None, Some(value)) => {
                                    panic!("memory range is done, but sqlite range is still iterating: {:?}", value);
                                }
                                (None, None) => break,
                            }
                        }
                    }
                }
            }
        }
    }
});
