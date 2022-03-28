#![no_main]
use std::borrow::Cow;
use std::ops;
use std::rc::Rc;
use std::time::Duration;

use binary_log::{Entry, MemoryStore, SqliteStore, Store, Range};

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rusqlite::Connection;

macro_rules! cmp {
    ($v1:expr, $v2:expr) => {
        match ($v1, $v2) {
            (Ok(v1), Ok(v2)) => {
                assert_eq!(v1, v2);
            }
            (v1, v2) => {
                assert_eq!(format!("{:?}", v1), format!("{:?}", v2));
            }
        }
    };
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
enum Op {
    Push(u64, String, Vec<u8>),
    Len(ArbitraryMicrosRange, Option<String>),
    Remove(ArbitraryMicrosRange, Option<String>),
    Iter(ArbitraryMicrosRange, Option<String>),
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
enum ArbitraryMicrosBound {
    Included(u64),
    Excluded(u64),
    Unbounded,
}

impl ArbitraryMicrosBound {
    fn to_duration_bound(&self) -> ops::Bound<Duration> {
        match self {
            ArbitraryMicrosBound::Included(micros) => ops::Bound::Included(Duration::from_micros(*micros)),
            ArbitraryMicrosBound::Excluded(micros) => ops::Bound::Excluded(Duration::from_micros(*micros)),
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
    fn to_duration_range(&self) -> (ops::Bound<Duration>, ops::Bound<Duration>) {
        (self.start_bound.to_duration_bound(), self.end_bound.to_duration_bound())
    }
}

fuzz_target!(|ops: Vec<Op>| {
    let memory_log = MemoryStore::default();
    let sqlite_log = SqliteStore::new_with_connection(Connection::open_in_memory().unwrap(), None).unwrap();

    for op in ops {
        match op {
            Op::Push(time, name, value) => {
                let time = Duration::from_micros(time);
                let entry = Entry::new_with_time(time, Rc::new(name), value);
                let memory_value = memory_log.push(Cow::Borrowed(&entry));
                let sqlite_value = sqlite_log.push(Cow::Owned(entry));
                cmp!(memory_value, sqlite_value);
            }
            Op::Len(range, name) => {
                let range = range.to_duration_range();
                let name = name.map(Rc::new);
                let memory_value = memory_log.range(range.clone(), name.clone()).len();
                let sqlite_value = sqlite_log.range(range, name).len();
                cmp!(memory_value, sqlite_value);
            }
            Op::Remove(range, name) => {
                let range = range.to_duration_range();
                let name = name.map(Rc::new);
                let memory_value = memory_log.range(range.clone(), name.clone()).remove();
                let sqlite_value = sqlite_log.range(range, name).remove();
                cmp!(memory_value, sqlite_value);
            }
            Op::Iter(range, name) => {
                let range = range.to_duration_range();
                let name = name.map(Rc::new);
                let memory_value = memory_log.range(range.clone(), name.clone()).iter();
                let sqlite_value = sqlite_log.range(range, name).iter();

                match (memory_value, sqlite_value) {
                    (Err(memory_err), Err(sqlite_err)) => {
                        assert_eq!(format!("{:?}", memory_err), format!("{:?}", sqlite_err));
                    }
                    (Err(memory_err), Ok(_)) => {
                        panic!("sqlite store operation passed, but memory store operation failed: {}", memory_err);
                    }
                    (Ok(_), Err(sqlite_err)) => {
                        panic!("memory store operation passed, but sqlite store operation failed: {}", sqlite_err);
                    }
                    (Ok(mut memory_iter), Ok(mut sqlite_iter)) => {
                        loop {
                            match (memory_iter.next(), sqlite_iter.next()) {
                                (Some(memory_value), Some(sqlite_value)) => {
                                    cmp!(memory_value, sqlite_value)
                                }
                                (Some(_), None) => {
                                    panic!("sqlite range is done, but memory range is still iterating");
                                }
                                (None, Some(_)) => {
                                    panic!("memory range is done, but sqlite range is still iterating");
                                }
                                (None, None) => break
                            }
                        }
                    }
                }
            }
        }
    }
});
