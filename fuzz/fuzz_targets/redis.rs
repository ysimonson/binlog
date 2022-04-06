#![no_main]
use std::borrow::Cow;

use arbitrary::Arbitrary;
use binlog::{Entry, MemoryStore, RedisStreamStore, Store, SubscribeableStore};
use libfuzzer_sys::fuzz_target;
use string_cache::DefaultAtom as Atom;

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
struct PushOp {
    timestamp: i64,
    name: String,
    value: Vec<u8>,
    subscription: Subscription,
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
enum Subscription {
    None,
    Before,
    After,
}

fuzz_target!(|ops: Vec<PushOp>| {
    let memory_log = MemoryStore::default();
    let redis_log = RedisStreamStore::new("redis://localhost:6379", 1).unwrap();

    for op in ops {
        let name = Atom::from(op.name);

        let subs = if op.subscription == Subscription::Before {
            let memory_sub = memory_log.subscribe(name.clone()).unwrap();
            let redis_sub = redis_log.subscribe(name.clone()).unwrap();
            Some((memory_sub, redis_sub))
        } else {
            None
        };

        let entry = Entry::new_with_timestamp(op.timestamp, name.clone(), op.value);
        let memory_value = memory_log.push(Cow::Borrowed(&entry));
        let redis_value = redis_log.push(Cow::Owned(entry));
        cmp!(memory_value, redis_value);

        match op.subscription {
            Subscription::Before => {
                let (mut memory_sub, mut redis_sub) = subs.unwrap();
                let memory_sub_value = memory_sub.next().unwrap();
                let redis_sub_value = redis_sub.next().unwrap();
                cmp!(memory_sub_value, redis_sub_value);
            }
            Subscription::After => {
                memory_log.subscribe(name.clone()).unwrap();
                redis_log.subscribe(name).unwrap();
            }
            _ => {}
        }
    }
});
