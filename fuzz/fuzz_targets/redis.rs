#![no_main]
use std::borrow::Cow;
use std::time::Duration;

use arbitrary::Arbitrary;
use binlog::{Entry, MemoryStore, RedisStreamStore, Store, SubscribeableStore, Subscription};
use libfuzzer_sys::fuzz_target;

macro_rules! cmp {
    ($memory_value:expr, $redis_value:expr) => {
        match ($memory_value, $redis_value) {
            (Ok(memory_value), Ok(redis_value)) => {
                assert_eq!(memory_value, redis_value);
            }
            (memory_value, redis_value) => {
                assert_eq!(format!("{:?}", memory_value), format!("{:?}", redis_value));
            }
        }
    };
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
enum Op {
    Push {
        timestamp: i64,
        name: String,
        value: Vec<u8>,
        subscribe_at: SubscribeAt,
        timeout: Option<Duration>,
    },
    Latest {
        name: String,
    },
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
enum SubscribeAt {
    None,
    Before,
    After,
}

fuzz_target!(|ops: Vec<Op>| {
    let memory_log = MemoryStore::default();
    let redis_log = RedisStreamStore::new("redis://localhost:6379").unwrap();

    for op in ops {
        match op {
            Op::Push {
                timestamp,
                name,
                value,
                subscribe_at,
                timeout,
            } => {
                let subs = if subscribe_at == SubscribeAt::Before {
                    let memory_sub = memory_log.subscribe(name.clone()).unwrap();
                    let redis_sub = redis_log.subscribe(name.clone()).unwrap();
                    Some((memory_sub, redis_sub))
                } else {
                    None
                };

                let entry = Entry::new_with_timestamp(timestamp, name.clone(), value);
                let memory_value = memory_log.push(Cow::Borrowed(&entry));
                let redis_value = redis_log.push(Cow::Owned(entry));
                cmp!(memory_value, redis_value);

                match subscribe_at {
                    SubscribeAt::Before => {
                        let (mut memory_sub, mut redis_sub) = subs.unwrap();
                        let memory_sub_value = memory_sub.next(timeout);
                        let redis_sub_value = redis_sub.next(timeout);
                        cmp!(memory_sub_value, redis_sub_value);
                    }
                    SubscribeAt::After => {
                        let mut memory_sub = memory_log.subscribe(name.clone()).unwrap();
                        let mut redis_sub = redis_log.subscribe(name).unwrap();
                        let memory_sub_value = memory_sub.next(timeout);
                        let redis_sub_value = redis_sub.next(timeout);
                        cmp!(memory_sub_value, redis_sub_value);
                    }
                    _ => {}
                }
            }
            Op::Latest { name } => {
                let memory_value = memory_log.latest(name.clone());
                let redis_value = redis_log.latest(name);
                cmp!(memory_value, redis_value);
            }
        }
    }
});
