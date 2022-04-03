use std::borrow::Cow;
use std::thread;
use std::time::Duration;

use crate::{Entry, Range, Store};

use string_cache::Atom;
use test::Bencher;

/// Defines a benchmark function.
#[macro_export]
macro_rules! define_bench {
    ($name:ident, $store_constructor:expr) => {
        #[bench]
        fn $name(b: &mut test::Bencher) {
            let store = $store_constructor;
            $crate::benches::$name(b, &store);
        }
    };
}

#[macro_export]
macro_rules! bench_store_impl {
    ($code:expr) => {
        define_bench!(push, $code);
        define_bench!(push_parallel, $code);
        define_bench!(iter, $code);
    };
}

pub fn push<S: Store>(b: &mut Bencher, store: &S) {
    let entry = Entry::new_with_time(Duration::from_micros(1), Atom::from("bench_push"), vec![1, 2, 3]);
    b.iter(|| {
        store.push(Cow::Borrowed(&entry)).unwrap();
    });
}

pub fn push_parallel<S: Store + Clone + 'static>(b: &mut Bencher, store: &S) {
    b.iter(|| {
        let mut threads = Vec::default();
        for i in 1..11 {
            let store = store.clone();
            threads.push(thread::spawn(move || {
                for j in 1..1001 {
                    let idx = i * j;
                    let entry = Entry::new_with_time(
                        Duration::from_micros(idx),
                        Atom::from("bench_push_parallel"),
                        vec![1, 2, 3],
                    );
                    store.push(Cow::Owned(entry)).unwrap();
                }
            }));
        }
        for thread in threads.into_iter() {
            thread.join().unwrap();
        }
    });
}

pub fn iter<S: Store>(b: &mut Bencher, store: &S) {
    for i in 0..=255u8 {
        let entry = Entry::new_with_time(Duration::from_micros(i.into()), Atom::from("bench_iter"), vec![i]);
        store.push(Cow::Owned(entry)).unwrap();
    }
    b.iter(|| {
        assert_eq!(store.range(.., None).unwrap().iter().unwrap().count(), 256);
    });
}