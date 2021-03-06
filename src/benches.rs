use std::borrow::Cow;
use std::thread;

use crate::{Entry, Range, RangeableStore, Store};

use string_cache::DefaultAtom as Atom;
use test::Bencher;

/// Defines a benchmark function.
#[doc(hidden)]
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

#[doc(hidden)]
#[macro_export]
macro_rules! bench_store_impl {
    ($code:expr) => {
        define_bench!(push, $code);
        define_bench!(push_parallel, $code);
        define_bench!(latest, $code);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! bench_rangeable_store_impl {
    ($code:expr) => {
        define_bench!(iter, $code);
    };
}

pub fn push<S: Store>(b: &mut Bencher, store: &S) {
    let entry = Entry::new_with_timestamp(1, "bench_push", vec![1, 2, 3]);
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
                let name = Atom::from("bench_push_parallel");
                for j in 1..101 {
                    let idx = i * j;
                    let entry = Entry::new_with_timestamp(idx, name.clone(), vec![1, 2, 3]);
                    store.push(Cow::Owned(entry)).unwrap();
                }
            }));
        }
        for thread in threads.into_iter() {
            thread.join().unwrap();
        }
    });
}

pub fn latest<S: Store + Clone + 'static>(b: &mut Bencher, store: &S) {
    let name = Atom::from("bench_latest");
    store
        .push(Cow::Owned(Entry::new_with_timestamp(1, name.clone(), vec![1, 2, 3])))
        .unwrap();

    b.iter(|| {
        store.latest(name.clone()).unwrap();
    });
}

pub fn iter<S: RangeableStore>(b: &mut Bencher, store: &S) {
    for i in 0..=255u8 {
        let entry = Entry::new_with_timestamp(i.into(), "bench_iter", vec![i]);
        store.push(Cow::Owned(entry)).unwrap();
    }
    b.iter(|| {
        assert_eq!(
            store.range(.., Option::<Atom>::None).unwrap().iter().unwrap().count(),
            256
        );
    });
}
