use std::borrow::Cow;
use std::thread;
use std::time::Duration;

use crate::{Entry, Range, Store};

use string_cache::Atom;

/// Defines a unit test function.
#[macro_export]
macro_rules! define_test {
    ($name:ident, $store_constructor:expr) => {
        #[test]
        fn $name() {
            let store = $store_constructor;
            $crate::tests::$name(&store);
        }
    };
}

#[macro_export]
macro_rules! test_store_impl {
    ($code:expr) => {
        define_test!(push, $code);
        define_test!(push_parallel, $code);
        // define_test!(remove, $code);
        // define_test!(iter, $code);
    }
}

pub fn push<S: Store>(store: &S) {
    let entry = Entry::new_with_time(Duration::from_micros(1), Atom::from("foo"), vec![1, 2, 3]);
    store.push(Cow::Owned(entry)).unwrap();
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 1);
}

pub fn push_parallel<S: Store + Clone + 'static>(store: &S) {
    let mut threads = Vec::default();
    for i in 0..10 {
        let store = store.clone();
        threads.push(thread::spawn(move || {
            for j in 0..10 {
                let idx: u8 = ((i + 1) * (j + 1)).try_into().unwrap();
                let entry = Entry::new_with_time(Duration::from_micros(idx.into()), Atom::from("foo"), vec![idx]);
                store.push(Cow::Owned(entry)).unwrap();
            }
        }));
    }
    for thread in threads.into_iter() {
        thread.join().unwrap();
    }
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 100);
}
