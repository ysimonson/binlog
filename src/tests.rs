use std::borrow::Cow;
use std::collections::VecDeque;
use std::thread;
use std::time::Duration;

use crate::{Entry, Error, Range, Store};

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
        define_test!(remove, $code);
        define_test!(iter, $code);
    }
}

pub fn push<S: Store>(store: &S) {
    let entry = Entry::new_with_time(Duration::from_micros(1), Atom::from("test_push"), vec![1, 2, 3]);
    store.push(Cow::Owned(entry)).unwrap();
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 1);
}

pub fn push_parallel<S: Store + Clone + 'static>(store: &S) {
    let mut threads = Vec::default();
    for i in 1..11 {
        let store = store.clone();
        threads.push(thread::spawn(move || {
            for j in 1..11 {
                let idx: u8 = (i * j).try_into().unwrap();
                let entry = Entry::new_with_time(Duration::from_micros(idx.into()), Atom::from("test_push_parallel"), vec![idx]);
                store.push(Cow::Owned(entry)).unwrap();
            }
        }));
    }
    for thread in threads.into_iter() {
        thread.join().unwrap();
    }
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 100);
}

pub fn remove<S: Store>(store: &S) {
    for i in 1..11 {
        let entry = Entry::new_with_time(Duration::from_micros(i.into()), Atom::from("test_remove"), vec![i]);
        store.push(Cow::Owned(entry)).unwrap();
    }
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 10);
    store.range(Duration::from_micros(2).., None).unwrap().remove().unwrap();
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 1);
    store.range(.., Some(Atom::from("test_remove"))).unwrap().remove().unwrap();
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 0);
}

pub fn iter<S: Store>(store: &S) {
    for i in 1..11u8 {
        let entry = Entry::new_with_time(Duration::from_micros(i.into()), Atom::from("test_iter"), vec![i]);
        store.push(Cow::Owned(entry)).unwrap();
    }
    let mut results: VecDeque<Result<Entry, Error>> = store.range(.., None).unwrap().iter().unwrap().collect();
    assert_eq!(results.len(), 10);
    for i in 1..11u8 {
        let result = results.pop_front().unwrap().unwrap();
        assert_eq!(result, Entry::new_with_time(Duration::from_micros(i.into()), Atom::from("test_iter"), vec![i]));
    }
}
