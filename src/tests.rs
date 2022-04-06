use std::borrow::Cow;
use std::collections::VecDeque;

use crate::{Entry, Error, Range, RangeableStore, Store, SubscribeableStore};

use string_cache::DefaultAtom as Atom;

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
macro_rules! test_rangeable_store_impl {
    ($code:expr) => {
        define_test!(remove, $code);
        define_test!(iter, $code);
    };
}

#[macro_export]
macro_rules! test_subscribeable_store_impl {
    ($code:expr) => {
        define_test!(pubsub, $code);
    };
}

fn insert_sample_data<S: Store>(store: &S, name: Atom) -> Result<(), Error> {
    for i in 1..11 {
        let entry = Entry::new_with_timestamp(i.into(), name.clone(), vec![i]);
        store.push(Cow::Owned(entry))?;
    }
    Ok(())
}

fn check_sample_data(mut results: VecDeque<Result<Entry, Error>>, name: Atom) -> Result<(), Error> {
    assert_eq!(results.len(), 10);
    for i in 1..11u8 {
        let result = results.pop_front().unwrap()?;
        assert_eq!(result, Entry::new_with_timestamp(i.into(), name.clone(), vec![i]));
    }
    Ok(())
}

pub fn remove<S: RangeableStore>(store: &S) {
    insert_sample_data(store, Atom::from("test_remove")).unwrap();
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 10);
    store.range(2.., None).unwrap().remove().unwrap();
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 1);
    store
        .range(.., Some(Atom::from("test_remove")))
        .unwrap()
        .remove()
        .unwrap();
    assert_eq!(store.range(.., None).unwrap().count().unwrap(), 0);
}

pub fn iter<S: RangeableStore>(store: &S) {
    insert_sample_data(store, Atom::from("test_iter")).unwrap();
    let results: VecDeque<Result<Entry, Error>> = store.range(.., None).unwrap().iter().unwrap().collect();
    check_sample_data(results, Atom::from("test_iter")).unwrap();
}

pub fn pubsub<S: SubscribeableStore + Clone>(store: &S) {
    let subscriber = store.subscribe(Atom::from("test_pubsub")).unwrap();
    // Give enough time for the thread to start up
    std::thread::sleep(std::time::Duration::from_millis(100));
    insert_sample_data(store, Atom::from("test_pubsub")).unwrap();
    let results: VecDeque<Result<Entry, Error>> = subscriber.take(10).collect();
    check_sample_data(results, Atom::from("test_pubsub")).unwrap();
}
