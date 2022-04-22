use std::borrow::Cow;
use std::collections::VecDeque;
use string_cache::DefaultAtom as Atom;

use crate::{Entry, Error, Range, RangeableStore, Store, SubscribeableStore};

/// Defines a unit test function.
#[doc(hidden)]
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

#[doc(hidden)]
#[macro_export]
macro_rules! test_store_impl {
    ($code:expr) => {
        define_test!(latest, $code);
    };
}

#[macro_export]
macro_rules! test_rangeable_store_impl {
    ($code:expr) => {
        define_test!(remove, $code);
        define_test!(iter, $code);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! test_subscribeable_store_impl {
    ($code:expr) => {
        define_test!(pubsub, $code);
    };
}

fn insert_sample_data<S: Store>(store: &S, name: &str) -> Result<(), Error> {
    for i in 1..11 {
        let entry = Entry::new_with_timestamp(i.into(), name, vec![i]);
        store.push(Cow::Owned(entry))?;
    }
    Ok(())
}

fn check_sample_data(mut results: VecDeque<Result<Entry, Error>>, name: &str) -> Result<(), Error> {
    assert_eq!(results.len(), 10);
    for i in 1..11u8 {
        let result = results.pop_front().unwrap()?;
        assert_eq!(result, Entry::new_with_timestamp(i.into(), name, vec![i]));
    }
    Ok(())
}

pub fn remove<S: RangeableStore>(store: &S) {
    insert_sample_data(store, "test_remove").unwrap();
    assert_eq!(store.range(.., Option::<Atom>::None).unwrap().count().unwrap(), 10);
    store.range(2.., Option::<Atom>::None).unwrap().remove().unwrap();
    assert_eq!(store.range(.., Option::<Atom>::None).unwrap().count().unwrap(), 1);
    store.range(.., Some("test_remove")).unwrap().remove().unwrap();
    assert_eq!(store.range(.., Option::<Atom>::None).unwrap().count().unwrap(), 0);
}

pub fn iter<S: RangeableStore>(store: &S) {
    insert_sample_data(store, "test_iter").unwrap();
    let results: VecDeque<Result<Entry, Error>> =
        store.range(.., Option::<Atom>::None).unwrap().iter().unwrap().collect();
    check_sample_data(results, "test_iter").unwrap();
}

pub fn pubsub<S: SubscribeableStore + Clone>(store: &S) {
    let subscriber = store.subscribe("test_pubsub").unwrap();
    insert_sample_data(store, "test_pubsub").unwrap();
    let results: VecDeque<Result<Entry, Error>> = subscriber.take(10).collect();
    check_sample_data(results, "test_pubsub").unwrap();
}

pub fn latest<S: Store + Clone>(store: &S) {
    assert_eq!(store.latest("test_latest").unwrap(), None);
    insert_sample_data(store, "test_latest").unwrap();
    assert_eq!(
        store.latest("test_latest").unwrap(),
        Some(Entry::new_with_timestamp(10, "test_latest", vec![10]))
    );
}
