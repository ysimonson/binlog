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
    let mut results: VecDeque<Result<Entry, Error>> = store.range(.., None).unwrap().iter().unwrap().collect();
    assert_eq!(results.len(), 10);
    for i in 1..11u8 {
        let result = results.pop_front().unwrap().unwrap();
        assert_eq!(
            result,
            Entry::new_with_timestamp(i.into(), Atom::from("test_iter"), vec![i])
        );
    }
}

pub fn pubsub<S: SubscribeableStore + Clone>(store: &S) {
    let mut s1 = store.subscribe(None).unwrap();
    let mut s2 = store.subscribe(Some(Atom::from("test_pubsub"))).unwrap();

    insert_sample_data(store, Atom::from("test_pubsub")).unwrap();

    // Pubsub is best-effort. It's possible some of the messages were dropped,
    // so just check that at least one message made it through.
    for v in vec![s1.next(), s2.next()].into_iter() {
        let v = v.unwrap().unwrap();
        assert!(v.timestamp >= 1);
        assert!(v.timestamp <= 10);
        assert_eq!(v.name, Atom::from("test_pubsub"));
        assert!(!v.value.is_empty());
    }
}
