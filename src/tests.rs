use std::borrow::Cow;
use std::collections::VecDeque;

use crate::{Entry, Error, Range, RangeableStore};

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
macro_rules! test_rangeable_store_impl {
    ($code:expr) => {
        define_test!(remove, $code);
        define_test!(iter, $code);
    };
}

pub fn remove<S: RangeableStore>(store: &S) {
    for i in 1..11 {
        let entry = Entry::new_with_timestamp(i.into(), Atom::from("test_remove"), vec![i]);
        store.push(Cow::Owned(entry)).unwrap();
    }
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
    for i in 1..11u8 {
        let entry = Entry::new_with_timestamp(i.into(), Atom::from("test_iter"), vec![i]);
        store.push(Cow::Owned(entry)).unwrap();
    }
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
