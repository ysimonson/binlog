use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, Weak};
use std::vec::IntoIter as VecIter;

use crate::{utils, Entry, Error, Range, RangeableStore, Store, SubscribeableStore};

use crossbeam_channel::{unbounded, Receiver, Sender};
use string_cache::DefaultAtom as Atom;

#[derive(Clone, Default)]
struct MemoryStoreInternal {
    entries: BTreeMap<(i64, Atom), Vec<Vec<u8>>>,
    subscribers: HashMap<Atom, Vec<Weak<MemoryStreamIteratorInternal>>>,
}

#[derive(Clone, Default)]
pub struct MemoryStore(Arc<Mutex<MemoryStoreInternal>>);

impl Store for MemoryStore {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let mut internal = self.0.lock().unwrap();

        internal
            .entries
            .entry((entry.timestamp, entry.name.clone()))
            .or_insert_with(Vec::default)
            .push(entry.value.clone());

        if let Some(subscribers) = internal.subscribers.get_mut(&entry.name) {
            let entry = entry.into_owned();
            let mut new_subscribers = Vec::<Weak<MemoryStreamIteratorInternal>>::default();
            for subscriber in subscribers.drain(..) {
                if let Some(subscriber) = Weak::upgrade(&subscriber) {
                    subscriber.notify(entry.clone());
                    new_subscribers.push(Arc::downgrade(&subscriber));
                }
            }
            *subscribers = new_subscribers;
        }

        Ok(())
    }
}

impl RangeableStore for MemoryStore {
    type Range = MemoryRange;

    fn range<R: RangeBounds<i64>>(&self, range: R, name: Option<Atom>) -> Result<Self::Range, Error> {
        utils::check_bounds(range.start_bound(), range.end_bound())?;
        Ok(Self::Range {
            internal: self.0.clone(),
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            name,
        })
    }
}

pub struct MemoryRange {
    internal: Arc<Mutex<MemoryStoreInternal>>,
    start_bound: Bound<i64>,
    end_bound: Bound<i64>,
    name: Option<Atom>,
}

impl MemoryRange {
    fn full_start_bound(&self) -> (i64, Atom) {
        match self.start_bound {
            Bound::Included(timestamp) => (timestamp, Atom::from("")),
            Bound::Excluded(timestamp) => (timestamp + 1, Atom::from("")),
            Bound::Unbounded => (i64::min_value(), Atom::from("")),
        }
    }

    fn done_iterating_in_range(&self, timestamp: i64) -> bool {
        match self.end_bound {
            Bound::Included(end_bound_timestamp) => timestamp <= end_bound_timestamp,
            Bound::Excluded(end_bound_timestamp) => timestamp < end_bound_timestamp,
            Bound::Unbounded => false,
        }
    }

    fn filter_name_in_range(&self, name: &Atom) -> bool {
        if let Some(ref expected_name) = self.name {
            name != expected_name
        } else {
            false
        }
    }
}

impl Range for MemoryRange {
    type Iter = VecIter<Result<Entry, Error>>;

    fn count(&self) -> Result<u64, Error> {
        let mut count: u64 = 0;
        let internal = self.internal.lock().unwrap();
        for ((timestamp, name), values) in internal.entries.range(self.full_start_bound()..) {
            if self.done_iterating_in_range(*timestamp) {
                break;
            } 
            if self.filter_name_in_range(name) {
                continue;
            }
            count += values.len() as u64;
        }
        Ok(count)
    }

    fn remove(self) -> Result<(), Error> {
        let mut removeable_keys = Vec::default();
        let mut internal = self.internal.lock().unwrap();
        for ((timestamp, name), _values) in internal.entries.range(self.full_start_bound()..) {
            if self.done_iterating_in_range(*timestamp) {
                break;
            } 
            if self.filter_name_in_range(name) {
                continue;
            }
            removeable_keys.push((*timestamp, name.clone()));
        }
        for key in removeable_keys {
            internal.entries.remove(&key);
        }
        Ok(())
    }

    fn iter(self) -> Result<Self::Iter, Error> {
        let mut returnable_entries = Vec::default();
        let internal = self.internal.lock().unwrap();
        for ((timestamp, name), values) in internal.entries.range(self.full_start_bound()..) {
            if self.done_iterating_in_range(*timestamp) {
                break;
            } 
            if self.filter_name_in_range(name) {
                continue;
            }
            for value in values.iter() {
                returnable_entries.push(Ok(Entry::new_with_timestamp(
                    *timestamp,
                    name.clone(),
                    value.clone(),
                )));
            }
        }
        Ok(returnable_entries.into_iter())
    }
}

impl SubscribeableStore for MemoryStore {
    type Subscription = MemoryStreamIterator;
    fn subscribe(&self, name: Atom) -> Result<Self::Subscription, Error> {
        let (tx, rx) = unbounded();
        let iterator_internal = Arc::new(MemoryStreamIteratorInternal { tx });
        let mut internal = self.0.lock().unwrap();
        internal
            .subscribers
            .entry(name)
            .or_insert_with(Vec::default)
            .push(Arc::downgrade(&iterator_internal));
        Ok(MemoryStreamIterator {
            _internal: iterator_internal,
            rx,
        })
    }
}

struct MemoryStreamIteratorInternal {
    tx: Sender<Entry>,
}

impl MemoryStreamIteratorInternal {
    fn notify(&self, entry: Entry) {
        self.tx.send(entry).unwrap();
    }
}

#[derive(Clone)]
pub struct MemoryStreamIterator {
    _internal: Arc<MemoryStreamIteratorInternal>,
    rx: Receiver<Entry>,
}

impl Iterator for MemoryStreamIterator {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.rx.recv().unwrap();
        Some(Ok(value))
    }
}

#[cfg(test)]
mod tests {
    use crate::{define_test, test_rangeable_store_impl, test_subscribeable_store_impl};
    test_rangeable_store_impl!({
        use super::MemoryStore;
        MemoryStore::default()
    });
    test_subscribeable_store_impl!({
        use super::MemoryStore;
        MemoryStore::default()
    });
}

#[cfg(feature = "benches")]
mod benches {
    use crate::{bench_rangeable_store_impl, bench_store_impl, define_bench};
    bench_store_impl!({
        use crate::MemoryStore;
        MemoryStore::default()
    });
    bench_rangeable_store_impl!({
        use crate::MemoryStore;
        MemoryStore::default()
    });
}
