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
    entries: BTreeMap<i64, Vec<(Atom, Vec<u8>)>>,
    subscribers: HashMap<Atom, Vec<Weak<MemoryStreamIteratorInternal>>>,
}

#[derive(Clone, Default)]
pub struct MemoryStore(Arc<Mutex<MemoryStoreInternal>>);

impl Store for MemoryStore {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let mut internal = self.0.lock().unwrap();

        internal
            .entries
            .entry(entry.timestamp)
            .or_insert_with(Vec::default)
            .push((entry.name.clone(), entry.value.clone()));

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

impl Range for MemoryRange {
    type Iter = VecIter<Result<Entry, Error>>;

    fn count(&self) -> Result<u64, Error> {
        let mut count: u64 = 0;
        let internal = self.internal.lock().unwrap();
        for (_, range) in internal.entries.range((self.start_bound, self.end_bound)) {
            if let Some(ref name) = self.name {
                count += range.iter().filter(|e| &e.0 == name).count() as u64;
            } else {
                count += range.len() as u64;
            }
        }
        Ok(count)
    }

    fn remove(self) -> Result<(), Error> {
        let mut internal = self.internal.lock().unwrap();
        for (_, range) in internal.entries.range_mut((self.start_bound, self.end_bound)) {
            if let Some(ref name) = self.name {
                *range = range.drain(..).filter(|e| &e.0 != name).collect();
            } else {
                *range = Vec::default();
            }
        }
        Ok(())
    }

    fn iter(self) -> Result<Self::Iter, Error> {
        let mut returnable_entries = Vec::default();
        let internal = self.internal.lock().unwrap();
        for (timestamp, range) in internal.entries.range((self.start_bound, self.end_bound)) {
            if let Some(ref name) = self.name {
                for entry in range.iter().filter(|e| &e.0 == name) {
                    returnable_entries.push(Ok(Entry::new_with_timestamp(
                        *timestamp,
                        entry.0.clone(),
                        entry.1.clone(),
                    )));
                }
            } else {
                for entry in range.iter() {
                    returnable_entries.push(Ok(Entry::new_with_timestamp(
                        *timestamp,
                        entry.0.clone(),
                        entry.1.clone(),
                    )));
                }
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
