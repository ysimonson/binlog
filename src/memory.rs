use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec::IntoIter as VecIter;

use super::{utils, Entry, Error, Range, Store};

use string_cache::DefaultAtom as Atom;

type EntriesStore = BTreeMap<Duration, Vec<(Atom, Vec<u8>)>>;

#[derive(Clone, Default)]
pub struct MemoryStore {
    entries: Arc<Mutex<EntriesStore>>,
}

impl Store for MemoryStore {
    type Range = MemoryRange;

    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let mut entries = self.entries.lock().unwrap();
        entries
            .entry(entry.time)
            .or_insert_with(Vec::default)
            .push((entry.name.clone(), entry.value.clone()));
        Ok(())
    }

    fn range<R: RangeBounds<Duration>>(&self, range: R, name: Option<Atom>) -> Result<Self::Range, Error> {
        utils::check_bounds(range.start_bound(), range.end_bound())?;
        Ok(Self::Range {
            entries: self.entries.clone(),
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            name,
        })
    }
}

pub struct MemoryRange {
    entries: Arc<Mutex<EntriesStore>>,
    start_bound: Bound<Duration>,
    end_bound: Bound<Duration>,
    name: Option<Atom>,
}

impl Range for MemoryRange {
    type Iter = VecIter<Result<Entry, Error>>;

    fn count(&self) -> Result<u64, Error> {
        let mut count: u64 = 0;
        let entries = self.entries.lock().unwrap();
        for (_, entries) in entries.range((self.start_bound, self.end_bound)) {
            if let Some(ref name) = self.name {
                count += entries.iter().filter(|e| &e.0 == name).count() as u64;
            } else {
                count += entries.len() as u64;
            }
        }
        Ok(count)
    }

    fn remove(self) -> Result<(), Error> {
        let mut entries = self.entries.lock().unwrap();
        for (_, entries) in entries.range_mut((self.start_bound, self.end_bound)) {
            if let Some(ref name) = self.name {
                *entries = entries.drain(..).filter(|e| &e.0 != name).collect();
            } else {
                *entries = Vec::default();
            }
        }
        Ok(())
    }

    fn iter(self) -> Result<Self::Iter, Error> {
        let mut returnable_entries = Vec::default();
        let entries = self.entries.lock().unwrap();
        for (time, entries) in entries.range((self.start_bound, self.end_bound)) {
            if let Some(ref name) = self.name {
                for entry in entries.iter().filter(|e| &e.0 == name) {
                    returnable_entries.push(Ok(Entry::new_with_time(*time, entry.0.clone(), entry.1.clone())));
                }
            } else {
                for entry in entries.iter() {
                    returnable_entries.push(Ok(Entry::new_with_time(*time, entry.0.clone(), entry.1.clone())));
                }
            }
        }
        Ok(returnable_entries.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use crate::{define_test, test_store_impl};
    test_store_impl!({
        use super::MemoryStore;
        MemoryStore::default()
    });
}

#[cfg(feature = "benches")]
mod benches {
    use crate::{bench_store_impl, define_bench};
    bench_store_impl!({
        use crate::MemoryStore;
        MemoryStore::default()
    });
}
