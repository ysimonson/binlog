use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec::IntoIter as VecIter;

use super::{Entry, Error, Range, Store, utils};

type EntriesStore = BTreeMap<Duration, Vec<(Rc<String>, Vec<u8>)>>;

#[derive(Default)]
pub struct MemoryStore {
    entries: Arc<Mutex<EntriesStore>>,
}

impl<'r> Store<'r> for MemoryStore {
    type Range = MemoryRange;

    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let mut entries = self.entries.lock().unwrap();
        entries
            .entry(entry.time)
            .or_insert_with(Vec::default)
            .push((entry.name.clone(), entry.value.clone()));
        Ok(())
    }

    fn range<'s, R>(&'s self, range: R, name: Option<Rc<String>>) -> Result<Self::Range, Error>
    where
        's: 'r,
        R: RangeBounds<Duration>,
    {
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
    name: Option<Rc<String>>,
}

impl<'r> Range<'r> for MemoryRange {
    type Iter = VecIter<Result<Entry, Error>>;

    fn len(&self) -> Result<u64, Error> {
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
                *entries = entries.drain(..).filter(|e| &e.0 == name).collect();
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
