use std::borrow::Cow;
use std::ops::RangeBounds;
use std::time::Duration;

use super::{Entry, Error};

use string_cache::DefaultAtom as Atom;

pub trait Store: Send + Sync {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error>;
}

pub trait RangeableStore: Store {
    type Range: Range;
    fn range<R: RangeBounds<Duration>>(&self, range: R, name: Option<Atom>) -> Result<Self::Range, Error>;
}

pub trait Range {
    type Iter: Iterator<Item = Result<Entry, Error>>;
    fn count(&self) -> Result<u64, Error>;
    fn remove(self) -> Result<(), Error>;
    fn iter(self) -> Result<Self::Iter, Error>;
}
