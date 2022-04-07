use std::borrow::Cow;
use std::ops::RangeBounds;

use super::{Entry, Error};

use string_cache::DefaultAtom as Atom;

pub trait Store: Send + Sync {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error>;
    fn latest(&self, name: Atom) -> Result<Option<Entry>, Error>;
}

pub trait RangeableStore: Store {
    type Range: Range;
    fn range<R: RangeBounds<i64>>(&self, range: R, name: Option<Atom>) -> Result<Self::Range, Error>;
}

pub trait Range {
    type Iter: Iterator<Item = Result<Entry, Error>>;
    fn count(&self) -> Result<u64, Error>;
    fn remove(self) -> Result<(), Error>;
    fn iter(self) -> Result<Self::Iter, Error>;
}

pub trait SubscribeableStore: Store {
    type Subscription: Iterator<Item = Result<Entry, Error>>;
    fn subscribe(&self, name: Atom) -> Result<Self::Subscription, Error>;
}
