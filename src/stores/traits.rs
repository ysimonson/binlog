use std::borrow::Cow;
use std::ops::RangeBounds;
use std::time::Duration;

use crate::{Entry, Error};

use string_cache::DefaultAtom as Atom;

pub trait Store: Send + Sync {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error>;
    fn latest<A: Into<Atom>>(&self, name: A) -> Result<Option<Entry>, Error>;
}

pub trait RangeableStore: Store {
    type Range: Range;
    fn range<A: Into<Atom>, R: RangeBounds<i64>>(&self, range: R, name: Option<A>) -> Result<Self::Range, Error>;
}

pub trait Range {
    type Iter: Iterator<Item = Result<Entry, Error>>;
    fn count(&self) -> Result<u64, Error>;
    fn remove(self) -> Result<(), Error>;
    fn iter(self) -> Result<Self::Iter, Error>;
}

pub trait SubscribeableStore: Store {
    type Subscription: Subscription;
    fn subscribe<A: Into<Atom>>(&self, name: A) -> Result<Self::Subscription, Error>;
}

pub trait Subscription {
    fn next(&mut self, timeout: Option<Duration>) -> Result<Option<Entry>, Error>;
}
