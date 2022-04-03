use std::borrow::Cow;
use std::ops::RangeBounds;

use super::{Entry, Error};

use string_cache::DefaultAtom as Atom;

pub trait Store: Send + Sync {
    type Range: Range;
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error>;
    fn range<R: RangeBounds<i64>>(&self, range: R, name: Option<Atom>) -> Result<Self::Range, Error>;
}

pub trait Range {
    type Iter: Iterator<Item = Result<Entry, Error>>;
    fn count(&self) -> Result<u64, Error>;
    fn remove(self) -> Result<(), Error>;
    fn iter(self) -> Result<Self::Iter, Error>;
}
