use std::borrow::Cow;
use std::ops::RangeBounds;
use std::rc::Rc;
use std::time::Duration;

use super::{Entry, Error};

pub trait Store<'r> {
    type Range: Range<'r>;
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error>;
    fn range<'s, R>(&'s self, range: R, name: Option<Rc<String>>) -> Result<Self::Range, Error>
    where
        's: 'r,
        R: RangeBounds<Duration>;
}

pub trait Range<'r> {
    type Iter: Iterator<Item = Result<Entry, Error>>;
    fn count(&self) -> Result<u64, Error>;
    fn remove(self) -> Result<(), Error>;
    fn iter(self) -> Result<Self::Iter, Error>;
}
