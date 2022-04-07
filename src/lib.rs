#![cfg_attr(feature = "benches", feature(test))]
#[cfg(feature = "benches")]
extern crate test;

mod entry;
mod errors;
mod stores;
mod traits;
mod utils;
#[macro_use]
pub mod tests;

#[cfg(feature = "python")]
mod python;
#[cfg(feature = "benches")]
#[macro_use]
pub mod benches;

pub use self::entry::Entry;
pub use self::errors::Error;
pub use self::stores::memory::{MemoryRange, MemoryStore, MemoryStreamIterator};
pub use self::traits::{Range, RangeableStore, Store, SubscribeableStore};

#[cfg(feature = "redis-store")]
pub use self::stores::redis::{RedisStreamIterator, RedisStreamStore};
#[cfg(feature = "sqlite-store")]
pub use self::stores::sqlite::{SqliteRange, SqliteRangeIterator, SqliteStore};
