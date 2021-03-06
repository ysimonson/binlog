#![cfg_attr(feature = "benches", feature(test))]
#[cfg(feature = "benches")]
extern crate test;

mod entry;
mod errors;
mod stores;
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
pub use self::stores::memory::{MemoryRange, MemoryStore, MemoryStreamSubscription};
pub use self::stores::traits::{Range, RangeableStore, Store, SubscribeableStore, Subscription};

#[cfg(feature = "redis-store")]
pub use self::stores::redis::{RedisStreamStore, RedisStreamSubscription};
#[cfg(feature = "sqlite-store")]
pub use self::stores::sqlite::{SqliteRange, SqliteRangeIterator, SqliteStore};
