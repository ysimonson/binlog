#![cfg_attr(feature = "benches", feature(test))]
#[cfg(feature = "benches")]
extern crate test;

mod entry;
mod errors;
mod memory;
mod traits;
mod utils;
#[macro_use]
pub mod tests;

#[cfg(feature = "python")]
mod python;
#[cfg(feature = "redis-store")]
mod redis;
#[cfg(feature = "sqlite-store")]
mod sqlite;
#[cfg(feature = "benches")]
#[macro_use]
pub mod benches;

pub use self::entry::Entry;
pub use self::errors::Error;
pub use self::memory::MemoryStore;
pub use self::traits::{Range, RangeableStore, Store, SubscribeableStore};

#[cfg(feature = "redis-store")]
pub use self::redis::{RedisStreamIterator, RedisStreamStore};
#[cfg(feature = "sqlite-store")]
pub use self::sqlite::{SqliteRange, SqliteRangeIterator, SqliteStore};
