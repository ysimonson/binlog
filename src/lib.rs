#![cfg_attr(feature = "benches", feature(test))]
#[cfg(feature = "benches")]
extern crate test;

mod entry;
mod errors;
mod memory;
mod traits;
#[macro_use]
pub mod tests;

#[cfg(feature = "python")]
mod python;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "benches")]
#[macro_use]
pub mod benches;

pub use self::entry::Entry;
pub use self::errors::Error;
pub use self::memory::MemoryStore;
pub use self::traits::{Range, RangeableStore, Store};

#[cfg(feature = "sqlite")]
pub use self::sqlite::SqliteStore;
