mod errors;
mod memory;
mod models;
mod traits;
mod utils;

#[cfg(feature = "sqlite")]
mod sqlite;

pub use self::errors::Error;
pub use self::memory::MemoryStore;
pub use self::models::Entry;
pub use self::traits::{Range, Store};

#[cfg(feature = "sqlite")]
pub use self::sqlite::SqliteStore;
