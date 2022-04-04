use std::time::{SystemTime, UNIX_EPOCH};

use string_cache::DefaultAtom as Atom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub timestamp: i64,
    pub name: Atom,
    pub value: Vec<u8>,
}

impl Entry {
    pub fn new(name: Atom, value: Vec<u8>) -> Entry {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("great scott!!")
            .as_micros()
            .try_into()
            .expect("great scott!!");
        Self::new_with_timestamp(now, name, value)
    }

    pub fn new_with_timestamp(timestamp: i64, name: Atom, value: Vec<u8>) -> Entry {
        Self { timestamp, name, value }
    }
}
