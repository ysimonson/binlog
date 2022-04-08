use std::time::{SystemTime, UNIX_EPOCH};

use string_cache::DefaultAtom as Atom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub timestamp: i64,
    pub name: Atom,
    pub value: Vec<u8>,
}

impl Entry {
    pub fn new<A: Into<Atom>>(name: A, value: Vec<u8>) -> Entry {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("great scott!!")
            .as_micros()
            .try_into()
            .expect("great scott!!");
        Self::new_with_timestamp(now, name.into(), value)
    }

    pub fn new_with_timestamp<A: Into<Atom>>(timestamp: i64, name: A, value: Vec<u8>) -> Entry {
        Self {
            timestamp,
            name: name.into(),
            value,
        }
    }
}
