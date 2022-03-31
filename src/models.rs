use std::time::{Duration, SystemTime, UNIX_EPOCH};

use string_cache::DefaultAtom as Atom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub time: Duration,
    pub name: Atom,
    pub value: Vec<u8>,
}

impl Entry {
    pub fn new(name: Atom, value: Vec<u8>) -> Entry {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("great scott!!");
        Self::new_with_time(now, name, value)
    }

    pub fn new_with_time(time: Duration, name: Atom, value: Vec<u8>) -> Entry {
        Self { time, name, value }
    }
}
