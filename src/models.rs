use std::rc::Rc;
use std::time::{Duration, SystemTime};

use super::utils;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub time: Duration,
    pub name: Rc<String>,
    pub value: Vec<u8>,
}

impl Entry {
    pub fn new(name: Rc<String>, value: Vec<u8>) -> Entry {
        let now = utils::duration_from_time(SystemTime::now());
        Self::new_with_time(now, name, value)
    }

    pub fn new_with_time(time: Duration, name: Rc<String>, value: Vec<u8>) -> Entry {
        Self { time, name, value }
    }
}
