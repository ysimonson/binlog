use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub time: Duration,
    pub name: Rc<String>,
    pub value: Vec<u8>,
}

impl Entry {
    pub fn new(name: Rc<String>, value: Vec<u8>) -> Entry {
        let duration_since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).expect("great scott!!");
        Self::new_with_time(duration_since_epoch, name, value)
    }

    pub fn new_with_time(time: Duration, name: Rc<String>, value: Vec<u8>) -> Entry {
        Self { time, name, value }
    }
}
