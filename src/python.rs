use std::borrow::Cow;
use std::convert::{TryFrom, TryInto};
use std::time::Duration;

use crate::{Error, Store};

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use string_cache::DefaultAtom as Atom;

fn map_binlog_result<T>(res: Result<T, Error>) -> PyResult<T> {
    res.map_err(|err| match err {
        Error::Database(err) => PyRuntimeError::new_err(format!("{}", err)),
        Error::Io(err) => PyIOError::new_err(err),
        Error::BadRange => PyValueError::new_err("bad range"),
        Error::TimeTooLarge => PyValueError::new_err("time too large"),
    })
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub time: i64,
    pub name: String,
    pub value: Vec<u8>,
}

#[pymethods]
impl Entry {
    #[new]
    pub fn new(time: i64, name: String, value: Vec<u8>) -> PyResult<Self> {
        if time < 0 {
            Err(PyValueError::new_err("time cannot be less than 0"))
        } else {
            Ok(Entry { time, name, value })
        }
    }
}

impl TryInto<crate::Entry> for Entry {
    type Error = PyErr;
    fn try_into(self) -> PyResult<crate::Entry> {
        let time = self
            .time
            .try_into()
            .map_err(|_| PyValueError::new_err("time cannot be less than 0"))?;
        let duration = Duration::from_micros(time);
        Ok(crate::Entry::new_with_time(duration, Atom::from(self.name), self.value))
    }
}

impl TryFrom<crate::Entry> for Entry {
    type Error = PyErr;
    fn try_from(entry: crate::Entry) -> Result<Entry, PyErr> {
        let time = entry
            .time
            .as_micros()
            .try_into()
            .map_err(|_| PyValueError::new_err("great scott!!"))?;
        Entry::new(time, entry.name.to_string(), entry.value)
    }
}

#[pyclass]
pub struct SqliteStore {
    store: crate::SqliteStore,
}

#[pymethods]
impl SqliteStore {
    pub fn push(&self, entry: Entry) -> PyResult<()> {
        map_binlog_result(self.store.push(Cow::Owned(entry.try_into()?)))
    }
}

#[pymodule]
fn binlog(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Entry>()?;
    m.add_class::<SqliteStore>()?;
    Ok(())
}
