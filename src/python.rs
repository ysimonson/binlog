use std::borrow::Cow;

use crate::{Error, Store};

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use string_cache::DefaultAtom as Atom;

fn map_binlog_result<T>(res: Result<T, Error>) -> PyResult<T> {
    res.map_err(|err| match err {
        Error::Database(err) => PyRuntimeError::new_err(format!("{}", err)),
        Error::Io(err) => PyIOError::new_err(err),
        Error::BadRange => PyValueError::new_err("bad range"),
    })
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub timestamp: i64,
    pub name: String,
    pub value: Vec<u8>,
}

#[pymethods]
impl Entry {
    #[new]
    pub fn new(timestamp: i64, name: String, value: Vec<u8>) -> Self {
        Entry { timestamp, name, value }
    }
}

impl From<Entry> for crate::Entry {
    fn from(entry: Entry) -> crate::Entry {
        crate::Entry::new_with_timestamp(entry.timestamp, Atom::from(entry.name), entry.value)
    }
}

impl From<crate::Entry> for Entry {
    fn from(entry: crate::Entry) -> Entry {
        Entry::new(entry.timestamp, entry.name.to_string(), entry.value)
    }
}

#[pyclass]
pub struct SqliteStore {
    store: crate::SqliteStore,
}

#[pymethods]
impl SqliteStore {
    #[new]
    pub fn new(path: String, compression_level: Option<i32>) -> PyResult<Self> {
        Ok(Self {
            store: map_binlog_result(crate::SqliteStore::new(path, compression_level))?,
        })
    }

    pub fn push(&self, entry: Entry) -> PyResult<()> {
        map_binlog_result(self.store.push(Cow::Owned(entry.into())))
    }
}

#[pymodule]
fn binlog(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Entry>()?;
    m.add_class::<SqliteStore>()?;
    Ok(())
}
