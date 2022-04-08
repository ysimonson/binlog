use std::borrow::Cow;

use crate::{Error, Store, SubscribeableStore};

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

fn map_result<T>(res: Result<T, Error>) -> PyResult<T> {
    res.map_err(|err| match err {
        Error::Database(err) => PyRuntimeError::new_err(format!("{}", err)),
        Error::Io(err) => PyIOError::new_err(err),
        Error::BadRange => PyValueError::new_err("bad range"),
    })
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    #[pyo3(get, set)]
    pub timestamp: i64,
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
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
        crate::Entry::new_with_timestamp(entry.timestamp, entry.name, entry.value)
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
            store: map_result(crate::SqliteStore::new(path, compression_level))?,
        })
    }

    pub fn push(&self, entry: Entry) -> PyResult<()> {
        map_result(self.store.push(Cow::Owned(entry.into())))
    }
}

#[pyclass]
pub struct RedisStreamStore {
    store: crate::RedisStreamStore,
}

#[pymethods]
impl RedisStreamStore {
    #[new]
    pub fn new(connection_url: String, max_stream_len: usize) -> PyResult<Self> {
        Ok(Self {
            store: map_result(crate::RedisStreamStore::new(connection_url, max_stream_len))?,
        })
    }

    pub fn push(&self, entry: Entry) -> PyResult<()> {
        map_result(self.store.push(Cow::Owned(entry.into())))
    }

    pub fn subscribe(&self, name: String) -> PyResult<RedisStreamIterator> {
        let iter = map_result(self.store.subscribe(name))?;
        Ok(RedisStreamIterator { iter })
    }
}

#[pyclass]
pub struct RedisStreamIterator {
    iter: crate::RedisStreamIterator,
}

#[pymethods]
impl RedisStreamIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> Option<PyObject> {
        match slf.iter.next() {
            Some(Ok(entry)) => Some(Entry::from(entry).into_py(py)),
            Some(Err(err)) => Some(map_result::<()>(Err(err)).unwrap_err().into_py(py)),
            None => None,
        }
    }
}

#[pymodule]
fn binlog(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Entry>()?;
    m.add_class::<RedisStreamStore>()?;
    m.add_class::<SqliteStore>()?;
    Ok(())
}
