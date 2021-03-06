use std::borrow::Cow;
use std::ops::Bound;
use std::time::Duration;

use crate::{Error, Range, RangeableStore, Store, SubscribeableStore, Subscription};

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

    pub fn push(&self, py: Python, entry: Entry) -> PyResult<()> {
        let entry = Cow::Owned(entry.into());
        py.allow_threads(move || map_result(self.store.push(entry)))
    }

    pub fn range(
        &self,
        start_bound: Option<i64>,
        end_bound: Option<i64>,
        name: Option<String>,
    ) -> PyResult<SqliteRange> {
        let start_bound = match start_bound {
            Some(ts) => Bound::Included(ts),
            None => Bound::Unbounded,
        };
        let end_bound = match end_bound {
            Some(ts) => Bound::Excluded(ts),
            None => Bound::Unbounded,
        };
        let range = map_result(self.store.range((start_bound, end_bound), name))?;
        Ok(SqliteRange { range: Some(range) })
    }
}

#[pyclass]
pub struct SqliteRange {
    range: Option<crate::SqliteRange>,
}

#[pymethods]
impl SqliteRange {
    pub fn count(&self, py: Python) -> PyResult<u64> {
        if let Some(range) = &self.range {
            py.allow_threads(move || map_result(range.count()))
        } else {
            Err(PyValueError::new_err("range already consumed"))
        }
    }

    pub fn remove(&mut self, py: Python) -> PyResult<()> {
        if let Some(range) = self.range.take() {
            py.allow_threads(move || map_result(range.remove()))
        } else {
            Err(PyValueError::new_err("range already consumed"))
        }
    }

    pub fn iter(&mut self, py: Python) -> PyResult<SqliteRangeIterator> {
        if let Some(range) = self.range.take() {
            py.allow_threads(move || {
                let iter = map_result(range.iter())?;
                Ok(SqliteRangeIterator { iter })
            })
        } else {
            Err(PyValueError::new_err("range already consumed"))
        }
    }
}

#[pyclass]
pub struct SqliteRangeIterator {
    iter: crate::SqliteRangeIterator,
}

#[pymethods]
impl SqliteRangeIterator {
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

#[pyclass]
pub struct RedisStreamStore {
    store: crate::RedisStreamStore,
}

#[pymethods]
impl RedisStreamStore {
    #[new]
    pub fn new(connection_url: String) -> PyResult<Self> {
        Ok(Self {
            store: map_result(crate::RedisStreamStore::new(connection_url))?,
        })
    }

    pub fn push(&self, py: Python, entry: Entry) -> PyResult<()> {
        let entry = Cow::Owned(entry.into());
        py.allow_threads(move || map_result(self.store.push(entry)))
    }

    pub fn subscribe(&self, name: String) -> PyResult<RedisStreamSubscription> {
        let subscription = map_result(self.store.subscribe(name))?;
        Ok(RedisStreamSubscription { subscription })
    }
}

#[pyclass]
pub struct RedisStreamSubscription {
    subscription: crate::RedisStreamSubscription,
}

#[pymethods]
impl RedisStreamSubscription {
    pub fn next(&mut self, py: Python, duration: Option<f32>) -> PyResult<Option<Entry>> {
        let duration = duration.map(Duration::from_secs_f32);
        py.allow_threads(move || {
            let entry = map_result(self.subscription.next(duration))?;
            Ok(entry.map(|e| e.into()))
        })
    }
}

#[pymodule]
fn binlog(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Entry>()?;
    m.add_class::<SqliteStore>()?;
    m.add_class::<SqliteRange>()?;
    m.add_class::<SqliteRangeIterator>()?;
    m.add_class::<RedisStreamStore>()?;
    m.add_class::<RedisStreamSubscription>()?;
    Ok(())
}
