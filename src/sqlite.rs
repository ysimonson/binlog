use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec::IntoIter as VecIter;

use super::{utils, Entry, Error, Range, Store};

use r2d2::{Error as R2d2Error, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Error as SqliteError;
use rusqlite::{params, params_from_iter, ParamsFromIter};
use string_cache::DefaultAtom as Atom;
use zstd::bulk::{Compressor, Decompressor};

static SCHEMA: &str = r#"
create table if not exists log (
    id integer primary key,
    ts integer not null,
    name text not null,
    size integer not null,
    value blob not null
);

create index idx_log_ts on log(ts);
"#;

// Do not compress entries smaller than this size
static MIN_SIZE_TO_COMPRESS: usize = 32;
static DEFAULT_COMPRESSION_LEVEL: i32 = 1;
static PAGINATION_LIMIT: usize = 1000;

impl From<SqliteError> for Error {
    fn from(err: SqliteError) -> Self {
        Error::Database(Box::new(err))
    }
}

impl From<R2d2Error> for Error {
    fn from(err: R2d2Error) -> Self {
        Error::Database(Box::new(err))
    }
}

struct StatementBuilder {
    start_bound: Bound<Duration>,
    end_bound: Bound<Duration>,
    name: Option<Atom>,
}

impl StatementBuilder {
    fn new<R: RangeBounds<Duration>>(range: R, name: Option<Atom>) -> StatementBuilder {
        Self {
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            name,
        }
    }

    fn params(&self) -> ParamsFromIter<VecIter<String>> {
        if let Some(name) = &self.name {
            params_from_iter(vec![name.to_string()].into_iter())
        } else {
            params_from_iter(vec![].into_iter())
        }
    }

    fn statement<'a>(&self, prefix: &'a str, suffix: &'a str) -> Cow<'a, str> {
        let mut clauses = Vec::new();

        match self.start_bound {
            Bound::Included(s) => clauses.push(format!("ts >= {}", s.as_micros())),
            Bound::Excluded(s) => clauses.push(format!("ts > {}", s.as_micros())),
            Bound::Unbounded => {}
        }

        match self.end_bound {
            Bound::Included(e) => clauses.push(format!("ts <= {}", e.as_micros())),
            Bound::Excluded(e) => clauses.push(format!("ts < {}", e.as_micros())),
            Bound::Unbounded => {}
        }

        if self.name.is_some() {
            clauses.push("name = ?".to_string());
        }

        let where_clause = if clauses.is_empty() {
            "".to_string()
        } else {
            format!("where {}", clauses.join(" and "))
        };

        if where_clause.is_empty() && suffix.is_empty() {
            Cow::Borrowed(prefix)
        } else {
            Cow::Owned(format!("{} {} {}", prefix, where_clause, suffix))
        }
    }
}

#[derive(Clone)]
pub struct SqliteStore {
    pool: Pool<SqliteConnectionManager>,
    compressor: Arc<Mutex<Compressor<'static>>>,
}

impl SqliteStore {
    pub fn new_with_pool(pool: Pool<SqliteConnectionManager>, compression_level: Option<i32>) -> Result<Self, Error> {
        pool.get()?.execute(SCHEMA, params![])?;
        let compressor = Compressor::new(compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL))?;
        Ok(Self {
            pool,
            compressor: Arc::new(Mutex::new(compressor)),
        })
    }

    pub fn new<P: AsRef<Path>>(path: P, compression_level: Option<i32>) -> Result<Self, Error> {
        let manager = SqliteConnectionManager::file(path);
        let pool = r2d2::Pool::new(manager)?;
        Self::new_with_pool(pool, compression_level)
    }
}

impl Store for SqliteStore {
    type Range = SqliteRange;

    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let ts: i64 = entry.time.as_micros().try_into().unwrap();
        let (blob_compressed, size) = if entry.value.len() >= MIN_SIZE_TO_COMPRESS {
            let mut compressor = self.compressor.lock().unwrap();
            (compressor.compress(&entry.value)?, entry.value.len())
        } else {
            (Vec::default(), 0)
        };
        let blob_ref = if blob_compressed.is_empty() {
            &entry.value
        } else {
            &blob_compressed
        };

        let conn = self.pool.get()?;
        let mut stmt = conn
            .prepare_cached("insert into log (ts, name, size, value) values (?, ?, ?, ?)")?;
        stmt.execute(params![ts, entry.name.as_ref(), size, blob_ref])?;
        Ok(())
    }

    fn range<R: RangeBounds<Duration>>(&self, range: R, name: Option<Atom>) -> Result<Self::Range, Error> {
        utils::check_bounds(range.start_bound(), range.end_bound())?;
        Ok(SqliteRange {
            pool: self.pool.clone(),
            statement_builder: StatementBuilder::new(range, name),
        })
    }
}

pub struct SqliteRange {
    pool: Pool<SqliteConnectionManager>,
    statement_builder: StatementBuilder,
}

impl Range for SqliteRange {
    type Iter = SqliteRangeIterator;

    fn count(&self) -> Result<u64, Error> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare(&self.statement_builder.statement("select count(id) from log", ""))?;
        let len: u64 = stmt.query_row(self.statement_builder.params(), |row| row.get(0))?;
        Ok(len)
    }

    fn remove(self) -> Result<(), Error> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare(&self.statement_builder.statement("delete from log", ""))?;
        stmt.execute(self.statement_builder.params())?;
        Ok(())
    }

    fn iter(self) -> Result<Self::Iter, Error> {
        Ok(SqliteRangeIterator {
            pool: self.pool,
            statement_builder: self.statement_builder,
            entries: VecDeque::default(),
            offset: 0,
            done: false,
        })
    }
}

pub struct SqliteRangeIterator {
    pool: Pool<SqliteConnectionManager>,
    statement_builder: StatementBuilder,
    entries: VecDeque<Entry>,
    offset: usize,
    done: bool,
}

impl SqliteRangeIterator {
    fn fill_entries(&mut self) -> Result<(), Error> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare(&self.statement_builder.statement(
            "select ts, name, size, value from log",
            &format!("order by ts limit {} offset {}", PAGINATION_LIMIT, self.offset),
        ))?;
        let mut rows = stmt.query(self.statement_builder.params())?;
        let mut decompressor = Decompressor::new()?;
        let mut added = 0;
        while let Some(row) = rows.next()? {
            let timestamp: i64 = row.get(0)?;
            let time = Duration::from_micros(timestamp.try_into().unwrap());
            let name: String = row.get(1)?;
            let name: Atom = Atom::from(name);
            let size: usize = row.get(2)?;
            let mut blob: Vec<u8> = row.get(3)?;
            if size > 0 {
                blob = decompressor.decompress(&blob, size)?;
            }
            self.entries.push_back(Entry::new_with_time(time, name, blob));
            added += 1;
        }
        if added < PAGINATION_LIMIT {
            self.done = true;
        }
        self.offset += PAGINATION_LIMIT;
        Ok(())
    }
}

impl Iterator for SqliteRangeIterator {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries.is_empty() && !self.done {
            if let Err(err) = self.fill_entries() {
                return Some(Err(err));
            }
        }
        self.entries.pop_front().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::SqliteStore;
    use tempfile::NamedTempFile;

    test_store_impl!({
        let file = NamedTempFile::new().unwrap().into_temp_path();
        SqliteStore::new(file, None).unwrap()
    });
}

