use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter as VecIter;

use crate::{utils, Entry, Error, Range, RangeableStore, Store};

use r2d2::{Error as R2d2Error, Pool};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, params_from_iter, Error as SqliteError, OptionalExtension, ParamsFromIter};
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

fn entry_from_row<S: Into<Atom>>(
    decompressor: &mut Decompressor<'_>,
    timestamp: i64,
    name: S,
    size: usize,
    blob: Vec<u8>,
) -> Result<Entry, Error> {
    if size > 0 {
        let blob_decompressed = decompressor.decompress(&blob, size)?;
        Ok(Entry::new_with_timestamp(timestamp, name.into(), blob_decompressed))
    } else {
        Ok(Entry::new_with_timestamp(timestamp, name.into(), blob))
    }
}

struct StatementBuilder {
    start_bound: Bound<i64>,
    end_bound: Bound<i64>,
    name: Option<Atom>,
}

impl StatementBuilder {
    fn new<R: RangeBounds<i64>>(range: R, name: Option<Atom>) -> StatementBuilder {
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
            Bound::Included(s) => clauses.push(format!("ts >= {}", s)),
            Bound::Excluded(s) => clauses.push(format!("ts > {}", s)),
            Bound::Unbounded => {}
        }

        match self.end_bound {
            Bound::Included(e) => clauses.push(format!("ts <= {}", e)),
            Bound::Excluded(e) => clauses.push(format!("ts < {}", e)),
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
    // TODO: investigate perf impact of locking these vs building
    // compressors/decompressors on-the-fly
    compressor: Arc<Mutex<Compressor<'static>>>,
    decompressor: Arc<Mutex<Decompressor<'static>>>,
}

impl SqliteStore {
    pub fn new_with_pool(pool: Pool<SqliteConnectionManager>, compression_level: Option<i32>) -> Result<Self, Error> {
        {
            let conn = pool.get()?;
            conn.pragma_update(None, "journal_mode", "wal2")?;
            conn.execute(SCHEMA, params![])?;
        }
        let compressor = Compressor::new(compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL))?;
        let decompressor = Decompressor::new()?;
        Ok(Self {
            pool,
            compressor: Arc::new(Mutex::new(compressor)),
            decompressor: Arc::new(Mutex::new(decompressor)),
        })
    }

    pub fn new<P: AsRef<Path>>(path: P, compression_level: Option<i32>) -> Result<Self, Error> {
        let manager = SqliteConnectionManager::file(path);
        let pool = r2d2::Pool::new(manager)?;
        Self::new_with_pool(pool, compression_level)
    }
}

impl Store for SqliteStore {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
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
        let mut stmt = conn.prepare_cached("insert into log (ts, name, size, value) values (?, ?, ?, ?)")?;
        stmt.execute(params![entry.timestamp, entry.name.as_ref(), size, blob_ref])?;
        Ok(())
    }

    fn latest(&self, name: Atom) -> Result<Option<Entry>, Error> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare_cached("select ts, size, value from log where name = ? order by ts desc")?;
        let row = stmt
            .query_row(params![name.as_ref()], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()?;

        if let Some((timestamp, size, blob)) = row {
            let mut decompressor = self.decompressor.lock().unwrap();
            let entry = entry_from_row(&mut decompressor, timestamp, name, size, blob)?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
}

impl RangeableStore for SqliteStore {
    type Range = SqliteRange;

    fn range<R: RangeBounds<i64>>(&self, range: R, name: Option<Atom>) -> Result<Self::Range, Error> {
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
            let name: String = row.get(1)?;
            let size: usize = row.get(2)?;
            let blob: Vec<u8> = row.get(3)?;
            self.entries
                .push_back(entry_from_row(&mut decompressor, timestamp, name, size, blob)?);
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
    use crate::{define_test, test_rangeable_store_impl};
    test_rangeable_store_impl!({
        use super::SqliteStore;
        use tempfile::NamedTempFile;
        let file = NamedTempFile::new().unwrap().into_temp_path();
        SqliteStore::new(file, None).unwrap()
    });
}

#[cfg(feature = "benches")]
mod benches {
    use crate::{bench_rangeable_store_impl, bench_store_impl, define_bench};
    bench_store_impl!({
        use super::SqliteStore;
        use tempfile::NamedTempFile;
        let file = NamedTempFile::new().unwrap().into_temp_path();
        SqliteStore::new(file, None).unwrap()
    });
    bench_rangeable_store_impl!({
        use super::SqliteStore;
        use tempfile::NamedTempFile;
        let file = NamedTempFile::new().unwrap().into_temp_path();
        SqliteStore::new(file, None).unwrap()
    });
}
