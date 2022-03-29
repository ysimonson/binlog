use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec::IntoIter as VecIter;

use super::{utils, Entry, Error, Range, Store};

use refcount_interner::RcInterner;
use rusqlite::Error as SqliteError;
use rusqlite::{params, params_from_iter, Connection, ParamsFromIter};
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

struct SqliteDatastore {
    conn: Connection,
    names: Arc<Mutex<RcInterner<String>>>,
}

struct StatementBuilder {
    start_bound: Bound<Duration>,
    end_bound: Bound<Duration>,
    name: Option<Rc<String>>,
}

impl StatementBuilder {
    fn new<R: RangeBounds<Duration>>(range: R, name: Option<Rc<String>>) -> StatementBuilder {
        Self {
            start_bound: range.start_bound().cloned(),
            end_bound: range.end_bound().cloned(),
            name,
        }
    }

    fn params(&self) -> ParamsFromIter<VecIter<String>> {
        if let Some(name) = &self.name {
            params_from_iter(vec![(**name).clone()].into_iter())
        } else {
            params_from_iter(vec![].into_iter())
        }
    }

    fn statement<'a>(&self, prefix: &'a str, suffix: &'a str) -> Cow<'a, str> {
        let mut clauses = Vec::new();
        let mut params = Vec::new();

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

        if let Some(name) = &self.name {
            clauses.push("name = ?".to_string());
            params.push((*name).clone());
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

pub struct SqliteStore<'a> {
    datastore: SqliteDatastore,
    compressor: Arc<Mutex<Compressor<'a>>>,
}

impl<'a> SqliteStore<'a> {
    pub fn new_with_connection(conn: Connection, compression_level: Option<i32>) -> Result<Self, Error> {
        conn.execute(SCHEMA, params![])?;
        let compressor = Compressor::new(compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL))?;
        Ok(Self {
            datastore: SqliteDatastore {
                conn,
                names: Arc::new(Mutex::new(RcInterner::default())),
            },
            compressor: Arc::new(Mutex::new(compressor)),
        })
    }

    pub fn new<P: AsRef<Path>>(path: P, compression_level: Option<i32>) -> Result<Self, Error> {
        let conn = Connection::open(&path)?;
        Self::new_with_connection(conn, compression_level)
    }
}

impl<'a, 'r> Store<'r> for SqliteStore<'a> {
    type Range = SqliteRange<'r>;

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

        let mut stmt = self
            .datastore
            .conn
            .prepare_cached("insert into log (ts, name, size, value) values (?, ?, ?, ?)")?;
        stmt.execute(params![ts, entry.name, size, blob_ref])?;
        Ok(())
    }

    fn range<'s, R>(&'s self, range: R, name: Option<Rc<String>>) -> Result<Self::Range, Error>
    where
        's: 'r,
        R: RangeBounds<Duration>,
    {
        utils::check_bounds(range.start_bound(), range.end_bound())?;
        Ok(SqliteRange {
            datastore: &self.datastore,
            statement_builder: StatementBuilder::new(range, name),
        })
    }
}

pub struct SqliteRange<'r> {
    datastore: &'r SqliteDatastore,
    statement_builder: StatementBuilder,
}

impl<'r> Range<'r> for SqliteRange<'r> {
    type Iter = SqliteRangeIterator<'r>;

    fn count(&self) -> Result<u64, Error> {
        let mut stmt = self
            .datastore
            .conn
            .prepare(&self.statement_builder.statement("select count(id) from log", ""))?;
        let len: u64 = stmt.query_row(self.statement_builder.params(), |row| row.get(0))?;
        Ok(len)
    }

    fn remove(self) -> Result<(), Error> {
        let mut stmt = self
            .datastore
            .conn
            .prepare(&self.statement_builder.statement("delete from log", ""))?;
        stmt.execute(self.statement_builder.params())?;
        Ok(())
    }

    fn iter(self) -> Result<Self::Iter, Error> {
        Ok(SqliteRangeIterator {
            datastore: self.datastore,
            statement_builder: self.statement_builder,
            entries: VecDeque::default(),
            offset: 0,
            done: false,
        })
    }
}

pub struct SqliteRangeIterator<'r> {
    datastore: &'r SqliteDatastore,
    statement_builder: StatementBuilder,
    entries: VecDeque<Entry>,
    offset: usize,
    done: bool,
}

impl<'r> SqliteRangeIterator<'r> {
    fn fill_entries(&mut self) -> Result<(), Error> {
        let mut stmt = self.datastore.conn.prepare(&self.statement_builder.statement(
            "select ts, name, size, value from log",
            &format!("order by ts limit {} offset {}", PAGINATION_LIMIT, self.offset),
        ))?;
        let mut rows = stmt.query(self.statement_builder.params())?;
        let mut names = self.datastore.names.lock().unwrap();
        let mut decompressor = Decompressor::new()?;
        let mut added = 0;
        while let Some(row) = rows.next()? {
            let timestamp: i64 = row.get(0)?;
            let time = Duration::from_micros(timestamp.try_into().unwrap());
            let name: String = row.get(1)?;
            let name: Rc<String> = names.intern(name);
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

impl<'r> Iterator for SqliteRangeIterator<'r> {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries.is_empty() && !self.done {
            if let Err(err) = self.fill_entries() {
                return Some(Err(err));
            }
        }
        if let Some(entry) = self.entries.pop_front() {
            Some(Ok(entry))
        } else {
            None
        }
    }
}
