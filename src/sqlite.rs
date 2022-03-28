use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::vec::IntoIter as VecIter;

use super::{utils, Entry, Error, Range, Store};

use bincode::{deserialize, serialize, Error as BincodeError};
use refcount_interner::RcInterner;
use rusqlite::Error as SqliteError;
use rusqlite::{params, params_from_iter, Connection, ParamsFromIter, Statement};
use zstd::bulk::{Compressor, Decompressor};

static SCHEMA: &str = r#"
create table if not exists compacted_log (
    id integer primary key,
    start_ts integer not null,
    end_ts integer not null,
    name text not null,
    size integer not null,
    count integer not null,
    value blob not null
) strict; 

create index idx_compacted_log_ts on compacted_log(ts);

create table if not exists log (
    id integer primary key,
    ts integer not null,
    name text not null,
    size integer not null,
    value blob not null
) strict;

create index idx_log_ts on log(ts);
"#;

static DEFAULT_COMPRESSION_LEVEL: i32 = 1;
static DEFAULT_COMPACTED_COMPRESSION_LEVEL: i32 = 20;
static PAGINATION_LIMIT: usize = 1000;
static COMPACTING_SIZE_THRESHOLD: usize = 10 * 1024 * 1024; // 10mb
static ONE_HOUR: Duration = Duration::from_secs(60 * 60);

impl From<SqliteError> for Error {
    fn from(err: SqliteError) -> Self {
        Error::Database(Box::new(err))
    }
}

impl From<BincodeError> for Error {
    fn from(err: BincodeError) -> Self {
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

    fn statement<'a>(
        &self,
        prefix: &'a str,
        suffix: &'a str,
        start_ts_column_name: &'a str,
        end_ts_column_name: &'a str,
    ) -> Cow<'a, str> {
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        match self.start_bound {
            Bound::Included(s) => clauses.push(format!("{} >= {}", end_ts_column_name, s.as_micros())),
            Bound::Excluded(s) => clauses.push(format!("{} > {}", end_ts_column_name, s.as_micros())),
            Bound::Unbounded => {}
        }

        match self.end_bound {
            Bound::Included(e) => clauses.push(format!("{} <= {}", start_ts_column_name, e.as_micros())),
            Bound::Excluded(e) => clauses.push(format!("{} < {}", start_ts_column_name, e.as_micros())),
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

    fn compacted_log_statement<'a>(&self, prefix: &'a str, suffix: &'a str) -> Cow<'a, str> {
        self.statement(prefix, suffix, "end_ts", "start_ts")
    }

    fn log_statement<'a>(&self, prefix: &'a str, suffix: &'a str) -> Cow<'a, str> {
        self.statement(prefix, suffix, "ts", "ts")
    }

    fn pagination(&self, offset: usize) -> String {
        format!("order by ts offset {} limit {}", offset, PAGINATION_LIMIT)
    }
}

pub struct SqliteStore<'a> {
    datastore: SqliteDatastore,
    entry_compressor: Arc<Mutex<Compressor<'a>>>,
}

impl<'a> SqliteStore<'a> {
    pub fn new_with_connection(conn: Connection, entry_compression_level: Option<i32>) -> Result<Self, Error> {
        conn.execute(SCHEMA, params![])?;
        let compressor = Compressor::new(entry_compression_level.unwrap_or(DEFAULT_COMPRESSION_LEVEL))?;
        Ok(Self {
            datastore: SqliteDatastore {
                conn,
                names: Arc::new(Mutex::new(RcInterner::default())),
            },
            entry_compressor: Arc::new(Mutex::new(compressor)),
        })
    }

    pub fn new<P: AsRef<Path>>(path: P, entry_compression_level: Option<i32>) -> Result<Self, Error> {
        let conn = Connection::open(&path)?;
        Self::new_with_connection(conn, entry_compression_level)
    }

    pub fn compact(&mut self, compacted_compression_level: Option<i32>) -> Result<(), Error> {
        let tx = self.datastore.conn.transaction()?;

        let one_hour_ago = utils::duration_from_time(SystemTime::now() - Duration::from_secs(60 * 60));
        let statement_builder = StatementBuilder::new(..one_hour_ago, None);
        let mut offset = 0usize;
        let mut entries_map = HashMap::<String, Vec<(u64, Duration, Vec<u8>)>>::new();
        let mut compacted_compressor =
            Compressor::new(compacted_compression_level.unwrap_or(DEFAULT_COMPACTED_COMPRESSION_LEVEL))?;
        let mut entry_decompressor = Decompressor::new()?;

        loop {
            let mut stmt = tx.prepare(&statement_builder.compacted_log_statement(
                "select id, ts, name, size, value from compacted_log",
                &statement_builder.pagination(offset),
            ))?;
            let mut rows = stmt.query(statement_builder.params())?;
            let mut has_rows = false;
            while let Some(row) = rows.next()? {
                let id: u64 = row.get(0)?;
                let timestamp: u64 = row.get(1)?;
                let time = Duration::from_micros(timestamp);
                let name: String = row.get(2)?;
                let size: usize = row.get(3)?;
                let blob_compressed: Vec<u8> = row.get(4)?;
                let blob: Vec<u8> = entry_decompressor.decompress(&blob_compressed, size)?;
                entries_map
                    .entry(name)
                    .or_insert_with(Vec::default)
                    .push((id, time, blob));
                has_rows = true;
            }
            if !has_rows {
                break;
            }
            offset += PAGINATION_LIMIT;

            for (name, entries) in entries_map.iter_mut() {
                let should_compact = (entries.len() >= 2 && (entries.last().unwrap().1 - entries[0].1 >= ONE_HOUR))
                    || (entries.iter().map(|e| e.2.len()).sum::<usize>() >= COMPACTING_SIZE_THRESHOLD);
                if should_compact {
                    let mut compacting_size = 0usize;
                    let mut compacting = Vec::<(u64, Duration, Vec<u8>)>::new();
                    for entry in entries.drain(..) {
                        if !compacting.is_empty()
                            && (compacting_size + entry.2.len() >= COMPACTING_SIZE_THRESHOLD
                                || entry.1 - compacting[0].1 >= ONE_HOUR)
                        {
                            let start_ts: u64 = compacting[0].1.as_micros().try_into().unwrap();
                            let end_ts: u64 = compacting.last().unwrap().1.as_micros().try_into().unwrap();
                            let blob_serialized = serialize(&compacting)?;
                            let blob_compressed = compacted_compressor.compress(&blob_serialized)?;
                            tx.execute("insert into compacted_log (start_ts, end_ts, name, size, count, value) values (?, ?, ?, ?, ?, ?)", params![start_ts, end_ts, name, compacting_size, compacting.len(), blob_compressed])?;
                            tx.execute(
                                "delete from log where name = ? and id >= ? and id <= ?",
                                params![name, compacting[0].0, compacting.last().unwrap().0],
                            )?;
                            compacting_size = 0;
                            compacting = Vec::default();
                        }
                        compacting_size += entry.2.len();
                        compacting.push(entry);
                    }
                    *entries = compacting;
                }
            }
        }

        tx.commit()?;
        Ok(())
    }
}

impl<'a, 'r> Store<'r> for SqliteStore<'a> {
    type Range = SqliteRange<'r>;

    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let ts: u64 = entry.time.as_micros().try_into().unwrap();
        let blob = {
            let mut compressor = self.entry_compressor.lock().unwrap();
            compressor.compress(&entry.value)?
        };
        let mut stmt = self
            .datastore
            .conn
            .prepare_cached("insert into log (ts, name, size, value) values (?, ?, ?, ?)")?;
        let insert_count = stmt.execute(params![ts, entry.name, entry.value.len(), blob])?;
        debug_assert_eq!(insert_count, 1);
        Ok(())
    }

    fn range<'s, R>(&'s self, range: R, name: Option<Rc<String>>) -> Self::Range
    where
        's: 'r,
        R: RangeBounds<Duration>,
    {
        SqliteRange {
            datastore: &self.datastore,
            statement_builder: StatementBuilder::new(range, name),
        }
    }
}

pub struct SqliteRange<'r> {
    datastore: &'r SqliteDatastore,
    statement_builder: StatementBuilder,
}

impl<'r> Range<'r> for SqliteRange<'r> {
    type Iter = SqliteRangeIterator<'r>;

    fn len(&self) -> Result<u64, Error> {
        let mut stmt = self.datastore.conn.prepare(
            &self
                .statement_builder
                .compacted_log_statement("select sum(count) from compacted_log", ""),
        )?;
        let compacted_log_len: u64 = stmt.query_row(self.statement_builder.params(), |row| row.get(0))?;
        let mut stmt = self
            .datastore
            .conn
            .prepare(&self.statement_builder.log_statement("select count(id) from log", ""))?;
        let log_len: u64 = stmt.query_row(self.statement_builder.params(), |row| row.get(0))?;
        Ok(compacted_log_len + log_len)
    }

    fn remove(self) -> Result<(), Error> {
        let mut stmt = self.datastore.conn.prepare(
            &self
                .statement_builder
                .compacted_log_statement("delete from compacted_log", ""),
        )?;
        stmt.execute(self.statement_builder.params())?;
        let mut stmt = self
            .datastore
            .conn
            .prepare(&self.statement_builder.log_statement("delete from log", ""))?;
        stmt.execute(self.statement_builder.params())?;
        Ok(())
    }

    fn iter(self) -> Result<Self::Iter, Error> {
        Ok(SqliteRangeIterator {
            datastore: self.datastore,
            statement_builder: self.statement_builder,
            entries: VecDeque::default(),
            state: 0,
            offset: 0,
        })
    }
}

pub struct SqliteRangeIterator<'r> {
    datastore: &'r SqliteDatastore,
    statement_builder: StatementBuilder,
    entries: VecDeque<Entry>,
    state: u8,
    offset: usize,
}

impl<'r> SqliteRangeIterator<'r> {
    fn get_entries<'a>(&mut self, mut stmt: Statement<'a>) -> Result<Vec<Entry>, Error> {
        let mut rows = stmt.query(self.statement_builder.params())?;
        let mut names = self.datastore.names.lock().unwrap();
        let mut decompressor = Decompressor::new()?;
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            let timestamp: u64 = row.get(0)?;
            let time = Duration::from_micros(timestamp);
            let name: String = row.get(1)?;
            let name: Rc<String> = names.intern(name);
            let size: usize = row.get(2)?;
            let blob_compressed: Vec<u8> = row.get(3)?;
            let blob: Vec<u8> = decompressor.decompress(&blob_compressed, size)?;
            entries.push(Entry::new_with_time(time, name, blob));
        }
        if entries.is_empty() {
            self.state += 1;
            self.offset = 0;
        } else {
            self.offset += PAGINATION_LIMIT;
        }
        Ok(entries)
    }

    fn fill_compacted_entries(&mut self) -> Result<(), Error> {
        let stmt = self
            .datastore
            .conn
            .prepare(&self.statement_builder.compacted_log_statement(
                "select start_ts, name, size, value from compacted_log",
                &self.statement_builder.pagination(self.offset),
            ))?;
        for compacted_entry in self.get_entries(stmt)?.into_iter() {
            let blob: Vec<(Duration, Vec<u8>)> = deserialize(&compacted_entry.value)?;
            for (time, value) in blob.into_iter() {
                self.entries
                    .push_back(Entry::new_with_time(time, compacted_entry.name.clone(), value));
            }
        }
        Ok(())
    }

    fn fill_entries(&mut self) -> Result<(), Error> {
        let stmt = self.datastore.conn.prepare(&self.statement_builder.log_statement(
            "select ts, name, size, value from log",
            &self.statement_builder.pagination(self.offset),
        ))?;
        let entries = self.get_entries(stmt)?;
        self.entries.extend(entries);
        Ok(())
    }
}

impl<'r> Iterator for SqliteRangeIterator<'r> {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries.is_empty() && self.state == 0 {
            if let Err(err) = self.fill_compacted_entries() {
                return Some(Err(err));
            }
        }
        if self.entries.is_empty() && self.state == 1 {
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
