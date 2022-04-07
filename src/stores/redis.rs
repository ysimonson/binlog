use std::borrow::Cow;
use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::{Entry, Error, Store, SubscribeableStore};

use byteorder::{ByteOrder, LittleEndian};
use crossbeam_channel::{unbounded, Receiver, Sender};
use redis::streams::{StreamId, StreamMaxlen, StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::{Client, Cmd, Commands, Connection, ConnectionLike, IntoConnectionInfo, RedisError, Value};
use string_cache::DefaultAtom as Atom;

static STREAM_READ_BLOCK_MS: usize = 1000;
static CONN_POOL_MAX_COUNT: usize = 4;

impl From<RedisError> for Error {
    fn from(err: RedisError) -> Self {
        Error::Database(Box::new(err))
    }
}

fn redis_channel(name: &Atom) -> String {
    format!("binlog:stream:v0:{}", name)
}

fn invalid_data_err<E: Into<Box<dyn StdError + Send + Sync>>>(msg: E) -> Error {
    IoError::new(IoErrorKind::InvalidData, msg).into()
}

fn unexpected_data_format() -> Error {
    invalid_data_err("unexpected data format received from redis")
}

fn entry_from_stream_id(stream_id: &StreamId, name: Atom) -> Result<Entry, Error> {
    let (timestamp, value) = match (stream_id.map.get("timestamp"), stream_id.map.get("value")) {
        (Some(Value::Data(timestamp_bytes)), Some(Value::Data(value_bytes))) => {
            (LittleEndian::read_i64(timestamp_bytes), value_bytes)
        }
        _ => {
            return Err(unexpected_data_format());
        }
    };
    Ok(Entry::new_with_timestamp(timestamp, name, value.clone()))
}

#[derive(Clone)]
pub struct RedisStreamStore {
    client: Client,
    conn_pool: Arc<Mutex<Vec<Connection>>>,
    max_stream_len: StreamMaxlen,
}

impl RedisStreamStore {
    pub fn new_with_client(client: Client, max_stream_len: usize) -> Self {
        Self {
            client,
            conn_pool: Arc::new(Mutex::new(Vec::default())),
            max_stream_len: StreamMaxlen::Approx(max_stream_len),
        }
    }

    pub fn new<T: IntoConnectionInfo>(params: T, max_stream_len: usize) -> Result<Self, Error> {
        Ok(Self::new_with_client(Client::open(params)?, max_stream_len))
    }

    fn with_connection<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce(&mut Connection) -> Result<T, Error>,
    {
        let mut conn = {
            let mut conn_pool = self.conn_pool.lock().unwrap();
            if let Some(conn) = conn_pool.pop() {
                conn
            } else {
                self.client.get_connection()?
            }
        };

        // It's possible that the connection is in a bad state, so don't return
        // it to the pool if an error occurred.
        let result = f(&mut conn)?;

        let mut conn_pool = self.conn_pool.lock().unwrap();
        if conn_pool.len() < CONN_POOL_MAX_COUNT {
            conn_pool.push(conn);
        }

        Ok(result)
    }
}

impl Store for RedisStreamStore {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let channel = redis_channel(&entry.name);
        let mut timestamp_bytes = [0; 8];
        LittleEndian::write_i64(&mut timestamp_bytes, entry.timestamp);
        let cmd = Cmd::xadd_maxlen(
            channel,
            self.max_stream_len,
            "*",
            &[
                ("timestamp", timestamp_bytes.as_slice()),
                ("value", entry.value.as_slice()),
            ],
        );

        self.with_connection(|conn| {
            conn.req_command(&cmd)?;
            Ok(())
        })
    }

    fn latest(&self, name: Atom) -> Result<Option<Entry>, Error> {
        let channel = redis_channel(&name);
        let reply: StreamRangeReply = self.with_connection(move |conn| {
            let value = conn.xrevrange_count(channel, "+", "-", 1i8)?;
            Ok(value)
        })?;

        debug_assert!(reply.ids.len() <= 1);

        if reply.ids.is_empty() {
            Ok(None)
        } else {
            match entry_from_stream_id(&reply.ids[0], name) {
                Ok(entry) => Ok(Some(entry)),
                Err(err) => Err(err),
            }
        }
    }
}

impl SubscribeableStore for RedisStreamStore {
    type Subscription = RedisStreamIterator;
    fn subscribe(&self, name: Atom) -> Result<Self::Subscription, Error> {
        let conn = self.client.get_connection()?;
        RedisStreamIterator::new(conn, name)
    }
}

pub struct RedisStreamIterator {
    shutdown: Arc<AtomicBool>,
    rx: Option<Receiver<Result<Entry, Error>>>,
    listener_thread: Option<thread::JoinHandle<()>>,
}

impl RedisStreamIterator {
    fn new(conn: Connection, name: Atom) -> Result<Self, Error> {
        let (tx, rx) = unbounded::<Result<Entry, Error>>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let listener_thread = {
            let shutdown = shutdown.clone();
            thread::spawn(|| stream_listener(conn, name, tx, shutdown))
        };
        Ok(RedisStreamIterator {
            shutdown,
            rx: Some(rx),
            listener_thread: Some(listener_thread),
        })
    }
}

impl Drop for RedisStreamIterator {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let rx = self.rx.take().unwrap();
        drop(rx);
        let listener_thread = self.listener_thread.take().unwrap();
        listener_thread.join().unwrap();
    }
}

impl Iterator for RedisStreamIterator {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.as_ref().unwrap().recv() {
            Ok(value) => Some(value),
            Err(_) => None,
        }
    }
}

fn stream_listener(mut conn: Connection, name: Atom, tx: Sender<Result<Entry, Error>>, shutdown: Arc<AtomicBool>) {
    let channels = vec![redis_channel(&name)];
    let mut last_id = "$".to_string();
    let opts = StreamReadOptions::default().block(STREAM_READ_BLOCK_MS);
    loop {
        let reply: StreamReadReply = match conn.xread_options(&channels, &[&last_id], &opts) {
            Ok(reply) => reply,
            Err(err) => {
                if tx.send(Err(err.into())).is_err() || shutdown.load(Ordering::Relaxed) {
                    return;
                } else {
                    continue;
                }
            }
        };

        if reply.keys.is_empty() && shutdown.load(Ordering::Relaxed) {
            return;
        }

        for stream_key in reply.keys {
            for stream_id in stream_key.ids {
                if tx.send(entry_from_stream_id(&stream_id, name.clone())).is_err() {
                    return;
                }
                last_id = stream_id.id;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{define_test, test_subscribeable_store_impl};
    test_subscribeable_store_impl!({ super::RedisStreamStore::new("redis://localhost:6379", 100).unwrap() });
}

#[cfg(feature = "benches")]
mod benches {
    use crate::{bench_store_impl, define_bench};
    bench_store_impl!({ super::RedisStreamStore::new("redis://localhost:6379", 100).unwrap() });
}
