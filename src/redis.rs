use std::borrow::Cow;
use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use super::{Entry, Error, Store, SubscribeableStore};

use byteorder::{ByteOrder, LittleEndian};
use crossbeam_channel::{unbounded, Receiver, Sender};
use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{Client, Cmd, Commands, Connection, ConnectionLike, IntoConnectionInfo, RedisError, Value};
use string_cache::DefaultAtom as Atom;

static STREAM_READ_BLOCK_MS: usize = 1000;
static PUSH_CONNS_MAX_COUNT: usize = 4;

macro_rules! get_field_from_stream_map {
    ($tx:expr, $stream_id:expr, $field_name:expr) => {
        match $stream_id.map.get($field_name) {
            Some(Value::Data(bytes)) => bytes,
            _ => {
                let err = invalid_data_err("unexpected data format received from redis");
                if $tx.send(Err(err)).is_err() {
                    return;
                } else {
                    break;
                }
            }
        }
    };
}

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

#[derive(Clone)]
pub struct RedisStreamStore {
    client: Client,
    push_conns: Arc<Mutex<Vec<Connection>>>,
    max_stream_len: StreamMaxlen,
}

impl RedisStreamStore {
    pub fn new_with_client(client: Client, max_stream_len: usize) -> Self {
        Self {
            client,
            push_conns: Arc::new(Mutex::new(Vec::default())),
            max_stream_len: StreamMaxlen::Approx(max_stream_len),
        }
    }

    pub fn new<T: IntoConnectionInfo>(params: T, max_stream_len: usize) -> Result<Self, Error> {
        Ok(Self::new_with_client(Client::open(params)?, max_stream_len))
    }
}

impl Store for RedisStreamStore {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let channel = redis_channel(&entry.name);
        let mut timestamp_bytes = [0; 8];
        LittleEndian::write_i64(&mut timestamp_bytes, entry.timestamp);
        let payload = &[
            ("timestamp", timestamp_bytes.as_slice()),
            ("value", entry.value.as_slice()),
        ];
        let cmd = Cmd::xadd_maxlen(channel, self.max_stream_len, "*", payload);

        let mut push_conn = {
            let mut push_conns = self.push_conns.lock().unwrap();
            if let Some(conn) = push_conns.pop() {
                conn
            } else {
                self.client.get_connection()?
            }
        };

        push_conn.req_command(&cmd)?;

        let mut push_conns = self.push_conns.lock().unwrap();
        if push_conns.len() < PUSH_CONNS_MAX_COUNT {
            push_conns.push(push_conn);
        }

        Ok(())
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
                let timestamp_bytes = get_field_from_stream_map!(tx, stream_id, "timestamp");
                let timestamp = LittleEndian::read_i64(timestamp_bytes);
                let value = get_field_from_stream_map!(tx, stream_id, "value");
                let entry = Entry::new_with_timestamp(timestamp, name.clone(), value.clone());

                if tx.send(Ok(entry)).is_err() {
                    return;
                }

                last_id = stream_id.id;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::define_test;
    test_subscribeable_store_impl!({ super::RedisStreamStore::new("redis://localhost:6379", 100).unwrap() });
}

#[cfg(feature = "benches")]
mod benches {
    use crate::{bench_store_impl, define_bench};
    bench_store_impl!({ super::RedisStreamStore::new("redis://localhost:6379", 100).unwrap() });
}
