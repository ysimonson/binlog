use std::borrow::Cow;
use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use super::{Entry, Error, Store, SubscribeableStore};

use redis::{Client, Commands, Cmd, Connection, ConnectionLike, IntoConnectionInfo, RedisError, Value};
use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use string_cache::DefaultAtom as Atom;

static STREAM_READ_BLOCK_MS: usize = 1000;

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
    push_conn: Arc<Mutex<Connection>>,
    max_stream_len: StreamMaxlen,
}

impl RedisStreamStore {
    pub fn new_with_client(client: Client, max_stream_len: usize) -> Result<Self, Error> {
        let push_conn = client.get_connection()?;
        Ok(Self {
            client,
            push_conn: Arc::new(Mutex::new(push_conn)),
            max_stream_len: StreamMaxlen::Approx(max_stream_len),
        })
    }

    pub fn new<T: IntoConnectionInfo>(params: T, max_stream_len: usize) -> Result<Self, Error> {
        Self::new_with_client(Client::open(params)?, max_stream_len)
    }
}

impl Store for RedisStreamStore {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let channel = redis_channel(&entry.name);
        let payload = &[("", &entry.value)];
        let mut push_conn = self.push_conn.lock().unwrap();
        // TODO: respect entry.timestamp?
        let cmd = Cmd::xadd_maxlen(channel, self.max_stream_len, "*", payload);
        push_conn.req_command(&cmd)?;
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
        let (tx, rx) = channel::<Result<Entry, Error>>();
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

        if reply.keys.len() == 0 && shutdown.load(Ordering::Relaxed) {
            return;
        }

        for stream_key in reply.keys {
            for stream_id in stream_key.ids {
                let timestamp_str = stream_id.id.split("-").next().unwrap();
                let timestamp = match timestamp_str.parse::<i64>() {
                    Ok(value) => {
                        if let Some(value) = value.checked_mul(1000) {
                            value
                        } else {
                            let err = invalid_data_err("unexpected key format received from redis: value too large");
                            if tx.send(Err(err.into())).is_err() {
                                return;
                            } else {
                                break;
                            }
                        }
                    },
                    Err(err) => {
                        let err = invalid_data_err(format!("unexpected key format received from redis: {}", err));
                        if tx.send(Err(err.into())).is_err() {
                            return;
                        } else {
                            break;
                        }
                    }
                };

                for (_, value) in stream_id.map {
                    let result = if let Value::Data(bytes) = value {
                        Ok(Entry::new_with_timestamp(timestamp, name.clone(), bytes))
                    } else {
                        Err(invalid_data_err("unexpected data format received from redis"))
                    };
                    if tx.send(result).is_err() {
                        return;
                    }
                }

                last_id = stream_id.id;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::define_test;
    test_subscribeable_store_impl!({
        let connection_url = std::env::var("BINLOG_REDIS")
            .expect("Must set the `BINLOG_REDIS` environment variable to run tests on the redis store");
        super::RedisStreamStore::new(connection_url, 100).unwrap()
    });
}

#[cfg(feature = "benches")]
mod benches {
    use crate::{bench_store_impl, define_bench};
    bench_store_impl!({
        let connection_url = std::env::var("BINLOG_REDIS")
            .expect("Must set the `BINLOG_REDIS` environment variable to run tests on the redis store");
        super::RedisPubSubStore::new(connection_url, 100).unwrap()
    });
}