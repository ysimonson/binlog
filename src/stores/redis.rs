use std::borrow::Cow;
use std::error::Error as StdError;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::sync::{Arc, Mutex};

use crate::{Entry, Error, Store, SubscribeableStore};

use byteorder::{ByteOrder, LittleEndian};
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
}

impl RedisStreamStore {
    pub fn new_with_client(client: Client) -> Self {
        Self {
            client,
            conn_pool: Arc::new(Mutex::new(Vec::default())),
        }
    }

    pub fn new<T: IntoConnectionInfo>(params: T) -> Result<Self, Error> {
        Ok(Self::new_with_client(Client::open(params)?))
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
            StreamMaxlen::Approx(1),
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

    fn latest<A: Into<Atom>>(&self, name: A) -> Result<Option<Entry>, Error> {
        let name = name.into();
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
    fn subscribe<A: Into<Atom>>(&self, name: A) -> Result<Self::Subscription, Error> {
        let conn = self.client.get_connection()?;
        Ok(RedisStreamIterator::new(conn, name.into()))
    }
}

pub struct RedisStreamIterator {
    conn: Connection,
    name: Atom,
    last_id: String,
}

impl RedisStreamIterator {
    fn new(conn: Connection, name: Atom) -> Self {
        RedisStreamIterator {
            conn,
            name,
            last_id: "0".to_string(),
        }
    }
}

impl Iterator for RedisStreamIterator {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let channels = vec![redis_channel(&self.name)];
        let opts = StreamReadOptions::default().block(STREAM_READ_BLOCK_MS);
        loop {
            let reply: StreamReadReply = match self.conn.xread_options(&channels, &[&self.last_id], &opts) {
                Ok(reply) => reply,
                Err(err) => return Some(Err(err.into())),
            };
            if let Some(stream_key) = reply.keys.into_iter().next() {
                if let Some(stream_id) = stream_key.ids.into_iter().next() {
                    let value = entry_from_stream_id(&stream_id, self.name.clone());
                    self.last_id = stream_id.id;
                    return Some(value);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{define_test, test_store_impl, test_subscribeable_store_impl, RedisStreamStore};
    test_store_impl!(RedisStreamStore::new("redis://localhost:6379").unwrap());
    test_subscribeable_store_impl!(RedisStreamStore::new("redis://localhost:6379").unwrap());
}

#[cfg(test)]
#[cfg(feature = "benches")]
mod benches {
    use crate::{bench_store_impl, define_bench, RedisStreamStore};
    bench_store_impl!(RedisStreamStore::new("redis://localhost:6379").unwrap());
}
