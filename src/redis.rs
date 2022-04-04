use std::borrow::Cow;
use std::io::{Cursor, Seek, SeekFrom};
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;

use super::{Entry, Error, Store, SubscribeableStore};

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use redis::{
    Client, Cmd, Connection, ConnectionLike, ControlFlow, IntoConnectionInfo, Msg, PubSubCommands, RedisError,
};
use string_cache::DefaultAtom as Atom;

impl From<RedisError> for Error {
    fn from(err: RedisError) -> Self {
        Error::Database(Box::new(err))
    }
}

#[derive(Clone)]
pub struct RedisPubSubStore {
    client: Client,
    push_conn: Arc<Mutex<Connection>>,
}

impl RedisPubSubStore {
    pub fn new_with_client(client: Client) -> Result<Self, Error> {
        let push_conn = client.get_connection()?;
        Ok(Self {
            client,
            push_conn: Arc::new(Mutex::new(push_conn)),
        })
    }

    pub fn new<T: IntoConnectionInfo>(params: T) -> Result<Self, Error> {
        Self::new_with_client(Client::open(params)?)
    }
}

impl Store for RedisPubSubStore {
    fn push(&self, entry: Cow<Entry>) -> Result<(), Error> {
        let channel = format!("binlog:pubsub:v0:{}", entry.name);
        let value = {
            let mut bytes = entry.value.clone();
            bytes.reserve_exact(8);
            let mut cursor = Cursor::new(bytes);
            cursor.seek(SeekFrom::End(8))?;
            cursor.write_i64::<LittleEndian>(entry.timestamp)?;
            cursor.into_inner()
        };

        let mut push_conn = self.push_conn.lock().unwrap();
        push_conn.req_command(&Cmd::publish(channel, value))?;
        Ok(())
    }
}

impl SubscribeableStore for RedisPubSubStore {
    type Subscription = RedisPubSubIterator;
    fn subscribe(&self, name: Option<Atom>) -> Result<Self::Subscription, Error> {
        let conn = self.client.get_connection()?;
        RedisPubSubIterator::new(conn, name)
    }
}

pub struct RedisPubSubIterator {
    rx: Option<Receiver<Result<Entry, Error>>>,
    listener_thread: Option<thread::JoinHandle<()>>,
}

impl RedisPubSubIterator {
    fn new(mut conn: Connection, name: Option<Atom>) -> Result<Self, Error> {
        let (tx, rx) = channel::<Result<Entry, Error>>();

        let listener_thread = thread::spawn(move || {
            let cb = |message: Msg| -> ControlFlow<()> {
                let channel = message.get_channel_name();
                debug_assert!(channel.len() >= 17);
                let name = &channel[17..];
                let payload = message.get_payload_bytes();
                debug_assert!(payload.len() >= 8);
                let (value, micros_bytes) = payload.split_at(payload.len() - 8);
                let timestamp = LittleEndian::read_i64(micros_bytes);
                let entry = Entry::new_with_timestamp(timestamp, Atom::from(name), value.into());
                if tx.send(Ok(entry)).is_ok() {
                    ControlFlow::Continue
                } else {
                    ControlFlow::Break(())
                }
            };

            let result = if let Some(name) = name {
                conn.subscribe(format!("binlog:pubsub:v0:{}", name), cb)
            } else {
                conn.psubscribe("binlog:pubsub:v0:*", cb)
            };

            if let Err(err) = result {
                tx.send(Err(err.into())).ok();
            }
        });

        Ok(RedisPubSubIterator {
            rx: Some(rx),
            listener_thread: Some(listener_thread),
        })
    }
}

impl Drop for RedisPubSubIterator {
    fn drop(&mut self) {
        let rx = self.rx.take().unwrap();
        drop(rx);
        let listener_thread = self.listener_thread.take().unwrap();
        listener_thread.join().unwrap();
    }
}

impl Iterator for RedisPubSubIterator {
    type Item = Result<Entry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.as_ref().unwrap().recv() {
            Ok(value) => Some(value),
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::define_test;
    test_subscribeable_store_impl!({
        let connection_url = std::env::var("BINLOG_REDIS").expect("Must set the `BINLOG_REDIS` environment variable to run tests on the redis store");
        super::RedisPubSubStore::new(connection_url).unwrap()
    });
}

// #[cfg(feature = "benches")]
// mod benches {
//     use crate::{bench_store_impl, define_bench};
//     bench_store_impl!({
//         use super::SqliteStore;
//         use tempfile::NamedTempFile;
//         let file = NamedTempFile::new().unwrap().into_temp_path();
//         SqliteStore::new(file, None).unwrap()
//     });
// }
