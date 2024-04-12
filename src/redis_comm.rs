use super::utils::TextError;
use core::fmt;
use futures::stream::StreamExt;
use redis::aio::Connection;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

use redis::AsyncCommands;
use redis::{Client, Msg, RedisError};

use tracing::error;

// Time delay in ms before we consider a no-response to a redis query to
// be an error.
const REDIS_QUERY_TIMEOUT_MS: u64 = 2000;

// Time delay when backing off from attempting redis connections.
const REDIS_BACKOFF_MS: u64 = 100;

/// Enum representing different Redis operations that can be sent asynchronously.
///
/// This enum includes variants for setting a single key-value pair with or without a time-to-live (TTL),
/// deleting a key, and setting multiple key-value pairs.
///
/// # Variants
///
/// * `Set(String, T, Option<usize>)` - Set a single key-value pair with an optional TTL.
/// * `Delete(String)` - Delete a key.
/// * `SetMultiple(HashMap<String, T>, Option<usize>)` - Set multiple key-value pairs, with an optional TTL for all.

#[derive(Debug, Clone)]
pub enum RedisSend<T>
where
    T: redis::ToRedisArgs + fmt::Debug + Send + Sync + Clone + 'static,
{
    Set(String, T, Option<usize>),
    Delete(String),
    SetMultiple(HashMap<String, T>, Option<usize>),
}

/// Asynchronous Redis sender that processes RedisSend messages and executes corresponding Redis operations.
///
/// This function establishes an asynchronous Redis connection and processes messages from the provided Tokio channel.
/// It performs Redis operations based on the received messages, including setting key-value pairs, deleting keys, and setting multiple key-value pairs.
/// In case of errors, a simple backoff mechanism is implemented, restarting the process after a certain number of consecutive errors.
///
/// # Arguments
///
/// * `r` - A Redis `Client` used for establishing an asynchronous connection.
/// * `receiver` - A Tokio `mpsc::Receiver` used for receiving `RedisSend` messages.
/// * `tick` - An optional `Duration` specifying the interval at which messages should be processed. If `None`, messages are processed as soon as they are received.
///
/// # Generic Parameters
///
/// * `T` - A type that implements the `redis::ToRedisArgs` trait, `std::fmt::Debug`, and is `Send + Sync`.
///
/// # Errors
///
/// Returns a `Result` with `Ok(())` on successful execution and `Err` on encountering errors during Redis operations.
///
pub async fn redis_send<T>(
    r: Client,
    mut receiver: mpsc::Receiver<RedisSend<T>>,
    tick: Option<Duration>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: redis::ToRedisArgs + fmt::Debug + Send + Sync + Clone + 'static,
{
    async fn inner<T>(
        r: &Client,
        receiver: &mut mpsc::Receiver<RedisSend<T>>,
        tick: Option<Duration>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: redis::ToRedisArgs + fmt::Debug + Send + Sync + Clone + 'static,
    {
        let mut conn = r.get_async_connection().await?;
        let mut tick = tick.map(tokio::time::interval);

        // Aggregate messages when we get a tick, or if no tick, just send the message
        loop {
            let to_send = if let Some(tick) = tick.as_mut() {
                tick.tick().await;
                let mut to_send = vec![];
                while let Ok(msg) = receiver.try_recv() {
                    to_send.push(msg);
                }
                to_send
            } else if let Some(x) = receiver.recv().await {
                vec![x]
            } else {
                vec![]
            };

            let mut pipe = redis::pipe();

            for mess in to_send.clone() {
                match mess {
                    RedisSend::Set(key, value, Some(ttl)) => {
                        let value = value.to_redis_args();
                        if !value.is_empty() {
                            pipe.set_ex(key, value, ttl);
                        }
                    }
                    RedisSend::Set(key, value, None) => {
                        let value = value.to_redis_args();
                        if !value.is_empty() {
                            pipe.set(key, value);
                        }
                    }
                    RedisSend::Delete(key) => {
                        pipe.del(key);
                    }
                    RedisSend::SetMultiple(map, ttl) => {
                        for (key, value) in map {
                            let value = value.to_redis_args();
                            if value.is_empty() {
                                continue;
                            }
                            if let Some(ttl) = ttl {
                                pipe.set_ex(key, value, ttl);
                            } else {
                                pipe.set(key, value);
                            }
                        }
                    }
                }
            }
            let x: Result<(), RedisError> = pipe.atomic().query_async(&mut conn).await;
            if x.is_err() {
                error!("Error sending: {:?}", to_send);
            }
            x?;
        }
    }

    let mut backoff_counter = 0;
    loop {
        let res = inner(&r, &mut receiver, tick).await;
        error!("redis_send: {res:?}");

        if backoff_counter > 10 {
            backoff_counter = 0;
            tokio::time::sleep(Duration::from_millis(REDIS_BACKOFF_MS)).await;
        } else {
            backoff_counter += 1;
        }
    }
}

pub async fn redis_send_watched<T>(
    r: Client,
    key: String,
    mut receiver: watch::Receiver<T>,
    tick: Option<Duration>,
    ttl: Option<usize>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: redis::ToRedisArgs + fmt::Debug + Send + Sync + Clone + 'static,
{
    let (tx, rx) = mpsc::channel::<RedisSend<T>>(5);
    let jh = tokio::task::spawn(redis_send(r, rx, tick));
    let jh_fm = tokio::task::spawn(async move {
        loop {
            let _ = receiver.changed().await;
            let value = receiver.borrow().clone();
            let x = RedisSend::Set(key.clone(), value, ttl);
            let _ = tx.send(x).await;
        }
    });
    let _ = tokio::try_join!(jh, jh_fm)?;
    Err(TextError::new("send watched task failed").into())
}

/// Asynchronous Redis subscriber that continuously fetches keys from Redis and sends them through a Tokio channel at regular intervals.
///
/// This function establishes an asynchronous Redis connection and uses a Tokio interval to periodically fetch keys from the specified Redis keys.
/// The fetched data is then sent through the provided Tokio channel.
/// In case of errors, the redis connection is reestablished and the function continues to run. It should never terminate.
///
/// # Arguments
///
/// * `r` - A Redis `Client` used for establishing an asynchronous connection.
/// * `keys` - A vector of strings representing the keys to fetch from Redis.
/// * `sender` - A Tokio `watch::Sender` used for sending the fetched data through a channel.
/// * `tick` - A `Duration` specifying the interval at which keys should be fetched.
///
/// # Generic Parameters
///
/// * `T` - A type that implements the `redis::FromRedisValue` trait, `std::fmt::Debug`, and is `Send + Sync`.
///
/// # Errors
///
/// Returns a `Result` with `Ok(())` on successful execution and `Err` on encountering errors during Redis operations. Curretly this will never return.
///
pub async fn redis_tick_recv<T>(
    r: Client,
    keys: Vec<String>,
    mut sender: watch::Sender<HashMap<String, T>>,
    tick: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: redis::FromRedisValue + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    async fn inner<T>(
        r: &Client,
        keys: &[String],
        sender: &mut watch::Sender<HashMap<String, T>>,
        tick: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: redis::FromRedisValue + fmt::Debug + Send + Sync + Clone + 'static,
    {
        let mut conn = r.get_async_connection().await?;
        let mut tick = tokio::time::interval(tick);

        loop {
            let result: HashMap<String, T> = fetch_keys(keys, &mut conn).await?;
            sender.send(result)?;
            tick.tick().await;
        }
    }

    let mut backoff_counter = 0;
    loop {
        let res = inner(&r, &keys, &mut sender, tick).await;
        error!("redis_tick_recv: {res:?}, keys: {keys:?}");

        if backoff_counter > 10 {
            backoff_counter = 0;
            tokio::time::sleep(Duration::from_millis(REDIS_BACKOFF_MS)).await;
        } else {
            backoff_counter += 1;
        }
    }
}

/// Asynchronous Redis subscriber that listens for changes on specified keys and sends notifications through a Tokio channel.
///
/// This function establishes an asynchronous Redis connection, subscribes to the specified keys, and listens for changes.
/// It sends notifications through the provided Tokio channel, indicating the key that changed and the corresponding value (if applicable).
///
/// # Arguments
///
/// * `r` - A Redis `Client` used for establishing an asynchronous connection.
/// * `keys` - A vector of strings representing the keys to subscribe to and monitor for changes.
/// * `sender` - A Tokio `watch::Sender` used for sending notifications through a channel. The notifications include the key that changed and the corresponding value (if applicable).
///
/// # Generic Parameters
///
/// * `T` - A type that implements the `redis::FromRedisValue` trait, `std::fmt::Debug`, and is `Send + Sync`.
///
/// # Errors
///
/// Returns a `Result` with `Ok(())` on successful execution and `Err` on encountering errors during Redis operations. Curretly, this will never return
///
pub async fn redis_subscribe_recv<T>(
    r: Client,
    keys: Vec<String>,
    mut sender: watch::Sender<(String, Option<T>)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: redis::FromRedisValue + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    async fn inner<T>(
        r: &Client,
        keys: &[String],
        sender: &mut watch::Sender<(String, Option<T>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: redis::FromRedisValue + fmt::Debug + Send + Sync + Clone + 'static,
    {
        let mut conn = r.get_async_connection().await?;
        let mut sub = r.get_async_connection().await?.into_pubsub();

        for key in keys {
            let channel = format!("__keyspace@0__:{}", key);
            sub.subscribe(channel).await?;
        }

        let mut messages = sub.on_message();
        loop {
            let p = messages
                .next()
                .await
                .ok_or(TextError::new("subscrib on message failed"))?;
            let key = p.get_channel_name().replace("__keyspace@0__:", "");
            match fetch_on_set(p, &mut conn, key.clone()).await {
                SetOrDelete::Set(c) => {
                    sender.send((key, Some(c)))?;
                }
                SetOrDelete::Deleted => {
                    sender.send((key, None))?;
                }
                SetOrDelete::NoUpdate => {}
                SetOrDelete::Error(error) => return Result::Err(Box::new(error)),
            }
        }
    }

    let mut backoff_counter = 0;
    loop {
        let res = inner(&r, &keys, &mut sender).await;
        error!("redis_subscribe_recv: {res:?}, keys: {keys:?}");

        if backoff_counter > 10 {
            backoff_counter = 0;
            tokio::time::sleep(Duration::from_millis(REDIS_BACKOFF_MS)).await;
        } else {
            backoff_counter += 1;
        }
    }
}

/// TODO: Change from getting keys to using SCAN
pub async fn redis_tick_pattern<T>(
    r: Client,
    pattern: String,
    mut sender: watch::Sender<HashMap<String, T>>,
    tick: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: redis::FromRedisValue + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    async fn inner<T>(
        r: &Client,
        pattern: &str,
        sender: &mut watch::Sender<HashMap<String, T>>,
        tick: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: redis::FromRedisValue + fmt::Debug + Send + Sync + Clone + 'static,
    {
        let mut conn = r.get_async_connection().await?;
        let mut tick = tokio::time::interval(tick);

        loop {
            let keys: Vec<String> = tokio::time::timeout(
                Duration::from_millis(REDIS_QUERY_TIMEOUT_MS),
                conn.keys(pattern),
            )
            .await??;
            let result: HashMap<String, T> = fetch_keys(&keys, &mut conn).await?;
            sender.send(result)?;
            tick.tick().await;
        }
    }

    let mut backoff_counter = 0;
    loop {
        let res = inner(&r, &pattern, &mut sender, tick).await;
        error!("redis_tick_pattern: {res:?}, pattern: {pattern:?}");

        if backoff_counter > 10 {
            backoff_counter = 0;
            tokio::time::sleep(Duration::from_millis(REDIS_BACKOFF_MS)).await;
        } else {
            backoff_counter += 1;
        }
    }
}

pub async fn redis_tick_single<T>(
    r: Client,
    key: String,
    sender: watch::Sender<Option<T>>,
    tick: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: redis::FromRedisValue + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    let (tx, mut rx) = watch::channel::<HashMap<String, T>>(HashMap::new());
    let jh = tokio::task::spawn(redis_tick_recv(r, vec![key.clone()], tx, tick));
    let jh_fm = tokio::task::spawn(async move {
        loop {
            let _ = rx.changed().await;
            let value = rx.borrow().get(&key).cloned();
            let _ = sender.send(value);
        }
    });

    let _ = tokio::try_join!(jh, jh_fm)?;

    Ok(())
}

pub enum SetOrDelete<T> {
    Set(T),                   // new value
    Deleted,                  // key deleted or expired
    NoUpdate,                 // no new value
    Error(redis::RedisError), // error
}

pub async fn fetch_on_set<T>(msg: Msg, r: &mut Connection, key: String) -> SetOrDelete<T>
where
    T: redis::FromRedisValue + std::fmt::Debug,
{
    let channel = format!("__keyspace@0__:{}", key);

    if msg.get_channel_name() == channel {
        match msg.get_payload::<String>() {
            Ok(_) => {
                let cp = r.get(key).await.ok();
                match cp {
                    Some(data) => {
                        return SetOrDelete::Set(data);
                    }
                    None => {
                        return SetOrDelete::Deleted;
                    }
                }
            }
            Err(e) => {
                // redis error
                error!("Hmm, sub got error: {}", e);
                return SetOrDelete::Error(e);
            }
        }
    }

    // channel does not match, nothing happens.
    SetOrDelete::NoUpdate
}

pub async fn fetch_keys<T>(
    keys: &[String],
    r: &mut Connection,
) -> Result<HashMap<String, T>, Box<dyn std::error::Error + Send + Sync>>
where
    T: redis::FromRedisValue + std::fmt::Debug,
{
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let xs: Vec<redis::Value> =
        tokio::time::timeout(Duration::from_millis(REDIS_QUERY_TIMEOUT_MS), r.mget(keys)).await??;
    let res = xs
        .iter()
        .zip(keys)
        .flat_map(|(x, k)| {
            if let redis::Value::Nil = x {
                return None;
            }
            let res = T::from_redis_value(x);
            if let Err(e) = &res {
                error!(
                    "Could not convert {:?}, from key: {}. Got error {:?}",
                    x, k, e
                );
            };
            res.map(|v| (k.clone(), v)).ok()
        })
        .collect();

    Ok(res)
}
