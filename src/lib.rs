use std::any::Any;
use std::collections::HashMap;
use std::fmt::format;

use anyhow::{anyhow, Context, Result};
use redis::AsyncCommands;
use redis::{Client, Msg, RedisError};

use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;

// mod redis_comm;

#[derive(Debug)]
pub struct RedisState {
    pub state: serde_json::Value,
    pub keys: Vec<RedisKey>,
}

impl RedisState {
    pub fn new(keys: Vec<RedisKey>) -> Self {
        let state = keys
            .iter()
            .filter(|sk| sk.initial.is_some() && !sk.key_type.is_pattern())
            .fold(Value::Object(serde_json::Map::new()), |mut json, key| {
                let _ = RedisState::update_key(&mut json, &key.key, key.initial.clone().unwrap());
                json
            });

        RedisState { state, keys }
    }

    pub fn insert_or_update<T: serde::Serialize>(&mut self, key: &str, value: T) -> Result<bool> {
        let value = serde_json::to_value(value)?;
        self.insert_or_update_value(key, value)
    }

    pub fn insert_or_update_value(&mut self, key: &str, value: serde_json::Value) -> Result<bool> {
        RedisState::update_key(&mut self.state, key, value)
    }

    pub fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<T> {
        let v = self
            .get_value(key)
            .ok_or(anyhow!("Key '{key}' is not found"))?;
        Ok(serde_json::from_value(v.clone())?)
    }

    pub fn get_redis_key(&self, key: &str) -> Option<&RedisKey> {
        self.keys.iter().find(|k| k.key == key)
    }

    pub fn get_value<'a>(&'a self, key: &str) -> Option<&'a Value> {
        key.split('/')
            .try_fold(&self.state, |target, token| match target {
                Value::Object(map) => map.get(&token as &str),
                _ => None,
            })
    }

    fn update_key(
        state: &mut serde_json::Value,
        key: &str,
        value: serde_json::Value,
    ) -> Result<bool> {
        // from https://docs.rs/serde_json/latest/src/serde_json/value/mod.rs.html#834-850
        // but no arrays
        let current_value = key
            .split('/')
            .try_fold(state, |target, token| match target {
                Value::Object(map) => {
                    if !map.contains_key(&token as &str) {
                        map.insert(token.to_string(), Value::Object(serde_json::Map::new()));
                    }
                    map.get_mut(&token as &str)
                }
                _ => None,
            });

        current_value
            .map(|v| {
                let old = std::mem::replace(v, value.clone());
                value != old
            })
            .context(format!("The key: '{key}' is not correct"))
    }
}

#[derive(Debug, Clone)]
pub struct RedisKey {
    pub key: String,
    pub key_type: KeyType,
    pub initial: Option<serde_json::Value>,
}

impl RedisKey {
    pub fn new(key: &str, key_type: KeyType, initial: Option<serde_json::Value>) -> Self {
        RedisKey {
            key: key.to_string(),
            key_type,
            initial,
        }
    }
    pub fn get_state_key(&self) -> String {
        match &self.key_type {
            KeyType::Map(x) => format!("{}/{x}", self.key),
            _ => self.key.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum KeyType {
    Key,
    List,
    Pattern,
    Map(String),
}

impl KeyType {
    pub fn is_key(&self) -> bool {
        self == &KeyType::Key
    }
    pub fn is_list(&self) -> bool {
        self == &KeyType::List
    }
    pub fn is_pattern(&self) -> bool {
        self == &KeyType::Pattern
    }
    pub fn is_map(&self) -> bool {
        matches!(self, KeyType::Map(_))
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use rand::Rng;

    use super::*;

    #[test]
    fn hacking() {
        let k1 = RedisKey::new("a/b/c", KeyType::Key, Some(json!("hej")));
        let k2 = RedisKey::new("a/b/d", KeyType::Key, Some(json!(1)));
        let k3 = RedisKey::new("b/c", KeyType::Map("nej".to_string()), Some(json!(true)));

        let mut state = RedisState::new(vec![k1, k2, k3]);
        println!("{}", serde_json::to_string_pretty(&state.state).unwrap());

        let keys = [
            "t1/t2/a",
            "t1/t2/t3/t4/t5/t6/b",
            "l1/l2/l3/l4/l5/l6/b",
            "l1/c",
        ];

        let mut rng = rand::thread_rng();

        let time = std::time::Instant::now();
        for _ in 0..100000 {
            let n: u8 = rng.gen();
            let i = rng.gen_range(0..keys.len() - 1);
            let key = keys[i];
            state.insert_or_update_value(key, json!(n)).unwrap();
        }
        let duration = std::time::Instant::now() - time;
        println!("Duration: {}ms", duration.as_millis());
        println!("{}", serde_json::to_string_pretty(&state.state).unwrap());
        panic!("hej")
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Kalle {
        a: usize,
        b: bool,
    }

    impl Display for Kalle {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "(Kalle: {}, {})", self.a, self.b)
        }
    }

    #[test]
    fn inserting() {
        let mut state = RedisState::new(vec![
            RedisKey::new("a/b/a", KeyType::Key, Some(json!(1))),
            RedisKey::new("a/b/b", KeyType::Key, Some(json!(false))),
            RedisKey::new("a/a/a", KeyType::Key, Some(json!("hej"))),
            RedisKey::new("a/a/b", KeyType::Key, Some(json!(false))),
            RedisKey::new("a/c/a", KeyType::Key, Some(json!(1))),
        ]);

        let k = Kalle { a: 10, b: false };
        println!("first");
        let res = state.insert_or_update("a/kalle", k);
        match res {
            Ok(k) => {
                assert!(k);
                println!("{k}")
            }
            Err(k) => panic!("{k}"),
        }

        println!("change");
        let k = Kalle { a: 11, b: true };
        let res = state.insert_or_update("a/kalle", k);
        match res {
            Ok(k) => {
                assert!(k);
                println!("{k}")
            }
            Err(k) => panic!("{k}"),
        }
        let k1: Result<Kalle> = state.get("a/kalle");
        match k1 {
            Ok(k) => println!("{k}"),
            Err(k) => println!("{k}"),
        }

        println!("the same");
        let k = Kalle { a: 11, b: true };
        let res = state.insert_or_update("a/kalle", k);
        match res {
            Ok(k) => {
                assert!(!k);
                println!("{k}")
            }
            Err(k) => panic!("{k}"),
        }

        let k = Kalle { a: 10, b: false };
        println!("bad");
        let res = state.insert_or_update("a/b/a/kalle", k);
        match res {
            Ok(k) => panic!("Should be an error"),
            Err(k) => println!("{k}"),
        }
    }

    #[test]
    fn structing() {
        let state = RedisState::new(vec![
            RedisKey::new("a/b/a", KeyType::Key, Some(json!(1))),
            RedisKey::new("a/b/b", KeyType::Key, Some(json!(false))),
            RedisKey::new("a/a/a", KeyType::Key, Some(json!("hej"))),
            RedisKey::new("a/a/b", KeyType::Key, Some(json!(false))),
            RedisKey::new("a/c/a", KeyType::Key, Some(json!(1))),
        ]);

        let k1: Result<Kalle> = state.get("a/b");
        let k2: Result<Kalle> = state.get("a/a");
        let k3: Result<Kalle> = state.get("a/c");
        let k4: Result<Kalle> = state.get("no key /// hej");

        match k1 {
            Ok(k) => println!("{k}"),
            Err(k) => println!("{k}"),
        }
        match k2 {
            Ok(k) => println!("{k}"),
            Err(k) => println!("{k}"),
        }
        match k3 {
            Ok(k) => println!("{k}"),
            Err(k) => println!("{k}"),
        }
        match k4 {
            Ok(k) => println!("{k}"),
            Err(k) => println!("{k}"),
        }
    }
}
