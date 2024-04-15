//! # Redis State
//!
//! `Redis State` is a library that is used to communicate state via applications using redis. The library will 
//! keep the local state eventually consistent with the state in redis. All values are stored as json


mod redis_state;
use redis_state::*;

pub struct RedisConnection {
    write: RedisState,
    read: RedisState,

}




#[cfg(test)]
mod tests {
    use std::fmt::Display;
    use anyhow::Result;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use super::redis_state::*;
    use rand::Rng;

    #[test]
    fn hacking() {
        
    }


}
