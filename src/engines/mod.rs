//! This module provides various key value storage engines.

use crate::Result;

/// Trait for a kv store engine
pub trait KvsEngine {
    /// set a key-value pair
    /// can overwrite existing value
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// return Ok(Some(value)) / Ok(None)
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// if key not found, return KvsError::KeyNotFound
    fn remove(&mut self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
