//! This module provides various key value storage engines.

use crate::Result;

/// Trait for a kv store engine
pub trait KvsEngine: Clone + Send + 'static {
    /// set a key-value pair
    /// can overwrite existing value
    async fn set(&self, key: String, value: String) -> Result<()>;
    /// return Ok(Some(value)) / Ok(None)
    async fn get(&self, key: String) -> Result<Option<String>>;
    /// if key not found, return KvsError::KeyNotFound
    async fn remove(&self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
