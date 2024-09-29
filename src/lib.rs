#![deny(missing_docs)]
//! A simple key/value store.

/// pub use 一下数据结构
pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;

/// mod 标记一下文件
mod client;
mod common;
mod engines;
mod error;
mod server;
