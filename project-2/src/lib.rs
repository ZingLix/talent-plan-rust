#![deny(missing_docs)]
//! 键值对存储工具
pub use crate::error::{KvsError, KvsErrorType, Result};
pub use crate::kvs::KvStore;

/// 错误处理模块
mod error;
/// 数据库核心模块
mod kvs;
