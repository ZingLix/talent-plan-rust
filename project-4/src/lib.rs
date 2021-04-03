#![deny(missing_docs)]
//! 键值对存储工具
//!
//!

#[macro_use]
extern crate log;
pub use crate::error::{KvsError, KvsErrorType, Result};
pub use engines::{KvStore, KvsEngine, SledServer};
use serde::{Deserialize, Serialize};
pub use server::KvsServer;
/// 数据库客户端
pub mod client;
pub mod engines;
/// 错误处理模块
mod error;
mod response;
/// 数据库服务端
pub mod server;
pub mod thread_pool;

/// 数据库操作
#[derive(Serialize, Deserialize, Debug)]
pub enum Operation {
    /// 设置元素
    Set {
        /// 键
        key: String,
        /// 值
        value: String,
    },
    /// 删除元素
    Remove {
        /// 键
        key: String,
    },
    /// 获取元素
    Get {
        /// 键
        key: String,
    },
}

impl Operation {
    /// 设置键值对
    pub fn set(key: &String, value: String) -> Operation {
        Operation::Set {
            key: key.clone(),
            value,
        }
    }

    /// 移除 key 对应的元素
    pub fn remove(key: &String) -> Operation {
        Operation::Remove { key: key.clone() }
    }

    /// 获取 key 对应的元素
    pub fn get(key: &String) -> Operation {
        Operation::Get { key: key.clone() }
    }
}
