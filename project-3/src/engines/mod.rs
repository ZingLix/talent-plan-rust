//! KvsServer Engine 模块

use super::Result;

/// 定义了可作为 KvsServer 的 Engine Trait
pub trait KvsEngine {
    /// 用于设置一个键值对
    ///
    /// key 存在则会更新 value
    ///
    /// key 不存在则会创建一个新的键值对
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// 获取 key 所对应的 value
    ///
    /// 不存在会返回 None
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// 删除 key 及其对应的 value
    ///
    /// 不存在会返回 KeyNotFound Error
    fn remove(&mut self, key: String) -> Result<()>;
    /// 获取 engine 的类型 (kvs || sled)
    fn get_type(&self) -> String;
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledServer;
