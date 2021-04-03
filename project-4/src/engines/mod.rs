//! KvsServer Engine 模块

use super::Result;

/// 定义了可作为 KvsServer 的 Engine Trait
///
/// 使用方法：
/// ```rust
/// # use kvs::{KvStore, KvsEngine};
/// # let kv: KvStore = KvStore::open("dir").unwrap();
/// kv.set("key".to_owned(), "value".to_owned());   // 设置键值对
/// let v = kv.get("key".to_owned());               // 获取 value
/// kv.remove("key".to_owned());                    // 删除
/// ```
pub trait KvsEngine: Clone + Send + 'static {
    /// 用于设置一个键值对
    ///
    /// key 存在则会更新 value
    ///
    /// key 不存在则会创建一个新的键值对
    fn set(&self, key: String, value: String) -> Result<()>;

    /// 获取 key 所对应的 value
    ///
    /// 不存在会返回 None
    fn get(&self, key: String) -> Result<Option<String>>;

    /// 删除 key 及其对应的 value
    ///
    /// 不存在会返回 KeyNotFound Error
    fn remove(&self, key: String) -> Result<()>;

    /// 获取 engine 的类型 (kvs || sled)
    fn get_type(&self) -> String;
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledServer;
