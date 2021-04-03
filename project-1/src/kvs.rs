use std::collections::HashMap;

/// KVS 核心组件
///
/// 用于存储键值对形式（key-value）的数据
///
/// 使用方法：
/// ```rust
/// # use kvs::KvStore;
/// let mut kv = KvStore::new();                    // 创建对象
/// kv.set("key".to_owned(), "value".to_owned());   // 设置键值对
/// let v = kv.get("key".to_owned());               // 获取 value
/// kv.remove("key".to_owned());                    // 删除
/// ```
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// 构造函数，用来创建一个 KvStore
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// 用于获取传入 key 值对应的 value
    ///
    /// 如果 key 不存在则返回 None
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).cloned()
    }

    /// 删除 key 及其对应的 value
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }

    /// 用于设置一个键值对
    ///
    /// key 存在则会更新 value
    ///
    /// key 不存在则会创建一个新的键值对
    pub fn set(&mut self, key: String, val: String) {
        self.map.insert(key, val);
    }
}
