use super::KvsEngine;
use crate::{KvsError, KvsErrorType, Result};
use sled::Db;

/// 以 sled 为核心的引擎
#[derive(Clone)]
pub struct SledServer {
    db: Db,
}

impl SledServer {
    /// 由 sled db 创建一个对象
    pub fn new(db: Db) -> Self {
        SledServer { db }
    }
}

impl KvsEngine for SledServer {
    /// 用于设置一个键值对
    ///
    /// key 存在则会更新 value
    ///
    /// key 不存在则会创建一个新的键值对
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.set(key, value.into_bytes()).unwrap();
        self.db.flush().unwrap();
        Ok(())
    }

    /// 获取 key 所对应的 value
    ///
    /// 不存在会返回 None
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let buf = self.db.get(key)?;
        match buf {
            Some(vec) => Ok(Some(String::from_utf8(vec.to_vec()).unwrap())),
            None => return Ok(None),
        }
    }

    /// 删除 key 及其对应的 value
    ///
    /// 不存在会返回 KeyNotFound Error
    fn remove(&mut self, key: String) -> Result<()> {
        self.db
            .del(key)
            .unwrap()
            .ok_or(KvsError::from(KvsErrorType::KeyNotFound))?;
        self.db.flush().unwrap();
        Ok(())
    }

    /// 获取 engine 的类型 (sled)
    fn get_type(&self) -> String {
        String::from("sled")
    }
}
