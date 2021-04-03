use crate::error::{KvsErrorType, Result};
use crate::Operation;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// 以 KvStore 为核心的引擎
///
/// 使用方法：
/// ```rust
/// # use kvs::{KvStore, KvsEngine};
/// let mut kv = KvStore::open("dir").unwrap();
/// kv.set("key".to_owned(), "value".to_owned());   // 设置键值对
/// let v = kv.get("key".to_owned());               // 获取 value
/// kv.remove("key".to_owned());                    // 删除
/// ```
pub struct KvStore {
    map: HashMap<String, Offset>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
    file_len: u64,
    could_be_compacted: u64,
    log_status: LogStatus,
    path: PathBuf,
}

/// 数据库开始压缩的阈值
const COMPACT_THERASHOLD: u64 = 1024 * 1024;

/// 数据库中数据在文件中的位置
struct Offset {
    /// 文件中的偏移量
    offset: u64,
    /// 条目长度
    length: u64,
}

/// 数据库状态
#[derive(Serialize, Deserialize, Debug)]
struct LogStatus {
    cur_file_id: u64,
}

impl LogStatus {
    /// 创建一个初始的 LogStatus
    fn new() -> LogStatus {
        LogStatus { cur_file_id: 0 }
    }
}

impl KvStore {
    /// 构造函数，用来创建一个存储位置为 path 的 KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path).unwrap();
        let status_file_name = status_filename(&path);
        let mut f: File;
        let status: LogStatus;
        if !status_file_name.exists() {
            f = File::create(&status_file_name)?;
            status = LogStatus::new();
            let serialized = serde_json::to_string(&status).unwrap();
            f.write_all(serialized.as_bytes())?;
        } else {
            f = File::open(&status_file_name)?;
            let mut content = String::new();
            f.read_to_string(&mut content)?;
            status = serde_json::from_str(&content).unwrap();
        }
        let log_path = log_filename(&path, status.cur_file_id);
        let log_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&log_path)?;
        let reader = BufReader::new(log_file.try_clone()?);
        let (map, uncompacted) = build_map(reader);

        Ok(KvStore {
            map,
            reader: BufReader::new(log_file.try_clone()?),
            file_len: log_file.metadata().unwrap().len(),
            writer: BufWriter::new(log_file),
            could_be_compacted: uncompacted,
            log_status: status,
            path,
        })
    }

    /// 数据库压缩
    ///
    /// 会将所有信息写入到一个新文件中，并在之后使用新文件进行后续操作
    ///
    /// 为识别不同版本 数据文件，会使得数据文件 id 增加
    ///
    /// 压缩主要工作是如果最后一次是 set 操作则删除之前所有该 key 的操作，如果是 remove 则删除所有该 key 的操作
    fn compact(&mut self) -> Result<()> {
        let new_log_file_path = log_filename(&self.path, self.log_status.cur_file_id + 1);
        let new_log_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&new_log_file_path)?;

        let mut writer = BufWriter::new(new_log_file.try_clone()?);
        let mut offset: u64 = 0;
        for v in self.map.values_mut() {
            self.reader.seek(SeekFrom::Start(v.offset))?;
            let mut content = self.reader.by_ref().take(v.length);
            let length = std::io::copy(&mut content, &mut writer)?;
            *v = Offset { offset, length };
            offset += length;
        }
        writer.flush()?;
        let mut status_f = File::create(&status_filename(&self.path))?;
        self.log_status.cur_file_id += 1;
        let serialized = serde_json::to_string(&self.log_status).unwrap();
        status_f.write_all(serialized.as_bytes())?;
        self.writer = writer;
        let reader = BufReader::new(new_log_file.try_clone()?);
        self.reader = reader;
        self.could_be_compacted = 0;
        fs::remove_file(log_filename(&self.path, self.log_status.cur_file_id - 1))?;
        Ok(())
    }
}

use super::KvsEngine;

impl KvsEngine for KvStore {
    /// 用于获取传入 key 值对应的 value
    ///
    /// 如果 key 不存在则返回 None
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.map.get(&key) {
            self.reader.seek(SeekFrom::Start(offset.offset))?;
            let op_reader = self.reader.by_ref().take(offset.length);
            if let Operation::Set { key: _, value } = serde_json::from_reader(op_reader).unwrap() {
                return Ok(Some(value));
            } else {
                return Err(KvsErrorType::UnknownOperation)?;
            }
        }
        Ok(None)

        //Ok(None)
        //return Ok(None);
        //self.map.get(&key).cloned()
    }

    /// 删除 key 及其对应的 value
    fn remove(&mut self, key: String) -> Result<()> {
        let op = Operation::remove(&key);
        let serialized = serde_json::to_string(&op).unwrap();
        if let Some(_) = self.map.remove(&key) {
            self.writer.write(serialized.as_bytes())?;
            self.file_len += serialized.len() as u64;
            self.writer.flush()?;
            Ok(())
        } else {
            return Err(KvsErrorType::KeyNotFound)?;
        }
    }

    /// 用于设置一个键值对
    ///
    /// key 存在则会更新 value
    ///
    /// key 不存在则会创建一个新的键值对
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::set(&key, value);
        let serialized = serde_json::to_string(&op).unwrap();

        let len = serialized.len() as u64;
        self.writer.write(serialized.as_bytes())?;
        self.writer.flush()?;
        if let Some(old_val) = self.map.insert(
            key,
            Offset {
                offset: self.file_len,
                length: len,
            },
        ) {
            self.could_be_compacted += old_val.length;
            if self.could_be_compacted > COMPACT_THERASHOLD {
                self.compact()?;
            }
        }
        self.file_len += len;
        Ok(())
        //self.map.insert(key, val);
    }

    /// 获取 engine 的类型 (kvs)
    fn get_type(&self) -> String {
        String::from("kvs")
    }
}

/// 根据文件夹路径获取存有数据库状态的文件路径
fn status_filename(path: &Path) -> PathBuf {
    path.join(format!("status.json"))
}

/// 根据文件夹路径和id获取当前数据文件路径
fn log_filename(path: &Path, id: u64) -> PathBuf {
    path.join(format!("{}.log", id))
}

/// 从路径为 path 的数据文件中构造 index map，同时计算可压缩的大小
fn build_map(reader: BufReader<File>) -> (HashMap<String, Offset>, u64) {
    let mut offset = 0;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Operation>();
    let mut map = HashMap::new();
    let mut uncompacted = 0;
    while let Some(op) = stream.next() {
        let end_offset = stream.byte_offset() as u64;
        let length = end_offset - offset;
        match op.unwrap() {
            Operation::Set { key, .. } => {
                if let Some(last_offset) = map.insert(key, Offset { offset, length }) {
                    uncompacted += last_offset.length;
                }
            }
            Operation::Remove { key } => {
                if let Some(last_offset) = map.remove(&key) {
                    uncompacted += last_offset.length;
                }
                uncompacted += length;
            }
            _ => unreachable!(),
        }
        offset = end_offset
    }
    (map, uncompacted)
}
