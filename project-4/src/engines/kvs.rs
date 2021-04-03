use super::KvsEngine;
use crate::error::{KvsErrorType, Result};
use crate::Operation;
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// 数据库开始压缩的阈值
const COMPACT_THERASHOLD: u64 = 1024 * 1024;

/// 数据库中数据在文件中的位置
struct Offset {
    /// 文件名
    path: PathBuf,
    /// 文件中的偏移量
    offset: u64,
    /// 条目长度
    length: u64,
}

/// 数据库状态
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct LogStatus {
    /// 对应的文件 id，随压缩次数递增
    cur_file_id: u64,
}

impl LogStatus {
    /// 创建一个初始的 LogStatus
    fn new() -> LogStatus {
        LogStatus { cur_file_id: 0 }
    }
}

/// 用于向数据库中写内容的对象
struct KvStoreWriter {
    map: Arc<SkipMap<String, Offset>>,
    path: Arc<PathBuf>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
    file_len: u64,
    log_status: LogStatus,
    could_be_compacted: u64,
}

/// 根据文件夹路径获取存有数据库状态的文件路径
fn status_filename(path: &Path) -> PathBuf {
    path.join(format!("status.json"))
}

/// 根据文件夹路径和id获取当前数据文件路径
fn log_filename(path: &Path, id: u64) -> PathBuf {
    path.join(format!("{}.log", id))
}

impl KvStoreWriter {
    /// 根据数据文件路径构建一个 writer
    fn new(path: PathBuf) -> Self {
        let status_file_name = status_filename(&path);
        let mut f: File;
        let status: LogStatus;
        // 检查数据库状态
        if !status_file_name.exists() {
            // 不存在则新建
            f = File::create(&status_file_name).unwrap();
            status = LogStatus::new();
            let serialized = serde_json::to_string(&status).unwrap();
            f.write_all(serialized.as_bytes()).unwrap();
        } else {
            // 存在则读取
            f = File::open(&status_file_name).unwrap();
            let mut content = String::new();
            f.read_to_string(&mut content).unwrap();
            status = serde_json::from_str(&content).unwrap();
        }
        let path = log_filename(&path, status.cur_file_id);
        let log_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&path)
            .unwrap();
        info!("server log path: {}", path.to_str().unwrap());
        // 从文件中读入信息构建 index map
        let (map, could_be_compacted) = build_map(&path);

        KvStoreWriter {
            map: Arc::new(map),
            path: Arc::new(path),
            reader: BufReader::new(log_file.try_clone().unwrap()),
            writer: BufWriter::new(log_file.try_clone().unwrap()),
            log_status: status,
            file_len: log_file.metadata().unwrap().len(),
            could_be_compacted,
        }
    }

    /// 删除 key 及其对应的 value
    ///
    /// 不存在会返回 KeyNotFound Error
    fn remove(&mut self, key: String) -> Result<()> {
        let op = Operation::remove(&key);
        let serialized = serde_json::to_string(&op).unwrap();
        if let Some(old_val) = self.map.get(&key) {
            self.could_be_compacted += old_val.value().length;
        }
        if let Some(_) = self.map.remove(&key) {
            self.writer.write(serialized.as_bytes())?;
            self.file_len += serialized.len() as u64;
            self.writer.flush()?;
            self.could_be_compacted += serialized.len() as u64;
        } else {
            info!("rm failed: key {} doesn't exist.", &key);
            Err(KvsErrorType::KeyNotFound)?
        }
        // 超过阈值则进行压缩
        if self.could_be_compacted > COMPACT_THERASHOLD {
            self.compact()?;
        }
        Ok(())
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
        if let Some(old_val) = self.map.get(&key) {
            self.could_be_compacted += old_val.value().length;
        }
        self.map.insert(
            key,
            Offset {
                path: (*self.path).clone(),
                offset: self.file_len,
                length: len,
            },
        );
        self.file_len += len;
        if self.could_be_compacted > COMPACT_THERASHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// 数据库压缩
    ///
    /// 会将所有信息写入到一个新文件中，并在之后使用新文件进行后续操作
    ///
    /// 为识别不同版本 数据文件，会使得数据文件 id 增加
    ///
    /// 压缩主要工作是如果最后一次是 set 操作则删除之前所有该 key 的操作，如果是 remove 则删除所有该 key 的操作
    fn compact(&mut self) -> Result<()> {
        let mut parent_path = (*self.path).clone();
        parent_path.pop();
        // 创建新文件
        let new_log_file_path = log_filename(&parent_path, self.log_status.cur_file_id + 1);
        let new_log_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&new_log_file_path)?;

        let mut writer = BufWriter::new(new_log_file.try_clone()?);
        let mut offset: u64 = 0;
        // 写入压缩后数据
        for v in (*self.map).iter() {
            self.reader.seek(SeekFrom::Start(v.value().offset))?;
            let mut content = self.reader.by_ref().take(v.value().length);
            let length = std::io::copy(&mut content, &mut writer)?;

            self.map.insert(
                v.key().clone(),
                Offset {
                    path: new_log_file_path.clone(),
                    offset,
                    length,
                },
            );
            offset += length;
        }
        writer.flush()?;

        // 更新 status
        let mut status_f = File::create(&status_filename(&parent_path))?;
        self.log_status.cur_file_id += 1;
        let serialized = serde_json::to_string(&self.log_status).unwrap();
        status_f.write_all(serialized.as_bytes())?;
        self.writer = writer;
        let reader = BufReader::new(new_log_file.try_clone()?);
        self.reader = reader;
        self.could_be_compacted = 0;
        // 删除旧文件
        fs::remove_file(log_filename(&parent_path, self.log_status.cur_file_id - 1))?;
        Ok(())
    }
}

/// 以 KvStore 为核心的引擎
pub struct KvStore {
    path: Arc<PathBuf>,
    writer: Arc<Mutex<KvStoreWriter>>,
    map: Arc<SkipMap<String, Offset>>,
}

impl Clone for KvStore {
    fn clone(&self) -> KvStore {
        KvStore {
            path: Arc::clone(&self.path),
            writer: Arc::clone(&self.writer),
            map: Arc::clone(&self.map),
        }
    }
}

impl KvsEngine for KvStore {
    /// 用于设置一个键值对
    ///
    /// key 存在则会更新 value
    ///
    /// key 不存在则会创建一个新的键值对
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)?;
        Ok(())
    }

    /// 获取 key 所对应的 value
    ///
    /// 不存在会返回 None
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.map.get(&key) {
            let file = match File::open(&offset.value().path) {
                Ok(file) => file,
                Err(_) => {
                    if let Some(offset) = self.map.get(&key) {
                        File::open(&offset.value().path)?
                    } else {
                        return Ok(None);
                    }
                }
            };
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(offset.value().offset))?;
            let op_reader = reader.by_ref().take(offset.value().length);
            if let Operation::Set { key: _, value } = serde_json::from_reader(op_reader).unwrap() {
                return Ok(Some(value));
            } else {
                Err(KvsErrorType::SerdeError)?
            }
        }
        Ok(None)
    }

    /// 删除 key 及其对应的 value
    ///
    /// 不存在会返回 KeyNotFound Error
    fn remove(&self, key: String) -> Result<()> {
        info!("rm key {}", &key);
        self.writer.lock().unwrap().remove(key)?;
        Ok(())
    }

    /// 获取 engine 的类型 (kvs)
    fn get_type(&self) -> String {
        String::from("kvs")
    }
}

impl KvStore {
    /// 构造函数，用来创建一个存储位置为 path 的 KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path).unwrap();
        let writer = KvStoreWriter::new(path);
        Ok(KvStore {
            path: Arc::clone(&writer.path),
            map: Arc::clone(&writer.map),
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

/// 从路径为 path 的数据文件中构造 index map，同时计算可压缩的大小
fn build_map(path: &PathBuf) -> (SkipMap<String, Offset>, u64) {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file.try_clone().unwrap());
    let mut offset = 0;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Operation>();
    let map = SkipMap::new();
    let mut uncompacted = 0;
    while let Some(op) = stream.next() {
        let end_offset = stream.byte_offset() as u64;
        let length = end_offset - offset;
        match op.unwrap() {
            Operation::Set { key, .. } => {
                if map.contains_key(&key) {
                    uncompacted += length;
                }
                map.insert(
                    key,
                    Offset {
                        path: path.clone(),
                        offset,
                        length,
                    },
                );
            }
            Operation::Remove { key } => {
                if let Some(last_offset) = map.remove(&key) {
                    uncompacted += last_offset.value().length;
                }
                uncompacted += length;
            }
            _ => unreachable!(),
        }
        offset = end_offset
    }
    (map, uncompacted)
}
