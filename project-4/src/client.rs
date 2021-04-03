use crate::{response::Response, KvsErrorType, Operation, Result};

use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

/// 用于向服务器发送信息进行数据库操作的客户端
///
/// 使用方法：
///
/// ```no_run
/// # use kvs::client::KvsClient;
/// let mut client = KvsClient::connent("127.0.0.1:4000".to_string()).unwrap();
/// client.get("key".to_owned());
/// client.set("key".to_owned(), "value".to_owned());
/// client.remove("key".to_owned());
/// ```
pub struct KvsClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// 连接至地址为 addr 的服务器
    pub fn connent(addr: String) -> Result<Self> {
        let stream = match TcpStream::connect(addr) {
            Ok(l) => l,
            Err(e) => {
                error!("{}", e);
                std::process::exit(1);
            }
        };

        Ok(KvsClient {
            reader: BufReader::new(stream.try_clone()?),
            writer: BufWriter::new(stream),
        })
    }

    /// 向服务器请求 key 所对应的 value
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.send(&Operation::get(&key));
        let response = self.recv().unwrap();
        return Ok(response.msg);
    }

    /// 向服务器发送操作 op
    fn send(&mut self, op: &Operation) {
        serde_json::to_writer(&mut self.writer, op).unwrap();
        self.writer.flush().unwrap();
    }

    /// 接收信息
    fn recv(&mut self) -> Result<Response> {
        let mut stream =
            serde_json::Deserializer::from_reader(self.reader.by_ref()).into_iter::<Response>();
        while let Some(op) = stream.next() {
            let op = op.unwrap();
            return Ok(op);
        }
        Err(KvsErrorType::UnknownOperation)?
    }

    /// 向服务器发送 (key, value) 用于设置键值对
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.send(&Operation::set(&key, value));
        let response = self.recv().unwrap();
        match response.status {
            0 => Ok(()),
            _ => Err(KvsErrorType::UnknownOperation)?,
        }
    }

    /// 在服务器中移除 key 所对应的元素
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.send(&Operation::remove(&key));
        let response = self.recv().unwrap();
        match response.status {
            0 => Ok(()),
            _ => Err(KvsErrorType::KeyNotFound)?,
        }
    }
}
