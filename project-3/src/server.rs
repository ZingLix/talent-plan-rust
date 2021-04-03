use crate::engines::KvsEngine;
use crate::response::Response;
use crate::{Operation, Result};
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

/// 用于处理数据库请求的服务器
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// 创建一个 KvsServer 实例
    ///
    /// 内部引擎由 engine 决定
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    /// 启动服务器，监听 addr
    pub fn run(mut self, addr: String) -> Result<()> {
        let listener = match TcpListener::bind(addr) {
            Ok(l) => l,
            Err(e) => {
                error!("{}", e);
                std::process::exit(1);
            }
        };
        info!("Server running...");
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => self.handle_request(stream),
                Err(e) => eprint!("{}", e),
            }
        }
        Ok(())
    }

    /// 处理请求
    fn handle_request(&mut self, stream: TcpStream) {
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let msg_reader = serde_json::Deserializer::from_reader(reader).into_iter::<Operation>();

        let mut send_response = |response: &Response| {
            serde_json::to_writer(&mut writer, response).unwrap();
            writer.flush().unwrap();
        };

        for msg in msg_reader {
            let msg = msg.unwrap().into();
            match msg {
                Operation::Set { key, value } => {
                    send_response(&match self.engine.set(key, value) {
                        Ok(_) => Response::ok_without_msg(),
                        Err(e) => Response::err(format!("{}", e)),
                    })
                }
                Operation::Get { key } => send_response(&match self.engine.get(key) {
                    Ok(val) => match val {
                        Some(value) => Response::ok(value),
                        None => Response::err(String::from("Key not found")),
                    },
                    Err(_) => Response::err(String::from("Key not found")),
                }),
                Operation::Remove { key } => send_response(&match self.engine.remove(key) {
                    Ok(_) => Response::ok_without_msg(),
                    Err(e) => Response::err(format!("{}", e)),
                }),
            }
        }
    }
}
