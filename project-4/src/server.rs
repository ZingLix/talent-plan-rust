use crate::engines::KvsEngine;
use crate::response::Response;
use crate::thread_pool::ThreadPool;
use crate::{Operation, Result};
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

/// 用于处理数据库请求的服务器

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    thread_pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// 创建一个 KvsServer 实例
    ///
    /// 内部引擎由 engine 决定
    ///
    /// thread_pool 为 server 处理请求时所使用的线程池
    pub fn new(engine: E, thread_pool: P) -> Self {
        KvsServer {
            engine,
            thread_pool,
        }
    }

    /// 启动服务器，监听 addr
    pub fn run(self, addr: String) -> Result<()> {
        let listener = match TcpListener::bind(addr) {
            Ok(l) => l,
            Err(e) => {
                error!("{}", e);
                std::process::exit(1);
            }
        };
        info!("Server running...");
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            // 交由线程池处理请求
            self.thread_pool.spawn(move || match stream {
                Ok(stream) => Self::handle_request(engine, stream),
                Err(e) => eprint!("{}", e),
            })
        }
        Ok(())
    }

    /// set 操作（用于 bench）
    pub fn set(&self, key: String, value: String) {
        let engine = self.engine.clone();
        self.thread_pool
            .spawn(move || match engine.set(key, value) {
                Ok(_) => {}
                Err(_) => {}
            });
    }

    /// get 操作（用于 bench）
    pub fn get(&self, key: String) {
        let engine = self.engine.clone();
        self.thread_pool.spawn(move || {
            match engine.get(key) {
                Ok(_) => {}
                Err(_) => {}
            };
        });
    }

    /// 处理请求
    fn handle_request(engine: E, stream: TcpStream) {
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
                Operation::Set { key, value } => send_response(&match engine.set(key, value) {
                    Ok(_) => Response::ok_without_msg(),
                    Err(e) => Response::err(format!("{}", e)),
                }),
                Operation::Get { key } => send_response(&match engine.get(key) {
                    Ok(val) => match val {
                        Some(value) => Response::ok(value),
                        None => Response::err(String::from("Key not found")),
                    },
                    Err(_) => Response::err(String::from("Key not found")),
                }),
                Operation::Remove { key } => send_response(&match engine.remove(key) {
                    Ok(_) => Response::ok_without_msg(),
                    Err(e) => Response::err(format!("{}", e)),
                }),
            }
        }
    }
}
