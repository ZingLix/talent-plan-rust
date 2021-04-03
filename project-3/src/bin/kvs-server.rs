use clap::{App, Arg};
use kvs::engines::{KvStore, SledServer};
use kvs::server::KvsServer;
use kvs::{KvsErrorType, Result};
#[macro_use]
extern crate log;
use log::LevelFilter;

use std::io::prelude::*;

fn check_engine(engine_type: &String) -> Result<()> {
    let engine_cfg_path = std::env::current_dir()?.join("server.cfg");
    if !engine_cfg_path.exists() {
        let mut engine_cfg_file = std::fs::File::create(engine_cfg_path)?;
        engine_cfg_file.write_all(engine_type.as_bytes())?;
        return Ok(());
    }
    let mut engine_cfg_file = std::fs::File::open(engine_cfg_path)?;
    let mut content = String::new();
    engine_cfg_file.read_to_string(&mut content)?;
    if &content != engine_type {
        return Err(KvsErrorType::Other)?;
    }
    Ok(())
}
fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    let matches = App::new("kvs-server")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(Arg::from_usage("--addr [ADDR] 'IP address'"))
        .arg(Arg::from_usage("--engine [ENGINE] 'IP address'"))
        .get_matches();
    let mut addr = String::from("127.0.0.1:4000");
    if let Some(v) = matches.value_of("addr") {
        addr = v.to_string();
    }
    let mut engine = String::from("sled");
    if let Some(v) = matches.value_of("engine") {
        engine = v.to_string();
    }
    match check_engine(&engine) {
        Ok(_) => (),
        Err(_) => {
            eprintln!("Wrong engine.");
            std::process::exit(1);
        }
    }
    info!("Server engine: {}", engine);
    info!("Server address: {}", addr);
    match engine.as_ref() {
        "kvs" => {
            KvsServer::new(KvStore::open(std::env::current_dir().unwrap().join("kvs")).unwrap())
                .run(addr)
                .unwrap()
        }

        "sled" => KvsServer::new(SledServer::new(
            match sled::Db::start_default(std::env::current_dir().unwrap().join("sled")) {
                Ok(db) => db,
                Err(_) => {
                    error!("Sled created failed.");
                    std::process::exit(1);
                }
            },
        ))
        .run(addr)
        .unwrap(),
        _ => {
            eprintln!("Invalid engine.");
            std::process::exit(1);
        }
    }
}
