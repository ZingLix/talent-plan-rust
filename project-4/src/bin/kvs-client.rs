extern crate clap;

use clap::{App, Arg, SubCommand};
use log::LevelFilter;

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let matches = App::new("kvs-client")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(Arg::from_usage("--addr [ADDR] 'IP address'"))
        .subcommand(
            // kvs set <KEY> <VALUE>
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("value").required(true))
                .arg(Arg::from_usage("--addr [ADDR] 'IP address'")),
        )
        .subcommand(
            // kvs get <KEY>
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::from_usage("--addr [ADDR] 'IP address'")),
        )
        .subcommand(
            // kvs rm <KEY>
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::from_usage("--addr [ADDR] 'IP address'")),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("key").expect("缺少参数 Key");
            let value = matches.value_of("value").expect("缺少参数 Value");
            let mut addr = String::from("127.0.0.1:4000");
            if let Some(v) = matches.value_of("addr") {
                addr = v.to_string();
            }

            let mut client = kvs::client::KvsClient::connent(addr).unwrap();
            client.set(key.to_string(), value.to_string()).unwrap();
        }
        ("get", Some(matches)) => {
            let mut addr = String::from("127.0.0.1:4000");
            if let Some(v) = matches.value_of("addr") {
                addr = v.to_string();
            }
            let mut client = kvs::client::KvsClient::connent(addr).unwrap();
            let key = matches.value_of("key").expect("缺少参数 Key");
            if let Some(value) = client.get(key.to_string()).unwrap() {
                println!("{}", value)
            } else {
                println!("Key not found")
            }
        }
        ("rm", Some(matches)) => {
            let mut addr = String::from("127.0.0.1:4000");
            if let Some(v) = matches.value_of("addr") {
                addr = v.to_string();
            }
            let mut client = kvs::client::KvsClient::connent(addr).unwrap();
            let key = matches.value_of("key").expect("缺少参数 Key");
            match client.remove(key.to_string()) {
                Ok(()) => {}
                Err(_) => {
                    eprintln!("Key not found");
                    std::process::exit(1);
                }
            }
        }
        _ => unreachable!(),
    }
}
