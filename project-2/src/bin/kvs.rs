extern crate clap;

use clap::{App, Arg, SubCommand};

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            // kvs set <KEY> <VALUE>
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("value").required(true)),
        )
        .subcommand(
            // kvs get <KEY>
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("key").required(true)),
        )
        .subcommand(
            // kvs rm <KEY>
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("key").required(true)),
        )
        .get_matches();
    let mut kv = kvs::KvStore::open(std::env::current_dir().unwrap()).unwrap();
    match matches.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("key").expect("缺少参数 Key");
            let value = matches.value_of("value").expect("缺少参数 Value");
            kv.set(key.to_string(), value.to_string()).unwrap();
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("key").expect("缺少参数 Key");
            if let Some(value) = kv.get(key.to_string()).unwrap() {
                println!("{}", value)
            } else {
                println!("Key not found")
            }
        }
        ("rm", Some(matches)) => {
            let key = matches.value_of("key").expect("缺少参数 Key");
            match kv.remove(key.to_string()) {
                Ok(()) => {}
                Err(_) => {
                    println!("Key not found");
                    std::process::exit(1);
                }
            }
        }
        _ => unreachable!(),
    }
}
