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
    match matches.subcommand() {
        (str, Some(_matches)) if ["set", "get", "rm"].contains(&str) => {
            eprint!("unimplemented");
            std::process::exit(1);
        }
        _ => unreachable!(),
    }
}
