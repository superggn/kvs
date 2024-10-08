use clap::{Parser, Subcommand};
use kvs::{KvsClient, Result};
use std::net::SocketAddr;
use std::process::exit;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const ADDRESS_FORMAT: &str = "IP:PORT";

#[derive(Parser, Debug)]
#[command(name = "kvs-client", version)]
struct Opt {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Get {
        /// a string key
        #[arg(name = "key")]
        key: String,
        /// server addr:ip
        #[arg(
            long,
            name = "addr",
            value_name = ADDRESS_FORMAT,
            default_value = DEFAULT_LISTENING_ADDRESS
        )]
        addr: SocketAddr,
    },
    Set {
        /// a string key
        key: String,
        /// value of this key
        #[arg(name = "value")]
        value: String,
        /// server addr:ip
        #[arg(
            long,
            name = "addr",
            value_name = ADDRESS_FORMAT,
            default_value = DEFAULT_LISTENING_ADDRESS
        )]
        addr: SocketAddr,
    },
    #[command(name = "rm")]
    Remove {
        /// a string key
        key: String,
        /// server addr:ip
        #[arg(
            long,
            name = "addr",
            value_name = ADDRESS_FORMAT,
            default_value = DEFAULT_LISTENING_ADDRESS
        )]
        addr: SocketAddr,
    },
}

fn main() {
    let opt = Opt::parse();
    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    match opt.command {
        Command::Get { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, value, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        Command::Remove { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }
    Ok(())
}
