use clap::{Parser, ValueEnum};
use kvs::*;
use log::{error, info, warn, LevelFilter};
use std::env::current_dir;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use std::str::FromStr;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, ValueEnum)]
enum Engine {
    kvs,
    sled,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Engine::kvs => "kvs",
            Engine::sled => "sled",
        };
        write!(f, "{}", s)
    }
}
impl FromStr for Engine {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kvs" => Ok(Engine::kvs),
            "sled" => Ok(Engine::sled),
            _ => Err(format!("'{}' is not a valid engine", s)),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "kvs-server", version)]
struct Opt {
    // 这里的 doc 就是 help 的内容， /// 是 doc, // 是注释
    /// Sets the listening address
    #[arg(
        long,
        value_name = "IP:PORT",
        default_value = DEFAULT_LISTENING_ADDRESS,
    )]
    addr: SocketAddr,

    /// Sets the storage engine
    #[arg(long, value_name = "ENGINE-NAME")]
    engine: Option<Engine>,
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    // let mut opt = Opt::from_args();
    let mut opt = Opt::parse();
    let res = current_engine().and_then(move |cur_engine| {
        if opt.engine.is_none() {
            opt.engine = cur_engine;
        }
        if cur_engine.is_some() && opt.engine != cur_engine {
            error!("wrong engine!");
            exit(1);
        }
        run(opt)
    });
    if let Err(e) = res {
        error!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", opt.addr);
    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;

    match engine {
        Engine::kvs => run_with_engine(KvStore::open(current_dir()?)?, opt.addr),
        Engine::sled => run_with_engine(SledKvsEngine::new(sled::open(current_dir()?)?), opt.addr),
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let server = KvsServer::new(engine);
    server.run(addr)
}

// 把 engine 文件里的字符串读出来
fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join("engine");
    if !engine.exists() {
        return Ok(None);
    }
    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
