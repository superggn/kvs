use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, Result};

use log::{debug, error};
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
// use std::net::{TcpListener, TcpStream, ToSocketAddrs};

// use crate::common::{Request, Response};
// use crate::{KvsEngine, KvsError, Result};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_serde::formats::Json;
use tokio_serde::{SymmetricallyFramed, SymmetricallyFramedSink, SymmetricallyFramedStream};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// kv store server
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// create a server from a given engine
    /// engine: a struct which implemented KvsEngine trait
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }
    /// run server on given SocketAddr
    pub fn run<A: ToSocketAddrs>(self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream_res in listener.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream_res {
                Ok(stream) => {
                    if let Err(e) = serve(engine, stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            })
        }
        Ok(())
    }
}

fn serve<E: KvsEngine>(engine: E, tcp: TcpStream) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    let reader = BufReader::new(&tcp);
    let mut writer = BufWriter::new(&tcp);
    let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

    // 把拿到的 response 写到 tcp stream writer 里
    macro_rules! send_resp {
        ($resp:expr) => {{
            let resp = $resp;
            serde_json::to_writer(&mut writer, &resp)?;
            writer.flush()?;
            debug!("Response sent to {}: {:?}", peer_addr, resp);
        }};
    }

    for req in req_reader {
        let req = req?;
        debug!("Receive request from {}: {:?}", peer_addr, req);
        match req {
            Request::Get { key } => {
                send_resp!(match engine.get(key) {
                    Ok(value) => {
                        GetResponse::Ok(value)
                    }
                    Err(e) => {
                        GetResponse::Err(format!("{}", e))
                    }
                })
            }
            Request::Set { key, value } => {
                send_resp!(match engine.set(key, value) {
                    Ok(_) => {
                        SetResponse::Ok(())
                    }
                    Err(e) => SetResponse::Err(format!("{}", e)),
                })
            }
            Request::Remove { key } => {
                send_resp!(match engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                })
            }
        }
    }

    Ok(())
}
