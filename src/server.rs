use crate::common::{Request, Response};
use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, Result};

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use log::{debug, error};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
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
        let rt = tokio::runtime::Runtime::new().unwrap();
        // async processed stream
        rt.block_on(async {
            let listener = TcpListener::bind(addr).await.unwrap();
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let engine = self.engine.clone();
                serve(engine, tcp)
                    .await
                    .map_err(|e| error!("Error on serving client: {}", e));
            }
            // let server = listener
            //     .incoming()
            //     .map_err(|e| error!("IO error: {}", e))
            //     .for_each(move |tcp| {
            //         let engine = self.engine.clone();
            //         serve(engine, tcp).map_err(|e| error!("Error on serving client: {}", e))
            //     });
        });
        Ok(())
    }
}

async fn serve<E: KvsEngine>(engine: E, tcp: TcpStream) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    let (read_half, write_half) = tcp.into_split();
    // let inner = FramedRead::new(read_half, LengthDelimitedCodec::new());
    let mut framed_read = FramedRead::new(read_half, LengthDelimitedCodec::new());
    let mut framed_write = FramedWrite::new(write_half, LengthDelimitedCodec::new());
    loop {
        match framed_read.next().await {
            Some(res) => match res {
                Ok(req_bytes) => {
                    let req_new = serde_json::from_slice(&req_bytes)?;
                    debug!("Receive request from {}: {:?}", peer_addr, req_new);
                    match req_new {
                        Request::Get { key } => {
                            let resp_str = engine.get(key).await?.unwrap();
                            let resp = Response::Get(Some(resp_str));
                            let resp_json = serde_json::to_string(&resp)?;
                            framed_write.send(resp_json.into()).await?;
                            debug!("Response sent to {}: {:?}", peer_addr, resp);
                        }
                        Request::Set { key, value } => {
                            engine.set(key, value).await?;
                            let resp = Response::Set;
                            let resp_json = serde_json::to_string(&resp)?;
                            framed_write.send(resp_json.into()).await?;
                            debug!("Response sent to {}: {:?}", peer_addr, resp);
                        }
                        Request::Remove { key } => {
                            engine.remove(key).await?;
                            let resp = Response::Remove;
                            let resp_json = serde_json::to_string(&resp)?;
                            framed_write.send(resp_json.into()).await?;
                            debug!("Response sent to {}: {:?}", peer_addr, resp);
                        }
                    }
                }
                Err(e) => {
                    error!("Error parsing request: {}", e);
                    framed_write
                        .send(serde_json::to_string(&Response::Err(format!("{}", e)))?.into())
                        .await?;
                }
            },
            None => println!("Parsed a None!!!!"),
        }
    }
}
