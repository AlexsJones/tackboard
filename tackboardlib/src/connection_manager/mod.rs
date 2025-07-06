use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::formats::*;
use futures::prelude::*;
use tokio::net::{TcpListener, TcpStream};
use crate::types::{ClientRequest, ServerResponse, Topic};
use crate::types::errors::TackboardError;
pub trait Connects {

    //Client side
    fn connect(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;
    fn subscribe(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;
    fn publish(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;

    // Server side
    async fn accept_connection<F, Fut>(&self,callback: F) where F: Fn(String) -> Fut + Send + 'static,
        Fut: Future<Output=()> + Send + 'static;
}

type OutConnection = tokio_serde::Framed<Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
ServerResponse,
ClientRequest, Json<ServerResponse, ClientRequest>>;

type InConnection = tokio_serde::Framed<Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
    ClientRequest,
    ServerResponse, Json<ClientRequest, ClientRequest>>;

#[derive(Default)]
pub struct ConnectionManager {
    path: String,
    listener: Option<TcpListener>,
    client_urls: Vec<String>,
    topic_listens: Vec<(Topic, String)>,

}

impl ConnectionManager {
    pub async fn create_listener(path: String) -> Result<ConnectionManager, TackboardError> {
        let listener = TcpListener::bind(&path).await.map_err(TackboardError::Io)?;
        Ok(ConnectionManager {
            path,
            listener: Some(listener),
            client_urls: vec![],
            topic_listens: vec![],
        })
    }
}

impl Connects for ConnectionManager {
    fn connect(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
        todo!()
    }

    fn subscribe(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
        todo!()
    }

    fn publish(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
        todo!()
    }
    

    async fn accept_connection<F, Fut>(&self, callback: F)
    where
        F: Fn(String) -> Fut + Send + 'static,
        Fut: Future<Output=()> + Send + 'static
    {
        todo!()
    }
}