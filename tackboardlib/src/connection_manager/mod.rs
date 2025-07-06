use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::codec::{length_delimited, Framed};
use tokio_util::codec::LengthDelimitedCodec;
use tokio_serde::formats::*;
use futures::prelude::*;
use log::error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use crate::types::{ClientRequest, ServerResponse, Topic};
use crate::types::errors::TackboardError;
pub trait Connects {

    //Client side
    async fn send(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;

    // This is a blocking function that will take updates from the server topics subscribed to
    async fn topic_sync<F, Fut>(&self, callback: F) where
        F: Fn(ServerResponse) -> Fut,
        Fut: Future<Output=()> + Send + 'static;

    // Server side
    async fn accept_connection<F, Fut>(&mut self,callback: F) -> Result<(), TackboardError>
    where         F: Fn(ClientRequest, InConnection) -> Fut + Send + 'static,
                  Fut: Future<Output=InConnection> + Send + 'static;
}

pub type OutConnection = tokio_serde::Framed<Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
ServerResponse,
ClientRequest, Json<ServerResponse, ClientRequest>>;

pub type InConnection = tokio_serde::Framed<Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
    ClientRequest,
    ServerResponse, Json<ClientRequest, ServerResponse>>;
pub fn create_incoming_connection(

    length_delimited: Framed<TcpStream, LengthDelimitedCodec>,
) -> InConnection {
    tokio_serde::Framed::new(
        length_delimited,
        Json::<ClientRequest, ServerResponse>::default(),
    )
}
pub fn create_outgoing_connection(
    length_delimited: Framed<TcpStream, LengthDelimitedCodec>,
) -> OutConnection {
    tokio_serde::Framed::new(
        length_delimited,
        Json::<ServerResponse, ClientRequest>::default(),
    )
}
#[derive(Default)]
pub struct ConnectionManager {
    path: String,
    listener: Option<TcpListener>,
    client_urls: Vec<String>,
    topics: Arc<Mutex<HashMap<String, Vec<String>>>>,
    client_connection: Option<Arc<Mutex<OutConnection>>>
}

impl ConnectionManager {

    pub async fn create_client(path: String) -> Result<ConnectionManager, TackboardError> {
        let socket = TcpStream::connect(&path).await.map_err(TackboardError::Io)?;
        let length_delimited = Framed::new(socket, LengthDelimitedCodec::new());
        let framed = create_outgoing_connection(length_delimited);

        Ok(ConnectionManager {
            path,
            client_connection: Some(Arc::from(Mutex::new(framed))),
            ..Default::default()
        })
    }
    pub async fn create_listener(path: String, topics: Arc<Mutex<HashMap<String, Vec<String>>>>) -> Result<ConnectionManager, TackboardError> {
        let listener = TcpListener::bind(&path).await.map_err(TackboardError::Io)?;
        Ok(ConnectionManager {
            path,
            listener: Some(listener),
            client_urls: vec![],
            topics,
            client_connection: None,
        })
    }
}

impl Connects for ConnectionManager {
    async fn send(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
        let framed = self.client_connection.as_ref().ok_or(TackboardError::InvalidRequest)?;
        let mut framed = framed.lock().await;

        framed.send(request).await?;
        if let Some(Ok(response)) = framed.next().await {
            return Ok(response);
        }
        Err(TackboardError::InvalidRequest)
    }

    async fn topic_sync<F, Fut>(&self, callback: F) where
        F: Fn(ServerResponse) -> Fut,
        Fut: Future<Output=()> + Send + 'static {
        let length_delimited = Framed::new(TcpStream::connect(&self.path).await.unwrap(),
                                           LengthDelimitedCodec::new());
        let mut framed: OutConnection = create_outgoing_connection(length_delimited);

        while let Some(result) = framed.next().await {
            match result {
                Ok(response) => {
                    callback(response).await;
                }
                Err(e) => {
                    panic!("{}", e)
                }
            }
        }


    }

    async fn accept_connection<F, Fut>(&mut self, callback: F) -> Result<(), TackboardError>
    where
        F: Fn(ClientRequest, InConnection) -> Fut + Send + 'static,
        Fut: Future<Output=InConnection> + Send + 'static
    {
        let (socket, _) = self.listener
            .as_mut()
            .unwrap()
            .accept()
            .await?;
        
        tokio::spawn(
            async move {
                let length_delimited = Framed::new(socket, LengthDelimitedCodec::new());
                let mut framed: InConnection = create_incoming_connection(length_delimited);
                while let Some(message) = framed.next().await {
                    match message {
                        Ok(message) => {
                            framed = callback(message, framed).await;
                        }
                        Err(e) => {
                            error!("{e}");
                        }
                    }
                }
            });
        Ok(())
    }
}
