use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::codec::Framed;
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
    async fn connect(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;
    fn subscribe(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;
    fn publish(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;

    // Server side
    async fn accept_connection<F, Fut>(&mut self,callback: F) -> Result<(), TackboardError>
    where F: Fn(ClientRequest, &InConnection) -> Fut + Send + 'static,
        Fut: Future<Output=()> + Send + 'static;
}

type OutConnection = tokio_serde::Framed<Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
ServerResponse,
ClientRequest, Json<ServerResponse, ClientRequest>>;

type InConnection = tokio_serde::Framed<Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
    ClientRequest,
    ServerResponse, Json<ClientRequest, ServerResponse>>;
fn create_incoming_connection(

    length_delimited: Framed<TcpStream, LengthDelimitedCodec>,
) -> InConnection {
    tokio_serde::Framed::new(
        length_delimited,
        Json::<ClientRequest, ServerResponse>::default(),
    )
}
fn create_outgoing_connection(
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
    topics: Arc<Mutex<HashMap<String, Vec<String>>>>
}

impl ConnectionManager {
    pub async fn create_listener(path: String, topics: Arc<Mutex<HashMap<String, Vec<String>>>>) -> Result<ConnectionManager, TackboardError> {
        let listener = TcpListener::bind(&path).await.map_err(TackboardError::Io)?;
        Ok(ConnectionManager {
            path,
            listener: Some(listener),
            client_urls: vec![],
            topics,
        })
    }
}

impl Connects for ConnectionManager {
    async fn connect(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
        todo!()
    }

    fn subscribe(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
        todo!()
    }

    fn publish(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
        todo!()
    }


    async fn accept_connection<F, Fut>(&mut self, callback: F) -> Result<(), TackboardError>
    where
        F: Fn(ClientRequest, &InConnection) -> Fut + Send + 'static,
        Fut: Future<Output=()> + Send + 'static
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
                            callback(message, &framed).await;
                            // framed.send(AddMessageResponse {
                            //     // Callback from the server to the client
                            //     message: "OK".to_string()
                            // }).await.expect("something went wrong!");
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