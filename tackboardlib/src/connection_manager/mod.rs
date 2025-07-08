#![crate_name = "tackboardlib"]

use crate::types::errors::TackboardError;
use crate::types::{ClientRequest, ServerResponse, Topic};
use futures::prelude::*;
use futures::stream::{SplitSink, SplitStream};
use log::{debug, error};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_serde::formats::*;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

type SinkMap = Arc<Mutex<HashMap<String, Arc<Mutex<SplitSink<InConnection, ServerResponse>>>>>>;
/// Trait for pub/sub client and server connection management.
///
/// Provides methods for sending requests, handling responses, accepting connections, and tracking connected clients.
pub trait Connects {
    /// Send a request to the server and wait for a single response (synchronous, request/response).
    ///
    /// # Arguments
    ///
    /// * `request` - The request to send to the server.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::{Connects, ConnectionManager};
    /// # use tackboardlib::types::ClientRequest;
    /// # async fn example(manager: &ConnectionManager) {
    /// let response = manager.send_sync(ClientRequest::ConnectionRequest { id: "id".to_string(), client_url: "url".to_string() }).await;
    /// # }
    /// ```
    async fn send_sync(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;

    /// Send a request to the server and handle responses asynchronously via a callback.
    /// The callback is invoked for each response received.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to send to the server.
    /// * `callback` - A function to handle each response.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::{Connects, ConnectionManager};
    /// # use tackboardlib::types::ClientRequest;
    /// # async fn example(manager: &ConnectionManager) {
    /// manager.send_async(ClientRequest::ConnectionRequest { id: "id".to_string(), client_url: "url".to_string() }, |resp| async move {
    ///     println!("Received: {:?}", resp);
    /// }).await;
    /// # }
    /// ```
    async fn send_async<F, Fut>(&self, request: ClientRequest, callback: F)
    where
        F: Fn(ServerResponse) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    /// Accept a new incoming client connection and process it using the provided callback.
    /// The callback receives the sink and stream halves of the connection.
    ///
    /// # Arguments
    ///
    /// * `callback` - A function that processes the sink and stream for the connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::{Connects, ConnectionManager};
    /// # async fn example(manager: &mut ConnectionManager) {
    /// manager.accept_connections(|sink, stream| async move {
    ///     // handle sink/stream
    /// }).await.unwrap();
    /// # }
    /// ```
    async fn accept_connections<F, Fut>(&mut self, callback: F) -> Result<(), TackboardError>
    where
        F: Fn(
                Arc<Mutex<SplitSink<InConnection, ServerResponse>>>,
                Arc<Mutex<SplitStream<InConnection>>>,
            ) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    /// Get a map of currently connected clients (by ID) to their sink handles.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::{Connects, ConnectionManager};
    /// # async fn example(manager: &ConnectionManager) {
    /// let clients = manager.get_connected_clients().await;
    /// # }
    /// ```
    async fn get_connected_clients(
        &self,
    ) -> Arc<Mutex<HashMap<String, Arc<Mutex<SplitSink<InConnection, ServerResponse>>>>>>;
}

// This is the definition for the objects we are sending over wire
// These objects(structs) are held in ../types and you can modify these to whatever you want
// Though they will need to be a A->B style, Client/Server
// OutConnection from the Client
pub type OutConnection = tokio_serde::Framed<
    Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
    ServerResponse,
    ClientRequest,
    Json<ServerResponse, ClientRequest>,
>;
//InConnection to the server
pub type InConnection = tokio_serde::Framed<
    Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
    ClientRequest,
    ServerResponse,
    Json<ClientRequest, ServerResponse>,
>;
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

/// Manages server and client connections for the pub/sub system.
#[derive(Default, Clone)]
pub struct ConnectionManager {
    path: String,
    /// Server-side: TCP listener for incoming connections
    listener: Arc<Mutex<Option<TcpListener>>>,
    /// Server-side: Map of connected client sinks
    connected_clients: SinkMap,
    /// Client-side: Durable connection to the server
    durable_client_connection: Option<Arc<Mutex<OutConnection>>>,
}

impl ConnectionManager {
    /// Create a new client connection to the given server address.
    ///
    /// # Arguments
    ///
    /// * `path` - The address of the server to connect to.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::ConnectionManager;
    /// # async fn example() {
    /// let client = ConnectionManager::create_client("127.0.0.1:1234".to_string()).await.unwrap();
    /// # }
    /// ```
    pub async fn create_client(path: String) -> Result<ConnectionManager, TackboardError> {
        let socket = TcpStream::connect(&path)
            .await
            .map_err(TackboardError::Io)?;
        let length_delimited = Framed::new(socket, LengthDelimitedCodec::new());
        let framed = create_outgoing_connection(length_delimited);

        Ok(ConnectionManager {
            path,
            durable_client_connection: Some(Arc::from(Mutex::new(framed))),
            ..Default::default()
        })
    }
    /// Create a new server listener bound to the given address.
    ///
    /// # Arguments
    ///
    /// * `path` - The address to bind the server listener to.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::ConnectionManager;
    /// # async fn example() {
    /// let server = ConnectionManager::create_listener("127.0.0.1:1234".to_string()).await.unwrap();
    /// # }
    /// ```
    pub async fn create_listener(path: String) -> Result<ConnectionManager, TackboardError> {
        let listener = TcpListener::bind(&path).await.map_err(TackboardError::Io)?;

        Ok(ConnectionManager {
            path,
            connected_clients: Arc::new(Mutex::new(HashMap::new())),
            listener: Arc::new(Mutex::new(Some(listener))),
            durable_client_connection: None,
        })
    }
}

impl Connects for ConnectionManager {
    /// Send a request to the server and wait for a single response (synchronous, request/response).
    ///
    /// # Arguments
    ///
    /// * `request` - The request to send to the server.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::{Connects, ConnectionManager};
    /// # use tackboardlib::types::ClientRequest;
    /// # async fn example(manager: &ConnectionManager) {
    /// let response = manager.send_sync(ClientRequest::ConnectionRequest { id: "id".to_string(), client_url: "url".to_string() }).await;
    /// # }
    /// ```
    async fn send_sync(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
        let framed = self
            .durable_client_connection
            .as_ref()
            .ok_or(TackboardError::InvalidRequest)?;
        let mut framed = framed.lock().await;

        framed.send(request).await?;
        if let Some(Ok(response)) = framed.next().await {
            return Ok(response);
        }
        Err(TackboardError::InvalidRequest)
    }
    /// Send a request to the server and handle responses asynchronously via a callback.
    /// The callback is invoked for each response received.
    ///
    /// # Arguments
    ///
    /// * `request` - The request to send to the server.
    /// * `callback` - A function to handle each response.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::{Connects, ConnectionManager};
    /// # use tackboardlib::types::ClientRequest;
    /// # async fn example(manager: &ConnectionManager) {
    /// manager.send_async(ClientRequest::ConnectionRequest { id: "id".to_string(), client_url: "url".to_string() }, |resp| async move {
    ///     println!("Received: {:?}", resp);
    /// }).await;
    /// # }
    /// ```
    async fn send_async<F, Fut>(&self, request: ClientRequest, callback: F)
    where
        F: Fn(ServerResponse) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let framed = self.durable_client_connection.as_ref().unwrap().clone();
        // Spawn a new task to handle topic updates
        tokio::spawn(async move {
            let mut framed = framed.lock().await;

            if let Err(e) = framed.send(request).await {
                error!("Failed to send topic listen request: {e}");
                return;
            }

            while let Some(result) = framed.next().await {
                match result {
                    Ok(response) => {
                        callback(response).await;
                    }
                    Err(e) => {
                        error!("Error receiving response: {e}");
                        break;
                    }
                }
            }
        });
    }

    /// Accept a new incoming client connection and process it using the provided callback.
    /// The callback receives the sink and stream halves of the connection.
    ///
    /// # Arguments
    ///
    /// * `callback` - A function that processes the sink and stream for the connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::{Connects, ConnectionManager};
    /// # async fn example(manager: &mut ConnectionManager) {
    /// manager.accept_connections(|sink, stream| async move {
    ///     // handle sink/stream
    /// }).await.unwrap();
    /// # }
    /// ```
    async fn accept_connections<F, Fut>(&mut self, callback: F) -> Result<(), TackboardError>
    where
        F: Fn(
                Arc<Mutex<SplitSink<InConnection, ServerResponse>>>,
                Arc<Mutex<SplitStream<InConnection>>>,
            ) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (socket, _) = self
            .listener
            .lock()
            .await
            .as_mut()
            .ok_or(TackboardError::InvalidRequest)?
            .accept()
            .await
            .map_err(TackboardError::Io)?;

        tokio::spawn(async move {
            let length_delimited = Framed::new(socket, LengthDelimitedCodec::new());
            let framed: InConnection = create_incoming_connection(length_delimited);
            let (sink, stream) = framed.split();
            let sink = Arc::new(Mutex::new(sink));
            let stream = Arc::new(Mutex::new(stream));

            callback(sink, stream).await;
        });
        Ok(())
    }
    /// Get a map of currently connected clients (by ID) to their sink handles.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tackboardlib::connection_manager::{Connects, ConnectionManager};
    /// # async fn example(manager: &ConnectionManager) {
    /// let clients = manager.get_connected_clients().await;
    /// # }
    /// ```
    async fn get_connected_clients(
        &self,
    ) -> Arc<Mutex<HashMap<String, Arc<Mutex<SplitSink<InConnection, ServerResponse>>>>>> {
        self.connected_clients.clone()
    }
}
