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
pub trait Connects {
    //Client side
    async fn send(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;

    // This is a blocking function that will take updates from the server topics subscribed to
    async fn send_with_callback<F, Fut>(&self, request: ClientRequest, callback: F)
    where
        F: Fn(ServerResponse) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    // Server side
    async fn accept_connections<F, Fut>(&mut self, callback: F) -> Result<(), TackboardError>
    where
        F: Fn(
                Arc<Mutex<SplitSink<InConnection, ServerResponse>>>,
                Arc<Mutex<SplitStream<InConnection>>>,
            ) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = ()> + Send + 'static;
}

pub type OutConnection = tokio_serde::Framed<
    Framed<tokio::net::TcpStream, LengthDelimitedCodec>,
    ServerResponse,
    ClientRequest,
    Json<ServerResponse, ClientRequest>,
>;

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

type SinkMap = Arc<Mutex<HashMap<String, Arc<Mutex<SplitSink<InConnection, ServerResponse>>>>>>;
#[derive(Default, Clone)]
pub struct ConnectionManager {
    path: String,
    // Server specific
    listener: Arc<Mutex<Option<TcpListener>>>,
    // A list of topic-id and their messages
    topics: Arc<Mutex<HashMap<Topic, Vec<String>>>>,
    // A list of topics and their associated clientIds ( stored in connected_clients )
    topic_client_association: Arc<Mutex<HashMap<Topic, Vec<String>>>>,
    connected_clients: SinkMap,

    // Client specific
    durable_client_connection: Option<Arc<Mutex<OutConnection>>>,
}

impl ConnectionManager {
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
    pub async fn create_listener(
        path: String,
        topics: Arc<Mutex<HashMap<String, Vec<String>>>>,
    ) -> Result<ConnectionManager, TackboardError> {
        let listener = TcpListener::bind(&path).await.map_err(TackboardError::Io)?;

        let topic_association: Arc<Mutex<HashMap<String, Vec<String>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        for topic in topics.lock().await.keys() {
            debug!("Setting up topic: {topic}");
            topic_association.lock().await.insert(topic.clone(), vec![]);
        }

        Ok(ConnectionManager {
            path,
            connected_clients: Arc::new(Mutex::new(HashMap::new())),
            listener: Arc::new(Mutex::new(Some(listener))),
            topics,
            topic_client_association: topic_association,
            durable_client_connection: None,
        })
    }
    pub async fn get_connected_clients(
        &self,
    ) -> Arc<Mutex<HashMap<String, Arc<Mutex<SplitSink<InConnection, ServerResponse>>>>>> {
        self.connected_clients.clone()
    }

    pub async fn get_topics(&self) -> Arc<Mutex<HashMap<Topic, Vec<String>>>> {
        self.topics.clone()
    }

    pub async fn get_topic_association(&self) -> Arc<Mutex<HashMap<Topic, Vec<String>>>> {
        self.topic_client_association.clone()
    }
}

impl Connects for ConnectionManager {
    async fn send(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError> {
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

    async fn send_with_callback<F, Fut>(&self, request: ClientRequest, callback: F)
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
}
