use crate::types::errors::TackboardError;
use crate::types::{ClientRequest, ServerResponse, Topic};
use futures::prelude::*;
use futures::stream::SplitSink;
use log::{debug, error};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_serde::formats::*;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{Framed, length_delimited};
pub trait Connects {
    //Client side
    async fn send(&self, request: ClientRequest) -> Result<ServerResponse, TackboardError>;

    // This is a blocking function that will take updates from the server topics subscribed to
    async fn topic_sync<F, Fut>(&self, request: ClientRequest, callback: F)
    where
        F: Fn(ServerResponse) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    // Server side
    async fn accept_connections(&mut self) -> Result<(), TackboardError>;
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
#[derive(Default)]
pub struct ConnectionManager {
    path: String,
    // Server specific
    listener: Option<TcpListener>,
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

        let mut topic_association: Arc<Mutex<HashMap<String,Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        for topic in topics.lock().await.keys() {
            debug!("Setting up topic: {}", topic);
            topic_association
                .lock()
                .await
                .insert(topic.clone(), vec![]);
        }

        Ok(ConnectionManager {
            path,
            connected_clients: Arc::new(Mutex::new(HashMap::new())),
            listener: Some(listener),
            topics,
            topic_client_association: topic_association,
            durable_client_connection: None,
        })
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

    async fn topic_sync<F, Fut>(&self, request: ClientRequest, callback: F)
    where
        F: Fn(ServerResponse) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let framed = self.durable_client_connection.as_ref().unwrap().clone();
        // Spawn a new task to handle topic updates
        tokio::spawn(async move {
            let mut framed = framed.lock().await;

            if let Err(e) = framed.send(request).await {
                error!("Failed to send topic listen request: {}", e);
                return;
            }

            while let Some(result) = framed.next().await {
                match result {
                    Ok(response) => {
                        callback(response).await;
                    }
                    Err(e) => {
                        error!("Error receiving response: {}", e);
                        break;
                    }
                }
            }
        });
    }
    async fn accept_connections(&mut self) -> Result<(), TackboardError> {
        let (socket, _) = self.listener.as_mut().unwrap().accept().await?;

        tokio::spawn({
            let connected_clients = self.connected_clients.clone();
            let topics = self.topics.clone();
            let topic_client_association = self.topic_client_association.clone();
            async move {
                let length_delimited = Framed::new(socket, LengthDelimitedCodec::new());
                let framed: InConnection = create_incoming_connection(length_delimited);

                let (sink, mut stream) = framed.split();
                let sink = Arc::new(Mutex::new(sink));
                // Topic variables
                let topic_client_association = topic_client_association.clone();

                while let Some(Ok(message)) = stream.next().await {
                    match message {
                        ClientRequest::ConnectionRequest { id, .. } => {
                            // check if client is already connected to connected_clients
                            let mut clients = connected_clients.lock().await;
                            if clients.contains_key(&id) {
                                error!("Client with id {} is already connected", id);
                                // resend the topics
                                let topic_keys: Vec<String> =
                                    topics.lock().await.keys().cloned().collect();
                                let response =
                                    ServerResponse::ConnectionResponse { topics: topic_keys };
                                // get the sink out of the hashmap
                                if let Some(sink) = connected_clients.lock().await.get(&id) {
                                    let mut sink = sink.lock().await;
                                    sink.send(ServerResponse::Ack).await.unwrap();
                                }
                                continue;
                            } else {
                                debug!("Client connection request: {:?}", id);
                                // Add the client to connected_clients
                                clients.insert(id.clone(), sink.clone());
                                debug!("Client connection created: {:?}", id);
                                // Send back the connection response with available topics
                                let topic_keys: Vec<String> =
                                    topics.lock().await.keys().cloned().collect();
                                let response =
                                    ServerResponse::ConnectionResponse { topics: topic_keys };
                                let sender = clients.get_mut(&id).unwrap();
                                sender.lock().await.send(response).await.unwrap();
                            }
                        }
                        ClientRequest::TopicListenRequest { id, topic_id } => {

                            // look up the client in connected_clients
                            let mut clients = connected_clients.lock().await;
                            if let Some(sink) = clients.get_mut(&id) {
                                let mut sink = sink.lock().await;
                                debug!("Client {} requested to listen to topic {}", id, topic_id);

                                // Check if the topic exists
                                let mut topics = topics.lock().await;
                                if let Some(messages) = topics.get_mut(&topic_id) {
                                    // If the topic exists, send an ACK
                                    debug!("Topic {} exists, sending ACK to client {}", topic_id, id);
                                    sink.send(ServerResponse::Ack).await.unwrap();

                                    // Optionally, you can send existing messages for the topic
                                    if !messages.is_empty() {
                                        sink.send(ServerResponse::TopicListenUpdate {
                                            topic_id: topic_id.clone(),
                                            messages: messages.clone(),
                                        }).await.unwrap();
                                    }
                                    // Add the client to the topic-client association
                                    let mut topic_clients = topic_client_association.lock().await;
                                    topic_clients
                                        .entry(topic_id.clone())
                                        .or_default()
                                        .push(id.clone());
                                } else {
                                    // If the topic does not exist, send an error response
                                    sink.send(ServerResponse::Error { reason: "Topic not found".to_string() }).await.unwrap();
                                }
                            } else {
                                error!("Client with id {} is not connected", id);
                            }

                        }
                        ClientRequest::PublishRequest { message, id, topic_id } => {
                            
                            let mut topics = topics.lock().await;
                            topics.get_mut(&topic_id).unwrap().push(message.clone());
                            debug!("Published message to topic {}: {}", topic_id, message);
                            // Notify all clients listening to this topic
                            let topic_clients = topic_client_association.lock().await;
                            if let Some(clients) = topic_clients.get(&topic_id) {
                                for client_id in clients {
                                    if let Some(sink) = connected_clients.lock().await.get(client_id) {
                                        let mut sink = sink.lock().await;
                                        sink.send(ServerResponse::TopicListenUpdate {
                                            topic_id: topic_id.clone(),
                                            messages: vec![message.clone()],
                                        }).await.unwrap();
                                    } else {
                                        error!("Client with id {} is not connected", client_id);
                                    }
                                }
                            } else {
                                error!("No clients are listening to topic {}", topic_id);
                            }
                        }
                    }
                }
            }
        });
        Ok(())
    }
}
