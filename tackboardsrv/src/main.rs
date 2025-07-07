use futures::{SinkExt, StreamExt};
use log::{debug, error};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tackboardlib::connection_manager::{ConnectionManager, Connects};
use tackboardlib::types::*;
use tokio::sync::Mutex;

static TOPICS: Lazy<Arc<Mutex<HashMap<String, Vec<String>>>>> = Lazy::new(|| {
    let mut map = HashMap::new();
    for id in 0..5 {
        let topic = format!("topic-{id}");
        map.insert(topic, vec![]); // No clients yet
    }
    Arc::new(Mutex::new(map))
});

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    //  Start some random topics
    let topics = TOPICS.clone();
    let mut connection_manager =
        ConnectionManager::create_listener("127.0.0.1:5621".to_string(), topics)
            .await
            .unwrap();

    let connected_clients = connection_manager.get_connected_clients().await;
    let topic_client_association = connection_manager.get_topic_association().await;
    let topics = connection_manager.get_topics().await;

    loop {
        let result = connection_manager.accept_connections({
            let connected_clients = connected_clients.clone();
            let topic_client_association = topic_client_association.clone();
            let topics = topics.clone();

            move |sink, stream| {
                let connected_clients = connected_clients.clone();
                let topic_client_association = topic_client_association.clone();
                let topics = topics.clone();

                async move {
                    while let Some(Ok(message)) = stream.lock().await.next().await {
                        match message {
                            ClientRequest::ConnectionRequest { id, .. } => {
                                // check if client is already connected to connected_clients
                                let mut clients = connected_clients.lock().await;
                                if clients.contains_key(&id) {
                                    error!("Client with id {id} is already connected");
                                    // resend the topics
                                    let topic_keys: Vec<String> =
                                        topics.lock().await.keys().cloned().collect();
                                    let response =
                                        ServerResponse::ConnectionResponse { topics: topic_keys };
                                    // get the sink out of the hashmap
                                    let maybe_sink = {
                                        let clients = connected_clients.lock().await;
                                        clients.get(&id).cloned()
                                    };
                                    if let Some(sink) = maybe_sink {
                                        let mut sink = sink.lock().await;
                                        sink.send(ServerResponse::Ack).await.unwrap();
                                    }
                                    continue;
                                } else {
                                    debug!("Client connection request: {id:?}");
                                    // Add the client to connected_clients
                                    clients.insert(id.clone(), sink.clone());
                                    debug!("Client connection created: {id:?}");
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
                                let maybe_sink = {
                                    let clients = connected_clients.lock().await;
                                    clients.get(&id).cloned()
                                };
                                if let Some(sink) = maybe_sink {
                                    let mut sink = sink.lock().await;
                                    debug!("Client {id} requested to listen to topic {topic_id}");

                                    // Check if the topic exists
                                    let mut topics = topics.lock().await;
                                    if let Some(messages) = topics.get_mut(&topic_id) {
                                        // If the topic exists, send an ACK
                                        debug!("Topic {topic_id} exists, sending ACK to client {id}");
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
                                    error!("Client with id {id} is not connected");
                                }
                            }
                            ClientRequest::PublishRequest { message, id, topic_id } => {
                                let mut topics = topics.lock().await;
                                topics.get_mut(&topic_id).unwrap().push(message.clone());
                                debug!("Published message to topic {topic_id}: {message}");
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
                                            error!("Client with id {client_id} is not connected");
                                        }
                                    }
                                } else {
                                    error!("No clients are listening to topic {topic_id}");
                                }
                                // ack
                                if let Some(sink) = connected_clients.lock().await.get(&id) {
                                    let mut sink = sink.lock().await;
                                    sink.send(ServerResponse::Ack).await.unwrap();
                                } else {
                                    error!("Client with id {id} is not connected");
                                }
                            }
                        }
                    }
                }
            }
        }).await;
    }
}
