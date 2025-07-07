use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use futures::{SinkExt, StreamExt};
use futures::stream::SplitSink;
use log::debug;
use tokio::sync::{mpsc, Mutex};
use tackboardlib::connection_manager::{ConnectionManager, Connects, InConnection};
use tackboardlib::types::*;
use once_cell::sync::Lazy;

static TOPICS: Lazy<Arc<Mutex<HashMap<String, Vec<String>>>>> = Lazy::new(|| {
    let mut map = HashMap::new();
    for id in 0..5 {
        let topic = format!("topic-{}", id);
        map.insert(topic, vec![]); // No clients yet
    }
    Arc::new(Mutex::new(map))
});

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    //  Start some random topics
    let topics = TOPICS.clone();
    let mut connection_manager = ConnectionManager::create_listener("127.0.0.1:5621".to_string(), topics).await.unwrap();
    // connected clients tracker
    let mut connected_clients: Arc<Mutex<HashMap<String,
        Vec<SplitSink<InConnection,ServerResponse>>>>> 
        = Arc::new(Mutex::new(HashMap::new()));
    
    loop {
        let connected_clients = connected_clients.clone();
        let _ = connection_manager.accept_connection(move |x, mut framed| {
            let connected_clients = connected_clients.clone();
            async move {
                match x {
                    ClientRequest::ConnectionRequest { id, client_url, .. } => {
                        debug!("Client connection request: {:?}", id);
                        
                        let topic_keys: Vec<String> = TOPICS
                            .lock()
                            .await
                            .keys()
                            .cloned()
                            .collect();
                        debug!("Wrote back to the client {}", client_url);
                        debug!("Number of connected clients {}", connected_clients.lock().await.len());
                        let response = ServerResponse::ConnectionResponse {
                            topics: topic_keys,
                        };
                        framed.send(response).await.unwrap();
                        framed
                    }
                    ClientRequest::TopicListenRequest { topic_id, client_url } => {
                        debug!("Client {} requested to listen to topic {}", client_url, topic_id);
                        // Check if the current client is captured
                        // Instead of splitting a reference, take ownership of the framed
                        
                        
                        // Now the sink should have the correct type when pushing to the vector
                        let mut clients = connected_clients.lock().await;
                        clients.entry(topic_id.clone())
                            .or_insert_with(Vec::new)
                            .push(sink);
                        
                        // handle topic listen request here
                        let mut topics = TOPICS.lock().await;
                        if let Some(messages) = topics.get_mut(&topic_id) {
                            // If the topic exists, send an ACK
                            debug!("Topic {} exists, sending ACK to client {}", topic_id, client_url);
                            framed.get_ref().send(ServerResponse::Ack).await.unwrap();
                        } else {
                            // If the topic does not exist, send an error response
                            framed.get_ref().send(ServerResponse::Error { reason: "Topic not found".to_string() }).await.unwrap();
                        }
                        framed
                    }
                    ClientRequest::PublishRequest { topic_id, message } => {
                        debug!("Received publish request");
                        // handle publish request here
                        let mut topics = TOPICS.lock().await;
                        if let Some(messages) = topics.get_mut(&topic_id) {
                            messages.push(message.clone());
                            debug!("Published message to topic {}: {}", topic_id, message);
                            // Notify all clients listening to this topic
                            // TODO: notify clients
                        } else {
                            debug!("Topic {} not found for publish request", topic_id);
                        }
                        framed
                    }
                }
            }
        }).await;

    }
}