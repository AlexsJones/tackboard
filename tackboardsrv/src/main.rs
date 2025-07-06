use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use futures::SinkExt;
use log::debug;
use tokio::sync::Mutex;
use tackboardlib::connection_manager::{ConnectionManager, Connects};
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
    let mut connected_clients: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    
    loop {
        let connected_clients = connected_clients.clone();
        let _ = connection_manager.accept_connection(move |x, mut framed| {
            let connected_clients = connected_clients.clone();
            async move {
                match x {
                    ClientRequest::ConnectionRequest { id, client_url, .. } => {
                        debug!("Client connection request: {:?}", id);
                        connected_clients.lock().await.push(client_url.clone());
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
                        // handle topic listen request here
                        framed
                    }
                    ClientRequest::PublishRequest { .. } => {
                        debug!("Received publish request");
                        // handle publish request here
                        framed
                    }
                }
            }
        }).await;

    }
}