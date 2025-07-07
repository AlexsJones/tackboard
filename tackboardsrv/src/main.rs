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

    
    loop {
            let _ = connection_manager.accept_connections().await;
    }
}