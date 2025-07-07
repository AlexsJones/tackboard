use std::time::Duration;
use futures::SinkExt;
use log::{debug, error};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tackboardlib::connection_manager::{create_outgoing_connection, ConnectionManager, Connects, OutConnection};
use tackboardlib::types::*;
use uuid::Uuid;
use tackboardlib::types::ClientRequest::ConnectionRequest;

#[tokio::main]

async fn main(){
    env_logger::builder().filter(None, log::LevelFilter::Debug).init();

    let client_path = "93.13.54.1:5621".to_string();
    let server_path = "127.0.0.1:5621".to_string();
    let connect_manager = ConnectionManager::create_client(server_path.clone()).await.unwrap();

    let request = ConnectionRequest {
        id: uuid::Uuid::new_v4().to_string(),
        client_url: client_path.clone(), // in reality this would be different
        server_url: server_path.clone(),
    };
    let mut server_topics = None;
    
    let server_response = connect_manager.send(request).await.unwrap();
    match server_response {
        ServerResponse::ConnectionResponse { topics } => {
            server_topics = Some(topics);
        }
        ServerResponse::TopicListenUpdate { .. } => {}
        ServerResponse::Ack => {}
        ServerResponse::Error { .. } => {}
    }
    if let Some(server_topics) = server_topics {
        debug!("Connected to server at {} with topics: {:?}", server_path, server_topics);
        let topic_id = "topic-1".to_string(); 


        // tokio::spawn(async move {
        //     sleep(Duration::from_secs(3)).await; // wait for connection to be established
        //     debug!("Sending test messages...");
        //     let mut interval = tokio::time::interval(Duration::from_secs(5));
        //     let generated_mesages = vec![
        //         "Hello, World!".to_string(),
        //         "This is a test message.".to_string(),
        //         "Another message for the topic.".to_string(),
        //     ];
        //     let cm = ConnectionManager::create_client(server_path.clone()).await.unwrap();
        //     let request = ConnectionRequest {
        //         id: uuid::Uuid::new_v4().to_string(),
        //         client_url: client_path.clone(), // in reality this would be different
        //         server_url: server_path.clone(),
        //     };
        //     let mut server_topics = None;
        //
        //     let server_response = cm.send(request).await.unwrap();
        //     match server_response {
        //         ServerResponse::ConnectionResponse { topics } => {
        //             server_topics = Some(topics);
        //         }
        //         ServerResponse::TopicListenUpdate { .. } => {}
        //         ServerResponse::Ack => {}
        //         ServerResponse::Error { .. } => {}
        //     }
        //
        //     loop {
        //         interval.tick().await;
        //         for message in &generated_mesages {
        //             let publish_request = ClientRequest::PublishRequest {
        //                 topic_id: "topic-1".to_string(),
        //                 message: message.clone(),
        //             };
        //             if let Err(e) = cm.send(publish_request).await {
        //                 error!("Failed to publish message: {}", e);
        //             } else {
        //                 debug!("Published message: {}", message);
        //             }
        //         }
        //
        //     }
        // });

        loop {
            let client_path = client_path.clone();
            let topic_request = ClientRequest::TopicListenRequest {
                topic_id: "topic-1".to_string(),
                client_url: client_path.clone(),
            };
            connect_manager.topic_sync(topic_request, |x| async move {
                match x {
                    ServerResponse::ConnectionResponse { .. } => {}
                    ServerResponse::TopicListenUpdate { topic_id, messages } => {
                        println!("{:?}", messages)
                    }
                    ServerResponse::Ack => {}
                    ServerResponse::Error { reason } => {
                        error!("{}", reason)
                    }
                }
            }).await;
        }
    }
}
