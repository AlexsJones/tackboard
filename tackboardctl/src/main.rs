use std::time::Duration;
use futures::SinkExt;
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

    // // Create a dummy example that will start publishing messages to the server, and give our subscribe something to print
    // tokio::spawn(
    //     {
    //         let request = request.clone();
    //     
    //     async move {
    //         let request = request.clone();
    //         let server_path = server_path.clone();
    //         sleep(Duration::from_secs(3));
    //         let length_delimited = Framed::new(TcpStream::connect(server_path).await.unwrap(),
    //                                            LengthDelimitedCodec::new());
    //         let mut framed: OutConnection = create_outgoing_connection(length_delimited);
    // 
    //         //create some messages to publish on topic "topic-1"
    //         let topic_id = "topic-1".to_string();
    //         let messages = vec![
    //             "Hello, World!".to_string(),
    //             "This is a test message.".to_string(),
    //             "Tackboard is awesome!".to_string(),
    //         ];
    //         for msg in messages {
    //             let request = request.clone();
    //             let publish_request = ClientRequest::PublishRequest { topic_id: topic_id.clone(), message: msg };
    //             framed
    //                 .send(request)
    //                 .await.unwrap();
    //             sleep(Duration::from_secs(1)).await; // simulate some delay between messages
    //         }
    //     }
    // });
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
        let topic_id = "topic-1".to_string(); 
        let topic_request = ClientRequest::TopicListenRequest { topic_id, client_url: client_path.clone() };
        let topic_response = connect_manager.send(topic_request).await.unwrap();
        if let ServerResponse::Ack = topic_response {
            
            // Start polling for topic updates
            loop {
                connect_manager.topic_sync(|x| async move {
                    if let ServerResponse::TopicListenUpdate {topic_id, messages} = x {
                        for msg in messages {
                            println!("{}", msg);
                        }
                    }
                }).await;
            }
        }
    }
}
