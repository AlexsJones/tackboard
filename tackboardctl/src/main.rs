use futures::SinkExt;
use log::{debug, error};
use std::time::Duration;
use tackboardlib::connection_manager::{
    ConnectionManager, Connects, OutConnection, create_outgoing_connection,
};
use tackboardlib::types::ClientRequest::ConnectionRequest;
use tackboardlib::types::*;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    generate_messages: Option<bool>,
}
#[tokio::main]
async fn main() {

    let args = Args::parse();

    env_logger::builder()
        .filter(None, log::LevelFilter::Debug)
        .init();

    let client_path = "93.13.54.1:5621".to_string();
    let server_path = "127.0.0.1:5621".to_string();
    let connect_manager = ConnectionManager::create_client(server_path.clone())
        .await
        .unwrap();
    let client_id = Uuid::new_v4().to_string();
    let request = ConnectionRequest {
        id: client_id.clone(),
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
    // Connected at this point --------------------------------------------------------------------

    if args.generate_messages.unwrap_or(false) {
        let mut count = 0;
        let random_messages = vec![
            "Hello, world!",
            "This is a test message.",
            "Tackboard is awesome!",
            "How are you doing today?",
            "Let's send some more messages.",
        ];
        loop {
            let topic_request = ClientRequest::PublishRequest {
                id: client_id.clone(),
                topic_id: "topic-1".to_string(),
                message: random_messages[count % random_messages.len()].to_string(),
            };
            connect_manager.send(topic_request).await.unwrap();
            count += 1;
        }
    }

    if let Some(server_topics) = server_topics {
        debug!(
            "Connected to server at {} with topics: {:?}",
            server_path, server_topics
        );
        let topic_id = "topic-1".to_string();


        loop {
            let topic_request = ClientRequest::TopicListenRequest {
                topic_id: "topic-1".to_string(),
                id: client_id.clone(),
            };
            connect_manager
                .topic_sync(topic_request, |x| async move {
                    match x {
                        ServerResponse::ConnectionResponse { .. } => {}
                        ServerResponse::TopicListenUpdate { topic_id, messages } => {
                            println!("{:?}", messages)
                        }
                        ServerResponse::Ack => {
                            debug!("Acknowledged topic listen request for topic");
                        }
                        ServerResponse::Error { reason } => {
                            error!("{}", reason)
                        }
                    }
                })
                .await;
        }
    }
}
