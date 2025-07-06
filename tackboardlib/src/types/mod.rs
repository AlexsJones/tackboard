use serde::{Deserialize, Serialize};

pub(crate) mod errors;

pub type Topic = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum ClientRequest {
    ConnectionRequest { id: String, client_url: String, server_url: String },
    TopicListenRequest { topic_id: Topic, client_url: String },
    PublishRequest { topic_id: Topic, message: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum ServerResponse {
    ConnectionResponse { topics: Vec<Topic> },
    TopicListenUpdate { topic_id: Topic, messages: Vec<String> },
    Ack,                      // Optional: generic success response
    Error { reason: String }, // Optional: for failures
}