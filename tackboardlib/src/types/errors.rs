
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TackboardError {
    #[error("Network error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Invalid request")]
    InvalidRequest,
}