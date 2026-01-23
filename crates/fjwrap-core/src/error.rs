use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Key not found")]
    KeyNotFound,

    #[error("Storage error: {0}")]
    Storage(#[from] fjall::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serde JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
