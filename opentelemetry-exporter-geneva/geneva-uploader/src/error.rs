#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    #[error("JSON serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Authentication failed: {0}")]
    AuthenticationError(String),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}

// Result type alias for convenience
pub type Result<T> = std::result::Result<T, Error>;
