//! Error types for configuration and provider creation.

/// Errors that can occur during configuration and provider creation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid configuration format or content.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Unknown exporter requested in configuration.
    #[error("Unknown {signal} exporter '{name}'. Available exporters must be registered before building providers.")]
    UnknownExporter {
        /// Signal type (e.g., "metrics", "traces", "logs")
        signal: &'static str,
        /// Requested exporter name
        name: String,
    },

    /// Invalid exporter-specific configuration.
    #[error("Invalid configuration for exporter '{exporter}': {reason}")]
    InvalidExporterConfig {
        /// Exporter name
        exporter: String,
        /// Reason for invalidity
        reason: String,
    },

    /// Exporter creation failed.
    #[error("Failed to create exporter '{exporter}': {source}")]
    ExporterCreationFailed {
        /// Exporter name
        exporter: String,
        /// Underlying error
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Unsupported feature in configuration.
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

    /// Generic error for compatibility.
    #[error("{0}")]
    Other(String),
}

// Implement conversion from common error types for convenience
impl From<serde_yaml::Error> for Error {
    fn from(err: serde_yaml::Error) -> Self {
        Error::InvalidConfiguration(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::InvalidConfiguration(err.to_string())
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error::Other(msg)
    }
}

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Error::Other(msg.to_string())
    }
}
