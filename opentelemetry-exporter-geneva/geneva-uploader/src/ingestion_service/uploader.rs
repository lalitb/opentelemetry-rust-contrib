use crate::config_service::client::{GenevaConfigClient, GenevaConfigClientError};
use crate::payload_encoder::central_blob::BatchMetadata;
use reqwest::{header, Client};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use url::form_urlencoded::byte_serialize;
use uuid::Uuid;

/// Error types for the Geneva Uploader
#[derive(Debug, Error)]
pub enum GenevaUploaderError {
    #[error("Service unavailable (503): {message}")]
    ServiceUnavailable {
        message: String,
        retry_after: Option<Duration>,
        headers: Option<HashMap<String, String>>,
    },

    #[error("Rate limited (429): {message}")]
    RateLimited {
        message: String,
        retry_after: Option<Duration>,
        rate_limit_reset: Option<SystemTime>,
        headers: Option<HashMap<String, String>>,
    },

    #[error("Client error ({status}): {message}")]
    ClientError {
        status: u16,
        message: String,
        headers: Option<HashMap<String, String>>,
    },

    #[error("Server error ({status}): {message}")]
    ServerError {
        status: u16,
        message: String,
        retry_after: Option<Duration>,
        headers: Option<HashMap<String, String>>,
    },

    #[error("Network error: {source}")]
    NetworkError {
        #[source]
        source: reqwest::Error,
        url: String,
        error_kind: String,
    },

    #[error("JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Config service error: {0}")]
    ConfigClient(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<GenevaConfigClientError> for GenevaUploaderError {
    fn from(err: GenevaConfigClientError) -> Self {
        // This preserves the original error message format from the code
        GenevaUploaderError::ConfigClient(format!("GenevaConfigClient error: {err}"))
    }
}

impl From<reqwest::Error> for GenevaUploaderError {
    fn from(err: reqwest::Error) -> Self {
        let url = err.url().map(|u| u.to_string()).unwrap_or_else(|| "unknown".to_string());

        // Determine error kind
        let error_kind = if err.is_timeout() {
            "timeout".to_string()
        } else if err.is_connect() {
            "connect".to_string()
        } else if err.is_body() {
            "body".to_string()
        } else if err.is_decode() {
            "decode".to_string()
        } else if err.is_request() {
            "request".to_string()
        } else {
            "unknown".to_string()
        };

        GenevaUploaderError::NetworkError {
            source: err,
            url,
            error_kind,
        }
    }
}

impl GenevaUploaderError {
    /// Check if this error indicates a retryable condition
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::ServiceUnavailable { .. } => true,
            Self::RateLimited { .. } => true,
            Self::ServerError { status, .. } => {
                // Retry on specific 5xx errors
                matches!(*status, 500 | 502 | 503 | 504)
            }
            Self::NetworkError { error_kind, .. } => {
                // Retry on network issues (except body/decode which indicate protocol issues)
                matches!(error_kind.as_str(), "timeout" | "connect" | "request")
            }
            _ => false,
        }
    }

    /// Get the retry-after duration if available
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::ServiceUnavailable { retry_after, .. } => *retry_after,
            Self::RateLimited { retry_after, .. } => *retry_after,
            Self::ServerError { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// Get error category for metrics/logging
    pub fn category(&self) -> &'static str {
        match self {
            Self::ServiceUnavailable { .. } => "service_unavailable",
            Self::RateLimited { .. } => "rate_limited",
            Self::ClientError { .. } => "client_error",
            Self::ServerError { .. } => "server_error",
            Self::NetworkError { .. } => "network_error",
            Self::SerdeJson(_) => "json_error",
            Self::ConfigClient(_) => "config_error",
            Self::InternalError(_) => "internal_error",
        }
    }

    /// Get HTTP status code if available
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::ServiceUnavailable { .. } => Some(503),
            Self::RateLimited { .. } => Some(429),
            Self::ClientError { status, .. } => Some(*status),
            Self::ServerError { status, .. } => Some(*status),
            _ => None,
        }
    }
}

/// Helper function to extract headers from reqwest response
fn extract_headers(headers: &header::HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value.to_str().ok().map(|v| (name.to_string(), v.to_string()))
        })
        .collect()
}

/// Helper function to parse Retry-After header
fn parse_retry_after(headers: &header::HeaderMap) -> Option<Duration> {
    headers
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            // Try parsing as seconds first
            if let Ok(seconds) = s.parse::<u64>() {
                Some(Duration::from_secs(seconds))
            } else {
                // Try parsing as HTTP date (not implemented for now)
                None
            }
        })
}

/// Helper function to parse rate limit reset time
fn parse_rate_limit_reset(headers: &header::HeaderMap) -> Option<SystemTime> {
    headers
        .get("x-ratelimit-reset")
        .or_else(|| headers.get("ratelimit-reset"))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(|timestamp| SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp))
}

pub(crate) type Result<T> = std::result::Result<T, GenevaUploaderError>;

/// Response from the ingestion API when submitting data
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct IngestionResponse {
    #[allow(dead_code)]
    pub(crate) ticket: String,
    #[serde(flatten)]
    #[allow(dead_code)]
    pub(crate) extra: HashMap<String, Value>,
}

/// Configuration for the Geneva Uploader
#[derive(Debug, Clone)]
pub(crate) struct GenevaUploaderConfig {
    pub namespace: String,
    pub source_identity: String,
    #[allow(dead_code)]
    pub environment: String,
    pub config_version: String,
}

/// Client for uploading data to Geneva Ingestion Gateway (GIG)
#[derive(Debug, Clone)]
pub struct GenevaUploader {
    pub config_client: Arc<GenevaConfigClient>,
    pub config: GenevaUploaderConfig,
    pub http_client: Client,
}

impl GenevaUploader {
    /// Constructs a GenevaUploader by calling the GenevaConfigClient
    ///
    /// # Arguments
    /// * `config_client` - Initialized GenevaConfigClient
    /// * `uploader_config` - Static config (namespace, event, version, etc.)
    ///
    /// # Returns
    /// * `Result<GenevaUploader>` with authenticated client and resolved moniker/endpoint
    #[allow(dead_code)]
    pub(crate) fn from_config_client(
        config_client: Arc<GenevaConfigClient>,
        uploader_config: GenevaUploaderConfig,
    ) -> Result<Self> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );
        let client = Self::build_h1_client(headers)?;

        Ok(Self {
            config_client,
            config: uploader_config,
            http_client: client,
        })
    }

    fn build_h1_client(headers: header::HeaderMap) -> Result<Client> {
        Ok(Client::builder()
            .timeout(Duration::from_secs(60))
            .default_headers(headers)
            .http1_only()
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Duration::from_secs(30))
            .build()?)
    }

    /// Creates the GIG upload URI with required parameters
    #[allow(dead_code)]
    fn create_upload_uri(
        &self,
        monitoring_endpoint: &str,
        moniker: &str,
        data_size: usize,
        event_name: &str,
        metadata: &BatchMetadata,
    ) -> Result<String> {
        // Get already formatted schema IDs and format timestamps using BatchMetadata methods
        let schema_ids = &metadata.schema_ids;
        let start_time_str = metadata.format_start_timestamp();
        let end_time_str = metadata.format_end_timestamp();

        // URL encode parameters
        // TODO - Maintain this as url-encoded in config service to avoid conversion here
        let encoded_monitoring_endpoint: String =
            byte_serialize(monitoring_endpoint.as_bytes()).collect();
        let encoded_source_identity: String =
            byte_serialize(self.config.source_identity.as_bytes()).collect();

        // Create a source unique ID - using a UUID to ensure uniqueness
        let source_unique_id = Uuid::new_v4();

        // Create the query string
        let mut query = String::with_capacity(512); // Preallocate enough space for the query string (decided based on expected size)
        write!(&mut query, "api/v1/ingestion/ingest?endpoint={}&moniker={}&namespace={}&event={}&version={}&sourceUniqueId={}&sourceIdentity={}&startTime={}&endTime={}&format=centralbond/lz4hc&dataSize={}&minLevel={}&schemaIds={}",
            encoded_monitoring_endpoint,
            moniker,
            self.config.namespace,
            event_name,
            self.config.config_version,
            source_unique_id,
            encoded_source_identity,
            start_time_str,
            end_time_str,
            data_size,
            2,
            schema_ids
        ).map_err(|e| GenevaUploaderError::InternalError(format!("Failed to write query string: {e}")))?;
        Ok(query)
    }

    /// Uploads data to the ingestion gateway
    ///
    /// # Arguments
    /// * `data` - The encoded data to upload (already in the required format)
    /// * `event_name` - Name of the event
    /// * `event_version` - Version of the event
    /// * `metadata` - Batch metadata containing timestamps and schema information
    ///
    /// # Returns
    /// * `Result<IngestionResponse>` - The response containing the ticket ID or an error
    #[allow(dead_code)]
    pub(crate) async fn upload(
        &self,
        data: Vec<u8>,
        event_name: &str,
        metadata: &BatchMetadata,
    ) -> Result<IngestionResponse> {
        // Always get fresh auth info
        let (auth_info, moniker_info, monitoring_endpoint) =
            self.config_client.get_ingestion_info().await?;
        let data_size = data.len();
        let upload_uri = self.create_upload_uri(
            &monitoring_endpoint,
            &moniker_info.name,
            data_size,
            event_name,
            metadata,
        )?;
        let full_url = format!(
            "{}/{}",
            auth_info.endpoint.trim_end_matches('/'),
            upload_uri
        );
        // Send the upload request
        let response = self
            .http_client
            .post(&full_url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", auth_info.auth_token),
            )
            .body(data)
            .send()
            .await?;

        let status = response.status();
        let headers = response.headers().clone();
        let extracted_headers = extract_headers(&headers);
        let body = response.text().await?;

        if status == reqwest::StatusCode::ACCEPTED {
            let ingest_response: IngestionResponse =
                serde_json::from_str(&body).map_err(GenevaUploaderError::SerdeJson)?;
            Ok(ingest_response)
        } else {
            // Create structured error based on status code
            let status_code = status.as_u16();
            let retry_after = parse_retry_after(&headers);

            match status_code {
                503 => Err(GenevaUploaderError::ServiceUnavailable {
                    message: body,
                    retry_after,
                    headers: Some(extracted_headers),
                }),
                429 => Err(GenevaUploaderError::RateLimited {
                    message: body,
                    retry_after,
                    rate_limit_reset: parse_rate_limit_reset(&headers),
                    headers: Some(extracted_headers),
                }),
                400..=499 => Err(GenevaUploaderError::ClientError {
                    status: status_code,
                    message: body,
                    headers: Some(extracted_headers),
                }),
                500..=599 => Err(GenevaUploaderError::ServerError {
                    status: status_code,
                    message: body,
                    retry_after,
                    headers: Some(extracted_headers),
                }),
                _ => Err(GenevaUploaderError::InternalError(format!(
                    "Unexpected status code {}: {}",
                    status_code, body
                ))),
            }
        }
    }
}
