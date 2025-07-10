use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::config_service::client::{GenevaConfigClient, GenevaConfigClientError};
use chrono::{Datelike, Timelike};
use reqwest::{header, Client};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use url::form_urlencoded::byte_serialize;
use uuid::Uuid;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

// Metrics for tracking upload performance
#[derive(Debug, Default)]
pub struct UploadMetrics {
    /// Total number of requests processed
    request_count: AtomicUsize,
    /// Total response time in microseconds
    total_response_time_micros: AtomicU64,
    /// Total data size processed in bytes
    total_data_size: AtomicU64,
    /// Ongoing requests
    ongoing_requests: AtomicUsize,
}

impl UploadMetrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a request with its response time and data size
    fn record_request(&self, response_time_micros: u64, data_size: u64) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.total_response_time_micros
            .fetch_add(response_time_micros, Ordering::Relaxed);
        self.total_data_size.fetch_add(data_size, Ordering::Relaxed);
    }

    /// Get the average response time in microseconds
    pub fn average_response_time_micros(&self) -> f64 {
        let count = self.request_count.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            self.total_response_time_micros.load(Ordering::Relaxed) as f64 / count as f64
        }
    }

    /// Get the average response time in milliseconds
    pub fn average_response_time_millis(&self) -> f64 {
        self.average_response_time_micros() / 1000.0
    }

    /// Get the total number of requests processed
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }

    /// Get the average data size in bytes
    pub fn average_data_size(&self) -> f64 {
        let count = self.request_count.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            self.total_data_size.load(Ordering::Relaxed) as f64 / count as f64
        }
    }

    /// Get the total data size processed in bytes
    pub fn total_data_size(&self) -> u64 {
        self.total_data_size.load(Ordering::Relaxed)
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.request_count.store(0, Ordering::Relaxed);
        self.total_response_time_micros.store(0, Ordering::Relaxed);
        self.total_data_size.store(0, Ordering::Relaxed);
    }
}

/// Error types for the Geneva Uploader
#[derive(Debug, Error)]
pub(crate) enum GenevaUploaderError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("Config service error: {0}")]
    ConfigClient(String),
    #[allow(dead_code)]
    #[error("Upload failed with status {status}: {message}")]
    UploadFailed { status: u16, message: String },
    #[allow(dead_code)]
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
        use std::fmt::Write;
        let mut msg = String::new();
        write!(&mut msg, "{err}").ok();

        if let Some(url) = err.url() {
            write!(msg, ", url: {url}").ok();
        }
        if let Some(status) = err.status() {
            write!(msg, ", status: {status}").ok();
        }

        // Print high-level error types
        if err.is_timeout() {
            write!(&mut msg, ", kind: timeout").ok();
        } else if err.is_connect() {
            write!(&mut msg, ", kind: connect").ok();
        } else if err.is_body() {
            write!(&mut msg, ", kind: body").ok();
        } else if err.is_decode() {
            write!(&mut msg, ", kind: decode").ok();
        } else if err.is_request() {
            write!(&mut msg, ", kind: request").ok();
        }

        // Traverse the whole source chain for detail
        let mut source = err.source();
        let mut idx = 0;
        let mut found_io = false;
        while let Some(s) = source {
            write!(msg, ", cause[{idx}]: {s}").ok();

            // Surface io::ErrorKind if found
            if let Some(io_err) = s.downcast_ref::<std::io::Error>() {
                write!(msg, " (io::ErrorKind::{:?})", io_err.kind()).ok();
                found_io = true;
            }
            source = s.source();
            idx += 1;
        }

        if !found_io {
            write!(&mut msg, ", (no io::Error in source chain)").ok();
        }

        GenevaUploaderError::Http(msg)
    }
}

pub(crate) type Result<T> = std::result::Result<T, GenevaUploaderError>;

#[allow(dead_code)]
/// Response from the ingestion API when submitting data
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct IngestionResponse {
    pub(crate) ticket: String,
    #[serde(flatten)]
    pub(crate) extra: HashMap<String, Value>,
}

/// Configuration for the Geneva Uploader
#[derive(Debug, Clone)]
pub(crate) struct GenevaUploaderConfig {
    pub namespace: String,
    pub source_identity: String,
    #[allow(dead_code)]
    pub environment: String,
    pub schema_ids: String,
}

/// Client for uploading data to Geneva Ingestion Gateway (GIG)
#[derive(Debug, Clone)]
pub struct GenevaUploader {
    pub config_client: Arc<GenevaConfigClient>,
    pub config: GenevaUploaderConfig,
    pub http_client: Client,
    pub metrics: Arc<UploadMetrics>,
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
    pub(crate) async fn from_config_client(
        config_client: Arc<GenevaConfigClient>,
        uploader_config: GenevaUploaderConfig,
    ) -> Result<Self> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );
        let http_client = Client::builder()
            .timeout(Duration::from_secs(100))
            .default_headers(headers)
            .build()?;

        Ok(Self {
            config_client,
            config: uploader_config,
            http_client,
            metrics: Arc::new(UploadMetrics::new()),
        })
    }

    pub(crate) fn metrics(&self) -> &Arc<UploadMetrics> {
        &self.metrics
    }

    pub(crate) fn ongoing_requests(&self) -> usize {
        self.metrics.ongoing_requests.load(Ordering::Relaxed)
    }

    /// Creates the GIG upload URI with required parameters
    #[allow(dead_code)]
    fn create_upload_uri(
        &self,
        monitoring_endpoint: &str,
        moniker: &str,
        data_size: usize,
        event_name: &str,
        event_version: &str,
    ) -> Result<String> {
        let now: DateTime<Utc> = Utc::now(); //TODO - this need to be calculated from the bond data
        let end_time = now + ChronoDuration::minutes(5); //TODO - this need to be calculated from the bond data

        // Format times in ISO 8601 format with fixed precision
        // Using .NET compatible format (matches DateTime.ToString("O"))

        let start_time = format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:07}Z",
            now.year(),
            now.month(),
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
            now.nanosecond() / 100 // Convert nanoseconds to 7-digit precision
        );

        let end_time = format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:07}Z",
            end_time.year(),
            end_time.month(),
            end_time.day(),
            end_time.hour(),
            end_time.minute(),
            end_time.second(),
            end_time.nanosecond() / 100 // Convert nanoseconds to 7-digit precision
        );

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
            event_version,
            source_unique_id,
            encoded_source_identity,
            start_time,
            end_time,
            data_size,
            2,
            self.config.schema_ids
        ).map_err(|e| GenevaUploaderError::InternalError(format!("Failed to write query string: {e}")))?;
        Ok(query)
    }

    /// Uploads data to the ingestion gateway
    ///
    /// # Arguments
    /// * `data` - The encoded data to upload (already in the required format)
    ///
    /// # Returns
    /// * `Result<IngestionResponse>` - The response containing the ticket ID or an error
    #[allow(dead_code)]
    pub(crate) async fn upload(
        &self,
        data: Vec<u8>,
        event_name: &str,
        event_version: &str,
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
            event_version,
        )?;
        let full_url = format!(
            "{}/{}",
            auth_info.endpoint.trim_end_matches('/'),
            upload_uri
        );
        // Send the upload request
        use std::time::Instant;
        //println!("Uploading data to: {}", full_url);
        let start = Instant::now();
        self.metrics
            .ongoing_requests
            .fetch_add(1, Ordering::Relaxed); // Increment ongoing requests
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
        self.metrics
            .ongoing_requests
            .fetch_sub(1, Ordering::Relaxed); // Decrement ongoing requests
        let elapsed = start.elapsed();
        self.metrics
            .record_request(elapsed.as_micros() as u64, data_size as u64); // ADD THIS LINE

        //println!("Request completed in: {:.2}μs data_size: {}", elapsed.as_micros(), data_size);

        let status = response.status();
        let body = response.text().await?;

        if status == reqwest::StatusCode::ACCEPTED {
            let ingest_response: IngestionResponse =
                serde_json::from_str(&body).map_err(GenevaUploaderError::SerdeJson)?;

            Ok(ingest_response)
        } else {
            Err(GenevaUploaderError::UploadFailed {
                status: status.as_u16(),
                message: body,
            })
        }
    }
}
