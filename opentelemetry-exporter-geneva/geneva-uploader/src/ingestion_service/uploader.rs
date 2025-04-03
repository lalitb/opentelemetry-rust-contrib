use chrono::{DateTime, Duration as ChronoDuration, Utc};
use chrono::{Datelike, Timelike};

use crate::config_service::client::{GenevaConfigClient, IngestionGatewayInfo};
use reqwest::{header, Client};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use url::form_urlencoded;
use url::form_urlencoded::byte_serialize;
use uuid::Uuid;

/// Error types for the Geneva Uploader
#[derive(Debug, Error)]
pub enum GenevaUploaderError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Upload failed with status {status}: {message}")]
    UploadFailed { status: u16, message: String },
    #[error("Uploader error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, GenevaUploaderError>;

/// Response from the ingestion API when submitting data
#[derive(Debug, Clone, Deserialize)]
pub struct IngestionResponse {
    pub ticket: String,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// Configuration for the Geneva Uploader
#[derive(Debug, Clone)]
pub struct GenevaUploaderConfig {
    pub namespace: String,
    pub source_identity: String,
    pub environment: String,
    pub schema_ids: Option<String>,
}

/// Client for uploading data to Geneva Ingestion Gateway (GIG)
#[derive(Debug, Clone)]
pub struct GenevaUploader {
    pub auth_info: IngestionGatewayInfo,
    pub moniker: String,
    pub config: GenevaUploaderConfig,
    pub http_client: Client,
    pub monitoring_endpoint: String,
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
    pub async fn from_config_client(
        config_client: &GenevaConfigClient,
        uploader_config: GenevaUploaderConfig,
    ) -> Result<Self> {
        let (auth_info, moniker_info, monitoring_endpoint) = config_client
            .get_ingestion_info()
            .await
            .map_err(|e| GenevaUploaderError::Other(format!("GCS error: {}", e)))?;

        println!("Monitoring endpoint: {}", monitoring_endpoint); // For debugging purposes
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(GenevaUploaderError::Http)?;

        Ok(Self {
            auth_info,
            moniker: moniker_info.name,
            config: uploader_config,
            http_client,
            monitoring_endpoint,
        })
    }

    /// Creates the GIG upload URI with required parameters
    fn create_upload_uri(&self, data_size: usize, event_name: &str, event_version: &str) -> String {
        // Current time and end time (5 minutes later)
        let now: DateTime<Utc> = Utc::now();
        let end_time = now + ChronoDuration::minutes(5);

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
            byte_serialize(self.monitoring_endpoint.as_bytes()).collect();
        println!(
            "Encoded monitoring endpoint: {}",
            encoded_monitoring_endpoint
        ); // For debugging purposes

        let encoded_source_identity: String =
            byte_serialize(self.config.source_identity.as_bytes()).collect();
        println!("Encoded source identity: {}", encoded_source_identity); // For debugging purposes

        // Create a source unique ID - using a UUID to ensure uniqueness
        let source_unique_id = Uuid::new_v4().to_string();

        // Use provided schema IDs or default if not specified
        let schema_ids = self.config.schema_ids.clone().unwrap_or_else(|| {
            "c1ce0ecea020359624c493bbe97f9e80;0da22cabbee419e000541a5eda732eb3".to_string()
        });

        // Create the query string
        format!(
            "api/v1/ingestion/ingest?endpoint={}&moniker={}&namespace={}&event={}&version={}&sourceUniqueId={}&sourceIdentity={}&startTime={}&endTime={}&format={}&dataSize={}&minLevel={}&schemaIds={}",
            encoded_monitoring_endpoint,
            self.moniker,
            self.config.namespace,
            event_name,
            event_version,
            source_unique_id,
            encoded_source_identity, // source identity (tenant/role/instance)
            start_time,
            end_time,
            "centralbond/lz4hc", // Format encoding
            data_size,
            2, // Min level
            schema_ids
        )
    }

    /// Uploads data to the ingestion gateway
    ///
    /// # Arguments
    /// * `data` - The encoded data to upload (already in the required format)
    ///
    /// # Returns
    /// * `Result<IngestionResponse>` - The response containing the ticket ID or an error
    pub async fn upload(
        &self,
        data: Vec<u8>,
        event_name: &str,
        event_version: &str,
    ) -> Result<IngestionResponse> {
        let data_size = data.len();
        let upload_uri = self.create_upload_uri(data_size, event_name, event_version);
        let full_url = format!(
            "{}/{}",
            self.auth_info.endpoint.trim_end_matches('/'),
            upload_uri
        );
        println!("Upload URI: {}", full_url); // For debugging purposes
        println!("Auth Token: {}", self.auth_info.auth_token); // For debugging purposes

        // Send the upload request
        let response = self
            .http_client
            .post(&full_url)
            .header(header::ACCEPT, "application/json")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.auth_info.auth_token),
            )
            .body(data)
            .send()
            .await
            .map_err(GenevaUploaderError::Http)?;

        let status = response.status();
        let body = response.text().await.map_err(GenevaUploaderError::Http)?;

        if status == reqwest::StatusCode::ACCEPTED {
            let ingest_response: IngestionResponse =
                serde_json::from_str(&body).map_err(|e| GenevaUploaderError::Json(e))?;
            Ok(ingest_response)
        } else {
            Err(GenevaUploaderError::UploadFailed {
                status: status.as_u16(),
                message: body,
            })
        }
    }
}
