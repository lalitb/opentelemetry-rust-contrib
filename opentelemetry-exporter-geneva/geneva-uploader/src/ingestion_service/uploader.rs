use base64::{engine::general_purpose, Engine as _};
<<<<<<< HEAD
use chrono::{Datelike, Timelike};
use chrono::{DateTime, Duration as ChronoDuration, Utc};

=======
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use url::form_urlencoded;
use uuid::Uuid;
<<<<<<< HEAD
use crate::config_service::client::{GenevaConfigClient, MonikerInfo, IngestionGatewayInfo};
=======

use crate::config_service::client::IngestionGatewayInfo;
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582

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
pub struct GenevaUploaderConfig {
<<<<<<< HEAD
=======
    pub moniker: String,
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582
    pub namespace: String,
    pub event_name: String,
    pub event_version: String,
    pub source_identity: String,
    pub environment: String,
<<<<<<< HEAD
    pub schema_ids: Option<String>,
=======
    pub monitoring_endpoint: String, // URL parameter from JWT or config
    pub schema_ids: Option<String>, // Optional schema IDs
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582
}

/// Client for uploading data to Geneva Ingestion Gateway (GIG)
pub struct GenevaUploader {
    auth_info: IngestionGatewayInfo,
<<<<<<< HEAD
    moniker: String,
=======
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582
    config: GenevaUploaderConfig,
    http_client: Client,
}

impl GenevaUploader {
    /// Creates a new Geneva Uploader with the provided configuration
    pub fn new(
        auth_info: IngestionGatewayInfo,
<<<<<<< HEAD
        moniker_info: MonikerInfo,
=======
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582
        config: GenevaUploaderConfig,
    ) -> Result<Self> {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(GenevaUploaderError::Http)?;
<<<<<<< HEAD
    
        Ok(Self {
            auth_info,
            moniker: moniker_info.name,
=======

        Ok(Self {
            auth_info,
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582
            config,
            http_client,
        })
    }

<<<<<<< HEAD
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
        let (auth_info, moniker_info) = config_client
            .get_ingestion_info()
            .await
            .map_err(|e| GenevaUploaderError::Other(format!("GCS error: {}", e)))?;

        GenevaUploader::new(auth_info, moniker_info, uploader_config)
    }

=======
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582
    /// Creates the GIG upload URI with required parameters
    fn create_upload_uri(&self, data_size: usize) -> String {
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
        let endpoint_param = form_urlencoded::Serializer::new(String::new())
            .append_pair("", self.config.environment.to_endpoint())
            .finish();

        let source_identity = form_urlencoded::Serializer::new(String::new())
            .append_pair("", &self.config.source_identity)
            .finish();

        // Create a source unique ID - using a UUID to ensure uniqueness
        let source_unique_id = Uuid::new_v4().to_string();
        
        // Use provided schema IDs or default if not specified
        let schema_ids = self.config.schema_ids.clone().unwrap_or_else(|| 
            "c1ce0ecea020359624c493bbe97f9e80;0da22cabbee419e000541a5eda732eb3".to_string()
        );

        // Create the query string
        format!(
            "api/v1/ingestion/ingest?endpoint={}&moniker={}&namespace={}&event={}&version={}&sourceUniqueId={}&sourceIdentity={}&startTime={}&endTime={}&format={}&dataSize={}&minLevel={}&schemaIds={}",
            endpoint_param,
<<<<<<< HEAD
            self.moniker,
=======
            self.config.moniker,
>>>>>>> 7e1a12abf92700a1a77dc8563402ac474f2c6582
            self.config.namespace,
            self.config.event_name,
            self.config.event_version,
            source_unique_id,
            source_identity,
            start_time,
            end_time,
            "centralbond/lz4hc", // Format encoding
            data_size,
            2, // Min level
            schema_ids
        )
    }

    // Helper function to extract monitoring endpoint from JWT token
    fn extract_monitoring_endpoint_from_jwt(token: &str) -> Result<String, GenevaUploaderError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(GenevaUploaderError::Other("Invalid JWT token format".to_string()));
        }
    
        // Decode the payload
        let payload = parts[1];
        let payload = match payload.len() % 4 {
            0 => payload.to_string(),
            2 => format!("{}==", payload),
            3 => format!("{}=", payload),
            _ => payload.to_string(),
        };
    
        let decoded = general_purpose::URL_SAFE_NO_PAD.decode(payload)
            .map_err(|e| GenevaUploaderError::Other(format!("Failed to decode JWT: {}", e)))?;
        
        let decoded_str = String::from_utf8(decoded)
            .map_err(|e| GenevaUploaderError::Other(format!("Invalid UTF-8 in JWT: {}", e)))?;
    
        // Parse as JSON and extract the Endpoint claim
        let payload_json: Value = serde_json::from_str(&decoded_str)
            .map_err(|e| GenevaUploaderError::Json(e))?;
            
        let endpoint = payload_json["Endpoint"]
            .as_str()
            .ok_or_else(|| GenevaUploaderError::Other("No Endpoint claim in JWT token".to_string()))?
            .to_string();
            
        Ok(endpoint)
    }

    /// Uploads data to the ingestion gateway
    ///
    /// # Arguments
    /// * `data` - The encoded data to upload (already in the required format)
    ///
    /// # Returns
    /// * `Result<IngestionResponse>` - The response containing the ticket ID or an error
    pub async fn upload(&self, data: Vec<u8>) -> Result<IngestionResponse> {
        let data_size = data.len();
        let upload_uri = self.create_upload_uri(data_size);
        let full_url = format!("{}/{}", self.auth_info.endpoint.trim_end_matches('/'), upload_uri);

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

        if status.is_success() {
            let ingest_response: IngestionResponse = serde_json::from_str(&body)
                .map_err(|e| GenevaUploaderError::Json(e))?;
            Ok(ingest_response)
        } else {
            Err(GenevaUploaderError::UploadFailed {
                status: status.as_u16(),
                message: body,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_service::client::IngestionGatewayInfo;
    use wiremock::matchers::{header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_upload_success() {
        // Start a mock server
        let mock_server = MockServer::start().await;

        // Set up mock for upload endpoint
        let mock_response = serde_json::json!({
            "ticket": "test-ticket-123",
            "extraField": "some-value"
        });

        Mock::given(method("POST"))
            .and(path_matches(r"^/api/v1/ingestion/ingest.*"))
            .and(header("Authorization", "Bearer mock-token"))
            .respond_with(ResponseTemplate::new(202).set_body_json(mock_response))
            .mount(&mock_server)
            .await;

        // Create the uploader with mock configuration
        let auth_info = IngestionGatewayInfo {
            endpoint: mock_server.uri(),
            auth_token: "mock-token".to_string(),
        };

        let config = GenevaUploaderConfig {
            moniker: "test-moniker".to_string(),
            namespace: "test-namespace".to_string(),
            event_name: "test-event".to_string(),
            event_version: "Ver1v0".to_string(),
            source_identity: "Tenant=Default/Role=TestRole/RoleInstance=TestInstance".to_string(),
            environment: GenevaEnvironment::Test,
            schema_ids: None,
        };

        let uploader = GenevaUploader::new(auth_info, config).unwrap();

        // Test data (could be any binary data)
        let test_data = vec![1, 2, 3, 4, 5];

        // Upload and check result
        let result = uploader.upload(test_data).await.unwrap();
        assert_eq!(result.ticket, "test-ticket-123");
    }

    // Helper function to match paths with query parameters
    fn path_matches(pattern: &str) -> impl wiremock::matchers::Matcher {
        wiremock::matchers::PathAndQueryMatcher::new(regex::Regex::new(pattern).unwrap())
    }
}