//! High-level GenevaClient for user code. Wraps config_service and ingestion_service.

use crate::config_service::client::{AuthMethod, GenevaConfigClient, GenevaConfigClientConfig};
use crate::ingestion_service::uploader::{GenevaUploader, GenevaUploaderConfig};
use crate::payload_encoder::TelemetryEncoder;
use std::sync::Arc;
use tracing::{debug, info};

/// Public batch type (already LZ4 chunked compressed).
/// Produced by encoders and returned to callers.
#[derive(Debug, Clone)]
pub struct EncodedBatch {
    pub event_name: String,
    pub data: Vec<u8>,
    pub metadata: crate::payload_encoder::central_blob::BatchMetadata,
}

/// Configuration for GenevaClient (user-facing)
#[derive(Clone, Debug)]
pub struct GenevaClientConfig {
    pub endpoint: String,
    pub environment: String,
    pub account: String,
    pub namespace: String,
    pub region: String,
    pub config_major_version: u32,
    pub auth_method: AuthMethod,
    pub tenant: String,
    pub role_name: String,
    pub role_instance: String,
    pub msi_resource: Option<String>, // Required for Managed Identity variants
}

/// Main user-facing client for Geneva ingestion.
/// Generic over the encoder type (OtlpEncoder or OtapEncoder).
#[derive(Clone)]
pub struct GenevaClient<E: TelemetryEncoder> {
    uploader: Arc<GenevaUploader>,
    encoder: E,
    metadata: String,
}

impl<E: TelemetryEncoder> GenevaClient<E> {
    /// Create a new GenevaClient with a specific encoder
    pub fn new_with_encoder(cfg: GenevaClientConfig, encoder: E) -> Result<Self, String> {
        info!(
            name: "client.new_with_encoder",
            target: "geneva-uploader",
            endpoint = %cfg.endpoint,
            namespace = %cfg.namespace,
            account = %cfg.account,
            "Initializing GenevaClient"
        );

        // Validate MSI resource presence for managed identity variants
        match cfg.auth_method {
            AuthMethod::SystemManagedIdentity
            | AuthMethod::UserManagedIdentity { .. }
            | AuthMethod::UserManagedIdentityByObjectId { .. }
            | AuthMethod::UserManagedIdentityByResourceId { .. } => {
                if cfg.msi_resource.is_none() {
                    debug!(
                        name: "client.new_with_encoder.validate_msi_resource",
                        target: "geneva-uploader",
                        "Validation failed: msi_resource must be provided for managed identity auth"
                    );
                    return Err(
                        "msi_resource must be provided for managed identity auth".to_string()
                    );
                }
            }
            AuthMethod::Certificate { .. } => {}
            AuthMethod::WorkloadIdentity { .. } => {}
            #[cfg(feature = "mock_auth")]
            AuthMethod::MockAuth => {}
        }

        let config_client_config = GenevaConfigClientConfig {
            endpoint: cfg.endpoint,
            environment: cfg.environment.clone(),
            account: cfg.account,
            namespace: cfg.namespace.clone(),
            region: cfg.region,
            config_major_version: cfg.config_major_version,
            auth_method: cfg.auth_method,
            msi_resource: cfg.msi_resource,
        };

        let config_client =
            Arc::new(GenevaConfigClient::new(config_client_config).map_err(|e| {
                debug!(
                    name: "client.new_with_encoder.config_client_init",
                    target: "geneva-uploader",
                    error = %e,
                    "GenevaConfigClient init failed"
                );
                format!("GenevaConfigClient init failed: {e}")
            })?);

        let source_identity = format!(
            "Tenant={}/Role={}/RoleInstance={}",
            cfg.tenant, cfg.role_name, cfg.role_instance
        );

        let config_version = format!("Ver{}v0", cfg.config_major_version);

        let metadata = format!(
            "namespace={}/eventVersion={}/tenant={}/role={}/roleinstance={}",
            cfg.namespace, config_version, cfg.tenant, cfg.role_name, cfg.role_instance,
        );

        let uploader_config = GenevaUploaderConfig {
            namespace: cfg.namespace.clone(),
            source_identity,
            environment: cfg.environment,
            config_version: config_version.clone(),
        };

        let uploader =
            GenevaUploader::from_config_client(config_client, uploader_config).map_err(|e| {
                debug!(
                    name: "client.new_with_encoder.uploader_init",
                    target: "geneva-uploader",
                    error = %e,
                    "GenevaUploader init failed"
                );
                format!("GenevaUploader init failed: {e}")
            })?;

        info!(
            name: "client.new_with_encoder.complete",
            target: "geneva-uploader",
            "GenevaClient initialized successfully"
        );

        Ok(Self {
            uploader: Arc::new(uploader),
            encoder,
            metadata,
        })
    }

    /// Get a log encoder for encoding telemetry logs
    pub fn log_encoder(&self) -> crate::payload_encoder::encoder_trait::LogEncoder<'_> {
        self.encoder.encode_logs(&self.metadata)
    }

    /// Get a span encoder for encoding telemetry spans
    pub fn span_encoder(&self) -> crate::payload_encoder::encoder_trait::SpanEncoder<'_> {
        self.encoder.encode_spans(&self.metadata)
    }

    /// Upload a single compressed batch.
    /// This allows for granular control over uploads, including custom retry logic for individual batches.
    pub async fn upload_batch(&self, batch: &EncodedBatch) -> Result<(), String> {
        debug!(
            name: "client.upload_batch",
            target: "geneva-uploader",
            event_name = %batch.event_name,
            size = batch.data.len(),
            "Uploading batch"
        );

        self.uploader
            .upload(batch.data.clone(), &batch.event_name, &batch.metadata)
            .await
            .map(|_| {
                debug!(
                    name: "client.upload_batch.success",
                    target: "geneva-uploader",
                    event_name = %batch.event_name,
                    "Successfully uploaded batch"
                );
            })
            .map_err(|e| {
                debug!(
                    name: "client.upload_batch.error",
                    target: "geneva-uploader",
                    event_name = %batch.event_name,
                    error = %e,
                    "Geneva upload failed"
                );
                format!("Geneva upload failed: {e} Event: {}", batch.event_name)
            })
    }
}

// Convenience type aliases
pub type OtlpGenevaClient = GenevaClient<crate::payload_encoder::otlp_encoder::OtlpEncoder>;
pub type OtapGenevaClient = GenevaClient<crate::payload_encoder::otap_encoder::OtapEncoder>;

// Convenience constructors for specific encoder types
impl OtlpGenevaClient {
    /// Create a new GenevaClient with OTLP encoder (default)
    pub fn new(cfg: GenevaClientConfig) -> Result<Self, String> {
        Self::new_with_encoder(cfg, crate::payload_encoder::otlp_encoder::OtlpEncoder::new())
    }
}

impl OtapGenevaClient {
    /// Create a new GenevaClient with OTAP (Arrow) encoder
    pub fn new_with_otap(cfg: GenevaClientConfig) -> Result<Self, String> {
        Self::new_with_encoder(cfg, crate::payload_encoder::otap_encoder::OtapEncoder::new())
    }
}
