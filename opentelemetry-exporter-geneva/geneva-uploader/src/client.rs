//! High-level GenevaClient for user code. Wraps config_service and ingestion_service.

use crate::config_service::client::{AuthMethod, GenevaConfigClient, GenevaConfigClientConfig};
use crate::ingestion_service::uploader::{GenevaUploader, GenevaUploaderConfig};
use crate::payload_encoder::lz4_chunked_compression::lz4_chunked_compression;
use crate::payload_encoder::otlp_encoder::OtlpEncoder;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;

use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;

pub struct CompressedLogBatch {
    pub event_name: String,
    pub compressed_blob: Vec<u8>,
    pub row_count: usize,
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
    // Add event name/version here if constant, or per-upload if you want them per call.
}

/// Main user-facing client for Geneva ingestion.
#[derive(Clone)]
pub struct GenevaClient {
    uploader: Arc<GenevaUploader>,
    encoder: OtlpEncoder,
    metadata: String,
}

impl GenevaClient {
    /// Construct a new client with minimal configuration. Fetches and caches ingestion info as needed.
    pub async fn new(cfg: GenevaClientConfig) -> Result<Self, String> {
        // Build config client config
        let config_client_config = GenevaConfigClientConfig {
            endpoint: cfg.endpoint,
            environment: cfg.environment.clone(),
            account: cfg.account,
            namespace: cfg.namespace.clone(),
            region: cfg.region,
            config_major_version: cfg.config_major_version,
            auth_method: cfg.auth_method,
        };
        let config_client = Arc::new(
            GenevaConfigClient::new(config_client_config)
                .map_err(|e| format!("GenevaConfigClient init failed: {e}"))?,
        );

        let source_identity = format!(
            "Tenant={}/Role={}/RoleInstance={}",
            cfg.tenant, cfg.role_name, cfg.role_instance
        );

        let schema_ids =
            "c1ce0ecea020359624c493bbe97f9e80;0da22cabbee419e000541a5eda732eb3".to_string(); // TODO - find the actual value to be populated

        // Uploader config
        let uploader_config = GenevaUploaderConfig {
            namespace: cfg.namespace.clone(),
            source_identity,
            environment: cfg.environment,
            schema_ids,
        };

        let uploader = GenevaUploader::from_config_client(config_client, uploader_config)
            .await
            .map_err(|e| format!("GenevaUploader init failed: {e}"))?;
        let metadata = format!(
            "namespace={}/eventVersion={}/tenant={}/role={}/roleinstance={}",
            cfg.namespace,
            "Ver1v0", // You can replace this with a cfg field if version should be dynamic
            cfg.tenant,
            cfg.role_name,
            cfg.role_instance,
        );
        Ok(Self {
            uploader: Arc::new(uploader),
            encoder: OtlpEncoder::new(),
            metadata,
        })
    }

    pub fn encode_and_compress_logs<C>(
        &self,
        logs: &[ResourceLogs],
        out: &mut C,
    ) -> Result<(), String>
    where
        C: Extend<CompressedLogBatch>,
    {
        let log_iter = logs
            .iter()
            .flat_map(|resource_log| resource_log.scope_logs.iter())
            .flat_map(|scope_log| scope_log.log_records.iter());
        let batches = self.encoder.encode_log_batch(log_iter, &self.metadata);
        let compressed_batches = batches
            .into_iter()
            .map(|(_schema_id, event_name, encoded_blob, row_count)| {
                let compressed_blob = lz4_chunked_compression(&encoded_blob)
                    .map_err(|e| format!("LZ4 compression failed: {e}"))?;
                Ok(CompressedLogBatch {
                    event_name,
                    compressed_blob,
                    row_count,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        out.extend(compressed_batches);
        Ok(())
    }

    pub async fn upload_compressed_blobs(
        &self,
        batches: Vec<CompressedLogBatch>,
    ) -> Result<(), String> {
        let max_concurrency = 10;
        stream::iter(batches)
            .map(|batch| {
                let uploader = self.uploader.clone();
                async move {
                    uploader
                        .upload(batch.compressed_blob, &batch.event_name, "Ver2v0")
                        .await
                        .map_err(|e| format!("Geneva upload failed: {e}"))
                }
            })
            .buffer_unordered(max_concurrency)
            .try_collect::<Vec<_>>() // Will return early on first error
            .await?;
        Ok(())
    }
}
