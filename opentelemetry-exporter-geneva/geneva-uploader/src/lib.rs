mod config_service;
mod ingestion_service;
mod payload_encoder;

pub mod client;

#[cfg(test)]
mod bench;

#[allow(unused_imports)]
pub(crate) use ingestion_service::uploader::{
    GenevaUploader, GenevaUploaderConfig, GenevaUploaderError, Result,
};

pub use client::EncodedBatch;
pub use client::{GenevaClient, GenevaClientConfig};
pub use config_service::{
    azure_arc_msi::AzureArcManagedIdentityCredential,
    client::{
        AuthMethod, GenevaConfigClient, GenevaConfigClientConfig,
        GenevaConfigClientError, IngestionGatewayInfo, MonikerInfo,
    },
};
