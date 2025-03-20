mod uploader;
pub use uploader::{create_uploader, GenevaUploader};

pub mod auth;
pub mod client;
pub mod config;
pub mod error;
pub mod models;
pub mod utils;

// Re-exports for easier access
//pub use client::{GcsClient, GigClient};
pub use config::ClientConfig;
pub use error::Error;
